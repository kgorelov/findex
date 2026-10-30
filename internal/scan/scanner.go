package scan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/tree"
	"github.com/kgorelov/findex/internal/volume"
)

// Progress holds real-time counters emitted during scan execution.
type Progress struct {
	EntriesScanned int64
	BytesScanned   int64
	FilesScanned   int64
	FastHits       int64
	DirsScanned    int64
	Elapsed        time.Duration
}

// Options configures a scan operation.
type Options struct {
	VolumeID     string
	TargetDir    string
	Workers      int
	BatchSize    int
	IncludeFull  bool
	FastMode     bool // If true (default), reuse fingerprints if size & mtime match existing index
	StrictVerify bool // If true, force re-computing fingerprints regardless of metadata
	DryRun       bool
	ProgressFn   func(p Progress)
}

// Summary contains final statistics and metadata for a completed or failed scan.
type Summary struct {
	ScanID             string           `json:"scan_id"`
	VolumeID           string           `json:"volume_id"`
	RootPath           string           `json:"root_path"`
	Status             model.ScanStatus `json:"status"`
	StartedAt          time.Time        `json:"started_at"`
	CompletedAt        time.Time        `json:"completed_at"`
	Duration           time.Duration    `json:"duration"`
	FilesCount         int64            `json:"files_count"`
	DirsCount          int64            `json:"dirs_count"`
	SymlinksCount      int64            `json:"symlinks_count"`
	OtherCount         int64            `json:"other_count"`
	TotalBytes         int64            `json:"total_bytes"`
	FastHitsCount      int64            `json:"fast_hits_count"`
	BytesHashed        int64            `json:"bytes_hashed"`
	ReconciledCount    int64            `json:"reconciled_count"`
	ErrorsCount        int64            `json:"errors_count"`
	UnstableFilesCount int64            `json:"unstable_files_count"`
	ErrorMessage       string           `json:"error_message,omitempty"`
}

// Scanner orchestrates parallel filesystem fingerprinting and atomic index updates.
type Scanner struct {
	db *index.DB
}

// New creates a new Scanner instance with the provided database connection.
func New(db *index.DB) *Scanner {
	return &Scanner{db: db}
}

// Run executes a scan on targetDir according to opts.
func (s *Scanner) Run(ctx context.Context, opts Options) (*Summary, error) {
	if opts.TargetDir == "" {
		return nil, fmt.Errorf("target directory cannot be empty")
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}

	startedAt := time.Now().UTC()
	scanID := uuid.NewString()

	// 1. Resolve volume and mount point
	var vol *model.Volume
	var err error

	if opts.VolumeID != "" {
		vol, err = s.db.GetVolume(ctx, opts.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("specified volume %q not found in index: %w", opts.VolumeID, err)
		}
	} else {
		// Auto-discover volume from target directory
		discovered, dErr := volume.ResolveVolume(opts.TargetDir, volume.VolumeOptions{})
		if dErr != nil {
			return nil, fmt.Errorf("failed resolving volume for %q: %w", opts.TargetDir, dErr)
		}
		// Register or update volume in database
		if regErr := s.db.RegisterVolume(ctx, discovered, false); regErr != nil {
			return nil, fmt.Errorf("failed registering volume: %w", regErr)
		}
		vol = discovered
	}

	// 2. Map target directory to volume-relative path
	rootPath, err := model.ToVolumeRelativePath(vol.LastMountPoint, opts.TargetDir)
	if err != nil {
		return nil, fmt.Errorf("failed mapping target directory %q to volume root %q: %w",
			opts.TargetDir, vol.LastMountPoint, err)
	}

	// 3. Record scan run as running
	scanRun := &model.ScanRun{
		ScanID:    scanID,
		VolumeID:  vol.VolumeID,
		RootPath:  rootPath,
		Status:    model.ScanStatusRunning,
		StartedAt: startedAt,
	}
	if !opts.DryRun {
		if err := s.db.CreateScanRun(ctx, scanRun); err != nil {
			return nil, fmt.Errorf("failed recording scan run: %w", err)
		}
	}

	summary := &Summary{
		ScanID:    scanID,
		VolumeID:  vol.VolumeID,
		RootPath:  rootPath,
		StartedAt: startedAt,
	}

	// 4. Build filesystem traversal tree
	rootNode, fileNodes, err := tree.BuildTree(ctx, vol.VolumeID, vol.LastMountPoint, opts.TargetDir)
	if err != nil {
		return s.failScan(ctx, scanRun, summary, fmt.Sprintf("filesystem walk error: %v", err), opts.DryRun)
	}

	// 5. Pre-load existing metadata for fast-path change detection
	useFastPath := !opts.StrictVerify
	var cachedMeta map[string]*index.EntryMetadata
	if useFastPath && !opts.DryRun {
		var metaErr error
		cachedMeta, metaErr = s.db.GetSubtreeMetadata(ctx, vol.VolumeID, rootPath)
		if metaErr != nil {
			// If pre-loading fails, gracefully fallback to full hashing
			useFastPath = false
		}
	}

	// 6. Setup pipeline channels
	fileTaskChan := make(chan *tree.Node, opts.Workers*4)
	fileDoneChan := make(chan *tree.Node, opts.Workers*4)
	writerChan := make(chan *model.Entry, opts.BatchSize*4)

	var scanErrors []string
	var errMu sync.Mutex
	recordError := func(e error) {
		errMu.Lock()
		defer errMu.Unlock()
		scanErrors = append(scanErrors, e.Error())
	}

	var totalEntriesScanned int64
	var totalBytesScanned int64

	reportProgress := func() {
		if opts.ProgressFn != nil {
			opts.ProgressFn(Progress{
				EntriesScanned: atomic.LoadInt64(&totalEntriesScanned),
				BytesScanned:   atomic.LoadInt64(&totalBytesScanned),
				FilesScanned:   atomic.LoadInt64(&summary.FilesCount),
				FastHits:       atomic.LoadInt64(&summary.FastHitsCount),
				DirsScanned:    atomic.LoadInt64(&summary.DirsCount),
				Elapsed:        time.Since(startedAt),
			})
		}
	}

	// 7. Start batch writer goroutine
	var writerWg sync.WaitGroup
	var writerErr error

	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		var batch []*model.Entry
		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()

		flush := func() {
			if len(batch) == 0 || opts.DryRun {
				batch = batch[:0]
				return
			}
			if wErr := s.db.UpsertEntries(ctx, batch); wErr != nil {
				if writerErr == nil {
					writerErr = wErr
				}
			}
			batch = batch[:0]
		}

		for {
			select {
			case entry, ok := <-writerChan:
				if !ok {
					flush()
					return
				}
				batch = append(batch, entry)
				if len(batch) >= opts.BatchSize {
					flush()
				}
			case <-flushTicker.C:
				flush()
			}
		}
	}()

	// 8. Start file fingerprint workers
	var workerWg sync.WaitGroup
	for w := 0; w < opts.Workers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for node := range fileTaskChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res, fpErr := fingerprint.FingerprintFile(node.FullPath, fingerprint.Options{
					IncludeFull: opts.IncludeFull,
				})
				if fpErr != nil {
					if errors.Is(fpErr, fingerprint.ErrFileUnstable) {
						atomic.AddInt64(&summary.UnstableFilesCount, 1)
					}
					recordError(fmt.Errorf("file fingerprint failed for %s: %w", node.FullPath, fpErr))
					continue
				}

				atomic.AddInt64(&summary.BytesHashed, node.SizeBytes)
				node.QuickFingerprint = res.QuickFingerprint
				node.FullFingerprint = res.FullFingerprint
				fileDoneChan <- node
			}
		}()
	}

	// 9. Producer: evaluate fast-path or queue to workers
	go func() {
		for _, fn := range fileNodes {
			select {
			case <-ctx.Done():
				break
			default:
			}

			// Fast-path change detection check
			if useFastPath && cachedMeta != nil {
				if cached, ok := cachedMeta[fn.RelativePath]; ok {
					if cached.SizeBytes == fn.SizeBytes && cached.MTimeNs == fn.MTimeNs && len(cached.QuickFingerprint) == 32 {
						if !opts.IncludeFull || len(cached.FullFingerprint) == 32 {
							fn.QuickFingerprint = cached.QuickFingerprint
							fn.FullFingerprint = cached.FullFingerprint
							atomic.AddInt64(&summary.FastHitsCount, 1)
							fileDoneChan <- fn
							continue
						}
					}
				}
			}

			// Fast-path miss: dispatch to worker pool
			select {
			case <-ctx.Done():
				break
			case fileTaskChan <- fn:
			}
		}
		close(fileTaskChan)
		workerWg.Wait()
		close(fileDoneChan)
	}()

	// 9. Bottom-up directory coordinator
	// A helper to recursively resolve directories once all their children have fingerprints.
	var resolveDir func(dirNode *tree.Node) error
	resolveDir = func(dirNode *tree.Node) error {
		var childDescs []fingerprint.ChildDescriptor
		for _, child := range dirNode.Children {
			var fp []byte
			switch child.EntryType {
			case model.EntryTypeRegular:
				fp = child.QuickFingerprint
			case model.EntryTypeDirectory:
				fp = child.DirectoryFingerprint
			case model.EntryTypeSymlink:
				fp = child.QuickFingerprint
			}

			childDescs = append(childDescs, fingerprint.ChildDescriptor{
				Type:        child.EntryType,
				SizeBytes:   child.SizeBytes,
				Fingerprint: fp,
			})
		}

		dirFP, err := fingerprint.ComputeDirectoryFingerprint(childDescs)
		if err != nil {
			return err
		}
		dirNode.DirectoryFingerprint = dirFP

		// Emit directory entry to writer
		writerChan <- &model.Entry{
			VolumeID:             dirNode.VolumeID,
			RelativePath:         dirNode.RelativePath,
			EntryType:            dirNode.EntryType,
			SizeBytes:            dirNode.SizeBytes,
			MTimeNs:              dirNode.MTimeNs,
			DirectoryFingerprint: dirFP,
			LastSeenScanID:       scanID,
		}

		atomic.AddInt64(&totalEntriesScanned, 1)
		atomic.AddInt64(&summary.DirsCount, 1)
		reportProgress()

		// Propagate upward if parent is now ready
		if dirNode.Parent != nil {
			if atomic.AddInt32(&dirNode.Parent.PendingCount, -1) == 0 {
				return resolveDir(dirNode.Parent)
			}
		}
		return nil
	}

	// Resolve all initial leaves (symlinks, other entries, and empty directories)
	var resolveLeaves func(n *tree.Node) error
	resolveLeaves = func(n *tree.Node) error {
		for _, child := range n.Children {
			switch child.EntryType {
			case model.EntryTypeSymlink:
				atomic.AddInt64(&summary.SymlinksCount, 1)
				writerChan <- &model.Entry{
					VolumeID:         child.VolumeID,
					RelativePath:     child.RelativePath,
					EntryType:        child.EntryType,
					MTimeNs:          child.MTimeNs,
					QuickFingerprint: child.QuickFingerprint,
					LastSeenScanID:   scanID,
				}
				atomic.AddInt64(&totalEntriesScanned, 1)
				if child.Parent != nil {
					if atomic.AddInt32(&child.Parent.PendingCount, -1) == 0 {
						if err := resolveDir(child.Parent); err != nil {
							return err
						}
					}
				}
			case model.EntryTypeOther:
				atomic.AddInt64(&summary.OtherCount, 1)
				writerChan <- &model.Entry{
					VolumeID:       child.VolumeID,
					RelativePath:   child.RelativePath,
					EntryType:      child.EntryType,
					MTimeNs:        child.MTimeNs,
					LastSeenScanID: scanID,
				}
				atomic.AddInt64(&totalEntriesScanned, 1)
				if child.Parent != nil {
					if atomic.AddInt32(&child.Parent.PendingCount, -1) == 0 {
						if err := resolveDir(child.Parent); err != nil {
							return err
						}
					}
				}
			case model.EntryTypeDirectory:
				if err := resolveLeaves(child); err != nil {
					return err
				}
			}
		}

		if n.EntryType.IsDirectory() && len(n.Children) == 0 {
			if err := resolveDir(n); err != nil {
				return err
			}
		}
		return nil
	}

	if rootNode.EntryType.IsDirectory() {
		if err := resolveLeaves(rootNode); err != nil {
			return s.failScan(ctx, scanRun, summary, err.Error(), opts.DryRun)
		}
	}

	// Process completed files as they arrive
	for fileNode := range fileDoneChan {
		writerChan <- &model.Entry{
			VolumeID:         fileNode.VolumeID,
			RelativePath:     fileNode.RelativePath,
			EntryType:        fileNode.EntryType,
			SizeBytes:        fileNode.SizeBytes,
			MTimeNs:          fileNode.MTimeNs,
			QuickFingerprint: fileNode.QuickFingerprint,
			FullFingerprint:  fileNode.FullFingerprint,
			LastSeenScanID:   scanID,
		}

		atomic.AddInt64(&totalEntriesScanned, 1)
		atomic.AddInt64(&totalBytesScanned, fileNode.SizeBytes)
		atomic.AddInt64(&summary.FilesCount, 1)
		atomic.AddInt64(&summary.TotalBytes, fileNode.SizeBytes)
		reportProgress()

		// Decrement parent pending count
		if fileNode.Parent != nil {
			if atomic.AddInt32(&fileNode.Parent.PendingCount, -1) == 0 {
				if err := resolveDir(fileNode.Parent); err != nil {
					recordError(err)
				}
			}
		}
	}

	// Close writer channel and wait for batch writer to flush
	close(writerChan)
	writerWg.Wait()

	if writerErr != nil {
		return s.failScan(ctx, scanRun, summary, fmt.Sprintf("database write error: %v", writerErr), opts.DryRun)
	}

	// 10. Check for scan errors
	summary.ErrorsCount = int64(len(scanErrors))
	if len(scanErrors) > 0 {
		errMsg := fmt.Sprintf("%d errors encountered during scan (first: %s)", len(scanErrors), scanErrors[0])
		return s.failScan(ctx, scanRun, summary, errMsg, opts.DryRun)
	}

	// 11. Success: Safe Reconciliation & Record Scan Root
	completedAt := time.Now().UTC()
	summary.CompletedAt = completedAt
	summary.Duration = completedAt.Sub(startedAt)
	summary.Status = model.ScanStatusCompleted

	if !opts.DryRun {
		reconciledCount, rErr := s.db.ReconcileStaleEntries(ctx, vol.VolumeID, rootPath, scanID)
		if rErr != nil {
			return s.failScan(ctx, scanRun, summary, fmt.Sprintf("stale entries reconciliation failed: %v", rErr), false)
		}
		summary.ReconciledCount = reconciledCount

		if err := s.db.RecordScanRootSuccess(ctx, vol.VolumeID, rootPath, scanID, completedAt); err != nil {
			return s.failScan(ctx, scanRun, summary, fmt.Sprintf("failed updating scan root: %v", err), false)
		}

		scanRun.Status = model.ScanStatusCompleted
		scanRun.CompletedAt = &completedAt
		scanRun.EntriesScanned = totalEntriesScanned
		scanRun.ErrorsCount = 0
		if err := s.db.UpdateScanRun(ctx, scanRun); err != nil {
			return nil, fmt.Errorf("failed completing scan run record: %w", err)
		}
	}

	return summary, nil
}

func (s *Scanner) failScan(ctx context.Context, run *model.ScanRun, summary *Summary, errMsg string, dryRun bool) (*Summary, error) {
	now := time.Now().UTC()
	summary.CompletedAt = now
	summary.Duration = now.Sub(summary.StartedAt)
	summary.Status = model.ScanStatusFailed
	summary.ErrorMessage = errMsg

	if !dryRun && run != nil {
		run.Status = model.ScanStatusFailed
		run.CompletedAt = &now
		run.ErrorMessage = errMsg
		_ = s.db.UpdateScanRun(ctx, run)
	}

	return summary, fmt.Errorf("scan failed: %s", errMsg)
}
