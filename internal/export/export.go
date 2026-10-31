package export

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// ExportOptions configures portable inventory export.
type ExportOptions struct {
	VolumeID   string
	Compress   bool
	IndentJSON bool
}

// Export dumps inventory data and metadata into a portable JSON or gzip-compressed file.
func Export(ctx context.Context, db *index.DB, outputPath string, opts ExportOptions) (*model.ExportSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	if outputPath == "" {
		return nil, fmt.Errorf("output path cannot be empty")
	}

	startTime := time.Now().UTC()

	// 1. Collect metadata
	metadata := make(map[string]string)
	if algo, err := db.GetMetadata(ctx, "fingerprint_algorithm"); err == nil {
		metadata["fingerprint_algorithm"] = algo
	}
	if ver, err := db.GetMetadata(ctx, "fingerprint_version"); err == nil {
		metadata["fingerprint_version"] = ver
	}
	if sVer, err := db.GetMetadata(ctx, "schema_version"); err == nil {
		metadata["schema_version"] = sVer
	}

	// 2. Collect volumes
	volFilter := model.VolumeFilter{}
	if opts.VolumeID != "" {
		volFilter.VolumeID = opts.VolumeID
	}
	volumes, err := db.ListVolumes(ctx, volFilter)
	if err != nil {
		return nil, fmt.Errorf("failed listing volumes for export: %w", err)
	}

	// 3. Collect scan roots, runs, entries for each volume
	var allRoots []*model.ScanRoot
	var allEntries []*model.Entry
	allScanRunsMap, err := db.GetAllScanRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed listing scan runs: %w", err)
	}

	var allScanRuns []*model.ScanRun
	includedVolIDs := make(map[string]bool)
	for _, v := range volumes {
		includedVolIDs[v.VolumeID] = true

		roots, err := db.GetScanRoots(ctx, v.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("failed getting scan roots for %s: %w", v.VolumeID, err)
		}
		allRoots = append(allRoots, roots...)

		entries, err := db.ListEntries(ctx, v.VolumeID, "")
		if err != nil {
			return nil, fmt.Errorf("failed listing entries for %s: %w", v.VolumeID, err)
		}
		allEntries = append(allEntries, entries...)
	}

	for _, run := range allScanRunsMap {
		if includedVolIDs[run.VolumeID] {
			allScanRuns = append(allScanRuns, run)
		}
	}

	inventory := &model.ExportInventory{
		ExportVersion: "1",
		ExportedAt:    startTime,
		Metadata:      metadata,
		Volumes:       volumes,
		ScanRoots:     allRoots,
		ScanRuns:      allScanRuns,
		Entries:       allEntries,
	}

	// 4. Output stream setup
	var outWriter io.Writer
	var closeFn func() error

	compress := opts.Compress || strings.HasSuffix(outputPath, ".gz")

	if outputPath == "-" {
		outWriter = os.Stdout
		closeFn = func() error { return nil }
	} else {
		dir := filepath.Dir(outputPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed creating output directory: %w", err)
		}
		file, err := os.Create(outputPath)
		if err != nil {
			return nil, fmt.Errorf("failed creating export file: %w", err)
		}
		outWriter = file
		closeFn = file.Close
	}
	defer closeFn()

	cw := &countingWriter{w: outWriter}
	var targetWriter io.Writer = cw

	var gzWriter *gzip.Writer
	if compress {
		gzWriter = gzip.NewWriter(cw)
		targetWriter = gzWriter
	}

	enc := json.NewEncoder(targetWriter)
	if opts.IndentJSON && !compress {
		enc.SetIndent("", "  ")
	}

	if err := enc.Encode(inventory); err != nil {
		return nil, fmt.Errorf("failed encoding export inventory: %w", err)
	}

	if gzWriter != nil {
		if err := gzWriter.Close(); err != nil {
			return nil, fmt.Errorf("failed closing gzip stream: %w", err)
		}
	}

	completedAt := time.Now().UTC()
	return &model.ExportSummary{
		Destination:    outputPath,
		Compressed:     compress,
		ExportedAt:     startTime,
		Duration:       completedAt.Sub(startTime),
		VolumesCount:   len(volumes),
		EntriesCount:   len(allEntries),
		ScanRootsCount: len(allRoots),
		ScanRunsCount:  len(allScanRuns),
		BytesWritten:   cw.bytesWritten,
	}, nil
}

// Import loads a portable JSON or gzip-compressed export into the database.
func Import(ctx context.Context, db *index.DB, inputPath string) (*model.ImportSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	if inputPath == "" {
		return nil, fmt.Errorf("input path cannot be empty")
	}

	startTime := time.Now().UTC()

	var inReader io.Reader
	var closeFn func() error

	if inputPath == "-" {
		inReader = os.Stdin
		closeFn = func() error { return nil }
	} else {
		file, err := os.Open(inputPath)
		if err != nil {
			return nil, fmt.Errorf("failed opening export file: %w", err)
		}
		inReader = file
		closeFn = file.Close
	}
	defer closeFn()

	br := bufio.NewReader(inReader)
	header, _ := br.Peek(2)
	isGzip := len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b

	var decReader io.Reader = br
	if isGzip {
		gzReader, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("failed creating gzip reader: %w", err)
		}
		defer gzReader.Close()
		decReader = gzReader
	}

	var inventory model.ExportInventory
	dec := json.NewDecoder(decReader)
	if err := dec.Decode(&inventory); err != nil {
		return nil, fmt.Errorf("failed parsing export JSON: %w", err)
	}

	if inventory.ExportVersion != "1" {
		return nil, fmt.Errorf("unsupported export version %q", inventory.ExportVersion)
	}

	// Read existing volumes before starting transaction to avoid single-connection pool deadlock
	existingVols, err := db.ListVolumes(ctx, model.VolumeFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed listing existing volumes: %w", err)
	}
	existingVolsByID := make(map[string]*model.Volume, len(existingVols))
	for _, v := range existingVols {
		existingVolsByID[v.VolumeID] = v
	}

	// Begin import transaction
	tx, err := db.BeginMergeTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed beginning import transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	summary := &model.ImportSummary{
		Source:     inputPath,
		ImportedAt: startTime,
	}

	// 1. Import volumes
	for _, vol := range inventory.Volumes {
		if _, exists := existingVolsByID[vol.VolumeID]; exists {
			if err := tx.UpdateVolume(ctx, vol); err != nil {
				return nil, fmt.Errorf("failed updating volume %s: %w", vol.VolumeID, err)
			}
		} else {
			if err := tx.InsertVolume(ctx, vol); err != nil {
				return nil, fmt.Errorf("failed inserting volume %s: %w", vol.VolumeID, err)
			}
			existingVolsByID[vol.VolumeID] = vol
		}
		summary.VolumesImported++
	}

	// 2. Import scan runs
	for _, run := range inventory.ScanRuns {
		if err := tx.UpsertScanRun(ctx, run); err != nil {
			return nil, fmt.Errorf("failed importing scan run %s: %w", run.ScanID, err)
		}
		summary.ScanRunsCount++
	}

	// 3. Import scan roots
	for _, root := range inventory.ScanRoots {
		if err := tx.UpsertScanRoot(ctx, root); err != nil {
			return nil, fmt.Errorf("failed importing scan root %s:%s: %w", root.VolumeID, root.RootPath, err)
		}
		summary.ScanRootsCount++
	}

	// 4. Import entries using tx.ListEntries to prevent pool deadlocks
	existingEntriesByVol := make(map[string]map[string]*model.Entry)
	for _, vol := range inventory.Volumes {
		entries, err := tx.ListEntries(ctx, vol.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("failed listing entries for volume %s: %w", vol.VolumeID, err)
		}
		m := make(map[string]*model.Entry, len(entries))
		for _, e := range entries {
			m[e.RelativePath] = e
		}
		existingEntriesByVol[vol.VolumeID] = m
	}

	for _, e := range inventory.Entries {
		e.RelativePath = model.NormalizeRelativePath(e.RelativePath)
		var existing *model.Entry
		if m, ok := existingEntriesByVol[e.VolumeID]; ok {
			existing = m[e.RelativePath]
		}

		if existing == nil {
			if err := tx.InsertEntry(ctx, e); err != nil {
				return nil, fmt.Errorf("failed inserting entry %s:%s: %w", e.VolumeID, e.RelativePath, err)
			}
			summary.EntriesInserted++
			if m, ok := existingEntriesByVol[e.VolumeID]; ok {
				m[e.RelativePath] = e
			}
		} else {
			if existing.SizeBytes == e.SizeBytes &&
				existing.EntryType == e.EntryType &&
				string(existing.QuickFingerprint) == string(e.QuickFingerprint) {
				summary.EntriesIdentical++
			} else {
				if err := tx.UpdateEntry(ctx, e); err != nil {
					return nil, fmt.Errorf("failed updating entry %s:%s: %w", e.VolumeID, e.RelativePath, err)
				}
				summary.EntriesUpdated++
				if m, ok := existingEntriesByVol[e.VolumeID]; ok {
					m[e.RelativePath] = e
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed committing import transaction: %w", err)
	}

	summary.Duration = time.Since(startTime)
	return summary, nil
}

type countingWriter struct {
	w            io.Writer
	bytesWritten int64
}

func (c *countingWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.bytesWritten += int64(n)
	return n, err
}
