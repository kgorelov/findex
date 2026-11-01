package tree

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/model"
)

// Node represents an entry in the filesystem tree during a scan.
type Node struct {
	VolumeID             string
	RelativePath         string
	FullPath             string
	EntryType            model.EntryType
	SizeBytes            int64
	MTimeNs              int64
	QuickFingerprint     []byte
	FullFingerprint      []byte
	DirectoryFingerprint []byte

	Children     []*Node
	Parent       *Node
	PendingCount int32
}

// WalkProgress reports entry counts discovered during filesystem traversal.
type WalkProgress struct {
	EntriesDiscovered int64
	FilesDiscovered   int64
	DirsDiscovered    int64
}

// WalkOptions configures filesystem traversal.
type WalkOptions struct {
	VolumeID   string
	MountPoint string
	StartDir   string
	ProgressFn func(WalkProgress)
	ErrorFn    func(error)
}

// BuildTree walks the filesystem rooted at startDir and constructs a tree of Nodes.
// It returns the root Node, a slice of all regular file nodes that need fingerprinting,
// and any non-fatal walk warnings/errors encountered (such as permission denied on subdirectories).
func BuildTree(ctx context.Context, volumeID, mountPoint, startDir string) (*Node, []*Node, []string, error) {
	return BuildTreeWithOptions(ctx, WalkOptions{
		VolumeID:   volumeID,
		MountPoint: mountPoint,
		StartDir:   startDir,
	})
}

// BuildTreeWithProgress walks the filesystem rooted at startDir, reporting discovery progress.
func BuildTreeWithProgress(ctx context.Context, volumeID, mountPoint, startDir string, progressFn func(WalkProgress)) (*Node, []*Node, []string, error) {
	return BuildTreeWithOptions(ctx, WalkOptions{
		VolumeID:   volumeID,
		MountPoint: mountPoint,
		StartDir:   startDir,
		ProgressFn: progressFn,
	})
}

// BuildTreeWithOptions walks the filesystem rooted at opts.StartDir according to WalkOptions.
func BuildTreeWithOptions(ctx context.Context, opts WalkOptions) (*Node, []*Node, []string, error) {
	absStart, err := filepath.Abs(opts.StartDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get absolute path for %q: %w", opts.StartDir, err)
	}

	startStat, err := os.Lstat(absStart)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to stat scan root %q: %w", absStart, err)
	}

	relPath, err := model.ToVolumeRelativePath(opts.MountPoint, absStart)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed mapping scan root to volume path: %w", err)
	}

	rootNode := &Node{
		VolumeID:     opts.VolumeID,
		RelativePath: relPath,
		FullPath:     absStart,
		EntryType:    model.EntryTypeFromMode(startStat.Mode()),
		SizeBytes:    startStat.Size(),
		MTimeNs:      startStat.ModTime().UnixNano(),
	}

	var fileNodes []*Node
	var walkErrors []string

	var entriesCount int64
	var filesCount int64
	var dirsCount int64

	report := func() {
		if opts.ProgressFn != nil {
			opts.ProgressFn(WalkProgress{
				EntriesDiscovered: atomic.LoadInt64(&entriesCount),
				FilesDiscovered:   atomic.LoadInt64(&filesCount),
				DirsDiscovered:    atomic.LoadInt64(&dirsCount),
			})
		}
	}

	if !rootNode.EntryType.IsDirectory() {
		// Single file scan
		atomic.AddInt64(&entriesCount, 1)
		if rootNode.EntryType == model.EntryTypeRegular {
			atomic.AddInt64(&filesCount, 1)
			fileNodes = append(fileNodes, rootNode)
		} else if rootNode.EntryType == model.EntryTypeSymlink {
			target, err := os.Readlink(absStart)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed reading symlink target: %w", err)
			}
			rootNode.QuickFingerprint = fingerprint.ComputeSymlinkFingerprint(target)
		}
		report()
		return rootNode, fileNodes, nil, nil
	}

	atomic.AddInt64(&entriesCount, 1)
	atomic.AddInt64(&dirsCount, 1)
	report()

	// Recursively walk directory
	if err := walkDir(ctx, rootNode, opts.MountPoint, &fileNodes, &walkErrors, opts.ErrorFn, &entriesCount, &filesCount, &dirsCount, report); err != nil {
		return nil, nil, nil, err
	}

	report()
	return rootNode, fileNodes, walkErrors, nil
}

func walkDir(ctx context.Context, dirNode *Node, mountPoint string, fileNodes *[]*Node, walkErrors *[]string, errFn func(error), entriesCount, filesCount, dirsCount *int64, report func()) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entries, err := os.ReadDir(dirNode.FullPath)
	if err != nil {
		wErr := fmt.Errorf("failed reading directory %q: %w", dirNode.FullPath, err)
		*walkErrors = append(*walkErrors, wErr.Error())
		if errFn != nil {
			errFn(wErr)
		}
		dirNode.PendingCount = 0
		return nil
	}

	dirNode.PendingCount = int32(len(entries))
	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		childPath := filepath.Join(dirNode.FullPath, entry.Name())
		info, err := entry.Info()
		if err != nil {
			wErr := fmt.Errorf("failed getting info for %q: %w", childPath, err)
			*walkErrors = append(*walkErrors, wErr.Error())
			if errFn != nil {
				errFn(wErr)
			}
			dirNode.PendingCount--
			continue
		}

		relPath, err := model.ToVolumeRelativePath(mountPoint, childPath)
		if err != nil {
			*walkErrors = append(*walkErrors, fmt.Sprintf("failed mapping child path %q: %v", childPath, err))
			dirNode.PendingCount--
			continue
		}

		entryType := model.EntryTypeFromMode(info.Mode())
		childNode := &Node{
			VolumeID:     dirNode.VolumeID,
			RelativePath: relPath,
			FullPath:     childPath,
			EntryType:    entryType,
			SizeBytes:    info.Size(),
			MTimeNs:      info.ModTime().UnixNano(),
			Parent:       dirNode,
		}

		dirNode.Children = append(dirNode.Children, childNode)
		cEntries := atomic.AddInt64(entriesCount, 1)

		switch entryType {
		case model.EntryTypeRegular:
			atomic.AddInt64(filesCount, 1)
			*fileNodes = append(*fileNodes, childNode)

		case model.EntryTypeSymlink:
			target, err := os.Readlink(childPath)
			if err != nil {
				wErr := fmt.Errorf("failed reading symlink %q: %w", childPath, err)
				*walkErrors = append(*walkErrors, wErr.Error())
				if errFn != nil {
					errFn(wErr)
				}
				continue
			}
			childNode.QuickFingerprint = fingerprint.ComputeSymlinkFingerprint(target)

		case model.EntryTypeDirectory:
			atomic.AddInt64(dirsCount, 1)
			if err := walkDir(ctx, childNode, mountPoint, fileNodes, walkErrors, errFn, entriesCount, filesCount, dirsCount, report); err != nil {
				return err
			}

		default:
			// Other types (FIFO, Socket, etc.): treated as zero-content or empty fingerprint
		}

		if cEntries%100 == 0 {
			report()
		}
	}

	return nil
}
