package tree

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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

// BuildTree walks the filesystem rooted at startDir and constructs a tree of Nodes.
// It returns the root Node, a slice of all regular file nodes that need fingerprinting,
// and any non-fatal walk warnings/errors encountered (such as permission denied on subdirectories).
func BuildTree(ctx context.Context, volumeID, mountPoint, startDir string) (*Node, []*Node, []string, error) {
	absStart, err := filepath.Abs(startDir)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get absolute path for %q: %w", startDir, err)
	}

	startStat, err := os.Lstat(absStart)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to stat scan root %q: %w", absStart, err)
	}

	relPath, err := model.ToVolumeRelativePath(mountPoint, absStart)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed mapping scan root to volume path: %w", err)
	}

	rootNode := &Node{
		VolumeID:     volumeID,
		RelativePath: relPath,
		FullPath:     absStart,
		EntryType:    model.EntryTypeFromMode(startStat.Mode()),
		SizeBytes:    startStat.Size(),
		MTimeNs:      startStat.ModTime().UnixNano(),
	}

	var fileNodes []*Node
	var walkErrors []string

	if !rootNode.EntryType.IsDirectory() {
		// Single file scan
		if rootNode.EntryType == model.EntryTypeRegular {
			fileNodes = append(fileNodes, rootNode)
		} else if rootNode.EntryType == model.EntryTypeSymlink {
			target, err := os.Readlink(absStart)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed reading symlink target: %w", err)
			}
			rootNode.QuickFingerprint = fingerprint.ComputeSymlinkFingerprint(target)
		}
		return rootNode, fileNodes, nil, nil
	}

	// Recursively walk directory
	if err := walkDir(ctx, rootNode, mountPoint, &fileNodes, &walkErrors); err != nil {
		return nil, nil, nil, err
	}

	return rootNode, fileNodes, walkErrors, nil
}

func walkDir(ctx context.Context, dirNode *Node, mountPoint string, fileNodes *[]*Node, walkErrors *[]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entries, err := os.ReadDir(dirNode.FullPath)
	if err != nil {
		*walkErrors = append(*walkErrors, fmt.Sprintf("failed reading directory %q: %v", dirNode.FullPath, err))
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
			*walkErrors = append(*walkErrors, fmt.Sprintf("failed getting info for %q: %v", childPath, err))
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

		switch entryType {
		case model.EntryTypeRegular:
			*fileNodes = append(*fileNodes, childNode)

		case model.EntryTypeSymlink:
			target, err := os.Readlink(childPath)
			if err != nil {
				*walkErrors = append(*walkErrors, fmt.Sprintf("failed reading symlink %q: %v", childPath, err))
				continue
			}
			childNode.QuickFingerprint = fingerprint.ComputeSymlinkFingerprint(target)

		case model.EntryTypeDirectory:
			if err := walkDir(ctx, childNode, mountPoint, fileNodes, walkErrors); err != nil {
				return err
			}

		default:
			// Other types (FIFO, Socket, etc.): treated as zero-content or empty fingerprint
		}
	}

	return nil
}
