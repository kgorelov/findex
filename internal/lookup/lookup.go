package lookup

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/tree"
	"github.com/zeebo/blake3"
)

// Lookup executes a search for matching database entries by either a fingerprint (partial or full)
// or by a filesystem path (file or directory) whose fingerprint is computed on-the-fly.
func Lookup(ctx context.Context, db *index.DB, target string) (*model.LookupSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	cleanTarget := strings.TrimSpace(target)
	if cleanTarget == "" {
		return nil, fmt.Errorf("lookup target cannot be empty")
	}

	fi, statErr := os.Lstat(cleanTarget)
	if statErr == nil {
		return lookupPath(ctx, db, cleanTarget, fi)
	}

	// Not a local filesystem path: verify if valid hex fingerprint
	fpQuery := strings.TrimPrefix(cleanTarget, "0x")
	fpQuery = strings.TrimPrefix(fpQuery, "0X")
	if !isHexString(fpQuery) {
		return nil, fmt.Errorf("target %q does not exist on disk and is not a valid hex fingerprint", target)
	}

	matches, err := db.LookupFingerprint(ctx, fpQuery)
	if err != nil {
		return nil, err
	}

	return &model.LookupSummary{
		Query:        cleanTarget,
		TargetType:   "fingerprint",
		Fingerprint:  strings.ToLower(fpQuery),
		MatchesCount: len(matches),
		Matches:      matches,
	}, nil
}

func lookupPath(ctx context.Context, db *index.DB, path string, fi os.FileInfo) (*model.LookupSummary, error) {
	summary := &model.LookupSummary{
		Query:      path,
		TargetType: "filepath",
		FilePath:   path,
	}

	var fpBytes []byte
	var fpType string

	switch {
	case fi.IsDir():
		dirFP, err := computeDirectoryFingerprint(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed calculating directory fingerprint for %q: %w", path, err)
		}
		fpBytes = dirFP
		fpType = "dir"

	case fi.Mode().IsRegular():
		res, err := fingerprint.FingerprintFile(path, fingerprint.Options{IncludeFull: true})
		if err != nil {
			return nil, fmt.Errorf("failed fingerprinting file %q: %w", path, err)
		}
		fpBytes = res.QuickFingerprint
		fpType = "quick"

	case fi.Mode()&os.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return nil, fmt.Errorf("failed reading symlink %q: %w", path, err)
		}
		h := blake3.Sum256([]byte(target))
		fpBytes = h[:]
		fpType = "symlink"

	default:
		return nil, fmt.Errorf("unsupported file mode %v for %q", fi.Mode(), path)
	}

	summary.Fingerprint = hex.EncodeToString(fpBytes)
	summary.FingerprintType = fpType

	matches, err := db.LookupFingerprint(ctx, summary.Fingerprint)
	if err != nil {
		return nil, err
	}

	summary.Matches = matches
	summary.MatchesCount = len(matches)
	return summary, nil
}

func computeDirectoryFingerprint(ctx context.Context, dirPath string) ([]byte, error) {
	rootNode, _, _, err := tree.BuildTree(ctx, "lookup", dirPath, dirPath)
	if err != nil {
		return nil, err
	}

	var resolveNode func(node *tree.Node) ([]byte, error)
	resolveNode = func(node *tree.Node) ([]byte, error) {
		var childDescs []fingerprint.ChildDescriptor
		for _, child := range node.Children {
			var childFP []byte
			switch child.EntryType {
			case model.EntryTypeRegular:
				res, err := fingerprint.FingerprintFile(child.FullPath, fingerprint.Options{})
				if err != nil {
					return nil, err
				}
				childFP = res.QuickFingerprint

			case model.EntryTypeDirectory:
				subDirFP, err := resolveNode(child)
				if err != nil {
					return nil, err
				}
				childFP = subDirFP

			case model.EntryTypeSymlink:
				target, err := os.Readlink(child.FullPath)
				if err != nil {
					return nil, err
				}
				h := blake3.Sum256([]byte(target))
				childFP = h[:]
			}

			childDescs = append(childDescs, fingerprint.ChildDescriptor{
				Type:        child.EntryType,
				SizeBytes:   child.SizeBytes,
				Fingerprint: childFP,
			})
		}

		return fingerprint.ComputeDirectoryFingerprint(childDescs)
	}

	return resolveNode(rootNode)
}

func isHexString(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
