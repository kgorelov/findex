package fingerprint

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/kgorelov/findex/internal/model"
	"github.com/zeebo/blake3"
)

const (
	dirMagicHeader     = "FINDEX_DIR_V1"
	symlinkMagicHeader = "FINDEX_SYMLINK_V1"
)

// ChildDescriptor represents the content identity of a single direct child in a directory.
// Child names and locations are excluded.
type ChildDescriptor struct {
	Type        model.EntryType
	SizeBytes   int64
	Fingerprint []byte // 32-byte content fingerprint
}

// ComputeSymlinkFingerprint returns a deterministic 32-byte content fingerprint for a symlink target.
func ComputeSymlinkFingerprint(target string) []byte {
	hasher := blake3.New()
	hasher.WriteString(symlinkMagicHeader)
	hasher.Write([]byte{0x00})
	hasher.WriteString(target)
	return hasher.Sum(nil)
}

// ComputeDirectoryFingerprint calculates the content fingerprint of a directory
// from its direct children's descriptors according to the multiset specification:
// - Descriptors exclude child names.
// - Descriptors are formatted as fixed-width binary fields (type, size, fingerprint).
// - Descriptors are sorted lexicographically before hashing.
// - Duplicate descriptors are preserved (multiset).
func ComputeDirectoryFingerprint(children []ChildDescriptor) ([]byte, error) {
	// Format each child into a 41-byte canonical binary descriptor:
	// - 1 byte EntryType
	// - 8 bytes SizeBytes (big-endian)
	// - 32 bytes Fingerprint
	encoded := make([][]byte, len(children))
	for i, c := range children {
		desc := make([]byte, 1+8+32)
		desc[0] = byte(c.Type)
		binary.BigEndian.PutUint64(desc[1:9], uint64(c.SizeBytes))
		if len(c.Fingerprint) == 32 {
			copy(desc[9:41], c.Fingerprint)
		} else if len(c.Fingerprint) > 0 {
			// Pad or copy up to 32 bytes
			copy(desc[9:41], c.Fingerprint)
		}
		encoded[i] = desc
	}

	// Sort descriptors lexicographically to make ordering independent of traversal/filenames
	sort.Slice(encoded, func(i, j int) bool {
		return bytes.Compare(encoded[i], encoded[j]) < 0
	})

	hasher := blake3.New()

	// Header: "FINDEX_DIR_V1\x00" + count of children (uint64)
	header := make([]byte, len(dirMagicHeader)+1+8)
	copy(header, dirMagicHeader)
	header[len(dirMagicHeader)] = 0x00
	binary.BigEndian.PutUint64(header[len(dirMagicHeader)+1:], uint64(len(encoded)))

	if _, err := hasher.Write(header); err != nil {
		return nil, fmt.Errorf("failed writing directory header: %w", err)
	}

	for _, desc := range encoded {
		if _, err := hasher.Write(desc); err != nil {
			return nil, fmt.Errorf("failed writing child descriptor: %w", err)
		}
	}

	return hasher.Sum(nil), nil
}
