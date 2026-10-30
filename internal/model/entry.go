package model

import (
	"fmt"
	"io/fs"
)

// EntryType indicates the kind of filesystem entry.
type EntryType int

const (
	EntryTypeRegular   EntryType = 1
	EntryTypeDirectory EntryType = 2
	EntryTypeSymlink   EntryType = 3
	EntryTypeOther     EntryType = 4
)

func (t EntryType) String() string {
	switch t {
	case EntryTypeRegular:
		return "file"
	case EntryTypeDirectory:
		return "directory"
	case EntryTypeSymlink:
		return "symlink"
	case EntryTypeOther:
		return "other"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// EntryTypeFromMode converts a Go fs.FileMode to an EntryType.
func EntryTypeFromMode(mode fs.FileMode) EntryType {
	switch {
	case mode.IsRegular():
		return EntryTypeRegular
	case mode.IsDir():
		return EntryTypeDirectory
	case mode&fs.ModeSymlink != 0:
		return EntryTypeSymlink
	default:
		return EntryTypeOther
	}
}

// Entry represents an indexed file or directory on a specific volume.
// The primary key in the store is (VolumeID, RelativePath).
type Entry struct {
	VolumeID             string    `json:"volume_id"`
	RelativePath         string    `json:"relative_path"`
	EntryType            EntryType `json:"entry_type"`
	SizeBytes            int64     `json:"size_bytes"`
	MTimeNs              int64     `json:"mtime_ns"`
	QuickFingerprint     []byte    `json:"quick_fingerprint,omitempty"`
	FullFingerprint      []byte    `json:"full_fingerprint,omitempty"`
	DirectoryFingerprint []byte    `json:"directory_fingerprint,omitempty"`
	LastSeenScanID       string    `json:"last_seen_scan_id,omitempty"`
}
