package model

import (
	"fmt"
	"time"
)

// Volume represents an indexed filesystem.
type Volume struct {
	VolumeID       string     `json:"volume_id"`
	FSUUID         string     `json:"fs_uuid,omitempty"`
	FSType         string     `json:"fs_type,omitempty"`
	Label          string     `json:"label,omitempty"`
	DeviceID       string     `json:"device_id,omitempty"`
	LastMountPoint string     `json:"last_mount_point,omitempty"`
	SourceHost     string     `json:"source_host,omitempty"`
	SourceLabel    string     `json:"source_label,omitempty"`
	FirstSeenAt    time.Time  `json:"first_seen_at"`
	LastSeenAt     *time.Time `json:"last_seen_at,omitempty"`
}

// VolumeFilter allows filtering volumes in queries.
type VolumeFilter struct {
	VolumeID    string
	FSUUID      string
	Label       string
	SourceHost  string
	SourceLabel string
}

// VolumeStats provides summary statistics for a volume.
type VolumeStats struct {
	Volume           Volume     `json:"volume"`
	ScanRootsCount   int64      `json:"scan_roots_count"`
	ScanRoots        []string   `json:"scan_roots,omitempty"`
	FileCount        int64      `json:"file_count"`
	DirectoryCount   int64      `json:"directory_count"`
	SymlinkCount     int64      `json:"symlink_count"`
	OtherCount       int64      `json:"other_count"`
	TotalBytes       int64      `json:"total_bytes"`
	LastSuccessfulAt *time.Time `json:"last_successful_scan_at,omitempty"`
}

// ConflictType describes what kind of volume conflict occurred.
type ConflictType string

const (
	ConflictTypeFSUUIDMismatch  ConflictType = "fs_uuid_mismatch"
	ConflictTypeDeviceMismatch  ConflictType = "device_id_mismatch"
	ConflictTypeFSTypeMismatch  ConflictType = "fs_type_mismatch"
	ConflictTypeDuplicateFSUUID ConflictType = "duplicate_fs_uuid"
)

// VolumeConflictError is returned when volume registration or merge detects conflicting metadata.
type VolumeConflictError struct {
	Type          ConflictType
	VolumeID      string
	ExistingValue string
	IncomingValue string
	Message       string
}

func (e *VolumeConflictError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("volume conflict (%s) for volume %q: existing=%q, incoming=%q",
		e.Type, e.VolumeID, e.ExistingValue, e.IncomingValue)
}
