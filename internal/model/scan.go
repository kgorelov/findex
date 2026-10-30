package model

import (
	"time"
)

// ScanStatus represents the state of a scan run.
type ScanStatus string

const (
	ScanStatusRunning   ScanStatus = "running"
	ScanStatusCompleted ScanStatus = "completed"
	ScanStatusFailed    ScanStatus = "failed"
)

// ScanRun tracks an individual scan operation.
type ScanRun struct {
	ScanID         string     `json:"scan_id"`
	VolumeID       string     `json:"volume_id"`
	RootPath       string     `json:"root_path"`
	Status         ScanStatus `json:"status"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	EntriesScanned int64      `json:"entries_scanned"`
	ErrorsCount    int64      `json:"errors_count"`
	ErrorMessage   string     `json:"error_message,omitempty"`
}

// ScanRoot tracks an indexed area of a volume.
type ScanRoot struct {
	VolumeID             string     `json:"volume_id"`
	RootPath             string     `json:"root_path"`
	LastSuccessfulScanAt *time.Time `json:"last_successful_scan_at,omitempty"`
	LastScanID           string     `json:"last_scan_id,omitempty"`
}
