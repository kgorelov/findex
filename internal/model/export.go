package model

import "time"

// ExportInventory represents a portable export of file inventory and volume metadata.
type ExportInventory struct {
	ExportVersion string            `json:"export_version"`
	ExportedAt    time.Time         `json:"exported_at"`
	Metadata      map[string]string `json:"metadata"`
	Volumes       []*Volume         `json:"volumes"`
	ScanRoots     []*ScanRoot       `json:"scan_roots"`
	ScanRuns      []*ScanRun        `json:"scan_runs,omitempty"`
	Entries       []*Entry          `json:"entries"`
}

// ExportSummary provides statistics about an export operation.
type ExportSummary struct {
	Destination    string        `json:"destination"`
	Compressed     bool          `json:"compressed"`
	ExportedAt     time.Time     `json:"exported_at"`
	Duration       time.Duration `json:"duration"`
	VolumesCount   int           `json:"volumes_count"`
	EntriesCount   int           `json:"entries_count"`
	ScanRootsCount int           `json:"scan_roots_count"`
	ScanRunsCount  int           `json:"scan_runs_count"`
	BytesWritten   int64         `json:"bytes_written"`
}

// ImportSummary provides statistics about an import operation.
type ImportSummary struct {
	Source           string        `json:"source"`
	ImportedAt       time.Time     `json:"imported_at"`
	Duration         time.Duration `json:"duration"`
	VolumesImported  int64         `json:"volumes_imported"`
	EntriesInserted  int64         `json:"entries_inserted"`
	EntriesUpdated   int64         `json:"entries_updated"`
	EntriesIdentical int64         `json:"entries_identical"`
	ScanRootsCount   int64         `json:"scan_roots_count"`
	ScanRunsCount    int64         `json:"scan_runs_count"`
}
