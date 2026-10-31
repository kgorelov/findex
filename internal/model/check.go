package model

import "time"

// CheckSeverity represents the severity level of a check finding.
type CheckSeverity string

const (
	SeverityInfo    CheckSeverity = "info"
	SeverityWarning CheckSeverity = "warning"
	SeverityError   CheckSeverity = "error"
)

// CheckIssue represents a specific diagnostic issue found during database check.
type CheckIssue struct {
	Severity CheckSeverity `json:"severity"`
	Category string        `json:"category"`
	Message  string        `json:"message"`
	Detail   string        `json:"detail,omitempty"`
}

// CheckSummary provides full results of database integrity and consistency checks.
type CheckSummary struct {
	CheckedAt      time.Time    `json:"checked_at"`
	DatabasePath   string       `json:"database_path"`
	DatabaseBytes  int64        `json:"database_bytes"`
	WALBytes       int64        `json:"wal_bytes,omitempty"`
	IntegrityOK    bool         `json:"integrity_ok"`
	ForeignKeysOK  bool         `json:"foreign_keys_ok"`
	MetadataOK     bool         `json:"metadata_ok"`
	VolumesCount   int64        `json:"volumes_count"`
	EntriesCount   int64        `json:"entries_count"`
	ScanRootsCount int64        `json:"scan_roots_count"`
	ScanRunsCount  int64        `json:"scan_runs_count"`
	ErrorsCount    int          `json:"errors_count"`
	WarningsCount  int          `json:"warnings_count"`
	Issues         []CheckIssue `json:"issues,omitempty"`
}
