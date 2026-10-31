package model

// DuplicateEntry represents an individual file or directory in a duplicate group.
type DuplicateEntry struct {
	VolumeID     string    `json:"volume_id"`
	VolumeLabel  string    `json:"volume_label,omitempty"`
	SourceHost   string    `json:"source_host,omitempty"`
	RelativePath string    `json:"relative_path"`
	EntryType    EntryType `json:"entry_type"`
	SizeBytes    int64     `json:"size_bytes"`
	Fingerprint  string    `json:"fingerprint"`
}

// DuplicateGroup represents a set of identical files or directories.
type DuplicateGroup struct {
	EntryType   EntryType         `json:"entry_type"`
	SizeBytes   int64             `json:"size_bytes"`
	Fingerprint string            `json:"fingerprint"`
	Entries     []*DuplicateEntry `json:"entries"`
}

// DuplicateSummary encapsulates all duplicate groups and statistics.
type DuplicateSummary struct {
	Volumes      []string          `json:"volumes"`
	TotalGroups  int               `json:"total_groups"`
	TotalEntries int               `json:"total_entries"`
	WastedBytes  int64             `json:"wasted_bytes"`
	Groups       []*DuplicateGroup `json:"groups"`
}
