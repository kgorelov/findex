package model

// LookupMatch represents an entry matching a lookup query.
type LookupMatch struct {
	VolumeID     string    `json:"volume_id"`
	VolumeLabel  string    `json:"volume_label,omitempty"`
	SourceHost   string    `json:"source_host,omitempty"`
	RelativePath string    `json:"relative_path"`
	EntryType    EntryType `json:"entry_type"`
	SizeBytes    int64     `json:"size_bytes"`
	MTimeNs      int64     `json:"mtime_ns"`
	Fingerprint  string    `json:"fingerprint"`
	MatchType    string    `json:"match_type"` // "quick", "full", "dir", "symlink"
}

// LookupSummary represents the full results of a findex lookup execution.
type LookupSummary struct {
	Query           string         `json:"query"`
	TargetType      string         `json:"target_type"` // "fingerprint" or "filepath"
	FilePath        string         `json:"filepath,omitempty"`
	Fingerprint     string         `json:"fingerprint"`
	FingerprintType string         `json:"fingerprint_type,omitempty"`
	MatchesCount    int            `json:"matches_count"`
	Matches         []*LookupMatch `json:"matches"`
}
