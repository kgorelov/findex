package model

// MatchStatus indicates whether an entry matched destination content.
type MatchStatus string

const (
	MatchStatusAbsent  MatchStatus = "absent"
	MatchStatusPresent MatchStatus = "present"
)

// VerificationConfidence indicates how content equivalence was verified.
type VerificationConfidence string

const (
	ConfidenceQuickFingerprint VerificationConfidence = "quick_fingerprint"
	ConfidenceFullHash         VerificationConfidence = "full_verified"
)

// LocationRef identifies a specific location of content.
type LocationRef struct {
	VolumeID     string `json:"volume_id"`
	RelativePath string `json:"relative_path"`
}

// CompareItem represents an entry compared between volume sets.
type CompareItem struct {
	VolumeID          string                 `json:"volume_id"`
	RelativePath      string                 `json:"relative_path"`
	EntryType         EntryType              `json:"entry_type"`
	SizeBytes         int64                  `json:"size_bytes"`
	Fingerprint       string                 `json:"fingerprint"`
	Status            MatchStatus            `json:"status"`
	Confidence        VerificationConfidence `json:"confidence"`
	MatchingLocations []LocationRef          `json:"matching_locations,omitempty"`
}

// CompareSummary provides statistics and results for volume set comparisons.
type CompareSummary struct {
	LeftVolumes        []string      `json:"left_volumes,omitempty"`
	RightVolumes       []string      `json:"right_volumes,omitempty"`
	PresentVolumes     []string      `json:"present_volumes"`
	AbsentVolumes      []string      `json:"absent_volumes,omitempty"`
	Mode               string        `json:"mode"`
	EntryTypeFilter    string        `json:"entry_type_filter"`
	TotalSourceEntries int64         `json:"total_source_entries"`
	TotalSourceBytes   int64         `json:"total_source_bytes"`
	MatchingEntries    int64         `json:"matching_entries"`
	MatchingBytes      int64         `json:"matching_bytes"`
	AbsentEntries      int64         `json:"absent_entries"`
	AbsentBytes        int64         `json:"absent_bytes"`
	Items              []CompareItem `json:"items,omitempty"`
}
