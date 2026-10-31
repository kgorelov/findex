package model

import (
	"time"
)

// ConflictKind describes what kind of conflict was encountered during a merge.
type ConflictKind string

const (
	ConflictKindSameLocationDifferentContent ConflictKind = "same_location_different_content"
	ConflictKindVolumeMetadata               ConflictKind = "volume_metadata_mismatch"
	ConflictKindDuplicateFSUUID              ConflictKind = "duplicate_fs_uuid"
)

// ConflictPolicy defines how content conflicts at the same location are resolved.
type ConflictPolicy string

const (
	PolicyPreferNewer    ConflictPolicy = "newer"
	PolicyKeepLocal      ConflictPolicy = "keep-local"
	PolicyKeepRemote     ConflictPolicy = "keep-remote"
	PolicyFailOnConflict ConflictPolicy = "fail"
)

// MergeConflict captures details about a detected conflict during merge.
type MergeConflict struct {
	ConflictKind ConflictKind `json:"conflict_kind"`
	VolumeID     string       `json:"volume_id"`
	RelativePath string       `json:"relative_path,omitempty"`
	LocalDetail  string       `json:"local_detail,omitempty"`
	RemoteDetail string       `json:"remote_detail,omitempty"`
	Resolution   string       `json:"resolution"`
}

// MergeRun represents a recorded merge operation.
type MergeRun struct {
	MergeID                 string     `json:"merge_id"`
	SourcePath              string     `json:"source_path"`
	Status                  string     `json:"status"` // "running", "completed", "failed"
	StartedAt               time.Time  `json:"started_at"`
	CompletedAt             *time.Time `json:"completed_at,omitempty"`
	VolumesMerged           int64      `json:"volumes_merged"`
	EntriesInserted         int64      `json:"entries_inserted"`
	EntriesUpdated          int64      `json:"entries_updated"`
	EntriesIdentical        int64      `json:"entries_identical"`
	SameContentDiffLocation int64      `json:"same_content_diff_location"`
	ConflictsCount          int64      `json:"conflicts_count"`
	ErrorMessage            string     `json:"error_message,omitempty"`
}

// MergeResult is the summary returned after executing a merge operation.
type MergeResult struct {
	MergeID                 string          `json:"merge_id"`
	SourcePath              string          `json:"source_path"`
	Status                  string          `json:"status"`
	StartedAt               time.Time       `json:"started_at"`
	CompletedAt             time.Time       `json:"completed_at"`
	Duration                time.Duration   `json:"duration"`
	VolumesMerged           int64           `json:"volumes_merged"`
	EntriesInserted         int64           `json:"entries_inserted"`
	EntriesUpdated          int64           `json:"entries_updated"`
	EntriesIdentical        int64           `json:"entries_identical"`
	SameContentDiffLocation int64           `json:"same_content_diff_location"`
	ConflictsCount          int64           `json:"conflicts_count"`
	Conflicts               []MergeConflict `json:"conflicts,omitempty"`
	ErrorMessage            string          `json:"error_message,omitempty"`
}
