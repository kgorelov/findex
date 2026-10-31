package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintMergeSummary(t *testing.T) {
	now := time.Now().UTC()
	res := &model.MergeResult{
		MergeID:                 "m-1234",
		Status:                  "completed",
		SourcePath:              "/tmp/remote.sqlite",
		StartedAt:               now,
		CompletedAt:             now.Add(150 * time.Millisecond),
		Duration:                150 * time.Millisecond,
		VolumesMerged:           2,
		EntriesInserted:         10,
		EntriesUpdated:          3,
		EntriesIdentical:        25,
		SameContentDiffLocation: 2,
		ConflictsCount:          1,
		Conflicts: []model.MergeConflict{
			{
				ConflictKind: model.ConflictKindSameLocationDifferentContent,
				VolumeID:     "vol-1",
				RelativePath: "/data/file.txt",
				LocalDetail:  "size=100",
				RemoteDetail: "size=200",
				Resolution:   "updated_to_remote_newer",
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintMergeSummary(&buf, res); err != nil {
		t.Fatalf("failed printing merge summary: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Merge Summary:") {
		t.Errorf("expected 'Merge Summary:' in output, got: %s", out)
	}
	if !strings.Contains(out, "Merge ID:                m-1234") {
		t.Errorf("expected merge ID in output, got: %s", out)
	}
	if !strings.Contains(out, "Volumes Merged:          2") {
		t.Errorf("expected volumes merged count, got: %s", out)
	}
	if !strings.Contains(out, "Entries Inserted:        10") {
		t.Errorf("expected entries inserted count, got: %s", out)
	}
	if !strings.Contains(out, "Entries Updated:         3") {
		t.Errorf("expected entries updated count, got: %s", out)
	}
	if !strings.Contains(out, "Same Content Diff Path:  2") {
		t.Errorf("expected same content diff path count, got: %s", out)
	}
	if !strings.Contains(out, "Conflicts Detected:      1") {
		t.Errorf("expected conflicts count, got: %s", out)
	}
	if !strings.Contains(out, "vol-1:/data/file.txt") {
		t.Errorf("expected conflict location in output, got: %s", out)
	}
}

func TestPrintMergeSummaryJSON(t *testing.T) {
	res := &model.MergeResult{
		MergeID:          "m-json",
		Status:           "completed",
		SourcePath:       "/tmp/test.sqlite",
		VolumesMerged:    1,
		EntriesInserted:  5,
		EntriesIdentical: 5,
	}

	var buf bytes.Buffer
	if err := PrintMergeSummaryJSON(&buf, res); err != nil {
		t.Fatalf("failed printing merge summary JSON: %v", err)
	}

	var parsed model.MergeResult
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed parsing generated JSON: %v", err)
	}
	if parsed.MergeID != "m-json" {
		t.Errorf("expected MergeID m-json, got %q", parsed.MergeID)
	}
	if parsed.EntriesInserted != 5 {
		t.Errorf("expected EntriesInserted 5, got %d", parsed.EntriesInserted)
	}
}
