package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintCompareSummary(t *testing.T) {
	summary := &model.CompareSummary{
		PresentVolumes:     []string{"volA"},
		AbsentVolumes:      []string{"volB"},
		Mode:               "absent",
		EntryTypeFilter:    "all",
		TotalSourceEntries: 10,
		TotalSourceBytes:   10000,
		MatchingEntries:    7,
		MatchingBytes:      7000,
		AbsentEntries:      3,
		AbsentBytes:        3000,
		Items: []model.CompareItem{
			{
				VolumeID:     "volA",
				RelativePath: "/missing/file1.txt",
				EntryType:    model.EntryTypeRegular,
				SizeBytes:    1500,
				Fingerprint:  "1122334455667788",
				Status:       model.MatchStatusAbsent,
			},
			{
				VolumeID:     "volA",
				RelativePath: "/missing/dir",
				EntryType:    model.EntryTypeDirectory,
				SizeBytes:    0,
				Fingerprint:  "aabbccddeeff0011",
				Status:       model.MatchStatusAbsent,
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintCompareSummary(&buf, summary); err != nil {
		t.Fatalf("failed printing compare summary: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Volume Comparison") {
		t.Errorf("expected header 'Volume Comparison', got: %s", out)
	}
	if !strings.Contains(out, "Present Volumes:       volA") {
		t.Errorf("expected present volumes, got: %s", out)
	}
	if !strings.Contains(out, "Absent Volumes:        volB") {
		t.Errorf("expected absent volumes, got: %s", out)
	}
	if !strings.Contains(out, "/missing/file1.txt") {
		t.Errorf("expected /missing/file1.txt in items, got: %s", out)
	}
	if !strings.Contains(out, "/missing/dir") {
		t.Errorf("expected /missing/dir in items, got: %s", out)
	}
}

func TestPrintCompareSummaryJSON(t *testing.T) {
	summary := &model.CompareSummary{
		PresentVolumes:     []string{"v1"},
		AbsentVolumes:      []string{"v2"},
		Mode:               "common",
		TotalSourceEntries: 5,
		MatchingEntries:    5,
	}

	var buf bytes.Buffer
	if err := PrintCompareSummaryJSON(&buf, summary); err != nil {
		t.Fatalf("failed printing compare summary JSON: %v", err)
	}

	var parsed model.CompareSummary
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed unmarshaling compare summary JSON: %v", err)
	}
	if parsed.Mode != "common" {
		t.Errorf("expected mode common, got %q", parsed.Mode)
	}
	if parsed.MatchingEntries != 5 {
		t.Errorf("expected 5 matching entries, got %d", parsed.MatchingEntries)
	}
}
