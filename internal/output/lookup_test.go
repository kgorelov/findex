package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintLookupSummary(t *testing.T) {
	summary := &model.LookupSummary{
		Query:           "/data/photo.jpg",
		TargetType:      "filepath",
		FilePath:        "/data/photo.jpg",
		Fingerprint:     "aabbccdd11223344",
		FingerprintType: "quick",
		MatchesCount:    1,
		Matches: []*model.LookupMatch{
			{
				VolumeID:     "vol1",
				VolumeLabel:  "BackupDisk",
				SourceHost:   "laptop",
				RelativePath: "/photos/photo.jpg",
				EntryType:    model.EntryTypeRegular,
				SizeBytes:    2048,
				Fingerprint:  "aabbccdd11223344",
				MatchType:    "quick",
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintLookupSummary(&buf, summary); err != nil {
		t.Fatalf("PrintLookupSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Filepath Lookup: /data/photo.jpg") {
		t.Errorf("expected header, got %q", out)
	}
	if !strings.Contains(out, "vol1 (BackupDisk)") {
		t.Errorf("expected volume label, got %q", out)
	}
	if !strings.Contains(out, "/photos/photo.jpg") {
		t.Errorf("expected matched path, got %q", out)
	}

	// Test zero matches
	summaryEmpty := &model.LookupSummary{
		Query:        "cafebabe",
		TargetType:   "fingerprint",
		Fingerprint:  "cafebabe",
		MatchesCount: 0,
	}
	buf.Reset()
	if err := PrintLookupSummary(&buf, summaryEmpty); err != nil {
		t.Fatalf("PrintLookupSummary empty failed: %v", err)
	}
	if !strings.Contains(buf.String(), "No matching entries found") {
		t.Errorf("expected no matching entries message, got %q", buf.String())
	}
}

func TestPrintLookupSummaryJSON(t *testing.T) {
	summary := &model.LookupSummary{
		Query:        "cafebabe",
		TargetType:   "fingerprint",
		Fingerprint:  "cafebabe",
		MatchesCount: 0,
	}

	var buf bytes.Buffer
	if err := PrintLookupSummaryJSON(&buf, summary); err != nil {
		t.Fatalf("PrintLookupSummaryJSON failed: %v", err)
	}
	if !strings.Contains(buf.String(), `"fingerprint": "cafebabe"`) {
		t.Errorf("expected json output, got %q", buf.String())
	}
}
