package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintCheckSummary(t *testing.T) {
	summary := &model.CheckSummary{
		CheckedAt:     time.Now().UTC(),
		DatabasePath:  "/tmp/test.sqlite",
		DatabaseBytes: 40960,
		IntegrityOK:   true,
		ForeignKeysOK: true,
		MetadataOK:    true,
		VolumesCount:  2,
		EntriesCount:  150,
		ErrorsCount:   0,
		WarningsCount: 1,
		Issues: []model.CheckIssue{
			{
				Severity: model.SeverityWarning,
				Category: "volumes",
				Message:  "Potential cloned filesystem detected",
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintCheckSummary(&buf, summary); err != nil {
		t.Fatalf("PrintCheckSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Database Diagnostic & Integrity Check:") {
		t.Errorf("expected header in output, got: %s", out)
	}
	if !strings.Contains(out, "SQLite Engine:   OK") {
		t.Errorf("expected SQLite Engine: OK, got: %s", out)
	}
	if !strings.Contains(out, "Potential cloned filesystem detected") {
		t.Errorf("expected warning message in output, got: %s", out)
	}
}

func TestPrintCheckSummaryJSON(t *testing.T) {
	summary := &model.CheckSummary{
		DatabasePath: "/tmp/check.sqlite",
		IntegrityOK:  true,
		VolumesCount: 3,
	}

	var buf bytes.Buffer
	if err := PrintCheckSummaryJSON(&buf, summary); err != nil {
		t.Fatalf("PrintCheckSummaryJSON failed: %v", err)
	}

	var parsed model.CheckSummary
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed unmarshaling JSON: %v", err)
	}
	if parsed.VolumesCount != 3 {
		t.Errorf("expected VolumesCount 3, got %d", parsed.VolumesCount)
	}
}
