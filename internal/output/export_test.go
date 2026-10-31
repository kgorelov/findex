package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintExportSummary(t *testing.T) {
	summary := &model.ExportSummary{
		Destination:  "/tmp/inventory.json.gz",
		Compressed:   true,
		Duration:     50 * time.Millisecond,
		VolumesCount: 2,
		EntriesCount: 500,
		BytesWritten: 12345,
	}

	var buf bytes.Buffer
	if err := PrintExportSummary(&buf, summary); err != nil {
		t.Fatalf("PrintExportSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Inventory Export Summary:") {
		t.Errorf("expected header, got: %s", out)
	}
	if !strings.Contains(out, "Destination:     /tmp/inventory.json.gz") {
		t.Errorf("expected destination, got: %s", out)
	}
	if !strings.Contains(out, "Entries:         500") {
		t.Errorf("expected entries count, got: %s", out)
	}
}

func TestPrintImportSummary(t *testing.T) {
	summary := &model.ImportSummary{
		Source:           "/tmp/inventory.json.gz",
		Duration:         100 * time.Millisecond,
		VolumesImported:  2,
		EntriesInserted:  450,
		EntriesIdentical: 50,
	}

	var buf bytes.Buffer
	if err := PrintImportSummary(&buf, summary); err != nil {
		t.Fatalf("PrintImportSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Inventory Import Summary:") {
		t.Errorf("expected header, got: %s", out)
	}
	if !strings.Contains(out, "Entries New:     450") {
		t.Errorf("expected entries new, got: %s", out)
	}
}

func TestPrintExportSummaryJSON(t *testing.T) {
	summary := &model.ExportSummary{
		Destination:  "/tmp/inv.json",
		EntriesCount: 10,
	}

	var buf bytes.Buffer
	if err := PrintExportSummaryJSON(&buf, summary); err != nil {
		t.Fatalf("PrintExportSummaryJSON failed: %v", err)
	}

	var parsed model.ExportSummary
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("failed unmarshaling JSON: %v", err)
	}
	if parsed.EntriesCount != 10 {
		t.Errorf("expected EntriesCount 10, got %d", parsed.EntriesCount)
	}
}
