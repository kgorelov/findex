package output

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/scan"
)

func TestPrintScanSummary(t *testing.T) {
	summary := &scan.Summary{
		ScanID:          "scan-test-1",
		VolumeID:        "vol-test-1",
		RootPath:        "/Photos",
		Status:          model.ScanStatusCompleted,
		Duration:        250 * time.Millisecond,
		FilesCount:      10,
		DirsCount:       2,
		TotalBytes:      5000,
		ReconciledCount: 1,
	}

	var buf bytes.Buffer
	if err := PrintScanSummary(&buf, summary); err != nil {
		t.Fatalf("PrintScanSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "completed") || !strings.Contains(out, "/Photos") || !strings.Contains(out, "vol-test-1") {
		t.Errorf("unexpected scan summary output:\n%s", out)
	}

	buf.Reset()
	if err := PrintScanSummaryJSON(&buf, summary); err != nil {
		t.Fatalf("PrintScanSummaryJSON failed: %v", err)
	}
	if !strings.Contains(buf.String(), `"root_path": "/Photos"`) {
		t.Errorf("unexpected json output:\n%s", buf.String())
	}
}
