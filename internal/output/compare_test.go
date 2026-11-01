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

	// 1. Verbose mode: includes stats block
	var buf bytes.Buffer
	if err := PrintCompareSummary(&buf, summary, FormatOptions{Verbose: true}); err != nil {
		t.Fatalf("failed printing compare summary: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Volume Comparison") {
		t.Errorf("expected header 'Volume Comparison', got: %s", out)
	}
	if !strings.Contains(out, "Left Volumes:          volA") {
		t.Errorf("expected left volumes, got: %s", out)
	}
	if !strings.Contains(out, "Right Volumes:         volB") {
		t.Errorf("expected right volumes, got: %s", out)
	}
	if !strings.Contains(out, "/missing/file1.txt") {
		t.Errorf("expected /missing/file1.txt in items, got: %s", out)
	}
	if !strings.Contains(out, "/missing/dir") {
		t.Errorf("expected /missing/dir in items, got: %s", out)
	}

	// 2. Default (non-verbose) mode: no stats block, only table entries
	buf.Reset()
	if err := PrintCompareSummary(&buf, summary, FormatOptions{Verbose: false}); err != nil {
		t.Fatalf("failed printing default compare summary: %v", err)
	}
	quietOut := buf.String()
	if strings.Contains(quietOut, "Volume Comparison") {
		t.Errorf("did not expect 'Volume Comparison' in quiet output, got: %s", quietOut)
	}
	if !strings.Contains(quietOut, "TYPE") || !strings.Contains(quietOut, "/missing/file1.txt") {
		t.Errorf("expected table rows in quiet output, got: %s", quietOut)
	}

	// 3. Custom columns ordering: --columns volume,path,size,type
	buf.Reset()
	opts := FormatOptions{
		Columns: []string{"volume", "path", "size", "type"},
	}
	if err := PrintCompareSummary(&buf, summary, opts); err != nil {
		t.Fatalf("failed printing custom columns: %v", err)
	}
	colsOut := buf.String()
	firstLine := strings.Split(colsOut, "\n")[0]
	if !strings.Contains(firstLine, "VOLUME") || !strings.Contains(firstLine, "PATH") || !strings.Contains(firstLine, "SIZE") || !strings.Contains(firstLine, "TYPE") {
		t.Errorf("expected custom header in first line, got: %s", firstLine)
	}

	// 4. TSV format
	buf.Reset()
	optsTSV := FormatOptions{
		Format:  "tsv",
		Columns: []string{"volume", "path"},
	}
	if err := PrintCompareSummary(&buf, summary, optsTSV); err != nil {
		t.Fatalf("failed printing TSV: %v", err)
	}
	tsvOut := buf.String()
	if !strings.Contains(tsvOut, "VOLUME\tPATH\nvolA\t/missing/file1.txt") {
		t.Errorf("expected TSV output, got: %q", tsvOut)
	}

	// 5. CSV format
	buf.Reset()
	optsCSV := FormatOptions{
		Format:  "csv",
		Columns: []string{"volume", "path"},
	}
	if err := PrintCompareSummary(&buf, summary, optsCSV); err != nil {
		t.Fatalf("failed printing CSV: %v", err)
	}
	csvOut := buf.String()
	if !strings.Contains(csvOut, "VOLUME,PATH\nvolA,/missing/file1.txt") {
		t.Errorf("expected CSV output, got: %q", csvOut)
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
