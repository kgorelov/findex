package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintDuplicateSummary(t *testing.T) {
	summary := &model.DuplicateSummary{
		TotalGroups:  2,
		TotalEntries: 4,
		Groups: []*model.DuplicateGroup{
			{
				EntryType:   model.EntryTypeDirectory,
				SizeBytes:   1048576,
				Fingerprint: "aabbccdd11223344",
				Entries: []*model.DuplicateEntry{
					{
						VolumeID:     "vol1",
						RelativePath: "/photos/vacation",
						EntryType:    model.EntryTypeDirectory,
						SizeBytes:    1048576,
						Fingerprint:  "aabbccdd11223344",
					},
					{
						VolumeID:     "vol2",
						RelativePath: "/backup/vacation",
						EntryType:    model.EntryTypeDirectory,
						SizeBytes:    1048576,
						Fingerprint:  "aabbccdd11223344",
					},
				},
			},
			{
				EntryType:   model.EntryTypeRegular,
				SizeBytes:   512,
				Fingerprint: "1122334455667788",
				Entries: []*model.DuplicateEntry{
					{
						VolumeID:     "vol1",
						RelativePath: "/notes.txt",
						EntryType:    model.EntryTypeRegular,
						SizeBytes:    512,
						Fingerprint:  "1122334455667788",
					},
					{
						VolumeID:     "vol2",
						RelativePath: "/notes_copy.txt",
						EntryType:    model.EntryTypeRegular,
						SizeBytes:    512,
						Fingerprint:  "1122334455667788",
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := PrintDuplicateSummary(&buf, summary); err != nil {
		t.Fatalf("PrintDuplicateSummary failed: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "/photos/vacation") {
		t.Errorf("expected /photos/vacation in output, got: %s", out)
	}
	if !strings.Contains(out, "/notes.txt") {
		t.Errorf("expected /notes.txt in output, got: %s", out)
	}

	// Verify groups are delimited with an empty line
	if !strings.Contains(out, "\n\n") {
		t.Errorf("expected groups to be delimited with empty line, got: %q", out)
	}
}

func TestPrintDuplicateSummary_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintDuplicateSummary(&buf, nil); err != nil {
		t.Fatalf("PrintDuplicateSummary failed: %v", err)
	}
	if !strings.Contains(buf.String(), "No duplicate") {
		t.Errorf("expected no duplicate message, got: %s", buf.String())
	}
}
