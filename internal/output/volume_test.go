package output

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestPrintVolumesTable(t *testing.T) {
	vols := []*model.Volume{
		{
			VolumeID:       "vol-1",
			Label:          "Data",
			FSUUID:         "uuid-1",
			FSType:         "ext4",
			LastMountPoint: "/mnt/data",
			SourceHost:     "localhost",
			FirstSeenAt:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	var buf bytes.Buffer
	if err := PrintVolumesTable(&buf, vols); err != nil {
		t.Fatalf("failed to print table: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "vol-1") || !strings.Contains(out, "Data") {
		t.Errorf("expected table to contain vol-1 and Data, got:\n%s", out)
	}

	// Empty table
	buf.Reset()
	if err := PrintVolumesTable(&buf, nil); err != nil {
		t.Fatalf("failed to print empty table: %v", err)
	}
	if !strings.Contains(buf.String(), "No volumes registered.") {
		t.Errorf("expected empty message, got %s", buf.String())
	}
}

func TestPrintVolumeStats(t *testing.T) {
	stats := &model.VolumeStats{
		Volume: model.Volume{
			VolumeID:       "vol-2",
			Label:          "Photos",
			FirstSeenAt:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			LastMountPoint: "/media/photos",
		},
		ScanRootsCount: 1,
		ScanRoots:      []string{"/Photos"},
		FileCount:      100,
		TotalBytes:     1024 * 1024 * 50, // 50 MB
		DirectoryCount: 10,
	}

	var buf bytes.Buffer
	if err := PrintVolumeStats(&buf, stats); err != nil {
		t.Fatalf("failed to print volume stats: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "vol-2") || !strings.Contains(out, "50.00 MB") {
		t.Errorf("expected volume stats output, got:\n%s", out)
	}
}

func TestPrintJSON(t *testing.T) {
	v := map[string]string{"foo": "bar"}
	var buf bytes.Buffer
	if err := PrintJSON(&buf, v); err != nil {
		t.Fatalf("failed to print json: %v", err)
	}
	if !strings.Contains(buf.String(), `"foo": "bar"`) {
		t.Errorf("expected json output, got: %s", buf.String())
	}
}
