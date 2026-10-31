package index

import (
	"context"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestCheckIntegrity_CleanDatabase(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	vol := &model.Volume{VolumeID: "volClean", FirstSeenAt: now}
	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	validFP := make([]byte, 32)
	copy(validFP, "32-bytes-blake3-quick-fingerprnt")

	if err := db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volClean",
			RelativePath:     "/clean_file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: validFP,
		},
	}); err != nil {
		t.Fatalf("failed upserting entries: %v", err)
	}

	summary, err := db.CheckIntegrity(ctx)
	if err != nil {
		t.Fatalf("CheckIntegrity failed: %v", err)
	}

	if !summary.IntegrityOK {
		t.Errorf("expected IntegrityOK to be true")
	}
	if !summary.ForeignKeysOK {
		t.Errorf("expected ForeignKeysOK to be true")
	}
	if !summary.MetadataOK {
		t.Errorf("expected MetadataOK to be true")
	}
	if summary.ErrorsCount != 0 {
		t.Errorf("expected 0 errors, got %d: %v", summary.ErrorsCount, summary.Issues)
	}
	if summary.VolumesCount != 1 {
		t.Errorf("expected 1 volume, got %d", summary.VolumesCount)
	}
	if summary.EntriesCount != 1 {
		t.Errorf("expected 1 entry, got %d", summary.EntriesCount)
	}
}

func TestCheckIntegrity_DetectedIssues(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	// Two volumes with same non-empty FSUUID (cloned filesystem)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "v1", FSUUID: "clone-uuid", FirstSeenAt: now}, false)
	_, _ = db.db.Exec("INSERT INTO volumes (volume_id, fs_uuid, first_seen_at) VALUES ('v2', 'clone-uuid', ?)", now.Unix())

	// Insert an uncompleted scan run
	_ = db.CreateScanRun(ctx, &model.ScanRun{
		ScanID:    "scan-interrupted",
		VolumeID:  "v1",
		RootPath:  "/",
		Status:    model.ScanStatusRunning,
		StartedAt: now,
	})

	// Temporarily disable foreign keys to insert an orphan entry and invalid path
	_, _ = db.db.Exec("PRAGMA foreign_keys = OFF;")
	_, _ = db.db.Exec(`
		INSERT INTO entries (volume_id, relative_path, entry_type, size_bytes, quick_fingerprint)
		VALUES ('nonexistent-vol', 'unnormalized/path.txt', 1, 50, X'1234')
	`)
	_, _ = db.db.Exec("PRAGMA foreign_keys = ON;")

	summary, err := db.CheckIntegrity(ctx)
	if err != nil {
		t.Fatalf("CheckIntegrity failed: %v", err)
	}

	if summary.ErrorsCount == 0 {
		t.Errorf("expected errors reported, got 0")
	}
	if summary.WarningsCount == 0 {
		t.Errorf("expected warnings reported, got 0")
	}

	foundOrphan := false
	foundDupUUID := false
	foundRunningScan := false
	foundInvalidFP := false
	foundUnnormalizedPath := false

	for _, issue := range summary.Issues {
		switch issue.Category {
		case "entries":
			if issue.Severity == model.SeverityError {
				foundOrphan = true
				foundInvalidFP = true
			} else if issue.Severity == model.SeverityWarning {
				foundUnnormalizedPath = true
			}
		case "volumes":
			foundDupUUID = true
		case "scan_runs":
			foundRunningScan = true
		}
	}

	if !foundOrphan {
		t.Errorf("expected orphan entries issue")
	}
	if !foundDupUUID {
		t.Errorf("expected duplicate FSUUID issue")
	}
	if !foundRunningScan {
		t.Errorf("expected running scan run warning")
	}
	if !foundInvalidFP {
		t.Errorf("expected invalid fingerprint issue")
	}
	if !foundUnnormalizedPath {
		t.Errorf("expected unnormalized path issue")
	}
}
