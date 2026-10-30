package index

import (
	"context"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestEntryStore_UpsertAndGet(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol := &model.Volume{
		VolumeID: "vol-entry-test",
		Label:    "Test Vol",
	}
	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	entry1 := &model.Entry{
		VolumeID:         "vol-entry-test",
		RelativePath:     "/docs/readme.txt",
		EntryType:        model.EntryTypeRegular,
		SizeBytes:        123,
		MTimeNs:          1700000000,
		QuickFingerprint: []byte("11111111111111111111111111111111"),
		LastSeenScanID:   "scan-001",
	}

	if err := db.UpsertEntries(ctx, []*model.Entry{entry1}); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}

	got, err := db.GetEntry(ctx, "vol-entry-test", "/docs/readme.txt")
	if err != nil {
		t.Fatalf("get entry failed: %v", err)
	}
	if got.SizeBytes != 123 || got.LastSeenScanID != "scan-001" {
		t.Errorf("unexpected entry data: %+v", got)
	}

	// Update existing entry
	entry1Updated := &model.Entry{
		VolumeID:         "vol-entry-test",
		RelativePath:     "/docs/readme.txt",
		EntryType:        model.EntryTypeRegular,
		SizeBytes:        456,
		MTimeNs:          1700000500,
		QuickFingerprint: []byte("22222222222222222222222222222222"),
		LastSeenScanID:   "scan-002",
	}
	if err := db.UpsertEntries(ctx, []*model.Entry{entry1Updated}); err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}

	gotUpdated, err := db.GetEntry(ctx, "vol-entry-test", "/docs/readme.txt")
	if err != nil {
		t.Fatalf("get updated entry failed: %v", err)
	}
	if gotUpdated.SizeBytes != 456 || gotUpdated.LastSeenScanID != "scan-002" {
		t.Errorf("entry was not updated: %+v", gotUpdated)
	}
}

func TestEntryStore_Reconciliation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol := &model.Volume{VolumeID: "vol-recon"}
	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	entries := []*model.Entry{
		// Under /Photos
		{VolumeID: "vol-recon", RelativePath: "/Photos", EntryType: model.EntryTypeDirectory, LastSeenScanID: "scan-old"},
		{VolumeID: "vol-recon", RelativePath: "/Photos/a.jpg", EntryType: model.EntryTypeRegular, LastSeenScanID: "scan-new"},
		{VolumeID: "vol-recon", RelativePath: "/Photos/deleted.jpg", EntryType: model.EntryTypeRegular, LastSeenScanID: "scan-old"},
		// Under /Music (different root)
		{VolumeID: "vol-recon", RelativePath: "/Music/song.mp3", EntryType: model.EntryTypeRegular, LastSeenScanID: "scan-old"},
	}
	if err := db.UpsertEntries(ctx, entries); err != nil {
		t.Fatalf("failed upserting entries: %v", err)
	}

	// Reconcile /Photos with scan-new
	deletedCount, err := db.ReconcileStaleEntries(ctx, "vol-recon", "/Photos", "scan-new")
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	// /Photos directory itself had scan-old and deleted.jpg had scan-old => 2 deleted
	if deletedCount != 2 {
		t.Errorf("expected 2 deleted entries, got %d", deletedCount)
	}

	// Verify /Photos/a.jpg is still present
	_, err = db.GetEntry(ctx, "vol-recon", "/Photos/a.jpg")
	if err != nil {
		t.Errorf("expected /Photos/a.jpg to exist: %v", err)
	}

	// Verify /Music/song.mp3 was NOT deleted
	_, err = db.GetEntry(ctx, "vol-recon", "/Music/song.mp3")
	if err != nil {
		t.Errorf("expected /Music/song.mp3 to NOT be deleted: %v", err)
	}
}

func TestEntryStore_ScanRunAndRoots(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol := &model.Volume{VolumeID: "vol-run"}
	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	run := &model.ScanRun{
		ScanID:    "scan-100",
		VolumeID:  "vol-run",
		RootPath:  "/data",
		Status:    model.ScanStatusRunning,
		StartedAt: time.Now().UTC(),
	}
	if err := db.CreateScanRun(ctx, run); err != nil {
		t.Fatalf("create scan run failed: %v", err)
	}

	now := time.Now().UTC()
	run.Status = model.ScanStatusCompleted
	run.CompletedAt = &now
	run.EntriesScanned = 50
	if err := db.UpdateScanRun(ctx, run); err != nil {
		t.Fatalf("update scan run failed: %v", err)
	}

	if err := db.RecordScanRootSuccess(ctx, "vol-run", "/data", "scan-100", now); err != nil {
		t.Fatalf("record scan root failed: %v", err)
	}

	stats, err := db.GetVolumeStats(ctx, "vol-run")
	if err != nil {
		t.Fatalf("get stats failed: %v", err)
	}
	if stats.ScanRootsCount != 1 || stats.ScanRoots[0] != "/data" {
		t.Errorf("unexpected scan roots: %+v", stats)
	}
}
