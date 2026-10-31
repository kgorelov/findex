package index

import (
	"context"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestMergeTx_Operations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	tx, err := db.BeginMergeTx(ctx)
	if err != nil {
		t.Fatalf("failed starting merge tx: %v", err)
	}

	vol := &model.Volume{
		VolumeID:    "vol-merge-1",
		FSUUID:      "uuid-111",
		FSType:      "ext4",
		Label:       "Data",
		FirstSeenAt: time.Now().UTC(),
	}
	if err := tx.InsertVolume(ctx, vol); err != nil {
		t.Fatalf("failed inserting volume: %v", err)
	}

	vol.Label = "DataUpdated"
	if err := tx.UpdateVolume(ctx, vol); err != nil {
		t.Fatalf("failed updating volume: %v", err)
	}

	now := time.Now().UTC()
	run := &model.ScanRun{
		ScanID:         "scan-1",
		VolumeID:       "vol-merge-1",
		RootPath:       "/",
		Status:         model.ScanStatusCompleted,
		StartedAt:      now.Add(-time.Minute),
		CompletedAt:    &now,
		EntriesScanned: 10,
	}
	if err := tx.UpsertScanRun(ctx, run); err != nil {
		t.Fatalf("failed upserting scan run: %v", err)
	}

	root := &model.ScanRoot{
		VolumeID:             "vol-merge-1",
		RootPath:             "/",
		LastSuccessfulScanAt: &now,
		LastScanID:           "scan-1",
	}
	if err := tx.UpsertScanRoot(ctx, root); err != nil {
		t.Fatalf("failed upserting scan root: %v", err)
	}

	entry := &model.Entry{
		VolumeID:         "vol-merge-1",
		RelativePath:     "file.txt",
		EntryType:        model.EntryTypeRegular,
		SizeBytes:        100,
		MTimeNs:          now.UnixNano(),
		QuickFingerprint: []byte("quickfp1"),
		LastSeenScanID:   "scan-1",
	}
	if err := tx.InsertEntry(ctx, entry); err != nil {
		t.Fatalf("failed inserting entry: %v", err)
	}

	entry.SizeBytes = 200
	if err := tx.UpdateEntry(ctx, entry); err != nil {
		t.Fatalf("failed updating entry: %v", err)
	}

	mRun := &model.MergeRun{
		MergeID:          "mrun-1",
		SourcePath:       "/tmp/test.sqlite",
		Status:           "completed",
		StartedAt:        now.Add(-time.Second),
		CompletedAt:      &now,
		VolumesMerged:    1,
		EntriesInserted:  1,
		EntriesUpdated:   1,
		EntriesIdentical: 0,
		ConflictsCount:   1,
	}
	if err := tx.RecordMergeRun(ctx, mRun); err != nil {
		t.Fatalf("failed recording merge run: %v", err)
	}

	mConflict := &model.MergeConflict{
		ConflictKind: model.ConflictKindSameLocationDifferentContent,
		VolumeID:     "vol-merge-1",
		RelativePath: "file.txt",
		LocalDetail:  "size=100",
		RemoteDetail: "size=200",
		Resolution:   "updated_to_remote_newer",
	}
	if err := tx.RecordMergeConflict(ctx, "mrun-1", mConflict); err != nil {
		t.Fatalf("failed recording merge conflict: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed committing merge tx: %v", err)
	}

	// Verify data
	v, err := db.GetVolume(ctx, "vol-merge-1")
	if err != nil {
		t.Fatalf("failed getting volume: %v", err)
	}
	if v.Label != "DataUpdated" {
		t.Errorf("expected label DataUpdated, got %q", v.Label)
	}

	runs, err := db.GetAllScanRuns(ctx)
	if err != nil {
		t.Fatalf("failed getting scan runs: %v", err)
	}
	if len(runs) != 1 || runs["scan-1"] == nil {
		t.Errorf("expected 1 scan run with key scan-1, got %v", runs)
	}

	roots, err := db.GetScanRoots(ctx, "vol-merge-1")
	if err != nil {
		t.Fatalf("failed getting scan roots: %v", err)
	}
	if len(roots) != 1 || roots[0].RootPath != "/" {
		t.Errorf("expected 1 root with path /, got %v", roots)
	}

	fps, err := db.GetAllFingerprints(ctx)
	if err != nil {
		t.Fatalf("failed getting fingerprints: %v", err)
	}
	if !fps["quickfp1"] {
		t.Errorf("expected quickfp1 in fingerprints set")
	}
}

func TestMergeTx_Rollback(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	tx, err := db.BeginMergeTx(ctx)
	if err != nil {
		t.Fatalf("failed starting merge tx: %v", err)
	}

	vol := &model.Volume{
		VolumeID:    "vol-rollback",
		FirstSeenAt: time.Now().UTC(),
	}
	if err := tx.InsertVolume(ctx, vol); err != nil {
		t.Fatalf("failed inserting volume: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("failed rolling back tx: %v", err)
	}

	_, err = db.GetVolume(ctx, "vol-rollback")
	if err != ErrVolumeNotFound {
		t.Fatalf("expected ErrVolumeNotFound after rollback, got %v", err)
	}
}
