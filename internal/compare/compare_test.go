package compare

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func createTestDB(t *testing.T) *index.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_compare.sqlite")
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed creating test db: %v", err)
	}
	return db
}

func TestResolveVolumes(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{
		VolumeID:   "vol-local-1",
		Label:      "LocalPhotos",
		SourceHost: "desktop",
		FSUUID:     "uuid-111",
		FirstSeenAt: now,
	}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{
		VolumeID:   "vol-local-2",
		Label:      "LocalMusic",
		SourceHost: "desktop",
		FSUUID:     "uuid-222",
		FirstSeenAt: now,
	}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{
		VolumeID:   "vol-remote-1",
		Label:      "BackupDrive",
		SourceHost: "nas",
		FSUUID:     "uuid-333",
		FirstSeenAt: now,
	}, false)

	// 1. Direct IDs
	ids, err := ResolveVolumes(ctx, db, []string{"vol-local-1,vol-remote-1"})
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}
	if len(ids) != 2 || ids[0] != "vol-local-1" || ids[1] != "vol-remote-1" {
		t.Errorf("unexpected resolved IDs: %v", ids)
	}

	// 2. Filter by host:desktop
	hostIDs, err := ResolveVolumes(ctx, db, []string{"host:desktop"})
	if err != nil {
		t.Fatalf("resolve host failed: %v", err)
	}
	if len(hostIDs) != 2 {
		t.Errorf("expected 2 volumes on desktop, got %d", len(hostIDs))
	}

	// 3. Filter by label:Backup
	labelIDs, err := ResolveVolumes(ctx, db, []string{"label:Backup"})
	if err != nil {
		t.Fatalf("resolve label failed: %v", err)
	}
	if len(labelIDs) != 1 || labelIDs[0] != "vol-remote-1" {
		t.Errorf("expected vol-remote-1 for label Backup, got %v", labelIDs)
	}

	// 4. Filter by uuid:uuid-222
	uuidIDs, err := ResolveVolumes(ctx, db, []string{"uuid:uuid-222"})
	if err != nil {
		t.Fatalf("resolve uuid failed: %v", err)
	}
	if len(uuidIDs) != 1 || uuidIDs[0] != "vol-local-2" {
		t.Errorf("expected vol-local-2, got %v", uuidIDs)
	}

	// 5. Unknown volume error
	_, err = ResolveVolumes(ctx, db, []string{"nonexistent-vol"})
	if err == nil {
		t.Fatalf("expected error for nonexistent volume, got nil")
	}
}

func TestCompare_PresentInA_AbsentFromB(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volA", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volB", FirstSeenAt: now}, false)

	// Volume A has 3 files
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volA",
			RelativePath:     "/both/shared.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-shared"),
		},
		{
			VolumeID:         "volA",
			RelativePath:     "/a_only/doc1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        250,
			QuickFingerprint: []byte("fp-doc1"),
		},
		{
			VolumeID:         "volA",
			RelativePath:     "/a_only/doc2.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        350,
			QuickFingerprint: []byte("fp-doc2"),
		},
	})

	// Volume B has shared file only
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volB",
			RelativePath:     "/remote/shared.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-shared"),
		},
	})

	summary, err := Compare(ctx, db, Options{
		Present: []string{"volA"},
		Absent:  []string{"volB"},
	})
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if summary.TotalSourceEntries != 3 {
		t.Errorf("expected 3 source entries, got %d", summary.TotalSourceEntries)
	}
	if summary.MatchingEntries != 1 {
		t.Errorf("expected 1 matching entry, got %d", summary.MatchingEntries)
	}
	if summary.AbsentEntries != 2 {
		t.Errorf("expected 2 absent entries, got %d", summary.AbsentEntries)
	}
	if summary.AbsentBytes != 600 {
		t.Errorf("expected 600 absent bytes, got %d", summary.AbsentBytes)
	}
	if len(summary.Items) != 2 {
		t.Fatalf("expected 2 absent items, got %d", len(summary.Items))
	}
}

func TestCompare_SetSemantics_Duplicates(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol2", FirstSeenAt: now}, false)

	// In Set A (vol1): 2 duplicate files with identical content
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol1",
			RelativePath:     "/folder1/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        500,
			QuickFingerprint: []byte("fp-dup"),
		},
		{
			VolumeID:         "vol1",
			RelativePath:     "/folder2/file_copy.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        500,
			QuickFingerprint: []byte("fp-dup"),
		},
	})

	// In Set B (vol2): 1 matching file exists
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol2",
			RelativePath:     "/somewhere/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        500,
			QuickFingerprint: []byte("fp-dup"),
		},
	})

	// Under set semantics: content is considered present because at least 1 copy exists in B
	summary, err := Compare(ctx, db, Options{
		Present: []string{"vol1"},
		Absent:  []string{"vol2"},
	})
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}

	if summary.MatchingEntries != 2 {
		t.Errorf("expected both copies in source to match under set semantics, got %d", summary.MatchingEntries)
	}
	if summary.AbsentEntries != 0 {
		t.Errorf("expected 0 absent entries, got %d", summary.AbsentEntries)
	}
}

func TestCompare_DirectoryFingerprintComparison(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vSrc", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vDst", FirstSeenAt: now}, false)

	// Source has directory /my_album with fingerprint fp-album
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:             "vSrc",
			RelativePath:         "/my_album",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-album"),
		},
		{
			VolumeID:             "vSrc",
			RelativePath:         "/other_album",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-other"),
		},
	})

	// Destination has directory renamed to /vacation_photos with same fingerprint fp-album
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:             "vDst",
			RelativePath:         "/vacation_photos",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-album"),
		},
	})

	summary, err := Compare(ctx, db, Options{
		Present:   []string{"vSrc"},
		Absent:    []string{"vDst"},
		EntryType: model.EntryTypeDirectory,
	})
	if err != nil {
		t.Fatalf("Compare directories failed: %v", err)
	}

	if summary.MatchingEntries != 1 {
		t.Errorf("expected 1 matching directory, got %d", summary.MatchingEntries)
	}
	if summary.AbsentEntries != 1 {
		t.Errorf("expected 1 absent directory, got %d", summary.AbsentEntries)
	}
	if len(summary.Items) != 1 || summary.Items[0].RelativePath != "/other_album" {
		t.Errorf("expected /other_album absent, got %v", summary.Items)
	}
}

func TestCompare_CommonMode(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "v1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "v2", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "v1",
			RelativePath:     "/common.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        10,
			QuickFingerprint: []byte("fp-common"),
		},
		{
			VolumeID:         "v1",
			RelativePath:     "/unique.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        20,
			QuickFingerprint: []byte("fp-v1-only"),
		},
	})

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "v2",
			RelativePath:     "/path/to/common.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        10,
			QuickFingerprint: []byte("fp-common"),
		},
	})

	summary, err := Compare(ctx, db, Options{
		Present:     []string{"v1"},
		Absent:      []string{"v2"},
		Mode:        "common",
		ShowMatches: true,
	})
	if err != nil {
		t.Fatalf("Common compare failed: %v", err)
	}

	if len(summary.Items) != 1 {
		t.Fatalf("expected 1 common item, got %d", len(summary.Items))
	}
	if summary.Items[0].RelativePath != "/common.txt" {
		t.Errorf("expected /common.txt, got %s", summary.Items[0].RelativePath)
	}
	if len(summary.Items[0].MatchingLocations) != 1 || summary.Items[0].MatchingLocations[0].VolumeID != "v2" {
		t.Errorf("expected matching location in v2, got %v", summary.Items[0].MatchingLocations)
	}
}

func TestUnique_SingleVolume(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vMain", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vOther1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vOther2", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vMain",
			RelativePath:     "/shared_with_1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-1"),
		},
		{
			VolumeID:         "vMain",
			RelativePath:     "/shared_with_2.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        200,
			QuickFingerprint: []byte("fp-2"),
		},
		{
			VolumeID:         "vMain",
			RelativePath:     "/truly_unique.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        300,
			QuickFingerprint: []byte("fp-unique"),
		},
	})

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vOther1",
			RelativePath:     "/copy.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-1"),
		},
	})

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vOther2",
			RelativePath:     "/copy.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        200,
			QuickFingerprint: []byte("fp-2"),
		},
	})

	// Find content unique to vMain among all other volumes
	summary, err := Unique(ctx, db, "vMain", nil, Options{})
	if err != nil {
		t.Fatalf("Unique failed: %v", err)
	}

	if summary.AbsentEntries != 1 {
		t.Errorf("expected 1 unique entry, got %d", summary.AbsentEntries)
	}
	if len(summary.Items) != 1 || summary.Items[0].RelativePath != "/truly_unique.txt" {
		t.Errorf("expected /truly_unique.txt, got %v", summary.Items)
	}
}

func TestUnique_MultiVolumeSet(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "set1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "set2", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "other", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "set1",
			RelativePath:     "/f1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        10,
			QuickFingerprint: []byte("fp-set-1"),
		},
		{
			VolumeID:         "set2",
			RelativePath:     "/f2.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        20,
			QuickFingerprint: []byte("fp-set-2"),
		},
		{
			VolumeID:         "other",
			RelativePath:     "/shared.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        20,
			QuickFingerprint: []byte("fp-set-2"),
		},
	})

	// Content unique to set1,set2 (f1 is unique, f2 exists in other)
	summary, err := Unique(ctx, db, "set1,set2", nil, Options{})
	if err != nil {
		t.Fatalf("multi-volume unique failed: %v", err)
	}

	if summary.AbsentEntries != 1 {
		t.Fatalf("expected 1 unique entry for set1,set2, got %d", summary.AbsentEntries)
	}
	if len(summary.Items) != 1 || summary.Items[0].RelativePath != "/f1.txt" {
		t.Errorf("expected /f1.txt to be unique to set1,set2, got %v", summary.Items)
	}
}

func TestCompare_ValidationErrors(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volOverlap", FirstSeenAt: time.Now().UTC()}, false)

	// Present is empty
	_, err := Compare(ctx, db, Options{
		Present: nil,
	})
	if err == nil {
		t.Fatalf("expected error when present is empty, got nil")
	}

	// Overlapping volume
	_, err = Compare(ctx, db, Options{
		Present: []string{"volOverlap"},
		Absent:  []string{"volOverlap"},
	})
	if err == nil {
		t.Fatalf("expected error on overlapping volume, got nil")
	}
}
