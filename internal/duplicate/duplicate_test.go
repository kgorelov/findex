package duplicate

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
	dbPath := filepath.Join(t.TempDir(), "test_duplicate.sqlite")
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	return db
}

func TestFindDuplicates_WholeDirectorySubsumption(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol1", Label: "Disk1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol2", Label: "Disk2", FirstSeenAt: now}, false)

	// Two identical directories: /photos/album on vol1 and /backup/album on vol2
	// Each directory contains 2 identical files.
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:             "vol1",
			RelativePath:         "/photos/album",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-album-123456789012345678901234"),
		},
		{
			VolumeID:         "vol1",
			RelativePath:     "/photos/album/pic1.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1000,
			QuickFingerprint: []byte("fp-pic1-content-quick"),
		},
		{
			VolumeID:         "vol1",
			RelativePath:     "/photos/album/pic2.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        2000,
			QuickFingerprint: []byte("fp-pic2-content-quick"),
		},
		{
			VolumeID:             "vol2",
			RelativePath:         "/backup/album",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-album-123456789012345678901234"),
		},
		{
			VolumeID:         "vol2",
			RelativePath:     "/backup/album/pic1.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1000,
			QuickFingerprint: []byte("fp-pic1-content-quick"),
		},
		{
			VolumeID:         "vol2",
			RelativePath:     "/backup/album/pic2.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        2000,
			QuickFingerprint: []byte("fp-pic2-content-quick"),
		},
	})

	// Standalone duplicate file (not inside duplicate directory)
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol1",
			RelativePath:     "/standalone1.pdf",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        5000,
			QuickFingerprint: []byte("fp-pdf-standalone"),
		},
		{
			VolumeID:         "vol2",
			RelativePath:     "/docs/standalone2.pdf",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        5000,
			QuickFingerprint: []byte("fp-pdf-standalone"),
		},
	})

	summary, err := FindDuplicates(ctx, db, nil, Options{})
	if err != nil {
		t.Fatalf("FindDuplicates failed: %v", err)
	}

	// Should have exactly 2 groups:
	// 1. The duplicate directory group (/photos/album and /backup/album)
	// 2. The standalone file group (/standalone1.pdf and /docs/standalone2.pdf)
	// The files pic1.jpg and pic2.jpg MUST be pruned because their whole directory is a duplicate!
	if summary.TotalGroups != 2 {
		t.Fatalf("expected 2 duplicate groups, got %d", summary.TotalGroups)
	}

	var foundDirGroup *model.DuplicateGroup
	var foundFileGroup *model.DuplicateGroup
	for _, g := range summary.Groups {
		if g.EntryType == model.EntryTypeDirectory {
			foundDirGroup = g
		} else if g.EntryType == model.EntryTypeRegular {
			foundFileGroup = g
		}
	}

	if foundDirGroup == nil {
		t.Fatalf("expected duplicate directory group, got nil")
	}
	if len(foundDirGroup.Entries) != 2 {
		t.Errorf("expected 2 entries in dir group, got %d", len(foundDirGroup.Entries))
	}
	if foundDirGroup.SizeBytes != 3000 {
		t.Errorf("expected directory content size 3000 bytes, got %d", foundDirGroup.SizeBytes)
	}

	if foundFileGroup == nil {
		t.Fatalf("expected duplicate file group, got nil")
	}
	if len(foundFileGroup.Entries) != 2 {
		t.Errorf("expected 2 entries in standalone file group, got %d", len(foundFileGroup.Entries))
	}
}

func TestFindDuplicates_VolumeFilter(t *testing.T) {
	ctx := context.Background()
	db := createTestDB(t)
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volA", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volB", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volC", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volA",
			RelativePath:     "/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-same"),
		},
		{
			VolumeID:         "volB",
			RelativePath:     "/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-same"),
		},
		{
			VolumeID:         "volC",
			RelativePath:     "/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-same"),
		},
	})

	// When searching only across volA and volB:
	summary, err := FindDuplicates(ctx, db, []string{"volA,volB"}, Options{})
	if err != nil {
		t.Fatalf("FindDuplicates failed: %v", err)
	}

	if summary.TotalGroups != 1 {
		t.Fatalf("expected 1 group, got %d", summary.TotalGroups)
	}
	if len(summary.Groups[0].Entries) != 2 {
		t.Errorf("expected 2 entries (volA and volB), got %d", len(summary.Groups[0].Entries))
	}
}
