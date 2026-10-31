package index

import (
	"context"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestQueryComparison_BasicAndTypes(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volA", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "volB", FirstSeenAt: now}, false)

	// volA entries
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volA",
			RelativePath:     "/file1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-shared-1"),
		},
		{
			VolumeID:         "volA",
			RelativePath:     "/file2.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        200,
			QuickFingerprint: []byte("fp-unique-a"),
		},
		{
			VolumeID:             "volA",
			RelativePath:         "/dir1",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-dir-shared"),
		},
		{
			VolumeID:             "volA",
			RelativePath:         "/dir2",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-dir-unique-a"),
		},
	})

	// volB entries
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volB",
			RelativePath:     "/backup/file1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-shared-1"),
		},
		{
			VolumeID:             "volB",
			RelativePath:         "/archive/dir1",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-dir-shared"),
		},
	})

	// 1. Absent mode (all types)
	resAbsent, err := db.QueryComparison(ctx, CompareQueryParams{
		PresentVolumes: []string{"volA"},
		AbsentVolumes:  []string{"volB"},
		Mode:           "absent",
	})
	if err != nil {
		t.Fatalf("absent query failed: %v", err)
	}

	if resAbsent.TotalSourceEntries != 4 {
		t.Errorf("expected 4 total source entries, got %d", resAbsent.TotalSourceEntries)
	}
	if resAbsent.MatchingEntries != 2 {
		t.Errorf("expected 2 matching entries, got %d", resAbsent.MatchingEntries)
	}
	if resAbsent.AbsentEntries != 2 {
		t.Errorf("expected 2 absent entries, got %d", resAbsent.AbsentEntries)
	}
	if len(resAbsent.Items) != 2 {
		t.Fatalf("expected 2 absent items, got %d", len(resAbsent.Items))
	}
	paths := map[string]bool{
		resAbsent.Items[0].RelativePath: true,
		resAbsent.Items[1].RelativePath: true,
	}
	if !paths["/file2.txt"] || !paths["/dir2"] {
		t.Errorf("unexpected absent items: %v", resAbsent.Items)
	}

	// 2. Common mode (all types)
	resCommon, err := db.QueryComparison(ctx, CompareQueryParams{
		PresentVolumes: []string{"volA"},
		AbsentVolumes:  []string{"volB"},
		Mode:           "common",
		ShowMatches:    true,
	})
	if err != nil {
		t.Fatalf("common query failed: %v", err)
	}
	if len(resCommon.Items) != 2 {
		t.Fatalf("expected 2 common items, got %d", len(resCommon.Items))
	}
	matchesByPath := make(map[string][]model.LocationRef)
	for _, it := range resCommon.Items {
		matchesByPath[it.RelativePath] = it.MatchingLocations
	}
	if len(matchesByPath["/file1.txt"]) != 1 || matchesByPath["/file1.txt"][0].RelativePath != "/backup/file1.txt" {
		t.Errorf("expected match at /backup/file1.txt, got %v", matchesByPath["/file1.txt"])
	}
	if len(matchesByPath["/dir1"]) != 1 || matchesByPath["/dir1"][0].RelativePath != "/archive/dir1" {
		t.Errorf("expected match at /archive/dir1, got %v", matchesByPath["/dir1"])
	}

	// 3. Filter by type = file
	resFilesOnly, err := db.QueryComparison(ctx, CompareQueryParams{
		PresentVolumes: []string{"volA"},
		AbsentVolumes:  []string{"volB"},
		Mode:           "absent",
		EntryType:      model.EntryTypeRegular,
	})
	if err != nil {
		t.Fatalf("files only query failed: %v", err)
	}
	if resFilesOnly.TotalSourceEntries != 2 {
		t.Errorf("expected 2 source files, got %d", resFilesOnly.TotalSourceEntries)
	}
	if resFilesOnly.AbsentEntries != 1 {
		t.Errorf("expected 1 absent file, got %d", resFilesOnly.AbsentEntries)
	}
	if len(resFilesOnly.Items) != 1 || resFilesOnly.Items[0].RelativePath != "/file2.txt" {
		t.Errorf("expected /file2.txt, got %v", resFilesOnly.Items)
	}
}

func TestQueryComparison_FullHashVerification(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol2", FirstSeenAt: now}, false)

	// Both have matching quick and full fingerprints
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol1",
			RelativePath:     "/verified.bin",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1000,
			QuickFingerprint: []byte("quick-match"),
			FullFingerprint:  []byte("full-match"),
		},
	})
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol2",
			RelativePath:     "/verified.bin",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1000,
			QuickFingerprint: []byte("quick-match"),
			FullFingerprint:  []byte("full-match"),
		},
	})

	res, err := db.QueryComparison(ctx, CompareQueryParams{
		PresentVolumes: []string{"vol1"},
		AbsentVolumes:  []string{"vol2"},
		Mode:           "common",
		ShowMatches:    true,
		VerifyFullHash: true,
	})
	if err != nil {
		t.Fatalf("verified query failed: %v", err)
	}

	if len(res.Items) != 1 {
		t.Fatalf("expected 1 common item, got %d", len(res.Items))
	}
	if res.Items[0].Confidence != model.ConfidenceFullHash {
		t.Errorf("expected ConfidenceFullHash, got %s", res.Items[0].Confidence)
	}
}
