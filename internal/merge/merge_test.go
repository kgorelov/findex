package merge

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func createTestDB(t *testing.T, filename string) (*index.DB, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), filename)
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed creating test db %q: %v", filename, err)
	}
	return db, dbPath
}

func TestMerge_DisjointVolumes(t *testing.T) {
	ctx := context.Background()

	// Machine A index
	dbA, _ := createTestDB(t, "machine_a.sqlite")
	defer dbA.Close()

	volA := &model.Volume{
		VolumeID:       "vol-a",
		Label:          "HostA-Disk",
		FSUUID:         "uuid-aaa",
		FSType:         "ext4",
		SourceHost:     "host-a",
		LastMountPoint: "/mnt/storage",
		FirstSeenAt:    time.Now().UTC().Add(-time.Hour),
	}
	if err := dbA.RegisterVolume(ctx, volA, false); err != nil {
		t.Fatalf("failed registering volA: %v", err)
	}

	entriesA := []*model.Entry{
		{
			VolumeID:         "vol-a",
			RelativePath:     "docs/readme.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        120,
			QuickFingerprint: []byte("fp-readme-a"),
			LastSeenScanID:   "scan-a1",
		},
	}
	if err := dbA.UpsertEntries(ctx, entriesA); err != nil {
		t.Fatalf("failed inserting entries into dbA: %v", err)
	}

	// Machine B index
	dbB, pathB := createTestDB(t, "machine_b.sqlite")
	defer dbB.Close()

	volB := &model.Volume{
		VolumeID:       "vol-b",
		Label:          "HostB-Disk",
		FSUUID:         "uuid-bbb",
		FSType:         "btrfs",
		SourceHost:     "host-b",
		LastMountPoint: "/data",
		FirstSeenAt:    time.Now().UTC().Add(-30 * time.Minute),
	}
	if err := dbB.RegisterVolume(ctx, volB, false); err != nil {
		t.Fatalf("failed registering volB: %v", err)
	}

	entriesB := []*model.Entry{
		{
			VolumeID:         "vol-b",
			RelativePath:     "photos/sunset.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        45000,
			QuickFingerprint: []byte("fp-sunset-b"),
			LastSeenScanID:   "scan-b1",
		},
	}
	if err := dbB.UpsertEntries(ctx, entriesB); err != nil {
		t.Fatalf("failed inserting entries into dbB: %v", err)
	}
	dbB.Close()

	// Merge Machine B into Machine A
	res, err := Merge(ctx, dbA, pathB, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	if res.VolumesMerged != 1 {
		t.Errorf("expected 1 volume merged, got %d", res.VolumesMerged)
	}
	if res.EntriesInserted != 1 {
		t.Errorf("expected 1 entry inserted, got %d", res.EntriesInserted)
	}
	if res.ConflictsCount != 0 {
		t.Errorf("expected 0 conflicts, got %d", res.ConflictsCount)
	}

	// Verify dbA now has both volumes
	volumes, err := dbA.ListVolumes(ctx, model.VolumeFilter{})
	if err != nil {
		t.Fatalf("failed listing volumes: %v", err)
	}
	if len(volumes) != 2 {
		t.Fatalf("expected 2 volumes in dbA, got %d", len(volumes))
	}

	// Verify dbA has entries from both volumes
	vBEntries, err := dbA.ListEntries(ctx, "vol-b", "")
	if err != nil {
		t.Fatalf("failed listing vol-b entries in dbA: %v", err)
	}
	if len(vBEntries) != 1 || vBEntries[0].RelativePath != "/photos/sunset.jpg" {
		t.Errorf("expected /photos/sunset.jpg in vol-b entries, got %v", vBEntries[0].RelativePath)
	}
}

func TestMerge_SameVolumePartialScans(t *testing.T) {
	ctx := context.Background()

	// Physical volume "vol-shared" moved between machines:
	// Scanned /docs on machine 1, then scanned /music on machine 2
	db1, _ := createTestDB(t, "machine1.sqlite")
	defer db1.Close()

	volShared1 := &model.Volume{
		VolumeID:       "vol-shared",
		FSUUID:         "uuid-shared-999",
		FSType:         "ext4",
		SourceHost:     "laptop",
		LastMountPoint: "/media/usb",
		FirstSeenAt:    time.Now().UTC().Add(-2 * time.Hour),
	}
	_ = db1.RegisterVolume(ctx, volShared1, false)
	_ = db1.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-shared",
			RelativePath:     "docs/notes.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        50,
			QuickFingerprint: []byte("fp-notes"),
			LastSeenScanID:   "scan-m1",
		},
	})

	db2, path2 := createTestDB(t, "machine2.sqlite")
	defer db2.Close()

	volShared2 := &model.Volume{
		VolumeID:       "vol-shared",
		FSUUID:         "uuid-shared-999",
		FSType:         "ext4",
		SourceHost:     "desktop",
		LastMountPoint: "/mnt/usb-drive",
		FirstSeenAt:    time.Now().UTC().Add(-time.Hour),
	}
	_ = db2.RegisterVolume(ctx, volShared2, false)
	_ = db2.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-shared",
			RelativePath:     "music/song.mp3",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        3000000,
			QuickFingerprint: []byte("fp-song"),
			LastSeenScanID:   "scan-m2",
		},
	})
	db2.Close()

	res, err := Merge(ctx, db1, path2, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	if res.EntriesInserted != 1 {
		t.Errorf("expected 1 entry inserted, got %d", res.EntriesInserted)
	}
	if res.ConflictsCount != 0 {
		t.Errorf("expected 0 conflicts, got %d", res.ConflictsCount)
	}

	entries, err := db1.ListEntries(ctx, "vol-shared", "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 combined entries on vol-shared, got %d", len(entries))
	}
}

func TestMerge_Idempotency(t *testing.T) {
	ctx := context.Background()

	dbDest, _ := createTestDB(t, "dest.sqlite")
	defer dbDest.Close()

	dbSrc, pathSrc := createTestDB(t, "src.sqlite")
	defer dbSrc.Close()

	vol := &model.Volume{
		VolumeID:    "vol-idemp",
		FSUUID:      "uuid-idemp",
		FSType:      "ext4",
		FirstSeenAt: time.Now().UTC(),
	}
	_ = dbSrc.RegisterVolume(ctx, vol, false)
	_ = dbSrc.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-idemp",
			RelativePath:     "data/file.bin",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1024,
			QuickFingerprint: []byte("fp-bin"),
		},
	})
	dbSrc.Close()

	// First merge
	res1, err := Merge(ctx, dbDest, pathSrc, Options{})
	if err != nil {
		t.Fatalf("first merge failed: %v", err)
	}
	if res1.EntriesInserted != 1 {
		t.Errorf("expected 1 entry inserted in first merge, got %d", res1.EntriesInserted)
	}

	// Second merge (repeated import)
	res2, err := Merge(ctx, dbDest, pathSrc, Options{})
	if err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	if res2.EntriesInserted != 0 {
		t.Errorf("expected 0 entries inserted in second merge, got %d", res2.EntriesInserted)
	}
	if res2.EntriesUpdated != 0 {
		t.Errorf("expected 0 entries updated in second merge, got %d", res2.EntriesUpdated)
	}
	if res2.EntriesIdentical != 1 {
		t.Errorf("expected 1 entry identical in second merge, got %d", res2.EntriesIdentical)
	}
	if res2.ConflictsCount != 0 {
		t.Errorf("expected 0 conflicts in repeated merge, got %d", res2.ConflictsCount)
	}

	// Verify database entries count is still 1
	entries, err := dbDest.ListEntries(ctx, "vol-idemp", "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 entry, got %d", len(entries))
	}
}

func TestMerge_VolumeConflicts(t *testing.T) {
	ctx := context.Background()

	// 1. FSUUID mismatch on same volume_id
	db1, _ := createTestDB(t, "dest_conf1.sqlite")
	defer db1.Close()
	_ = db1.RegisterVolume(ctx, &model.Volume{
		VolumeID:    "vol-same-id",
		FSUUID:      "uuid-local",
		FSType:      "ext4",
		FirstSeenAt: time.Now().UTC(),
	}, false)

	db2, path2 := createTestDB(t, "src_conf1.sqlite")
	_ = db2.RegisterVolume(ctx, &model.Volume{
		VolumeID:    "vol-same-id",
		FSUUID:      "uuid-remote-diff",
		FSType:      "ext4",
		FirstSeenAt: time.Now().UTC(),
	}, false)
	db2.Close()

	res, err := Merge(ctx, db1, path2, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if res.ConflictsCount != 1 {
		t.Fatalf("expected 1 volume conflict, got %d", res.ConflictsCount)
	}
	if res.Conflicts[0].ConflictKind != model.ConflictKindVolumeMetadata {
		t.Errorf("expected volume metadata conflict, got %s", res.Conflicts[0].ConflictKind)
	}
	if res.Conflicts[0].Resolution != "skipped_due_to_volume_conflict" {
		t.Errorf("expected skipped resolution, got %s", res.Conflicts[0].Resolution)
	}

	// With Force: true
	resForced, err := Merge(ctx, db1, path2, Options{Force: true})
	if err != nil {
		t.Fatalf("forced merge failed: %v", err)
	}
	if resForced.Conflicts[0].Resolution != "forced" {
		t.Errorf("expected forced resolution, got %s", resForced.Conflicts[0].Resolution)
	}

	// 2. Duplicate FSUUID on different volume_id (cloned disk)
	db3, _ := createTestDB(t, "dest_conf2.sqlite")
	defer db3.Close()
	_ = db3.RegisterVolume(ctx, &model.Volume{
		VolumeID:    "vol-first",
		FSUUID:      "uuid-clone",
		FirstSeenAt: time.Now().UTC(),
	}, false)

	db4, path4 := createTestDB(t, "src_conf2.sqlite")
	_ = db4.RegisterVolume(ctx, &model.Volume{
		VolumeID:    "vol-second",
		FSUUID:      "uuid-clone",
		FirstSeenAt: time.Now().UTC(),
	}, false)
	db4.Close()

	resClone, err := Merge(ctx, db3, path4, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if resClone.ConflictsCount != 1 {
		t.Fatalf("expected 1 duplicate fs_uuid conflict, got %d", resClone.ConflictsCount)
	}
	if resClone.Conflicts[0].ConflictKind != model.ConflictKindDuplicateFSUUID {
		t.Errorf("expected duplicate_fs_uuid kind, got %s", resClone.Conflicts[0].ConflictKind)
	}
}

func TestMerge_EntryConflictPolicies(t *testing.T) {
	ctx := context.Background()

	tNow := time.Now().UTC()
	tOld := tNow.Add(-time.Hour)
	tNew := tNow.Add(time.Hour)

	setup := func(t *testing.T) (*index.DB, *index.DB, string) {
		dest, _ := createTestDB(t, "dest.sqlite")
		src, srcPath := createTestDB(t, "src.sqlite")

		vol := &model.Volume{VolumeID: "v1", FirstSeenAt: tNow}
		_ = dest.RegisterVolume(ctx, vol, false)
		_ = src.RegisterVolume(ctx, vol, false)

		_ = dest.CreateScanRun(ctx, &model.ScanRun{
			ScanID:      "scan-dest-old",
			VolumeID:    "v1",
			RootPath:    "/",
			Status:      model.ScanStatusCompleted,
			StartedAt:   tOld,
			CompletedAt: &tOld,
		})
		_ = src.CreateScanRun(ctx, &model.ScanRun{
			ScanID:      "scan-src-new",
			VolumeID:    "v1",
			RootPath:    "/",
			Status:      model.ScanStatusCompleted,
			StartedAt:   tNew,
			CompletedAt: &tNew,
		})

		_ = dest.UpsertEntries(ctx, []*model.Entry{
			{
				VolumeID:         "v1",
				RelativePath:     "/file.txt",
				EntryType:        model.EntryTypeRegular,
				SizeBytes:        100,
				MTimeNs:          tOld.UnixNano(),
				QuickFingerprint: []byte("fp-old"),
				LastSeenScanID:   "scan-dest-old",
			},
		})
		_ = src.UpsertEntries(ctx, []*model.Entry{
			{
				VolumeID:         "v1",
				RelativePath:     "/file.txt",
				EntryType:        model.EntryTypeRegular,
				SizeBytes:        200,
				MTimeNs:          tNew.UnixNano(),
				QuickFingerprint: []byte("fp-new"),
				LastSeenScanID:   "scan-src-new",
			},
		})
		src.Close()

		return dest, src, srcPath
	}

	// 1. PolicyPreferNewer: remote is newer -> updates to remote
	t.Run("PolicyPreferNewer_RemoteIsNewer", func(t *testing.T) {
		dest, _, srcPath := setup(t)
		defer dest.Close()

		res, err := Merge(ctx, dest, srcPath, Options{Policy: PolicyPreferNewer})
		if err != nil {
			t.Fatalf("merge failed: %v", err)
		}
		if res.EntriesUpdated != 1 {
			t.Errorf("expected 1 entry updated, got %d", res.EntriesUpdated)
		}
		if res.Conflicts[0].Resolution != "updated_to_remote_newer" {
			t.Errorf("expected updated_to_remote_newer resolution, got %s", res.Conflicts[0].Resolution)
		}

		e, _ := dest.GetEntry(ctx, "v1", "file.txt")
		if e.SizeBytes != 200 {
			t.Errorf("expected size 200 after update, got %d", e.SizeBytes)
		}
	})

	// 2. PolicyKeepLocal: keeps local despite remote being newer
	t.Run("PolicyKeepLocal", func(t *testing.T) {
		dest, _, srcPath := setup(t)
		defer dest.Close()

		res, err := Merge(ctx, dest, srcPath, Options{Policy: PolicyKeepLocal})
		if err != nil {
			t.Fatalf("merge failed: %v", err)
		}
		if res.EntriesUpdated != 0 {
			t.Errorf("expected 0 entries updated, got %d", res.EntriesUpdated)
		}
		if res.Conflicts[0].Resolution != "kept_local" {
			t.Errorf("expected kept_local resolution, got %s", res.Conflicts[0].Resolution)
		}

		e, _ := dest.GetEntry(ctx, "v1", "file.txt")
		if e.SizeBytes != 100 {
			t.Errorf("expected size 100 kept, got %d", e.SizeBytes)
		}
	})

	// 3. PolicyFailOnConflict: aborts on conflict
	t.Run("PolicyFailOnConflict", func(t *testing.T) {
		dest, _, srcPath := setup(t)
		defer dest.Close()

		_, err := Merge(ctx, dest, srcPath, Options{Policy: PolicyFailOnConflict})
		if err == nil {
			t.Fatalf("expected error on conflict under fail policy, got nil")
		}

		// Ensure transaction rolled back
		e, err := dest.GetEntry(ctx, "v1", "file.txt")
		if err != nil {
			t.Fatalf("failed getting entry: %v", err)
		}
		if e.SizeBytes != 100 {
			t.Errorf("expected size 100 unchanged after rollback, got %d", e.SizeBytes)
		}
	})
}

func TestMerge_VerificationPreservation(t *testing.T) {
	ctx := context.Background()

	dest, _ := createTestDB(t, "dest_verif.sqlite")
	defer dest.Close()

	src, srcPath := createTestDB(t, "src_verif.sqlite")
	defer src.Close()

	vol := &model.Volume{VolumeID: "v-verif", FirstSeenAt: time.Now().UTC()}
	_ = dest.RegisterVolume(ctx, vol, false)
	_ = src.RegisterVolume(ctx, vol, false)

	// Local entry has quick fingerprint only
	_ = dest.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "v-verif",
			RelativePath:     "/data.tar",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        5000,
			QuickFingerprint: []byte("quick-tar"),
		},
	})

	// Remote entry has quick fingerprint + full BLAKE3 fingerprint
	_ = src.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "v-verif",
			RelativePath:     "/data.tar",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        5000,
			QuickFingerprint: []byte("quick-tar"),
			FullFingerprint:  []byte("full-blake3-tar"),
		},
	})
	src.Close()

	res, err := Merge(ctx, dest, srcPath, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	if res.EntriesIdentical != 1 {
		t.Errorf("expected 1 entry identical, got %d", res.EntriesIdentical)
	}

	// Verify local entry now possesses full fingerprint
	e, err := dest.GetEntry(ctx, "v-verif", "data.tar")
	if err != nil {
		t.Fatalf("failed getting entry: %v", err)
	}
	if string(e.FullFingerprint) != "full-blake3-tar" {
		t.Errorf("expected enriched full fingerprint full-blake3-tar, got %q", string(e.FullFingerprint))
	}
}

func TestMerge_SameContentDiffLocation(t *testing.T) {
	ctx := context.Background()

	dest, _ := createTestDB(t, "dest_diff_loc.sqlite")
	defer dest.Close()

	src, srcPath := createTestDB(t, "src_diff_loc.sqlite")
	defer src.Close()

	vol1 := &model.Volume{VolumeID: "vol-1", FirstSeenAt: time.Now().UTC()}
	vol2 := &model.Volume{VolumeID: "vol-2", FirstSeenAt: time.Now().UTC()}
	_ = dest.RegisterVolume(ctx, vol1, false)
	_ = src.RegisterVolume(ctx, vol2, false)

	// In dest, file is at vol-1:/backup/doc.pdf
	_ = dest.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-1",
			RelativePath:     "backup/doc.pdf",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1234,
			QuickFingerprint: []byte("identical-fp-content"),
		},
	})

	// In src, file is at vol-2:/work/doc.pdf with same fingerprint
	_ = src.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-2",
			RelativePath:     "work/doc.pdf",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1234,
			QuickFingerprint: []byte("identical-fp-content"),
		},
	})
	src.Close()

	res, err := Merge(ctx, dest, srcPath, Options{})
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	if res.SameContentDiffLocation != 1 {
		t.Errorf("expected SameContentDiffLocation to be 1, got %d", res.SameContentDiffLocation)
	}
	if res.EntriesInserted != 1 {
		t.Errorf("expected 1 entry inserted, got %d", res.EntriesInserted)
	}
}

func TestMerge_DryRun(t *testing.T) {
	ctx := context.Background()

	dest, _ := createTestDB(t, "dest_dryrun.sqlite")
	defer dest.Close()

	src, srcPath := createTestDB(t, "src_dryrun.sqlite")
	defer src.Close()

	vol := &model.Volume{VolumeID: "vol-dryrun", FirstSeenAt: time.Now().UTC()}
	_ = src.RegisterVolume(ctx, vol, false)
	_ = src.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-dryrun",
			RelativePath:     "file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        10,
			QuickFingerprint: []byte("fp-dry"),
		},
	})
	src.Close()

	res, err := Merge(ctx, dest, srcPath, Options{DryRun: true})
	if err != nil {
		t.Fatalf("dry run merge failed: %v", err)
	}

	if res.EntriesInserted != 1 {
		t.Errorf("expected dry run to report 1 entry inserted, got %d", res.EntriesInserted)
	}

	// Verify nothing was written to dest
	vols, _ := dest.ListVolumes(ctx, model.VolumeFilter{})
	if len(vols) != 0 {
		t.Errorf("expected 0 volumes in dest after dry-run, got %d", len(vols))
	}
}

func TestMerge_AlgorithmMismatch(t *testing.T) {
	ctx := context.Background()

	dest, _ := createTestDB(t, "dest_algo.sqlite")
	defer dest.Close()
	_ = dest.SetMetadata(ctx, "fingerprint_algorithm", "sha256")

	src, srcPath := createTestDB(t, "src_algo.sqlite")
	_ = src.SetMetadata(ctx, "fingerprint_algorithm", "blake3-sparse")
	src.Close()

	_, err := Merge(ctx, dest, srcPath, Options{})
	if err == nil {
		t.Fatalf("expected error on algorithm mismatch, got nil")
	}
}

func TestMerge_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context before/during merge

	dest, _ := createTestDB(t, "dest_cancel.sqlite")
	defer dest.Close()

	src, srcPath := createTestDB(t, "src_cancel.sqlite")
	_ = src.RegisterVolume(context.Background(), &model.Volume{VolumeID: "volCancelled", FirstSeenAt: time.Now().UTC()}, false)
	src.Close()

	_, err := Merge(ctx, dest, srcPath, Options{})
	if err == nil {
		t.Fatalf("expected error from cancelled context merge, got nil")
	}

	// Verify nothing was committed to dest
	vols, _ := dest.ListVolumes(context.Background(), model.VolumeFilter{})
	if len(vols) != 0 {
		t.Errorf("expected 0 volumes committed after cancelled merge, got %d", len(vols))
	}
}
