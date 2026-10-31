package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/scan"
	"github.com/kgorelov/findex/internal/volume"
)

func TestCLI_VolumeWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	defer db.Close()

	// 1. Initially empty
	vols, err := db.ListVolumes(context.Background(), model.VolumeFilter{})
	if err != nil {
		t.Fatalf("failed to list volumes: %v", err)
	}
	if len(vols) != 0 {
		t.Fatalf("expected 0 volumes, got %d", len(vols))
	}

	// 2. Register volume for a test path with sidecar fallback
	testMountDir := filepath.Join(tempDir, "fake_mount")
	if err := os.MkdirAll(testMountDir, 0755); err != nil {
		t.Fatalf("failed creating fake mount: %v", err)
	}

	vol, err := volume.ResolveVolume(testMountDir, volume.VolumeOptions{
		Label:       "TestMount",
		SourceLabel: "Test Lab",
	})
	if err != nil {
		t.Fatalf("failed resolving volume: %v", err)
	}
	vol.FSUUID = "test-fs-uuid-001"

	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	// Verify volume can be retrieved
	storedVol, err := db.GetVolume(context.Background(), vol.VolumeID)
	if err != nil {
		t.Fatalf("failed getting volume: %v", err)
	}
	if storedVol.VolumeID != vol.VolumeID {
		t.Errorf("expected volume id %q, got %q", vol.VolumeID, storedVol.VolumeID)
	}
	if storedVol.Label != "TestMount" {
		t.Errorf("expected label 'TestMount', got %q", storedVol.Label)
	}

	// Verify stats
	stats, err := db.GetVolumeStats(context.Background(), vol.VolumeID)
	if err != nil {
		t.Fatalf("failed getting stats: %v", err)
	}
	if stats.Volume.VolumeID != vol.VolumeID {
		t.Errorf("expected stats volume id %q, got %q", vol.VolumeID, stats.Volume.VolumeID)
	}

	// 3. Test conflict rejection
	conflictingVol := &model.Volume{
		VolumeID:   vol.VolumeID,
		FSUUID:     "different-uuid-999",
		FSType:     vol.FSType,
		DeviceID:   vol.DeviceID,
		SourceHost: vol.SourceHost,
	}
	err = db.RegisterVolume(context.Background(), conflictingVol, false)
	if err == nil {
		t.Fatalf("expected error on conflicting registration, got nil")
	}

	// 4. Test duplicate FSUUID rejection
	if vol.FSUUID != "" {
		duplicateFSUUIDVol := &model.Volume{
			VolumeID: "different-vol-id",
			FSUUID:   vol.FSUUID,
		}
		err = db.RegisterVolume(context.Background(), duplicateFSUUIDVol, false)
		if err == nil {
			t.Fatalf("expected error on duplicate FSUUID, got nil")
		}
	}
}

func TestCLI_FingerprintExecution(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sample.txt")
	if err := os.WriteFile(filePath, []byte("Findex fingerprint integration test"), 0644); err != nil {
		t.Fatalf("failed creating sample file: %v", err)
	}

	res, err := fingerprint.FingerprintFile(filePath, fingerprint.Options{IncludeFull: true})
	if err != nil {
		t.Fatalf("FingerprintFile failed: %v", err)
	}

	if len(res.QuickFingerprint) != 32 {
		t.Errorf("expected 32 bytes quick fingerprint, got %d", len(res.QuickFingerprint))
	}
	if len(res.FullFingerprint) != 32 {
		t.Errorf("expected 32 bytes full fingerprint, got %d", len(res.FullFingerprint))
	}
}

func TestCLI_ScanWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	// Create test tree:
	// data/
	//   file1.txt
	//   docs/
	//     file2.txt
	dataDir := filepath.Join(tempDir, "data")
	docsDir := filepath.Join(dataDir, "docs")
	if err := os.MkdirAll(docsDir, 0755); err != nil {
		t.Fatalf("failed creating test dirs: %v", err)
	}
	_ = os.WriteFile(filepath.Join(dataDir, "file1.txt"), []byte("content 1"), 0644)
	_ = os.WriteFile(filepath.Join(docsDir, "file2.txt"), []byte("content 2"), 0644)

	scanner := scan.New(db)
	summary, err := scanner.Run(context.Background(), scan.Options{
		TargetDir: dataDir,
		Workers:   2,
	})
	if err != nil {
		t.Fatalf("scanner.Run failed: %v", err)
	}

	if summary.FilesCount != 2 {
		t.Errorf("expected 2 files, got %d", summary.FilesCount)
	}
	if summary.DirsCount != 2 {
		t.Errorf("expected 2 dirs, got %d", summary.DirsCount)
	}

	// Verify entries listed
	entries, err := db.ListEntries(context.Background(), summary.VolumeID, "")
	if err != nil {
		t.Fatalf("ListEntries failed: %v", err)
	}
	if len(entries) != 4 {
		t.Errorf("expected 4 entries in db, got %d", len(entries))
	}
}

func TestCLI_DefaultDBPath(t *testing.T) {
	orig := os.Getenv("FINDEX_DB")
	defer os.Setenv("FINDEX_DB", orig)

	os.Setenv("FINDEX_DB", "/tmp/custom-findex.sqlite")
	if defaultDBPath() != "/tmp/custom-findex.sqlite" {
		t.Errorf("expected /tmp/custom-findex.sqlite, got %q", defaultDBPath())
	}

	os.Unsetenv("FINDEX_DB")
	path := defaultDBPath()
	if !strings.HasSuffix(path, "index.sqlite") && !strings.HasSuffix(path, "findex.sqlite") {
		t.Errorf("unexpected default db path: %q", path)
	}
}

func TestCLI_MergeWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	destDBFile := filepath.Join(tempDir, "dest.sqlite")
	srcDBFile := filepath.Join(tempDir, "src.sqlite")

	destDB, err := index.Open(destDBFile)
	if err != nil {
		t.Fatalf("failed opening dest db: %v", err)
	}
	_ = destDB.RegisterVolume(context.Background(), &model.Volume{
		VolumeID: "local-vol",
		Label:    "LocalDisk",
	}, false)
	destDB.Close()

	srcDB, err := index.Open(srcDBFile)
	if err != nil {
		t.Fatalf("failed opening src db: %v", err)
	}
	_ = srcDB.RegisterVolume(context.Background(), &model.Volume{
		VolumeID: "remote-vol",
		Label:    "RemoteBackup",
	}, false)
	_ = srcDB.UpsertEntries(context.Background(), []*model.Entry{
		{
			VolumeID:         "remote-vol",
			RelativePath:     "/remote/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        42,
			QuickFingerprint: []byte("fp-remote-42"),
		},
	})
	srcDB.Close()

	cmd := buildRootCmd()
	cmd.SetArgs([]string{"--db", destDBFile, "merge", srcDBFile})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("findex merge CLI command failed: %v", err)
	}

	// Verify merged in destDB
	destDB, err = index.Open(destDBFile)
	if err != nil {
		t.Fatalf("failed re-opening dest db: %v", err)
	}
	defer destDB.Close()

	vols, err := destDB.ListVolumes(context.Background(), model.VolumeFilter{})
	if err != nil {
		t.Fatalf("failed listing volumes: %v", err)
	}
	if len(vols) != 2 {
		t.Fatalf("expected 2 volumes after CLI merge, got %d", len(vols))
	}

	entries, err := destDB.ListEntries(context.Background(), "remote-vol", "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 1 || entries[0].RelativePath != "/remote/file.txt" {
		t.Errorf("expected /remote/file.txt in merged entries, got %v", entries)
	}
}

func TestCLI_CompareWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	now := time.Now().UTC()
	_ = db.RegisterVolume(context.Background(), &model.Volume{VolumeID: "vol1", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(context.Background(), &model.Volume{VolumeID: "vol2", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(context.Background(), []*model.Entry{
		{
			VolumeID:         "vol1",
			RelativePath:     "/data/file1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-file1"),
		},
		{
			VolumeID:         "vol1",
			RelativePath:     "/data/file2.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        200,
			QuickFingerprint: []byte("fp-file2"),
		},
		{
			VolumeID:         "vol2",
			RelativePath:     "/backup/file1.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        100,
			QuickFingerprint: []byte("fp-file1"),
		},
	})
	db.Close()

	cmd := buildRootCmd()
	cmd.SetArgs([]string{"--db", dbFile, "compare", "--present", "vol1", "--absent", "vol2"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("findex compare failed: %v", err)
	}

	cmdJSON := buildRootCmd()
	cmdJSON.SetArgs([]string{"--db", dbFile, "--json", "compare", "--present", "vol1", "--absent", "vol2", "--common"})
	if err := cmdJSON.Execute(); err != nil {
		t.Fatalf("findex compare --common failed: %v", err)
	}
}

func TestCLI_UniqueWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	now := time.Now().UTC()
	_ = db.RegisterVolume(context.Background(), &model.Volume{VolumeID: "vA", FirstSeenAt: now}, false)
	_ = db.RegisterVolume(context.Background(), &model.Volume{VolumeID: "vB", FirstSeenAt: now}, false)

	_ = db.UpsertEntries(context.Background(), []*model.Entry{
		{
			VolumeID:         "vA",
			RelativePath:     "/unique.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        150,
			QuickFingerprint: []byte("fp-unique"),
		},
	})
	db.Close()

	cmd := buildRootCmd()
	cmd.SetArgs([]string{"--db", dbFile, "unique", "--volume", "vA", "--among", "vA,vB"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("findex unique failed: %v", err)
	}
}
