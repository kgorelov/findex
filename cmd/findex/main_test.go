package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
