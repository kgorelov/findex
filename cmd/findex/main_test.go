package main

import (
	"context"
	"fmt"
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

	// Omitting comparison mode must fail (mandatory option)
	cmdNoMode := buildRootCmd()
	cmdNoMode.SetArgs([]string{"--db", dbFile, "compare", "vol1", "vol2"})
	if err := cmdNoMode.Execute(); err == nil {
		t.Fatalf("expected error when comparison mode option is omitted, got nil")
	}

	// findex compare --left vol1 vol2
	cmdLeft := buildRootCmd()
	cmdLeft.SetArgs([]string{"--db", dbFile, "compare", "--left", "vol1", "vol2"})
	if err := cmdLeft.Execute(); err != nil {
		t.Fatalf("findex compare --left vol1 vol2 failed: %v", err)
	}

	// findex compare --right vol1 vol2
	cmdRight := buildRootCmd()
	cmdRight.SetArgs([]string{"--db", dbFile, "compare", "--right", "vol1", "vol2"})
	if err := cmdRight.Execute(); err != nil {
		t.Fatalf("findex compare --right vol1 vol2 failed: %v", err)
	}

	// findex compare --common vol1 vol2
	cmdCommon := buildRootCmd()
	cmdCommon.SetArgs([]string{"--db", dbFile, "--json", "compare", "--common", "vol1", "vol2"})
	if err := cmdCommon.Execute(); err != nil {
		t.Fatalf("findex compare --common vol1 vol2 failed: %v", err)
	}

	// Combining options must fail
	cmdMulti := buildRootCmd()
	cmdMulti.SetArgs([]string{"--db", dbFile, "compare", "--left", "--right", "vol1", "vol2"})
	if err := cmdMulti.Execute(); err == nil {
		t.Fatalf("expected error when combining --left and --right, got nil")
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

	// Positional syntax: findex uniq vA
	cmd := buildRootCmd()
	cmd.SetArgs([]string{"--db", dbFile, "uniq", "vA"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("findex uniq vA failed: %v", err)
	}

	// Positional syntax: findex unique vA
	cmdAlias := buildRootCmd()
	cmdAlias.SetArgs([]string{"--db", dbFile, "unique", "vA"})
	if err := cmdAlias.Execute(); err != nil {
		t.Fatalf("findex unique alias failed: %v", err)
	}

	// Legacy flag syntax
	cmdLegacy := buildRootCmd()
	cmdLegacy.SetArgs([]string{"--db", dbFile, "unique", "--volume", "vA", "--among", "vA,vB"})
	if err := cmdLegacy.Execute(); err != nil {
		t.Fatalf("findex unique legacy failed: %v", err)
	}
}

func TestCLI_LookupWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	dataDir := filepath.Join(tempDir, "lookup_data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	filePath := filepath.Join(dataDir, "item.txt")
	if err := os.WriteFile(filePath, []byte("Lookup content 12345"), 0644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	_ = db.RegisterVolume(context.Background(), &model.Volume{
		VolumeID:       "volLookup",
		Label:          "LookupDisk",
		LastMountPoint: dataDir,
		FirstSeenAt:    time.Now().UTC(),
	}, false)

	scanner := scan.New(db)
	_, err = scanner.Run(context.Background(), scan.Options{
		VolumeID:  "volLookup",
		TargetDir: dataDir,
	})
	if err != nil {
		t.Fatalf("scanner.Run failed: %v", err)
	}

	// 1. Lookup by filepath
	cmdFile := buildRootCmd()
	cmdFile.SetArgs([]string{"--db", dbFile, "lookup", filePath})
	if err := cmdFile.Execute(); err != nil {
		t.Fatalf("findex lookup filepath failed: %v", err)
	}

	// 2. Lookup by directory path
	cmdDir := buildRootCmd()
	cmdDir.SetArgs([]string{"--db", dbFile, "lookup", dataDir})
	if err := cmdDir.Execute(); err != nil {
		t.Fatalf("findex lookup directory failed: %v", err)
	}

	// 3. Lookup by partial hex fingerprint
	entries, _ := db.ListEntries(context.Background(), "volLookup", "")
	var partialFP string
	for _, e := range entries {
		if len(e.QuickFingerprint) > 0 {
			hexFP := fmt.Sprintf("%x", e.QuickFingerprint)
			partialFP = hexFP[:8]
			break
		}
	}
	if partialFP == "" {
		t.Fatalf("expected scanned file entry with quick fingerprint")
	}

	cmdFP := buildRootCmd()
	cmdFP.SetArgs([]string{"--db", dbFile, "--json", "lookup", partialFP})
	if err := cmdFP.Execute(); err != nil {
		t.Fatalf("findex lookup partial hex failed: %v", err)
	}
}

func TestCLI_CheckWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile := filepath.Join(tempDir, "index.sqlite")

	db, err := index.Open(dbFile)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	_ = db.RegisterVolume(context.Background(), &model.Volume{VolumeID: "volCheck", FirstSeenAt: time.Now().UTC()}, false)
	db.Close()

	cmd := buildRootCmd()
	cmd.SetArgs([]string{"--db", dbFile, "check"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("findex check failed: %v", err)
	}

	cmdJSON := buildRootCmd()
	cmdJSON.SetArgs([]string{"--db", dbFile, "--json", "check"})
	if err := cmdJSON.Execute(); err != nil {
		t.Fatalf("findex check --json failed: %v", err)
	}
}

func TestCLI_ExportImportWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	dbFile1 := filepath.Join(tempDir, "db1.sqlite")
	dbFile2 := filepath.Join(tempDir, "db2.sqlite")
	exportGz := filepath.Join(tempDir, "export.json.gz")

	// Set up db1
	db1, err := index.Open(dbFile1)
	if err != nil {
		t.Fatalf("failed opening db1: %v", err)
	}
	_ = db1.RegisterVolume(context.Background(), &model.Volume{
		VolumeID:    "volExportCLI",
		Label:       "ExportLabel",
		FirstSeenAt: time.Now().UTC(),
	}, false)
	_ = db1.UpsertEntries(context.Background(), []*model.Entry{
		{
			VolumeID:         "volExportCLI",
			RelativePath:     "/exported.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        88,
			QuickFingerprint: []byte("fp-export-cli"),
		},
	})
	db1.Close()

	// 1. Export
	cmdExp := buildRootCmd()
	cmdExp.SetArgs([]string{"--db", dbFile1, "export", exportGz})
	if err := cmdExp.Execute(); err != nil {
		t.Fatalf("findex export failed: %v", err)
	}

	// 2. Import into db2
	cmdImp := buildRootCmd()
	cmdImp.SetArgs([]string{"--db", dbFile2, "import", exportGz})
	if err := cmdImp.Execute(); err != nil {
		t.Fatalf("findex import failed: %v", err)
	}

	// Verify db2 has data
	db2, err := index.Open(dbFile2)
	if err != nil {
		t.Fatalf("failed opening db2: %v", err)
	}
	defer db2.Close()

	v, err := db2.GetVolume(context.Background(), "volExportCLI")
	if err != nil {
		t.Fatalf("failed getting imported volume: %v", err)
	}
	if v.Label != "ExportLabel" {
		t.Errorf("expected label ExportLabel, got %q", v.Label)
	}

	entries, err := db2.ListEntries(context.Background(), "volExportCLI", "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 1 || entries[0].RelativePath != "/exported.txt" {
		t.Errorf("expected /exported.txt, got %v", entries)
	}
}
