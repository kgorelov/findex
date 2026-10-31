package lookup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/scan"
)

func TestLookup_HexFingerprint(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_lookup.sqlite")
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol1", Label: "DiskA", FirstSeenAt: now}, false)
	_ = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol1",
			RelativePath:     "/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        123,
			QuickFingerprint: []byte("fp-file-content!"),
		},
	})

	// 1. Partial hex lookup
	summary, err := Lookup(ctx, db, "66702d") // "fp-" in hex
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if summary.TargetType != "fingerprint" {
		t.Errorf("expected target_type fingerprint, got %q", summary.TargetType)
	}
	if summary.MatchesCount != 1 {
		t.Fatalf("expected 1 match, got %d", summary.MatchesCount)
	}
	if summary.Matches[0].RelativePath != "/file.txt" {
		t.Errorf("unexpected matched path: %q", summary.Matches[0].RelativePath)
	}

	// 2. Invalid hex string that does not exist on disk
	_, err = Lookup(ctx, db, "not-a-hex-string-and-not-a-file")
	if err == nil {
		t.Fatalf("expected error for non-hex non-file query, got nil")
	}
}

func TestLookup_FilePath(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	// Create test directory with files
	dataDir := filepath.Join(tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	filePath := filepath.Join(dataDir, "hello.txt")
	if err := os.WriteFile(filePath, []byte("Hello Findex Lookup!"), 0644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	// Scan this directory into database
	dbPath := filepath.Join(tempDir, "scan.sqlite")
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	_ = db.RegisterVolume(ctx, &model.Volume{
		VolumeID:       "vol-scanned",
		Label:          "ScannedDisk",
		LastMountPoint: dataDir,
		FirstSeenAt:    time.Now().UTC(),
	}, false)

	scanner := scan.New(db)
	_, err = scanner.Run(ctx, scan.Options{
		VolumeID:  "vol-scanned",
		TargetDir: dataDir,
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	// 1. Lookup existing regular file
	summary, err := Lookup(ctx, db, filePath)
	if err != nil {
		t.Fatalf("lookup file failed: %v", err)
	}
	if summary.TargetType != "filepath" {
		t.Errorf("expected target_type filepath, got %q", summary.TargetType)
	}
	if summary.MatchesCount != 1 {
		t.Fatalf("expected 1 match for file, got %d", summary.MatchesCount)
	}
	if summary.Matches[0].RelativePath != "/hello.txt" {
		t.Errorf("expected match for /hello.txt, got %q", summary.Matches[0].RelativePath)
	}

	// 2. Lookup existing directory
	dirSummary, err := Lookup(ctx, db, dataDir)
	if err != nil {
		t.Fatalf("lookup dir failed: %v", err)
	}
	if dirSummary.TargetType != "filepath" {
		t.Errorf("expected target_type filepath, got %q", dirSummary.TargetType)
	}
	if dirSummary.MatchesCount != 1 {
		t.Fatalf("expected 1 match for dir, got %d", dirSummary.MatchesCount)
	}
	if dirSummary.Matches[0].RelativePath != "/" {
		t.Errorf("expected match for /, got %q", dirSummary.Matches[0].RelativePath)
	}
}
