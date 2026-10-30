package scan

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func newTestDB(t *testing.T) *index.DB {
	t.Helper()
	db, err := index.Open(":memory:")
	if err != nil {
		t.Fatalf("failed opening in-memory db: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func TestScan_Basic(t *testing.T) {
	db := newTestDB(t)
	tempDir := t.TempDir()

	// Create test tree:
	// tempDir/
	//   hello.txt (13 bytes)
	//   docs/
	//     notes.md (20 bytes)
	//     empty/
	if err := os.WriteFile(filepath.Join(tempDir, "hello.txt"), []byte("Hello, World!"), 0644); err != nil {
		t.Fatalf("failed writing hello.txt: %v", err)
	}
	docsDir := filepath.Join(tempDir, "docs")
	if err := os.Mkdir(docsDir, 0755); err != nil {
		t.Fatalf("failed creating docs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "notes.md"), []byte("Some markdown notes."), 0644); err != nil {
		t.Fatalf("failed writing notes.md: %v", err)
	}
	if err := os.Mkdir(filepath.Join(docsDir, "empty"), 0755); err != nil {
		t.Fatalf("failed creating empty: %v", err)
	}

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		TargetDir:   tempDir,
		Workers:     2,
		BatchSize:   10,
		IncludeFull: true,
	})
	if err != nil {
		t.Fatalf("scanner.Run failed: %v", err)
	}

	if summary.Status != model.ScanStatusCompleted {
		t.Errorf("expected completed status, got %s", summary.Status)
	}
	if summary.FilesCount != 2 {
		t.Errorf("expected 2 files, got %d", summary.FilesCount)
	}
	if summary.DirsCount != 3 { // root, docs, empty
		t.Errorf("expected 3 directories, got %d", summary.DirsCount)
	}
	if summary.TotalBytes != 13+20 {
		t.Errorf("expected 33 bytes, got %d", summary.TotalBytes)
	}

	// Verify entries in database
	entries, err := db.ListEntries(context.Background(), summary.VolumeID, "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 5 { // 2 files + 3 dirs
		t.Errorf("expected 5 total entries in db, got %d", len(entries))
	}

	for _, e := range entries {
		if e.EntryType == model.EntryTypeRegular {
			if len(e.QuickFingerprint) != 32 {
				t.Errorf("file %s missing 32-byte quick fingerprint", e.RelativePath)
			}
			if len(e.FullFingerprint) != 32 {
				t.Errorf("file %s missing 32-byte full fingerprint", e.RelativePath)
			}
		} else if e.EntryType == model.EntryTypeDirectory {
			if len(e.DirectoryFingerprint) != 32 {
				t.Errorf("directory %s missing 32-byte directory fingerprint", e.RelativePath)
			}
		}
	}
}

func TestScan_Incremental_MultipleRoots(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	// Register volume explicitly for mountDir
	vol := &model.Volume{
		VolumeID:       "vol-multi-root",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register volume failed: %v", err)
	}

	// Subtree 1: /Photos
	photosDir := filepath.Join(mountDir, "Photos")
	if err := os.Mkdir(photosDir, 0755); err != nil {
		t.Fatalf("mkdir Photos failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(photosDir, "p1.jpg"), []byte("photo 1"), 0644); err != nil {
		t.Fatalf("write p1.jpg failed: %v", err)
	}

	// Subtree 2: /Music
	musicDir := filepath.Join(mountDir, "Music")
	if err := os.Mkdir(musicDir, 0755); err != nil {
		t.Fatalf("mkdir Music failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(musicDir, "m1.mp3"), []byte("music 1"), 0644); err != nil {
		t.Fatalf("write m1.mp3 failed: %v", err)
	}

	scanner := New(db)

	// Scan 1: /Photos
	summary1, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-multi-root",
		TargetDir: photosDir,
	})
	if err != nil {
		t.Fatalf("scan 1 failed: %v", err)
	}
	if summary1.RootPath != "/Photos" {
		t.Errorf("expected root path /Photos, got %s", summary1.RootPath)
	}

	// Verify only Photos entries exist
	entriesAfter1, _ := db.ListEntries(context.Background(), "vol-multi-root", "")
	if len(entriesAfter1) != 2 { // /Photos dir and p1.jpg
		t.Fatalf("expected 2 entries after scan 1, got %d", len(entriesAfter1))
	}

	// Scan 2: /Music
	summary2, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-multi-root",
		TargetDir: musicDir,
	})
	if err != nil {
		t.Fatalf("scan 2 failed: %v", err)
	}
	if summary2.RootPath != "/Music" {
		t.Errorf("expected root path /Music, got %s", summary2.RootPath)
	}

	// Verify BOTH Photos and Music entries exist! (Appendability)
	entriesAfter2, _ := db.ListEntries(context.Background(), "vol-multi-root", "")
	if len(entriesAfter2) != 4 { // /Photos, p1.jpg, /Music, m1.mp3
		t.Fatalf("expected 4 entries after scan 2, got %d", len(entriesAfter2))
	}

	// Verify scan roots recorded in DB
	stats, err := db.GetVolumeStats(context.Background(), "vol-multi-root")
	if err != nil {
		t.Fatalf("get stats failed: %v", err)
	}
	if stats.ScanRootsCount != 2 {
		t.Errorf("expected 2 scan roots, got %d", stats.ScanRootsCount)
	}
}

func TestScan_Reconciliation_StaleFilesRemoved(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	vol := &model.Volume{
		VolumeID:       "vol-reconcile",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	folder := filepath.Join(mountDir, "Projects")
	if err := os.Mkdir(folder, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	fileA := filepath.Join(folder, "a.go")
	fileB := filepath.Join(folder, "b.go")
	_ = os.WriteFile(fileA, []byte("package a"), 0644)
	_ = os.WriteFile(fileB, []byte("package b"), 0644)

	scanner := New(db)

	// First scan: a.go and b.go
	_, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-reconcile",
		TargetDir: folder,
	})
	if err != nil {
		t.Fatalf("first scan failed: %v", err)
	}

	// Remove b.go from filesystem
	if err := os.Remove(fileB); err != nil {
		t.Fatalf("failed removing b.go: %v", err)
	}

	// Second scan
	summary2, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-reconcile",
		TargetDir: folder,
	})
	if err != nil {
		t.Fatalf("second scan failed: %v", err)
	}

	if summary2.ReconciledCount != 1 {
		t.Errorf("expected 1 stale entry reconciled, got %d", summary2.ReconciledCount)
	}

	// Verify b.go is gone, a.go remains
	_, err = db.GetEntry(context.Background(), "vol-reconcile", "/Projects/b.go")
	if err == nil {
		t.Errorf("expected /Projects/b.go to be deleted")
	}

	entryA, err := db.GetEntry(context.Background(), "vol-reconcile", "/Projects/a.go")
	if err != nil || entryA == nil {
		t.Errorf("expected /Projects/a.go to exist: %v", err)
	}
}

func TestScan_DirectoryContentFingerprint_RenameIndependent(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	vol := &model.Volume{
		VolumeID:       "vol-rename",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	dir1 := filepath.Join(mountDir, "Data")
	_ = os.Mkdir(dir1, 0755)
	_ = os.WriteFile(filepath.Join(dir1, "first.txt"), []byte("identical content 1"), 0644)
	_ = os.WriteFile(filepath.Join(dir1, "second.txt"), []byte("identical content 2"), 0644)

	scanner := New(db)
	_, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-rename",
		TargetDir: dir1,
	})
	if err != nil {
		t.Fatalf("initial scan failed: %v", err)
	}

	entryDir1, err := db.GetEntry(context.Background(), "vol-rename", "/Data")
	if err != nil {
		t.Fatalf("get entry /Data failed: %v", err)
	}
	initialFP := entryDir1.DirectoryFingerprint

	// Now rename both files inside /Data
	_ = os.Rename(filepath.Join(dir1, "first.txt"), filepath.Join(dir1, "renamed_1.txt"))
	_ = os.Rename(filepath.Join(dir1, "second.txt"), filepath.Join(dir1, "renamed_2.txt"))

	// Rescan
	_, err = scanner.Run(context.Background(), Options{
		VolumeID:  "vol-rename",
		TargetDir: dir1,
	})
	if err != nil {
		t.Fatalf("rescan failed: %v", err)
	}

	entryDir2, err := db.GetEntry(context.Background(), "vol-rename", "/Data")
	if err != nil {
		t.Fatalf("get entry /Data after rename failed: %v", err)
	}

	if string(entryDir2.DirectoryFingerprint) != string(initialFP) {
		t.Errorf("directory fingerprint changed after renaming files! Content identity should be rename-independent.")
	}
}

func TestScan_EmptyRootDirectory(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	vol := &model.Volume{
		VolumeID:       "vol-empty-root",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	emptyFolder := filepath.Join(mountDir, "EmptyRoot")
	_ = os.Mkdir(emptyFolder, 0755)

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-empty-root",
		TargetDir: emptyFolder,
	})
	if err != nil {
		t.Fatalf("scan empty root failed: %v", err)
	}

	if summary.DirsCount != 1 {
		t.Errorf("expected 1 dir, got %d", summary.DirsCount)
	}
	if summary.FilesCount != 0 {
		t.Errorf("expected 0 files, got %d", summary.FilesCount)
	}

	entry, err := db.GetEntry(context.Background(), "vol-empty-root", "/EmptyRoot")
	if err != nil {
		t.Fatalf("get entry failed: %v", err)
	}
	if len(entry.DirectoryFingerprint) != 32 {
		t.Errorf("expected 32-byte empty directory fingerprint, got %d", len(entry.DirectoryFingerprint))
	}
}

func TestScan_SingleFileRoot(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	vol := &model.Volume{
		VolumeID:       "vol-single-file",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	filePath := filepath.Join(mountDir, "single.txt")
	_ = os.WriteFile(filePath, []byte("single file scan test"), 0644)

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-single-file",
		TargetDir: filePath,
	})
	if err != nil {
		t.Fatalf("single file scan failed: %v", err)
	}

	if summary.FilesCount != 1 {
		t.Errorf("expected 1 file, got %d", summary.FilesCount)
	}
	if summary.DirsCount != 0 {
		t.Errorf("expected 0 dirs, got %d", summary.DirsCount)
	}

	entry, err := db.GetEntry(context.Background(), "vol-single-file", "/single.txt")
	if err != nil {
		t.Fatalf("get entry failed: %v", err)
	}
	if len(entry.QuickFingerprint) != 32 {
		t.Errorf("expected 32-byte quick fingerprint, got %d", len(entry.QuickFingerprint))
	}
}
