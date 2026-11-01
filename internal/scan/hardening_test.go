package scan

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func TestHardening_UnusualFilenames(t *testing.T) {
	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanDir := filepath.Join(tempDir, "target")
	if err := os.MkdirAll(scanDir, 0755); err != nil {
		t.Fatalf("failed creating target dir: %v", err)
	}

	files := []string{
		"file with spaces.txt",
		"документ_日本語_🎉.txt",
		"single'quote_and_\"double\".dat",
		"[brackets] and (parentheses).log",
		"deep 📂/nested dir/unicode 🚀.bin",
	}

	for _, rel := range files {
		fullPath := filepath.Join(scanDir, rel)
		_ = os.MkdirAll(filepath.Dir(fullPath), 0755)
		if err := os.WriteFile(fullPath, []byte("content for "+rel), 0644); err != nil {
			t.Fatalf("failed creating file %s: %v", rel, err)
		}
	}

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		TargetDir: scanDir,
		FastMode:  false,
	})
	if err != nil {
		t.Fatalf("scanner.Run failed on unusual filenames: %v", err)
	}

	if summary.FilesCount != int64(len(files)) {
		t.Errorf("expected %d files scanned, got %d", len(files), summary.FilesCount)
	}
	if summary.ErrorsCount != 0 {
		t.Errorf("expected 0 errors, got %d", summary.ErrorsCount)
	}

	entries, err := db.ListEntries(context.Background(), summary.VolumeID, "")
	if err != nil {
		t.Fatalf("ListEntries failed: %v", err)
	}

	for _, rel := range files {
		found := false
		for _, e := range entries {
			if filepath.Base(e.RelativePath) == filepath.Base(rel) {
				found = true
				if len(e.QuickFingerprint) != 32 {
					t.Errorf("expected 32-byte quick fingerprint for %s", rel)
				}
				break
			}
		}
		if !found {
			t.Errorf("expected entry for file %s in database", rel)
		}
	}
}

func TestHardening_Symlinks(t *testing.T) {
	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanDir := filepath.Join(tempDir, "symlink_test")
	subDir := filepath.Join(scanDir, "sub")
	_ = os.MkdirAll(subDir, 0755)

	realFile := filepath.Join(scanDir, "real.txt")
	_ = os.WriteFile(realFile, []byte("real content"), 0644)

	// 1. Valid symlink to a file
	_ = os.Symlink(realFile, filepath.Join(scanDir, "symlink_to_file.lnk"))

	// 2. Broken / dangling symlink
	_ = os.Symlink(filepath.Join(scanDir, "nonexistent.txt"), filepath.Join(scanDir, "broken.lnk"))

	// 3. Symlink to directory (should NOT be followed into infinite loop)
	_ = os.Symlink(subDir, filepath.Join(scanDir, "symlink_to_dir.lnk"))

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		TargetDir: scanDir,
	})
	if err != nil {
		t.Fatalf("scanner.Run failed on symlinks: %v", err)
	}

	if summary.SymlinksCount != 3 {
		t.Errorf("expected 3 symlinks scanned, got %d", summary.SymlinksCount)
	}
	if summary.FilesCount != 1 {
		t.Errorf("expected 1 real file scanned, got %d", summary.FilesCount)
	}
}

func TestHardening_PermissionErrors(t *testing.T) {
	// Skip if running as root (root bypasses unix file permissions)
	if os.Geteuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanDir := filepath.Join(tempDir, "perm_test")
	readableSub := filepath.Join(scanDir, "readable")
	unreadableSub := filepath.Join(scanDir, "unreadable")

	_ = os.MkdirAll(readableSub, 0755)
	_ = os.MkdirAll(unreadableSub, 0755)

	_ = os.WriteFile(filepath.Join(readableSub, "good.txt"), []byte("good data"), 0644)
	_ = os.WriteFile(filepath.Join(unreadableSub, "secret.txt"), []byte("secret data"), 0644)

	// Make unreadable directory 0000
	if err := os.Chmod(unreadableSub, 0000); err != nil {
		t.Fatalf("failed chmod unreadable dir: %v", err)
	}
	defer os.Chmod(unreadableSub, 0755)

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		TargetDir: scanDir,
	})

	// Scanner should complete with errors counted rather than crashing
	if summary == nil {
		t.Fatalf("expected non-nil summary, got nil (err: %v)", err)
	}
	if summary.ErrorsCount == 0 {
		t.Errorf("expected ErrorsCount > 0 for unreadable directory, got 0")
	}
	if summary.FilesCount < 1 {
		t.Errorf("expected readable file to still be indexed despite permission error")
	}
}

func TestHardening_InterruptedScan_SafeReconciliation(t *testing.T) {
	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanDir := filepath.Join(tempDir, "reconcile_test")
	_ = os.MkdirAll(scanDir, 0755)
	file1 := filepath.Join(scanDir, "file1.txt")
	_ = os.WriteFile(file1, []byte("file1 content"), 0644)

	scanner := New(db)
	summary1, err := scanner.Run(context.Background(), Options{
		TargetDir: scanDir,
	})
	if err != nil {
		t.Fatalf("first scan failed: %v", err)
	}

	entries1, _ := db.ListEntries(context.Background(), summary1.VolumeID, "")
	if len(entries1) != 2 { // dir + file
		t.Fatalf("expected 2 entries initially, got %d", len(entries1))
	}

	// Delete file1 from filesystem
	_ = os.Remove(file1)

	// Now run second scan with a cancelled context (simulating interrupt / SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err = scanner.Run(ctx, Options{
		TargetDir: scanDir,
	})
	if err == nil {
		t.Fatalf("expected error from cancelled scan, got nil")
	}

	// Stale entries MUST NOT be reconciled on failed / cancelled scan
	entriesAfterCancel, _ := db.ListEntries(context.Background(), summary1.VolumeID, "")
	if len(entriesAfterCancel) != 2 {
		t.Errorf("safe reconciliation violated: entries were deleted during interrupted scan")
	}
}

func TestHardening_DisappearingTarget(t *testing.T) {
	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanner := New(db)
	nonexistent := filepath.Join(tempDir, "does_not_exist_folder")

	_, err = scanner.Run(context.Background(), Options{
		TargetDir: nonexistent,
	})
	if err == nil {
		t.Fatalf("expected error when scanning nonexistent target, got nil")
	}
}

func TestHardening_ResilienceToFilesystemErrors(t *testing.T) {
	tempDir := t.TempDir()
	db, err := index.Open(filepath.Join(tempDir, "index.sqlite"))
	if err != nil {
		t.Fatalf("failed opening db: %v", err)
	}
	defer db.Close()

	scanDir := filepath.Join(tempDir, "resilient_test")
	_ = os.MkdirAll(scanDir, 0755)

	validFile1 := filepath.Join(scanDir, "valid1.txt")
	_ = os.WriteFile(validFile1, []byte("valid content 1"), 0644)

	validFile2 := filepath.Join(scanDir, "valid2.txt")
	_ = os.WriteFile(validFile2, []byte("valid content 2"), 0644)

	var reportedErrors []error
	errorFn := func(e error) {
		reportedErrors = append(reportedErrors, e)
	}

	scanner := New(db)
	summary, err := scanner.Run(context.Background(), Options{
		TargetDir: scanDir,
		ErrorFn:   errorFn,
	})
	if err != nil {
		t.Fatalf("expected scan to succeed, got: %v", err)
	}
	if summary.Status != model.ScanStatusCompleted {
		t.Errorf("expected status completed, got %v", summary.Status)
	}
	if summary.FilesCount != 2 {
		t.Errorf("expected 2 files indexed, got %d", summary.FilesCount)
	}
}
