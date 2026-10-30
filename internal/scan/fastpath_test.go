package scan

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestScan_FastPath_HitsAndMisses(t *testing.T) {
	db := newTestDB(t)
	mountDir := t.TempDir()

	vol := &model.Volume{
		VolumeID:       "vol-fastpath",
		LastMountPoint: mountDir,
	}
	if err := db.RegisterVolume(context.Background(), vol, false); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	testDir := filepath.Join(mountDir, "TestData")
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// Create 5 files
	for i := 1; i <= 5; i++ {
		p := filepath.Join(testDir, fmt.Sprintf("file_%d.dat", i))
		content := []byte(fmt.Sprintf("file content number %d", i))
		if err := os.WriteFile(p, content, 0644); err != nil {
			t.Fatalf("failed writing %s: %v", p, err)
		}
	}

	scanner := New(db)

	// 1. Initial Cold Scan: all 5 files hashed
	s1, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-fastpath",
		TargetDir: testDir,
	})
	if err != nil {
		t.Fatalf("initial scan failed: %v", err)
	}
	if s1.FilesCount != 5 {
		t.Errorf("expected 5 files, got %d", s1.FilesCount)
	}
	if s1.FastHitsCount != 0 {
		t.Errorf("expected 0 fast hits on initial scan, got %d", s1.FastHitsCount)
	}
	if s1.BytesHashed == 0 {
		t.Errorf("expected non-zero bytes hashed on cold scan")
	}

	// 2. Re-scan with FastMode (default): all 5 files should hit fast-path
	s2, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-fastpath",
		TargetDir: testDir,
		FastMode:  true,
	})
	if err != nil {
		t.Fatalf("fast re-scan failed: %v", err)
	}
	if s2.FastHitsCount != 5 {
		t.Errorf("expected 5 fast hits, got %d", s2.FastHitsCount)
	}
	if s2.BytesHashed != 0 {
		t.Errorf("expected 0 bytes hashed on fast-path hit, got %d", s2.BytesHashed)
	}

	// 3. Modify file_3: change content and mtime
	file3Path := filepath.Join(testDir, "file_3.dat")
	newContent := []byte("modified content for file 3 which is now different")
	if err := os.WriteFile(file3Path, newContent, 0644); err != nil {
		t.Fatalf("modify file 3 failed: %v", err)
	}
	_ = os.Chtimes(file3Path, time.Now().Add(5*time.Second), time.Now().Add(5*time.Second))

	// Re-scan: 4 hits, 1 miss
	s3, err := scanner.Run(context.Background(), Options{
		VolumeID:  "vol-fastpath",
		TargetDir: testDir,
		FastMode:  true,
	})
	if err != nil {
		t.Fatalf("re-scan after modification failed: %v", err)
	}
	if s3.FastHitsCount != 4 {
		t.Errorf("expected 4 fast hits after modifying 1 file, got %d", s3.FastHitsCount)
	}
	if s3.BytesHashed != int64(len(newContent)) {
		t.Errorf("expected %d bytes hashed for modified file, got %d", len(newContent), s3.BytesHashed)
	}

	// 4. Strict verify mode: forces re-fingerprinting all 5 files
	s4, err := scanner.Run(context.Background(), Options{
		VolumeID:     "vol-fastpath",
		TargetDir:    testDir,
		StrictVerify: true,
	})
	if err != nil {
		t.Fatalf("strict verify scan failed: %v", err)
	}
	if s4.FastHitsCount != 0 {
		t.Errorf("expected 0 fast hits in strict verify mode, got %d", s4.FastHitsCount)
	}
	if s4.FilesCount != 5 {
		t.Errorf("expected 5 files scanned, got %d", s4.FilesCount)
	}
}
