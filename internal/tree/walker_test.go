package tree

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildTree(t *testing.T) {
	tempDir := t.TempDir()

	// Setup directory structure
	file1 := filepath.Join(tempDir, "file1.txt")
	if err := os.WriteFile(file1, []byte("file1 content"), 0644); err != nil {
		t.Fatalf("failed creating file1: %v", err)
	}

	subDir := filepath.Join(tempDir, "sub")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("failed creating sub: %v", err)
	}

	file2 := filepath.Join(subDir, "file2.txt")
	if err := os.WriteFile(file2, []byte("file2 content"), 0644); err != nil {
		t.Fatalf("failed creating file2: %v", err)
	}

	emptyDir := filepath.Join(subDir, "empty")
	if err := os.Mkdir(emptyDir, 0755); err != nil {
		t.Fatalf("failed creating empty: %v", err)
	}

	sym := filepath.Join(subDir, "link")
	if err := os.Symlink("../file1.txt", sym); err != nil {
		t.Fatalf("failed creating symlink: %v", err)
	}

	rootNode, fileNodes, err := BuildTree(context.Background(), "vol-test", tempDir, tempDir)
	if err != nil {
		t.Fatalf("BuildTree failed: %v", err)
	}

	if rootNode.RelativePath != "/" {
		t.Errorf("expected root relative path '/', got %q", rootNode.RelativePath)
	}

	if len(fileNodes) != 2 {
		t.Errorf("expected 2 file nodes (file1, file2), got %d", len(fileNodes))
	}

	// Verify empty directory fingerprint is already set
	var foundEmpty *Node
	for _, child := range rootNode.Children {
		if child.RelativePath == "/sub" {
			for _, subChild := range child.Children {
				if subChild.RelativePath == "/sub/empty" {
					foundEmpty = subChild
				}
			}
		}
	}

	if foundEmpty == nil {
		t.Fatalf("did not find /sub/empty node")
	}
	if foundEmpty.PendingCount != 0 {
		t.Errorf("expected 0 pending count for empty dir, got %d", foundEmpty.PendingCount)
	}
}
