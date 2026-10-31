package tree

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkBuildTree(b *testing.B) {
	tempDir := b.TempDir()

	// Create 10 directories with 100 files each = 1,000 files
	for d := 0; d < 10; d++ {
		dirPath := filepath.Join(tempDir, fmt.Sprintf("dir_%d", d))
		_ = os.Mkdir(dirPath, 0755)
		for f := 0; f < 100; f++ {
			filePath := filepath.Join(dirPath, fmt.Sprintf("file_%d.txt", f))
			_ = os.WriteFile(filePath, []byte("benchmark test content"), 0644)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := BuildTree(context.Background(), "vol-bench", tempDir, tempDir)
		if err != nil {
			b.Fatalf("BuildTree failed: %v", err)
		}
	}
}
