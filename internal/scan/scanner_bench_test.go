package scan

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func BenchmarkScan_InitialVsFastRescan(b *testing.B) {
	mountDir := b.TempDir()
	dataDir := filepath.Join(mountDir, "BenchData")
	_ = os.Mkdir(dataDir, 0755)

	// Create 200 files
	for i := 0; i < 200; i++ {
		p := filepath.Join(dataDir, fmt.Sprintf("file_%d.txt", i))
		_ = os.WriteFile(p, []byte(fmt.Sprintf("benchmark payload %d", i)), 0644)
	}

	b.Run("initial_cold_scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, _ := index.Open(":memory:")
			_ = db.RegisterVolume(context.Background(), &model.Volume{
				VolumeID:       "vol-bench",
				LastMountPoint: mountDir,
			}, false)
			scanner := New(db)
			b.StartTimer()

			_, _ = scanner.Run(context.Background(), Options{
				VolumeID:  "vol-bench",
				TargetDir: dataDir,
				FastMode:  false,
			})
			_ = db.Close()
		}
	})

	b.Run("fast_path_rescan", func(b *testing.B) {
		db, _ := index.Open(":memory:")
		defer db.Close()
		_ = db.RegisterVolume(context.Background(), &model.Volume{
			VolumeID:       "vol-bench",
			LastMountPoint: mountDir,
		}, false)
		scanner := New(db)

		// Initial scan to populate DB
		_, _ = scanner.Run(context.Background(), Options{
			VolumeID:  "vol-bench",
			TargetDir: dataDir,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = scanner.Run(context.Background(), Options{
				VolumeID:  "vol-bench",
				TargetDir: dataDir,
				FastMode:  true,
			})
		}
	})

	b.Run("strict_verify_rescan", func(b *testing.B) {
		db, _ := index.Open(":memory:")
		defer db.Close()
		_ = db.RegisterVolume(context.Background(), &model.Volume{
			VolumeID:       "vol-bench",
			LastMountPoint: mountDir,
		}, false)
		scanner := New(db)

		// Initial scan to populate DB
		_, _ = scanner.Run(context.Background(), Options{
			VolumeID:  "vol-bench",
			TargetDir: dataDir,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = scanner.Run(context.Background(), Options{
				VolumeID:     "vol-bench",
				TargetDir:    dataDir,
				StrictVerify: true,
			})
		}
	})
}
