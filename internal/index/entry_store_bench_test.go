package index

import (
	"context"
	"fmt"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func BenchmarkUpsertEntries(b *testing.B) {
	batchSizes := []int{100, 500, 1000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", size), func(b *testing.B) {
			db := newTestDB(b)
			ctx := context.Background()
			_ = db.RegisterVolume(ctx, &model.Volume{VolumeID: "vol-bench"}, false)

			entries := make([]*model.Entry, size)
			for i := 0; i < size; i++ {
				entries[i] = &model.Entry{
					VolumeID:         "vol-bench",
					RelativePath:     fmt.Sprintf("/bench/file_%d.dat", i),
					EntryType:        model.EntryTypeRegular,
					SizeBytes:        int64(i * 1024),
					MTimeNs:          1700000000,
					QuickFingerprint: []byte("01234567890123456789012345678901"),
					LastSeenScanID:   "scan-bench",
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.UpsertEntries(ctx, entries)
			}
		})
	}
}
