package fingerprint

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kgorelov/findex/internal/model"
)

func BenchmarkSparseFingerprint(b *testing.B) {
	sizes := []int64{
		4 * 1024,         // 4 KiB (small)
		64 * 1024,        // 64 KiB (chunk)
		10 * 1024 * 1024, // 10 MiB (large)
	}

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", sz), func(b *testing.B) {
			data := make([]byte, sz)
			r := bytes.NewReader(data)
			b.SetBytes(sz)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = ComputeSparseFingerprint(r, sz, DefaultChunkSize)
			}
		})
	}
}

func BenchmarkFullFingerprint(b *testing.B) {
	sizes := []int64{
		64 * 1024,        // 64 KiB
		10 * 1024 * 1024, // 10 MiB
	}

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", sz), func(b *testing.B) {
			data := make([]byte, sz)
			b.SetBytes(sz)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(data)
				_, _ = ComputeFullFingerprint(r, sz)
			}
		})
	}
}

func BenchmarkDirectoryFingerprint(b *testing.B) {
	counts := []int{10, 100, 1000}

	for _, cnt := range counts {
		b.Run(fmt.Sprintf("%d_children", cnt), func(b *testing.B) {
			children := make([]ChildDescriptor, cnt)
			for i := 0; i < cnt; i++ {
				children[i] = ChildDescriptor{
					Type:        model.EntryTypeRegular,
					SizeBytes:   int64(i * 1024),
					Fingerprint: []byte(fmt.Sprintf("%032d", i)),
				}
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = ComputeDirectoryFingerprint(children)
			}
		})
	}
}
