package fingerprint

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestComputeSampleRanges(t *testing.T) {
	chunk := int64(DefaultChunkSize) // 64 KiB

	tests := []struct {
		name     string
		fileSize int64
		expected []SampleRange
	}{
		{
			name:     "empty file",
			fileSize: 0,
			expected: []SampleRange{{Offset: 0, Length: 0}},
		},
		{
			name:     "small file 100 bytes",
			fileSize: 100,
			expected: []SampleRange{{Offset: 0, Length: 100}},
		},
		{
			name:     "exact threshold 3 chunks",
			fileSize: 3 * chunk,
			expected: []SampleRange{{Offset: 0, Length: 3 * chunk}},
		},
		{
			name:     "threshold plus one byte",
			fileSize: 3*chunk + 1,
			expected: []SampleRange{
				{Offset: 0, Length: chunk},
				{Offset: (3*chunk + 1 - chunk) / 2, Length: chunk},
				{Offset: 3*chunk + 1 - chunk, Length: chunk},
			},
		},
		{
			name:     "1 megabyte",
			fileSize: 1024 * 1024,
			expected: []SampleRange{
				{Offset: 0, Length: chunk},
				{Offset: (1024*1024 - chunk) / 2, Length: chunk},
				{Offset: 1024*1024 - chunk, Length: chunk},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := ComputeSampleRanges(tc.fileSize, chunk)
			if len(actual) != len(tc.expected) {
				t.Fatalf("expected %d ranges, got %d", len(tc.expected), len(actual))
			}
			for i := range actual {
				if actual[i] != tc.expected[i] {
					t.Errorf("range %d: expected %+v, got %+v", i, tc.expected[i], actual[i])
				}
			}
		})
	}
}

func TestDeterministicVectors(t *testing.T) {
	// 1. Empty file
	emptyBuf := bytes.NewReader([]byte{})
	fpEmpty, err := ComputeSparseFingerprint(emptyBuf, 0, DefaultChunkSize)
	if err != nil {
		t.Fatalf("failed computing empty sparse fp: %v", err)
	}
	if len(fpEmpty) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(fpEmpty))
	}
	hexEmpty := hex.EncodeToString(fpEmpty)

	// Recompute to verify determinism
	fpEmpty2, err := ComputeSparseFingerprint(emptyBuf, 0, DefaultChunkSize)
	if err != nil {
		t.Fatalf("failed recomputing empty sparse fp: %v", err)
	}
	if hex.EncodeToString(fpEmpty2) != hexEmpty {
		t.Fatalf("sparse fingerprint of empty file is not deterministic")
	}

	// 2. Known small pattern
	pattern := []byte("The quick brown fox jumps over the lazy dog.")
	fpSmall, err := ComputeSparseFingerprint(bytes.NewReader(pattern), int64(len(pattern)), DefaultChunkSize)
	if err != nil {
		t.Fatalf("failed computing small pattern fp: %v", err)
	}
	hexSmall := hex.EncodeToString(fpSmall)

	fpSmall2, _ := ComputeSparseFingerprint(bytes.NewReader(pattern), int64(len(pattern)), DefaultChunkSize)
	if hex.EncodeToString(fpSmall2) != hexSmall {
		t.Fatalf("sparse fingerprint of small pattern is not deterministic")
	}

	// Verify full fingerprint of small pattern
	fullSmall, err := ComputeFullFingerprint(bytes.NewReader(pattern), int64(len(pattern)))
	if err != nil {
		t.Fatalf("failed computing full pattern fp: %v", err)
	}
	if len(fullSmall) != 32 {
		t.Fatalf("expected 32 bytes for full fp, got %d", len(fullSmall))
	}
}

func TestSparseVsFullWithUnsampledDifferences(t *testing.T) {
	chunk := int64(DefaultChunkSize)
	// Create a 5-chunk buffer (320 KiB)
	size := 5 * chunk
	dataA := make([]byte, size)
	for i := range dataA {
		dataA[i] = byte(i % 251)
	}

	dataB := make([]byte, size)
	copy(dataB, dataA)

	// Modify dataB in an unsampled region:
	// Sampled regions for 5 chunks:
	// - Head: [0, 64K)
	// - Mid: [(5*64K - 64K)/2, ...] = [128K, 192K)
	// - Tail: [4*64K, 5*64K) = [256K, 320K)
	// Unsampled region 1: [64K, 128K)
	// Change bytes in unsampled region:
	unsampledOffset := chunk + 1000
	dataB[unsampledOffset] ^= 0xFF

	// Quick fingerprints should be IDENTICAL
	fpA, err := ComputeSparseFingerprint(bytes.NewReader(dataA), size, chunk)
	if err != nil {
		t.Fatalf("fpA error: %v", err)
	}
	fpB, err := ComputeSparseFingerprint(bytes.NewReader(dataB), size, chunk)
	if err != nil {
		t.Fatalf("fpB error: %v", err)
	}

	if !bytes.Equal(fpA, fpB) {
		t.Errorf("expected sparse fingerprints to match for unsampled difference")
	}

	// Full fingerprints MUST DIFFER
	fullA, err := ComputeFullFingerprint(bytes.NewReader(dataA), size)
	if err != nil {
		t.Fatalf("fullA error: %v", err)
	}
	fullB, err := ComputeFullFingerprint(bytes.NewReader(dataB), size)
	if err != nil {
		t.Fatalf("fullB error: %v", err)
	}

	if bytes.Equal(fullA, fullB) {
		t.Errorf("expected full fingerprints to differ, but they were identical")
	}
}

func TestFingerprintFile_NormalAndFull(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "testfile.bin")

	content := []byte("Hello, findex fingerprinting world!")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed writing test file: %v", err)
	}

	res, err := FingerprintFile(filePath, Options{IncludeFull: true})
	if err != nil {
		t.Fatalf("FingerprintFile failed: %v", err)
	}

	if res.SizeBytes != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), res.SizeBytes)
	}
	if len(res.QuickFingerprint) != 32 {
		t.Errorf("expected 32 bytes quick fingerprint, got %d", len(res.QuickFingerprint))
	}
	if len(res.FullFingerprint) != 32 {
		t.Errorf("expected 32 bytes full fingerprint, got %d", len(res.FullFingerprint))
	}
	if res.Algorithm != AlgorithmSparse {
		t.Errorf("expected algorithm %q, got %q", AlgorithmSparse, res.Algorithm)
	}
	if res.Version != AlgorithmVersion {
		t.Errorf("expected version %d, got %d", AlgorithmVersion, res.Version)
	}
}

func TestFingerprintFile_DirectoryRejection(t *testing.T) {
	tempDir := t.TempDir()
	_, err := FingerprintFile(tempDir, Options{})
	if !errors.Is(err, ErrNotRegularFile) {
		t.Fatalf("expected ErrNotRegularFile for directory, got: %v", err)
	}
}

func TestFingerprintFile_UnstableModification(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "unstable.bin")

	// Create a large enough file so reading takes measurable time or can be simulated
	data := make([]byte, 1024*1024)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("failed writing unstable test file: %v", err)
	}

	// Verify normal fingerprint succeeds
	_, err := FingerprintFile(filePath, Options{})
	if err != nil {
		t.Fatalf("expected normal fingerprint to succeed, got %v", err)
	}

	// Change mtime manually after reading start by testing fingerprintOnce directly
	// Or simulate with a modified file
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed opening file: %v", err)
	}
	defer file.Close()

	// Modify file content and mtime
	_, _ = file.WriteAt([]byte("MODIFIED"), 0)
	_ = os.Chtimes(filePath, time.Now().Add(time.Second), time.Now().Add(time.Second))

	// Re-running FingerprintFile should succeed with the new state unless continuously changing
	res, err := FingerprintFile(filePath, Options{})
	if err != nil {
		t.Fatalf("expected re-fingerprint to succeed, got %v", err)
	}
	if res.SizeBytes != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), res.SizeBytes)
	}
}
