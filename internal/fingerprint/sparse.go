package fingerprint

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

const (
	// AlgorithmSparse is the identifier for the default fast sparse fingerprint algorithm.
	AlgorithmSparse = "blake3-sparse"
	// AlgorithmVersion is the version of the sparse fingerprint algorithm.
	AlgorithmVersion = 1

	// AlgorithmFull is the identifier for the full-content cryptographic hash.
	AlgorithmFull = "blake3-full"
	// FullAlgorithmVersion is the version of the full fingerprint algorithm.
	FullAlgorithmVersion = 1

	// DefaultChunkSize is the size of each sampled chunk (64 KiB).
	DefaultChunkSize = 64 * 1024

	// SmallFileThreshold defines the size up to which the full file is hashed (192 KiB).
	SmallFileThreshold = 3 * DefaultChunkSize

	sparseMagicHeader = "FINDEX_SPARSE_V1"
)

// SampleRange defines a byte range [Offset, Offset+Length) to read.
type SampleRange struct {
	Offset int64
	Length int64
}

// ComputeSampleRanges returns the deterministic sample ranges for a given file size.
func ComputeSampleRanges(fileSize int64, chunkSize int64) []SampleRange {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	if fileSize <= 0 {
		return []SampleRange{{Offset: 0, Length: 0}}
	}

	threshold := 3 * chunkSize
	if fileSize <= threshold {
		return []SampleRange{{Offset: 0, Length: fileSize}}
	}

	midOffset := (fileSize - chunkSize) / 2
	tailOffset := fileSize - chunkSize

	return []SampleRange{
		{Offset: 0, Length: chunkSize},
		{Offset: midOffset, Length: chunkSize},
		{Offset: tailOffset, Length: chunkSize},
	}
}

// ComputeSparseFingerprint hashes the specified ranges from a ReaderAt with unambiguous framing.
func ComputeSparseFingerprint(r io.ReaderAt, fileSize int64, chunkSize int64) ([]byte, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	ranges := ComputeSampleRanges(fileSize, chunkSize)

	hasher := blake3.New()

	// Write framing header:
	// - Magic string: "FINDEX_SPARSE_V1\x00"
	// - FileSize: 8 bytes uint64 big-endian
	// - Number of ranges: 4 bytes uint32 big-endian
	header := make([]byte, len(sparseMagicHeader)+1+8+4)
	copy(header, sparseMagicHeader)
	header[len(sparseMagicHeader)] = 0x00

	offset := len(sparseMagicHeader) + 1
	binary.BigEndian.PutUint64(header[offset:offset+8], uint64(fileSize))
	offset += 8
	binary.BigEndian.PutUint32(header[offset:offset+4], uint32(len(ranges)))

	if _, err := hasher.Write(header); err != nil {
		return nil, fmt.Errorf("failed writing sparse header: %w", err)
	}

	// Write range descriptors:
	// For each range: Offset (8 bytes) + Length (8 bytes)
	rangeDesc := make([]byte, 16)
	for _, rng := range ranges {
		binary.BigEndian.PutUint64(rangeDesc[0:8], uint64(rng.Offset))
		binary.BigEndian.PutUint64(rangeDesc[8:16], uint64(rng.Length))
		if _, err := hasher.Write(rangeDesc); err != nil {
			return nil, fmt.Errorf("failed writing range descriptor: %w", err)
		}
	}

	// Read and hash each sample range
	buf := make([]byte, chunkSize)
	for _, rng := range ranges {
		if rng.Length == 0 {
			continue
		}

		var bytesRead int64
		for bytesRead < rng.Length {
			toRead := rng.Length - bytesRead
			if toRead > int64(len(buf)) {
				toRead = int64(len(buf))
			}

			n, err := r.ReadAt(buf[:toRead], rng.Offset+bytesRead)
			if n > 0 {
				if _, wErr := hasher.Write(buf[:n]); wErr != nil {
					return nil, fmt.Errorf("failed hashing sample bytes: %w", wErr)
				}
				bytesRead += int64(n)
			}
			if err != nil {
				if err == io.EOF && bytesRead == rng.Length {
					break
				}
				return nil, fmt.Errorf("failed reading range [%d, %d): %w", rng.Offset, rng.Offset+rng.Length, err)
			}
		}
	}

	sum := hasher.Sum(nil)
	return sum, nil
}
