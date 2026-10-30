package fingerprint

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

const fullMagicHeader = "FINDEX_FULL_V1"

// ComputeFullFingerprint computes the full BLAKE3-256 hash of r with domain separation.
func ComputeFullFingerprint(r io.Reader, fileSize int64) ([]byte, error) {
	hasher := blake3.New()

	// Write framing header:
	// - Magic string: "FINDEX_FULL_V1\x00"
	// - FileSize: 8 bytes uint64 big-endian
	header := make([]byte, len(fullMagicHeader)+1+8)
	copy(header, fullMagicHeader)
	header[len(fullMagicHeader)] = 0x00
	binary.BigEndian.PutUint64(header[len(fullMagicHeader)+1:], uint64(fileSize))

	if _, err := hasher.Write(header); err != nil {
		return nil, fmt.Errorf("failed writing full header: %w", err)
	}

	buf := make([]byte, DefaultChunkSize)
	var totalRead int64

	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, wErr := hasher.Write(buf[:n]); wErr != nil {
				return nil, fmt.Errorf("failed hashing full content: %w", wErr)
			}
			totalRead += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed reading content for full hash: %w", err)
		}
	}

	if totalRead != fileSize {
		return nil, fmt.Errorf("file size mismatch during full hash: expected %d bytes, read %d bytes", fileSize, totalRead)
	}

	return hasher.Sum(nil), nil
}
