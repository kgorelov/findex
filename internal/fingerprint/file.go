package fingerprint

import (
	"errors"
	"fmt"
	"os"
	"time"
)

var (
	// ErrFileUnstable is returned when a file changes during fingerprint calculation.
	ErrFileUnstable = errors.New("file modified during reading")
	// ErrNotRegularFile is returned when fingerprinting is attempted on non-regular files.
	ErrNotRegularFile = errors.New("cannot fingerprint non-regular file")
)

// Options configures file fingerprint computation.
type Options struct {
	ChunkSize        int64
	IncludeFull      bool
	MaxRetryAttempts int
}

// Result contains the computed fingerprints and metadata.
type Result struct {
	Path             string `json:"path"`
	SizeBytes        int64  `json:"size_bytes"`
	MTimeNs          int64  `json:"mtime_ns"`
	QuickFingerprint []byte `json:"quick_fingerprint"`
	FullFingerprint  []byte `json:"full_fingerprint,omitempty"`
	Algorithm        string `json:"algorithm"`
	Version          int    `json:"version"`
}

// FingerprintFile computes the quick (sparse) fingerprint, and optionally the full fingerprint,
// verifying file stability before and after reading.
func FingerprintFile(path string, opts Options) (*Result, error) {
	if opts.MaxRetryAttempts <= 0 {
		opts.MaxRetryAttempts = 2
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultChunkSize
	}

	var lastErr error
	for attempt := 0; attempt <= opts.MaxRetryAttempts; attempt++ {
		res, err := fingerprintOnce(path, opts)
		if err == nil {
			return res, nil
		}

		if errors.Is(err, ErrFileUnstable) {
			lastErr = err
			// Backoff briefly before retry
			time.Sleep(time.Duration(10*(attempt+1)) * time.Millisecond)
			continue
		}

		// Non-stability errors (e.g. permission denied, not regular file) fail immediately
		return nil, err
	}

	return nil, fmt.Errorf("%w after %d attempts for %q", lastErr, opts.MaxRetryAttempts, path)
}

func fingerprintOnce(path string, opts Options) (*Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %q: %w", path, err)
	}
	defer f.Close()

	initialStat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %q: %w", path, err)
	}

	if !initialStat.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q (mode %v)", ErrNotRegularFile, path, initialStat.Mode())
	}

	fileSize := initialStat.Size()
	mtimeNs := initialStat.ModTime().UnixNano()

	// Compute sparse fingerprint
	quickFP, err := ComputeSparseFingerprint(f, fileSize, opts.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("sparse fingerprint failed: %w", err)
	}

	var fullFP []byte
	if opts.IncludeFull {
		if _, err := f.Seek(0, 0); err != nil {
			return nil, fmt.Errorf("failed to seek to file start: %w", err)
		}
		fullFP, err = ComputeFullFingerprint(f, fileSize)
		if err != nil {
			return nil, fmt.Errorf("full fingerprint failed: %w", err)
		}
	}

	// Stability check: stat again
	postStat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("post-read stat failed: %w", err)
	}

	if postStat.Size() != fileSize || postStat.ModTime().UnixNano() != mtimeNs {
		return nil, fmt.Errorf("%w: size or mtime changed for %q", ErrFileUnstable, path)
	}

	return &Result{
		Path:             path,
		SizeBytes:        fileSize,
		MTimeNs:          mtimeNs,
		QuickFingerprint: quickFP,
		FullFingerprint:  fullFP,
		Algorithm:        AlgorithmSparse,
		Version:          AlgorithmVersion,
	}, nil
}
