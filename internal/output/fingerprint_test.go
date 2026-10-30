package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kgorelov/findex/internal/fingerprint"
)

func TestPrintFingerprint(t *testing.T) {
	res := &fingerprint.Result{
		Path:             "/path/to/test.txt",
		SizeBytes:        12345,
		QuickFingerprint: []byte("01234567890123456789012345678901"),
		FullFingerprint:  []byte("abcdefabcdefabcdefabcdefabcdefab"),
		Algorithm:        fingerprint.AlgorithmSparse,
		Version:          fingerprint.AlgorithmVersion,
	}

	var buf bytes.Buffer
	if err := PrintFingerprint(&buf, res); err != nil {
		t.Fatalf("PrintFingerprint error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "/path/to/test.txt") || !strings.Contains(out, "blake3-sparse") {
		t.Errorf("expected output to contain path and algorithm, got:\n%s", out)
	}

	buf.Reset()
	if err := PrintFingerprintJSON(&buf, res); err != nil {
		t.Fatalf("PrintFingerprintJSON error: %v", err)
	}
	if !strings.Contains(buf.String(), `"path": "/path/to/test.txt"`) {
		t.Errorf("expected json to contain path, got:\n%s", buf.String())
	}
}
