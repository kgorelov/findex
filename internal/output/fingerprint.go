package output

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/kgorelov/findex/internal/fingerprint"
)

// FingerprintDisplay represents the human and machine readable fingerprint output.
type FingerprintDisplay struct {
	Path             string `json:"path"`
	SizeBytes        int64  `json:"size_bytes"`
	FormattedSize    string `json:"formatted_size"`
	QuickFingerprint string `json:"quick_fingerprint"`
	FullFingerprint  string `json:"full_fingerprint,omitempty"`
	Algorithm        string `json:"algorithm"`
	Version          int    `json:"version"`
}

// PrintFingerprint writes a formatted summary of fingerprint results to out.
func PrintFingerprint(out io.Writer, res *fingerprint.Result) error {
	fmt.Fprintf(out, "Path:                %s\n", res.Path)
	fmt.Fprintf(out, "Size:                %d (%s)\n", res.SizeBytes, formatBytes(res.SizeBytes))
	fmt.Fprintf(out, "Algorithm:           %s (v%d)\n", res.Algorithm, res.Version)
	fmt.Fprintf(out, "Quick Fingerprint:   %s\n", hex.EncodeToString(res.QuickFingerprint))
	if len(res.FullFingerprint) > 0 {
		fmt.Fprintf(out, "Full Fingerprint:    %s\n", hex.EncodeToString(res.FullFingerprint))
	}
	return nil
}

// PrintFingerprintJSON writes formatted JSON of the fingerprint result to out.
func PrintFingerprintJSON(out io.Writer, res *fingerprint.Result) error {
	display := FingerprintDisplay{
		Path:             res.Path,
		SizeBytes:        res.SizeBytes,
		FormattedSize:    formatBytes(res.SizeBytes),
		QuickFingerprint: hex.EncodeToString(res.QuickFingerprint),
		Algorithm:        res.Algorithm,
		Version:          res.Version,
	}
	if len(res.FullFingerprint) > 0 {
		display.FullFingerprint = hex.EncodeToString(res.FullFingerprint)
	}
	return PrintJSON(out, display)
}
