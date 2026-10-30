package output

import (
	"fmt"
	"io"
	"time"

	"github.com/kgorelov/findex/internal/scan"
)

// PrintScanSummary prints a human-readable summary of a scan operation to out.
func PrintScanSummary(out io.Writer, s *scan.Summary) error {
	fmt.Fprintln(out, "Scan Summary:")
	fmt.Fprintf(out, "  Scan ID:         %s\n", s.ScanID)
	fmt.Fprintf(out, "  Status:          %s\n", s.Status)
	fmt.Fprintf(out, "  Volume ID:       %s\n", s.VolumeID)
	fmt.Fprintf(out, "  Root Path:       %s\n", s.RootPath)
	fmt.Fprintf(out, "  Duration:        %s\n", s.Duration.Round(time.Millisecond))
	fmt.Fprintf(out, "  Files:           %d (%s)\n", s.FilesCount, formatBytes(s.TotalBytes))
	if s.FastHitsCount > 0 {
		fmt.Fprintf(out, "    Fast Hits:     %d (reused without re-hashing)\n", s.FastHitsCount)
	}
	if s.BytesHashed > 0 && s.Duration.Seconds() > 0 {
		throughput := float64(s.BytesHashed) / (1024 * 1024) / s.Duration.Seconds()
		fmt.Fprintf(out, "    Hashed:        %s (%.2f MB/s)\n", formatBytes(s.BytesHashed), throughput)
	}
	fmt.Fprintf(out, "  Directories:     %d\n", s.DirsCount)
	if s.SymlinksCount > 0 {
		fmt.Fprintf(out, "  Symlinks:        %d\n", s.SymlinksCount)
	}
	if s.OtherCount > 0 {
		fmt.Fprintf(out, "  Other:           %d\n", s.OtherCount)
	}
	if s.ReconciledCount > 0 {
		fmt.Fprintf(out, "  Reconciled:      %d stale entries removed\n", s.ReconciledCount)
	}
	if s.UnstableFilesCount > 0 {
		fmt.Fprintf(out, "  Unstable Files:  %d (modified during read)\n", s.UnstableFilesCount)
	}
	if s.ErrorsCount > 0 {
		fmt.Fprintf(out, "  Errors:          %d\n", s.ErrorsCount)
	}
	if s.ErrorMessage != "" {
		fmt.Fprintf(out, "  Error Message:   %s\n", s.ErrorMessage)
	}
	return nil
}

// PrintScanSummaryJSON prints machine-readable JSON summary of a scan operation to out.
func PrintScanSummaryJSON(out io.Writer, s *scan.Summary) error {
	return PrintJSON(out, s)
}
