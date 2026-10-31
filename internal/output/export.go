package output

import (
	"fmt"
	"io"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// PrintExportSummary writes a human-readable summary of an export operation to out.
func PrintExportSummary(out io.Writer, s *model.ExportSummary) error {
	fmt.Fprintln(out, "Inventory Export Summary:")
	fmt.Fprintf(out, "  Destination:     %s\n", s.Destination)
	fmt.Fprintf(out, "  Compressed:      %v\n", s.Compressed)
	fmt.Fprintf(out, "  Duration:        %s\n", s.Duration.Round(time.Millisecond))
	fmt.Fprintf(out, "  Volumes:         %d\n", s.VolumesCount)
	fmt.Fprintf(out, "  Entries:         %d\n", s.EntriesCount)
	fmt.Fprintf(out, "  Scan Roots:      %d\n", s.ScanRootsCount)
	fmt.Fprintf(out, "  Scan Runs:       %d\n", s.ScanRunsCount)
	fmt.Fprintf(out, "  Bytes Written:   %s\n", formatBytes(s.BytesWritten))
	return nil
}

// PrintExportSummaryJSON writes machine-readable JSON summary of an export operation to out.
func PrintExportSummaryJSON(out io.Writer, s *model.ExportSummary) error {
	return PrintJSON(out, s)
}

// PrintImportSummary writes a human-readable summary of an import operation to out.
func PrintImportSummary(out io.Writer, s *model.ImportSummary) error {
	fmt.Fprintln(out, "Inventory Import Summary:")
	fmt.Fprintf(out, "  Source:          %s\n", s.Source)
	fmt.Fprintf(out, "  Duration:        %s\n", s.Duration.Round(time.Millisecond))
	fmt.Fprintf(out, "  Volumes:         %d\n", s.VolumesImported)
	fmt.Fprintf(out, "  Entries New:     %d\n", s.EntriesInserted)
	fmt.Fprintf(out, "  Entries Updated: %d\n", s.EntriesUpdated)
	fmt.Fprintf(out, "  Entries Same:    %d\n", s.EntriesIdentical)
	fmt.Fprintf(out, "  Scan Roots:      %d\n", s.ScanRootsCount)
	fmt.Fprintf(out, "  Scan Runs:       %d\n", s.ScanRunsCount)
	return nil
}

// PrintImportSummaryJSON writes machine-readable JSON summary of an import operation to out.
func PrintImportSummaryJSON(out io.Writer, s *model.ImportSummary) error {
	return PrintJSON(out, s)
}
