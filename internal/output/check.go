package output

import (
	"fmt"
	"io"

	"github.com/kgorelov/findex/internal/model"
)

// PrintCheckSummary writes a human-readable integrity and consistency report to out.
func PrintCheckSummary(out io.Writer, s *model.CheckSummary) error {
	fmt.Fprintln(out, "Database Diagnostic & Integrity Check:")
	fmt.Fprintf(out, "  Database Path:   %s\n", s.DatabasePath)
	if s.DatabaseBytes > 0 {
		fmt.Fprintf(out, "  Database Size:   %s\n", formatBytes(s.DatabaseBytes))
	}
	if s.WALBytes > 0 {
		fmt.Fprintf(out, "  WAL Size:        %s\n", formatBytes(s.WALBytes))
	}
	fmt.Fprintf(out, "  SQLite Engine:   %s\n", statusOK(s.IntegrityOK))
	fmt.Fprintf(out, "  Foreign Keys:    %s\n", statusOK(s.ForeignKeysOK))
	fmt.Fprintf(out, "  Metadata:        %s\n", statusOK(s.MetadataOK))
	fmt.Fprintf(out, "  Volumes Count:   %d\n", s.VolumesCount)
	fmt.Fprintf(out, "  Entries Count:   %d\n", s.EntriesCount)
	fmt.Fprintf(out, "  Scan Roots:      %d\n", s.ScanRootsCount)
	fmt.Fprintf(out, "  Scan Runs:       %d\n", s.ScanRunsCount)

	if len(s.Issues) > 0 {
		fmt.Fprintf(out, "\nDiagnostic Findings (%d errors, %d warnings):\n", s.ErrorsCount, s.WarningsCount)
		for i, issue := range s.Issues {
			fmt.Fprintf(out, "  [%d] [%s] (%s): %s\n", i+1, issue.Severity, issue.Category, issue.Message)
			if issue.Detail != "" {
				fmt.Fprintf(out, "      Detail: %s\n", issue.Detail)
			}
		}
	} else {
		fmt.Fprintln(out, "\nAll integrity and consistency checks passed successfully.")
	}

	return nil
}

// PrintCheckSummaryJSON writes machine-readable JSON check results to out.
func PrintCheckSummaryJSON(out io.Writer, s *model.CheckSummary) error {
	return PrintJSON(out, s)
}

func statusOK(ok bool) string {
	if ok {
		return "OK"
	}
	return "FAIL"
}
