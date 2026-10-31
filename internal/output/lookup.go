package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kgorelov/findex/internal/model"
)

// PrintLookupSummary writes a human-readable lookup report to out.
func PrintLookupSummary(out io.Writer, s *model.LookupSummary) error {
	if s.TargetType == "filepath" {
		fmt.Fprintf(out, "Filepath Lookup: %s\n", s.FilePath)
		if s.FingerprintType != "" {
			fmt.Fprintf(out, "  Calculated Fingerprint: %s (%s)\n", s.Fingerprint, s.FingerprintType)
		} else {
			fmt.Fprintf(out, "  Calculated Fingerprint: %s\n", s.Fingerprint)
		}
	} else {
		fmt.Fprintf(out, "Fingerprint Lookup: %s\n", s.Fingerprint)
	}

	if s.MatchesCount == 0 {
		fmt.Fprintln(out, "No matching entries found in database.")
		return nil
	}

	fmt.Fprintf(out, "Matches found: %d\n\n", s.MatchesCount)

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tSIZE\tVOLUME\tPATH\tMATCH")
	for _, m := range s.Matches {
		sizeStr := "-"
		if m.EntryType == model.EntryTypeRegular {
			sizeStr = formatBytes(m.SizeBytes)
		}
		volStr := m.VolumeID
		if m.VolumeLabel != "" {
			volStr = fmt.Sprintf("%s (%s)", m.VolumeID, m.VolumeLabel)
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			m.EntryType.String(), sizeStr, volStr, m.RelativePath, m.MatchType)
	}
	_ = w.Flush()

	return nil
}

// PrintLookupSummaryJSON writes machine-readable JSON lookup summary to out.
func PrintLookupSummaryJSON(out io.Writer, s *model.LookupSummary) error {
	return PrintJSON(out, s)
}
