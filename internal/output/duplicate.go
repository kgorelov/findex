package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kgorelov/findex/internal/model"
)

// PrintDuplicateSummary writes duplicate groups delimited by an empty line to out.
func PrintDuplicateSummary(out io.Writer, s *model.DuplicateSummary) error {
	if s == nil || len(s.Groups) == 0 {
		fmt.Fprintln(out, "No duplicate files or directories found.")
		return nil
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for i, group := range s.Groups {
		if i > 0 {
			// Delimit groups with an empty line
			fmt.Fprintln(out)
		}

		fmt.Fprintln(w, "TYPE\tSIZE\tVOLUME\tPATH\tFINGERPRINT")
		for _, e := range group.Entries {
			sizeStr := "-"
			if e.SizeBytes > 0 || e.EntryType == model.EntryTypeRegular {
				sizeStr = formatBytes(e.SizeBytes)
			}

			volStr := e.VolumeID
			if e.VolumeLabel != "" {
				volStr = fmt.Sprintf("%s (%s)", e.VolumeID, e.VolumeLabel)
			}

			fp := e.Fingerprint
			if len(fp) > 16 {
				fp = fp[:16] + "..."
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				e.EntryType.String(), sizeStr, volStr, e.RelativePath, fp)
		}
		_ = w.Flush()
	}

	return nil
}

// PrintDuplicateSummaryJSON writes machine-readable JSON duplicate summary to out.
func PrintDuplicateSummaryJSON(out io.Writer, s *model.DuplicateSummary) error {
	return PrintJSON(out, s)
}
