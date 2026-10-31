package output

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/kgorelov/findex/internal/model"
)

// PrintCompareSummary writes a human-readable comparison report to out.
func PrintCompareSummary(out io.Writer, s *model.CompareSummary) error {
	presentList := strings.Join(s.PresentVolumes, ", ")
	absentList := strings.Join(s.AbsentVolumes, ", ")
	if absentList == "" {
		absentList = "(none)"
	}

	modeLabel := s.Mode
	if modeLabel == "absent" {
		modeLabel = "Present in source, absent from target"
	} else if modeLabel == "common" {
		modeLabel = "Common content (present in both sets)"
	} else if modeLabel == "unique" {
		modeLabel = "Unique to volume"
	}

	fmt.Fprintf(out, "Volume Comparison (%s)\n", modeLabel)
	fmt.Fprintf(out, "  Present Volumes:       %s\n", presentList)
	fmt.Fprintf(out, "  Absent Volumes:        %s\n", absentList)
	fmt.Fprintf(out, "  Type Filter:           %s\n", s.EntryTypeFilter)
	fmt.Fprintf(out, "  Total Source Content:  %d entries (%s)\n", s.TotalSourceEntries, formatBytes(s.TotalSourceBytes))
	fmt.Fprintf(out, "  Matching in Target:    %d entries (%s)\n", s.MatchingEntries, formatBytes(s.MatchingBytes))
	fmt.Fprintf(out, "  Absent from Target:    %d entries (%s)\n", s.AbsentEntries, formatBytes(s.AbsentBytes))

	if len(s.Items) > 0 {
		var sectionTitle string
		if s.Mode == "common" {
			sectionTitle = fmt.Sprintf("\nCommon Entries (%d):", len(s.Items))
		} else if s.Mode == "unique" {
			sectionTitle = fmt.Sprintf("\nUnique Entries (%d):", len(s.Items))
		} else {
			sectionTitle = fmt.Sprintf("\nAbsent Entries (%d):", len(s.Items))
		}
		fmt.Fprintln(out, sectionTitle)

		w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "TYPE\tSIZE\tVOLUME\tPATH\tFINGERPRINT")
		for _, item := range s.Items {
			sizeStr := "-"
			if item.EntryType == model.EntryTypeRegular {
				sizeStr = formatBytes(item.SizeBytes)
			}
			fp := item.Fingerprint
			if len(fp) > 16 {
				fp = fp[:16] + "..."
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				item.EntryType.String(), sizeStr, item.VolumeID, item.RelativePath, fp)

			if len(item.MatchingLocations) > 0 {
				for _, loc := range item.MatchingLocations {
					fmt.Fprintf(w, "  ->\t\t%s\t%s\t(match)\n", loc.VolumeID, loc.RelativePath)
				}
			}
		}
		_ = w.Flush()
	}

	return nil
}

// PrintCompareSummaryJSON writes machine-readable JSON comparison summary to out.
func PrintCompareSummaryJSON(out io.Writer, s *model.CompareSummary) error {
	return PrintJSON(out, s)
}
