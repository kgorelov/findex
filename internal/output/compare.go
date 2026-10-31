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
	leftList := strings.Join(s.LeftVolumes, ", ")
	if leftList == "" {
		leftList = strings.Join(s.PresentVolumes, ", ")
	}
	rightList := strings.Join(s.RightVolumes, ", ")
	if rightList == "" {
		rightList = strings.Join(s.AbsentVolumes, ", ")
		if rightList == "" {
			rightList = "(none)"
		}
	}

	var modeLabel, sectionTitle string
	switch s.Mode {
	case "left":
		modeLabel = "--left (exist only in left set)"
		sectionTitle = fmt.Sprintf("\nLeft Only Entries (%d):", len(s.Items))
	case "right":
		modeLabel = "--right (exist only in right set)"
		sectionTitle = fmt.Sprintf("\nRight Only Entries (%d):", len(s.Items))
	case "common":
		modeLabel = "--common (exist in both sets)"
		sectionTitle = fmt.Sprintf("\nCommon Entries (%d):", len(s.Items))
	case "unique":
		modeLabel = "unique (exist only in volume set)"
		sectionTitle = fmt.Sprintf("\nUnique Entries (%d):", len(s.Items))
	default:
		modeLabel = s.Mode
		sectionTitle = fmt.Sprintf("\nEntries (%d):", len(s.Items))
	}

	fmt.Fprintf(out, "Volume Comparison (%s)\n", modeLabel)
	fmt.Fprintf(out, "  Left Volumes:          %s\n", leftList)
	fmt.Fprintf(out, "  Right Volumes:         %s\n", rightList)
	fmt.Fprintf(out, "  Type Filter:           %s\n", s.EntryTypeFilter)
	fmt.Fprintf(out, "  Total Source Content:  %d entries (%s)\n", s.TotalSourceEntries, formatBytes(s.TotalSourceBytes))
	fmt.Fprintf(out, "  Matching in Target:    %d entries (%s)\n", s.MatchingEntries, formatBytes(s.MatchingBytes))
	fmt.Fprintf(out, "  Absent from Target:    %d entries (%s)\n", s.AbsentEntries, formatBytes(s.AbsentBytes))

	if len(s.Items) > 0 {
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
