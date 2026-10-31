package output

import (
	"fmt"
	"io"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// PrintMergeSummary writes a human-readable summary of a merge operation to out.
func PrintMergeSummary(out io.Writer, r *model.MergeResult) error {
	fmt.Fprintln(out, "Merge Summary:")
	fmt.Fprintf(out, "  Merge ID:                %s\n", r.MergeID)
	fmt.Fprintf(out, "  Status:                  %s\n", r.Status)
	fmt.Fprintf(out, "  Source Database:         %s\n", r.SourcePath)
	fmt.Fprintf(out, "  Duration:                %s\n", r.Duration.Round(time.Millisecond))
	fmt.Fprintf(out, "  Volumes Merged:          %d\n", r.VolumesMerged)
	fmt.Fprintf(out, "  Entries Inserted:        %d\n", r.EntriesInserted)
	fmt.Fprintf(out, "  Entries Updated:         %d\n", r.EntriesUpdated)
	fmt.Fprintf(out, "  Entries Identical:       %d\n", r.EntriesIdentical)
	if r.SameContentDiffLocation > 0 {
		fmt.Fprintf(out, "  Same Content Diff Path:  %d\n", r.SameContentDiffLocation)
	}
	if r.ConflictsCount > 0 {
		fmt.Fprintf(out, "  Conflicts Detected:      %d\n", r.ConflictsCount)
		fmt.Fprintln(out, "\nConflicts Detail:")
		for i, c := range r.Conflicts {
			loc := c.VolumeID
			if c.RelativePath != "" {
				loc = fmt.Sprintf("%s:%s", c.VolumeID, c.RelativePath)
			}
			fmt.Fprintf(out, "  [%d] %s (%s):\n", i+1, loc, c.ConflictKind)
			if c.LocalDetail != "" {
				fmt.Fprintf(out, "      Local:      %s\n", c.LocalDetail)
			}
			if c.RemoteDetail != "" {
				fmt.Fprintf(out, "      Remote:     %s\n", c.RemoteDetail)
			}
			fmt.Fprintf(out, "      Resolution: %s\n", c.Resolution)
		}
	}
	if r.ErrorMessage != "" {
		fmt.Fprintf(out, "  Error:                   %s\n", r.ErrorMessage)
	}
	return nil
}

// PrintMergeSummaryJSON writes machine-readable JSON summary of a merge operation to out.
func PrintMergeSummaryJSON(out io.Writer, r *model.MergeResult) error {
	return PrintJSON(out, r)
}
