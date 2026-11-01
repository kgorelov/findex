package output

import (
	"fmt"
	"io"

	"github.com/kgorelov/findex/internal/model"
)

// PrintDuplicateSummary writes duplicate groups to out according to opts.
// Groups are delimited with an empty line. The table header is printed only once at the top.
func PrintDuplicateSummary(out io.Writer, s *model.DuplicateSummary, opts ...FormatOptions) error {
	var opt FormatOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if s == nil || len(s.Groups) == 0 {
		if opt.Format == "" || opt.Format == "table" {
			fmt.Fprintln(out, "No duplicate files or directories found.")
		}
		return nil
	}

	if opt.Verbose {
		fmt.Fprintf(out, "Duplicate Summary (%d groups, %d entries):\n\n", s.TotalGroups, s.TotalEntries)
	}

	defaultCols := []string{"group_id", "type", "size", "volume", "path", "fingerprint"}

	var groups [][]Row
	for i, group := range s.Groups {
		groupID := i + 1
		var groupRows []Row
		for _, e := range group.Entries {
			volStr := e.VolumeID
			if (opt.Format == "" || opt.Format == "table") && e.VolumeLabel != "" {
				volStr = fmt.Sprintf("%s (%s)", e.VolumeID, e.VolumeLabel)
			}
			hasSize := e.SizeBytes > 0 || e.EntryType == model.EntryTypeRegular
			groupRows = append(groupRows, Row{
				GroupID:     groupID,
				Type:        e.EntryType.String(),
				SizeBytes:   e.SizeBytes,
				HasSize:     hasSize,
				Volume:      volStr,
				Path:        e.RelativePath,
				Fingerprint: cleanFingerprint(e.Fingerprint),
			})
		}
		groups = append(groups, groupRows)
	}

	return PrintGroupedRows(out, groups, defaultCols, opt)
}

// PrintDuplicateSummaryJSON writes machine-readable JSON duplicate summary to out.
func PrintDuplicateSummaryJSON(out io.Writer, s *model.DuplicateSummary) error {
	return PrintJSON(out, s)
}
