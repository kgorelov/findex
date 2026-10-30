package output

import (
	"encoding/hex"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/kgorelov/findex/internal/model"
)

// PrintEntriesTable writes a tabular view of indexed entries to out.
func PrintEntriesTable(out io.Writer, entries []*model.Entry) error {
	if len(entries) == 0 {
		fmt.Fprintln(out, "No entries found.")
		return nil
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tSIZE\tPATH\tFINGERPRINT")

	for _, e := range entries {
		var fp string
		if e.EntryType == model.EntryTypeDirectory {
			if len(e.DirectoryFingerprint) > 0 {
				fp = hex.EncodeToString(e.DirectoryFingerprint[:8]) + "..."
			} else {
				fp = "-"
			}
		} else if len(e.QuickFingerprint) > 0 {
			fp = hex.EncodeToString(e.QuickFingerprint[:8]) + "..."
		} else {
			fp = "-"
		}

		sizeStr := "-"
		if e.EntryType == model.EntryTypeRegular {
			sizeStr = formatBytes(e.SizeBytes)
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			e.EntryType.String(), sizeStr, e.RelativePath, fp)
	}

	return w.Flush()
}
