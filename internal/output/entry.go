package output

import (
	"encoding/hex"
	"io"

	"github.com/kgorelov/findex/internal/model"
)

// PrintEntriesTable writes indexed entries to out according to opts.
func PrintEntriesTable(out io.Writer, entries []*model.Entry, opts ...FormatOptions) error {
	var opt FormatOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if len(entries) == 0 {
		return nil
	}

	defaultCols := []string{"type", "size", "path", "fingerprint"}

	var rows []Row
	for _, e := range entries {
		var fp string
		if e.EntryType == model.EntryTypeDirectory {
			if len(e.DirectoryFingerprint) > 0 {
				fp = hex.EncodeToString(e.DirectoryFingerprint[:8])
			} else {
				fp = "-"
			}
		} else if len(e.QuickFingerprint) > 0 {
			fp = hex.EncodeToString(e.QuickFingerprint[:8])
		} else {
			fp = "-"
		}

		hasSize := e.EntryType == model.EntryTypeRegular
		rows = append(rows, Row{
			Type:        e.EntryType.String(),
			SizeBytes:   e.SizeBytes,
			HasSize:     hasSize,
			Volume:      e.VolumeID,
			Path:        e.RelativePath,
			Fingerprint: fp,
		})
	}

	return PrintRows(out, rows, defaultCols, opt)
}
