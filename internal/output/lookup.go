package output

import (
	"fmt"
	"io"

	"github.com/kgorelov/findex/internal/model"
)

// PrintLookupSummary writes a lookup report to out according to opts.
// Additional info and stats are only printed when opts.Verbose is true.
func PrintLookupSummary(out io.Writer, s *model.LookupSummary, opts ...FormatOptions) error {
	var opt FormatOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if opt.Verbose {
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
	}

	if s == nil || s.MatchesCount == 0 {
		if opt.Format == "" || opt.Format == "table" {
			fmt.Fprintln(out, "No matching entries found in database.")
		}
		return nil
	}

	defaultCols := []string{"type", "size", "volume", "path", "match"}

	var rows []Row
	for _, m := range s.Matches {
		volStr := m.VolumeID
		if (opt.Format == "" || opt.Format == "table") && m.VolumeLabel != "" {
			volStr = fmt.Sprintf("%s (%s)", m.VolumeID, m.VolumeLabel)
		}
		hasSize := m.EntryType == model.EntryTypeRegular
		rows = append(rows, Row{
			Type:        m.EntryType.String(),
			SizeBytes:   m.SizeBytes,
			HasSize:     hasSize,
			Volume:      volStr,
			Path:        m.RelativePath,
			Fingerprint: cleanFingerprint(m.Fingerprint),
			Match:       m.MatchType,
		})
	}

	return PrintRows(out, rows, defaultCols, opt)
}

// PrintLookupSummaryJSON writes machine-readable JSON lookup summary to out.
func PrintLookupSummaryJSON(out io.Writer, s *model.LookupSummary) error {
	return PrintJSON(out, s)
}
