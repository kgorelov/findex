package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// FormatOptions defines formatting and column selection options for command output.
type FormatOptions struct {
	Format  string   // "table" (default), "tsv", "csv", "jsonl"
	Columns []string // optional list of column names, e.g. ["volume", "path", "size", "type"]
	Verbose bool     // whether to display additional information and stats
}

// NormalizeFormat validates and normalizes the format name.
// Accepts: "table", "tsv", "csv", "cvs" (alias for csv), "jsonl" (or "ndjson").
func NormalizeFormat(fmtStr string) (string, error) {
	norm := strings.ToLower(strings.TrimSpace(fmtStr))
	switch norm {
	case "", "table":
		return "table", nil
	case "tsv":
		return "tsv", nil
	case "csv", "cvs":
		return "csv", nil
	case "jsonl", "ndjson", "json-lines":
		return "jsonl", nil
	default:
		return "", fmt.Errorf("invalid format %q: expected table, tsv, csv, or jsonl", fmtStr)
	}
}

// ParseColumns splits a comma-separated column specification and normalizes each column name.
func ParseColumns(colStr string) []string {
	colStr = strings.TrimSpace(colStr)
	if colStr == "" {
		return nil
	}
	raw := strings.Split(colStr, ",")
	var cols []string
	for _, c := range raw {
		norm := NormalizeColumnName(c)
		if norm != "" {
			cols = append(cols, norm)
		}
	}
	return cols
}

// NormalizeColumnName normalizes user-provided column identifiers and synonyms.
func NormalizeColumnName(name string) string {
	clean := strings.ToLower(strings.TrimSpace(name))
	switch clean {
	case "group", "group_id", "id", "gid":
		return "group_id"
	case "type", "entry_type":
		return "type"
	case "size", "size_bytes", "bytes":
		return "size"
	case "volume", "volume_id", "vol", "vol_id":
		return "volume"
	case "path", "relative_path", "filepath", "file":
		return "path"
	case "fingerprint", "fp", "hash":
		return "fingerprint"
	case "match", "match_type", "status":
		return "match"
	case "label", "volume_label":
		return "label"
	case "host", "source_host":
		return "host"
	case "fs_type", "fstype":
		return "fs_type"
	case "mount_point", "mount", "last_mount_point":
		return "mount_point"
	case "fs_uuid", "uuid":
		return "fs_uuid"
	default:
		return clean
	}
}

// ColumnHeader returns the user-facing header label for a normalized column name.
func ColumnHeader(col string) string {
	switch col {
	case "group_id":
		return "GROUP_ID"
	case "fs_type":
		return "FS_TYPE"
	case "mount_point":
		return "MOUNT_POINT"
	case "fs_uuid":
		return "FS_UUID"
	default:
		return strings.ToUpper(col)
	}
}

// cleanFingerprint truncates a fingerprint string to 16 hex characters without appending "...".
func cleanFingerprint(fp string) string {
	if len(fp) > 16 {
		return fp[:16]
	}
	return fp
}

// Row represents a unified data row that can be formatted across commands.
type Row struct {
	GroupID     int
	Type        string
	SizeBytes   int64
	HasSize     bool
	Volume      string
	Path        string
	Fingerprint string
	Match       string
	Label       string
	Host        string
	FSType      string
	MountPoint  string
	FSUUID      string
	Extra       map[string]string
}

// Value returns the string representation of a column in the row.
func (r Row) Value(col string) string {
	switch col {
	case "group_id":
		if r.GroupID > 0 {
			return fmt.Sprintf("%d", r.GroupID)
		}
		return "-"
	case "type":
		return r.Type
	case "size":
		if r.HasSize {
			return formatBytes(r.SizeBytes)
		}
		return "-"
	case "volume":
		return r.Volume
	case "path":
		return r.Path
	case "fingerprint":
		return r.Fingerprint
	case "match":
		return r.Match
	case "label":
		return r.Label
	case "host":
		return r.Host
	case "fs_type":
		return r.FSType
	case "mount_point":
		return r.MountPoint
	case "fs_uuid":
		return r.FSUUID
	default:
		if r.Extra != nil {
			if v, ok := r.Extra[col]; ok {
				return v
			}
		}
		return ""
	}
}

// ToJSONMap converts the row into a JSON-compatible map according to the selected columns.
func (r Row) ToJSONMap(columns []string) map[string]any {
	m := make(map[string]any, len(columns))
	for _, col := range columns {
		switch col {
		case "group_id":
			if r.GroupID > 0 {
				m[col] = r.GroupID
			} else {
				m[col] = nil
			}
		case "type":
			m[col] = r.Type
		case "size":
			if r.HasSize {
				m["size"] = r.SizeBytes
				m["size_human"] = formatBytes(r.SizeBytes)
			} else {
				m["size"] = nil
				m["size_human"] = "-"
			}
		case "volume":
			m[col] = r.Volume
		case "path":
			m[col] = r.Path
		case "fingerprint":
			m[col] = r.Fingerprint
		case "match":
			m[col] = r.Match
		case "label":
			m[col] = r.Label
		case "host":
			m[col] = r.Host
		case "fs_type":
			m[col] = r.FSType
		case "mount_point":
			m[col] = r.MountPoint
		case "fs_uuid":
			m[col] = r.FSUUID
		default:
			if r.Extra != nil {
				if v, ok := r.Extra[col]; ok {
					m[col] = v
				}
			}
		}
	}
	return m
}

// resolveColumns determines the active columns given user options and default columns.
func resolveColumns(opts FormatOptions, defaultCols []string) []string {
	if len(opts.Columns) > 0 {
		return opts.Columns
	}
	return defaultCols
}

// PrintRows formats and writes a flat list of rows to out.
func PrintRows(out io.Writer, rows []Row, defaultCols []string, opts FormatOptions) error {
	cols := resolveColumns(opts, defaultCols)
	format := opts.Format
	if format == "" {
		format = "table"
	}

	switch format {
	case "table":
		colWidths := make([]int, len(cols))
		for j, c := range cols {
			colWidths[j] = len(ColumnHeader(c))
		}
		for _, r := range rows {
			for j, c := range cols {
				if l := len(r.Value(c)); l > colWidths[j] {
					colWidths[j] = l
				}
			}
		}

		for j, c := range cols {
			hdr := ColumnHeader(c)
			if j < len(cols)-1 {
				fmt.Fprintf(out, "%-*s  ", colWidths[j], hdr)
			} else {
				fmt.Fprintln(out, hdr)
			}
		}

		for _, r := range rows {
			for j, c := range cols {
				val := r.Value(c)
				if j < len(cols)-1 {
					fmt.Fprintf(out, "%-*s  ", colWidths[j], val)
				} else {
					fmt.Fprintln(out, val)
				}
			}
		}
		return nil

	case "tsv":
		var headers []string
		for _, c := range cols {
			headers = append(headers, ColumnHeader(c))
		}
		fmt.Fprintln(out, strings.Join(headers, "\t"))

		for _, r := range rows {
			var vals []string
			for _, c := range cols {
				vals = append(vals, r.Value(c))
			}
			fmt.Fprintln(out, strings.Join(vals, "\t"))
		}
		return nil

	case "csv":
		cw := csv.NewWriter(out)
		var headers []string
		for _, c := range cols {
			headers = append(headers, ColumnHeader(c))
		}
		if err := cw.Write(headers); err != nil {
			return err
		}

		for _, r := range rows {
			var vals []string
			for _, c := range cols {
				vals = append(vals, r.Value(c))
			}
			if err := cw.Write(vals); err != nil {
				return err
			}
		}
		cw.Flush()
		return cw.Error()

	case "jsonl":
		enc := json.NewEncoder(out)
		for _, r := range rows {
			if err := enc.Encode(r.ToJSONMap(cols)); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintGroupedRows formats and writes grouped rows to out, separating groups with an empty line.
// The header row is printed only once at the beginning of the output.
func PrintGroupedRows(out io.Writer, groups [][]Row, defaultCols []string, opts FormatOptions) error {
	cols := resolveColumns(opts, defaultCols)
	format := opts.Format
	if format == "" {
		format = "table"
	}

	switch format {
	case "table":
		colWidths := make([]int, len(cols))
		for j, c := range cols {
			colWidths[j] = len(ColumnHeader(c))
		}
		for _, g := range groups {
			for _, r := range g {
				for j, c := range cols {
					if l := len(r.Value(c)); l > colWidths[j] {
						colWidths[j] = l
					}
				}
			}
		}

		for j, c := range cols {
			hdr := ColumnHeader(c)
			if j < len(cols)-1 {
				fmt.Fprintf(out, "%-*s  ", colWidths[j], hdr)
			} else {
				fmt.Fprintln(out, hdr)
			}
		}

		for i, g := range groups {
			if i > 0 {
				fmt.Fprintln(out)
			}
			for _, r := range g {
				for j, c := range cols {
					val := r.Value(c)
					if j < len(cols)-1 {
						fmt.Fprintf(out, "%-*s  ", colWidths[j], val)
					} else {
						fmt.Fprintln(out, val)
					}
				}
			}
		}
		return nil

	case "tsv":
		var headers []string
		for _, c := range cols {
			headers = append(headers, ColumnHeader(c))
		}
		fmt.Fprintln(out, strings.Join(headers, "\t"))

		for i, g := range groups {
			if i > 0 {
				fmt.Fprintln(out)
			}
			for _, r := range g {
				var vals []string
				for _, c := range cols {
					vals = append(vals, r.Value(c))
				}
				fmt.Fprintln(out, strings.Join(vals, "\t"))
			}
		}
		return nil

	case "csv":
		cw := csv.NewWriter(out)
		var headers []string
		for _, c := range cols {
			headers = append(headers, ColumnHeader(c))
		}
		if err := cw.Write(headers); err != nil {
			return err
		}

		for i, g := range groups {
			if i > 0 {
				cw.Flush()
				if _, err := out.Write([]byte("\n")); err != nil {
					return err
				}
			}
			for _, r := range g {
				var vals []string
				for _, c := range cols {
					vals = append(vals, r.Value(c))
				}
				if err := cw.Write(vals); err != nil {
					return err
				}
			}
		}
		cw.Flush()
		return cw.Error()

	case "jsonl":
		enc := json.NewEncoder(out)
		for _, g := range groups {
			for _, r := range g {
				if err := enc.Encode(r.ToJSONMap(cols)); err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}
