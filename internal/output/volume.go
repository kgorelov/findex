package output

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// PrintVolumesTable writes a view of volumes to out according to opts.
func PrintVolumesTable(out io.Writer, volumes []*model.Volume, opts ...FormatOptions) error {
	var opt FormatOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	if len(volumes) == 0 {
		if opt.Format == "" || opt.Format == "table" {
			fmt.Fprintln(out, "No volumes registered.")
		}
		return nil
	}

	defaultCols := []string{"volume", "label", "fs_uuid", "fs_type", "mount_point", "host"}

	var rows []Row
	for _, v := range volumes {
		label := v.Label
		if label == "" {
			label = "-"
		}
		fsUUID := v.FSUUID
		if fsUUID == "" {
			fsUUID = "-"
		}
		fsType := v.FSType
		if fsType == "" {
			fsType = "-"
		}
		lastMount := v.LastMountPoint
		if lastMount == "" {
			lastMount = "-"
		}
		host := v.SourceHost
		if host == "" {
			host = "-"
		}

		rows = append(rows, Row{
			Volume:     v.VolumeID,
			Label:      label,
			FSUUID:     fsUUID,
			FSType:     fsType,
			MountPoint: lastMount,
			Host:       host,
		})
	}

	return PrintRows(out, rows, defaultCols, opt)
}

// PrintVolumeStats writes a detailed human-readable summary of a single volume to out.
func PrintVolumeStats(out io.Writer, stats *model.VolumeStats) error {
	v := stats.Volume

	fmt.Fprintf(out, "Volume ID:         %s\n", v.VolumeID)
	if v.Label != "" {
		fmt.Fprintf(out, "Label:             %s\n", v.Label)
	}
	if v.FSUUID != "" {
		fmt.Fprintf(out, "FS UUID:           %s\n", v.FSUUID)
	}
	if v.FSType != "" {
		fmt.Fprintf(out, "FS Type:           %s\n", v.FSType)
	}
	if v.DeviceID != "" {
		fmt.Fprintf(out, "Device:            %s\n", v.DeviceID)
	}
	if v.LastMountPoint != "" {
		fmt.Fprintf(out, "Last Mount Point:  %s\n", v.LastMountPoint)
	}
	if v.SourceHost != "" {
		fmt.Fprintf(out, "Source Host:       %s\n", v.SourceHost)
	}
	if v.SourceLabel != "" {
		fmt.Fprintf(out, "Source Label:      %s\n", v.SourceLabel)
	}
	fmt.Fprintf(out, "First Seen:        %s\n", v.FirstSeenAt.Format(time.RFC3339))
	if v.LastSeenAt != nil {
		fmt.Fprintf(out, "Last Seen:         %s\n", v.LastSeenAt.Format(time.RFC3339))
	}

	fmt.Fprintf(out, "\nScan Roots (%d):\n", stats.ScanRootsCount)
	if len(stats.ScanRoots) == 0 {
		fmt.Fprintf(out, "  (none)\n")
	} else {
		for _, r := range stats.ScanRoots {
			fmt.Fprintf(out, "  - %s\n", r)
		}
	}

	if stats.LastSuccessfulAt != nil {
		fmt.Fprintf(out, "Last Scan:         %s\n", stats.LastSuccessfulAt.Format(time.RFC3339))
	}

	fmt.Fprintf(out, "\nEntries:\n")
	fmt.Fprintf(out, "  Files:           %d (%s)\n", stats.FileCount, formatBytes(stats.TotalBytes))
	fmt.Fprintf(out, "  Directories:     %d\n", stats.DirectoryCount)
	fmt.Fprintf(out, "  Symlinks:        %d\n", stats.SymlinkCount)
	if stats.OtherCount > 0 {
		fmt.Fprintf(out, "  Other:           %d\n", stats.OtherCount)
	}

	return nil
}

// PrintJSON writes a formatted JSON representation of v to out.
func PrintJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	val := float64(b) / float64(div)
	return fmt.Sprintf("%.2f %s", val, units[exp])
}
