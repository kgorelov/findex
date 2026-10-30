package output

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// PrintVolumesTable writes a tabular view of volumes to out.
func PrintVolumesTable(out io.Writer, volumes []*model.Volume) error {
	if len(volumes) == 0 {
		fmt.Fprintln(out, "No volumes registered.")
		return nil
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VOLUME ID\tLABEL\tFS UUID\tFS TYPE\tLAST MOUNT\tHOST\tFIRST SEEN")

	for _, v := range volumes {
		firstSeen := v.FirstSeenAt.Format("2006-01-02 15:04")
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

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			v.VolumeID, label, fsUUID, fsType, lastMount, host, firstSeen)
	}

	return w.Flush()
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
