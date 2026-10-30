//go:build linux

package volume

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type mountEntry struct {
	device     string
	mountPoint string
	fsType     string
}

// DiscoverVolume inspects the filesystem at targetPath on Linux.
func DiscoverVolume(targetPath string) (*DiscoveredVolume, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(targetPath, &stat); err != nil {
		return nil, fmt.Errorf("failed to stat target path %q: %w", targetPath, err)
	}

	mounts, err := parseProcMounts()
	if err != nil {
		return nil, fmt.Errorf("failed to read /proc/mounts: %w", err)
	}

	// Match mount point by device number and longest prefix
	var bestMount *mountEntry
	bestLen := -1

	cleanTarget := filepath.Clean(targetPath)

	for i := range mounts {
		m := &mounts[i]
		var mStat syscall.Stat_t
		if err := syscall.Stat(m.mountPoint, &mStat); err == nil {
			if mStat.Dev == stat.Dev {
				if cleanTarget == m.mountPoint || strings.HasPrefix(cleanTarget, m.mountPoint+"/") || m.mountPoint == "/" {
					if len(m.mountPoint) > bestLen {
						bestMount = m
						bestLen = len(m.mountPoint)
					}
				}
			}
		}
	}

	// Fallback to lexical prefix match if dev check didn't match (e.g. certain overlayfs or chroot environments)
	if bestMount == nil {
		for i := range mounts {
			m := &mounts[i]
			if cleanTarget == m.mountPoint || strings.HasPrefix(cleanTarget, m.mountPoint+"/") || m.mountPoint == "/" {
				if len(m.mountPoint) > bestLen {
					bestMount = m
					bestLen = len(m.mountPoint)
				}
			}
		}
	}

	discovered := &DiscoveredVolume{
		MountPoint: cleanTarget,
		SourceHost: getHostname(),
	}

	if bestMount != nil {
		discovered.MountPoint = bestMount.mountPoint
		discovered.DevicePath = bestMount.device
		discovered.FSType = bestMount.fsType
	}

	// Try reading native UUID and Label from /dev/disk
	if discovered.DevicePath != "" {
		discovered.FSUUID = findDiskUUID(discovered.DevicePath)
		discovered.Label = findDiskLabel(discovered.DevicePath)
	}

	// Try reading persistent sidecar if present at mount point
	if discovered.MountPoint != "" {
		if sidecar, err := ReadSidecar(discovered.MountPoint); err == nil && sidecar != "" {
			discovered.SidecarID = sidecar
		}
	}

	return discovered, nil
}

func parseProcMounts() ([]mountEntry, error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []mountEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 3 {
			entries = append(entries, mountEntry{
				device:     unescapeOctal(fields[0]),
				mountPoint: unescapeOctal(fields[1]),
				fsType:     unescapeOctal(fields[2]),
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

func unescapeOctal(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if val, err := strconv.ParseInt(s[i+1:i+4], 8, 32); err == nil {
				b.WriteByte(byte(val))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func findDiskUUID(devicePath string) string {
	cleanDev, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		cleanDev = devicePath
	}

	entries, err := os.ReadDir("/dev/disk/by-uuid")
	if err != nil {
		return ""
	}

	for _, entry := range entries {
		linkPath := filepath.Join("/dev/disk/by-uuid", entry.Name())
		target, err := filepath.EvalSymlinks(linkPath)
		if err == nil && target == cleanDev {
			return entry.Name()
		}
	}
	return ""
}

func findDiskLabel(devicePath string) string {
	cleanDev, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		cleanDev = devicePath
	}

	entries, err := os.ReadDir("/dev/disk/by-label")
	if err != nil {
		return ""
	}

	for _, entry := range entries {
		linkPath := filepath.Join("/dev/disk/by-label", entry.Name())
		target, err := filepath.EvalSymlinks(linkPath)
		if err == nil && target == cleanDev {
			return entry.Name()
		}
	}
	return ""
}

func getHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}
