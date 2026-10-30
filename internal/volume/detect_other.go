//go:build !linux

package volume

import (
	"os"
	"path/filepath"
)

// DiscoverVolume is a fallback implementation for non-Linux operating systems.
func DiscoverVolume(targetPath string) (*DiscoveredVolume, error) {
	absPath, err := filepath.Abs(targetPath)
	if err != nil {
		return nil, err
	}

	vol := &DiscoveredVolume{
		MountPoint: absPath,
		SourceHost: getHostname(),
	}

	if sidecar, err := ReadSidecar(absPath); err == nil && sidecar != "" {
		vol.SidecarID = sidecar
	}

	return vol, nil
}

func getHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-host"
	}
	return h
}
