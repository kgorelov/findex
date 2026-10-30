package volume

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/kgorelov/findex/internal/model"
)

// DiscoveredVolume holds hardware and filesystem attributes discovered from the OS.
type DiscoveredVolume struct {
	MountPoint string
	DevicePath string
	FSType     string
	FSUUID     string
	Label      string
	SourceHost string
	SidecarID  string
}

// VolumeOptions specifies user overrides and settings during volume resolution.
type VolumeOptions struct {
	VolumeID       string
	Label          string
	SourceHost     string
	SourceLabel    string
	DisableSidecar bool
}

// ResolveVolume inspects the filesystem at targetPath and constructs a model.Volume
// applying the identity precedence:
// 1. Explicit user volume ID (from options)
// 2. Native filesystem UUID
// 3. Persistent sidecar file (.findex-volume-id)
// 4. Fallback generated UUID (persisted to sidecar unless disabled)
func ResolveVolume(targetPath string, opts VolumeOptions) (*model.Volume, error) {
	absPath, err := filepath.Abs(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to determine absolute path for %q: %w", targetPath, err)
	}

	discovered, err := DiscoverVolume(absPath)
	if err != nil {
		return nil, fmt.Errorf("filesystem discovery failed for %q: %w", absPath, err)
	}

	volID := opts.VolumeID
	if volID == "" {
		if discovered.FSUUID != "" {
			volID = discovered.FSUUID
		} else if discovered.SidecarID != "" {
			volID = discovered.SidecarID
		} else {
			// Fallback: generate a persistent UUID
			newID := uuid.NewString()
			volID = newID

			if !opts.DisableSidecar && discovered.MountPoint != "" {
				// Try writing sidecar file; if read-only filesystem, ignore write error
				_ = WriteSidecar(discovered.MountPoint, newID)
			}
		}
	}

	label := opts.Label
	if label == "" {
		label = discovered.Label
	}

	sourceHost := opts.SourceHost
	if sourceHost == "" {
		sourceHost = discovered.SourceHost
	}

	now := time.Now().UTC()
	vol := &model.Volume{
		VolumeID:       volID,
		FSUUID:         discovered.FSUUID,
		FSType:         discovered.FSType,
		Label:          label,
		DeviceID:       discovered.DevicePath,
		LastMountPoint: discovered.MountPoint,
		SourceHost:     sourceHost,
		SourceLabel:    opts.SourceLabel,
		FirstSeenAt:    now,
		LastSeenAt:     &now,
	}

	return vol, nil
}
