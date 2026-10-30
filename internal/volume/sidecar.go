package volume

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// SidecarFileName is the persistent identifier filename stored at the root of a volume.
const SidecarFileName = ".findex-volume-id"

// ReadSidecar reads the volume ID from a sidecar file at rootPath.
// If the file does not exist, returns ("", nil).
func ReadSidecar(rootPath string) (string, error) {
	sidecarPath := filepath.Join(rootPath, SidecarFileName)
	data, err := os.ReadFile(sidecarPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("failed to read sidecar file at %q: %w", sidecarPath, err)
	}

	id := strings.TrimSpace(string(data))
	if id == "" {
		return "", nil
	}

	return id, nil
}

// WriteSidecar writes the volume ID to a sidecar file at rootPath.
func WriteSidecar(rootPath, volumeID string) error {
	if volumeID == "" {
		return fmt.Errorf("cannot write empty volume ID to sidecar")
	}

	sidecarPath := filepath.Join(rootPath, SidecarFileName)
	content := []byte(strings.TrimSpace(volumeID) + "\n")
	if err := os.WriteFile(sidecarPath, content, 0644); err != nil {
		return fmt.Errorf("failed to write sidecar file at %q: %w", sidecarPath, err)
	}

	return nil
}

// EnsureVolumeID checks for an existing sidecar ID or creates a new persistent UUID sidecar.
func EnsureVolumeID(rootPath string) (string, error) {
	existing, err := ReadSidecar(rootPath)
	if err != nil {
		return "", err
	}
	if existing != "" {
		return existing, nil
	}

	newID := uuid.NewString()
	if err := WriteSidecar(rootPath, newID); err != nil {
		return "", err
	}

	return newID, nil
}
