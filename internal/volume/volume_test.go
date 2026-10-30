package volume

import (
	"os"
	"testing"
)

func TestSidecar(t *testing.T) {
	tempDir := t.TempDir()

	// Initial check: no sidecar
	id, err := ReadSidecar(tempDir)
	if err != nil {
		t.Fatalf("unexpected error reading non-existent sidecar: %v", err)
	}
	if id != "" {
		t.Errorf("expected empty string, got %q", id)
	}

	// Write sidecar
	expectedID := "custom-volume-uuid-456"
	if err := WriteSidecar(tempDir, expectedID); err != nil {
		t.Fatalf("failed to write sidecar: %v", err)
	}

	// Read sidecar
	id, err = ReadSidecar(tempDir)
	if err != nil {
		t.Fatalf("unexpected error reading sidecar: %v", err)
	}
	if id != expectedID {
		t.Errorf("expected %q, got %q", expectedID, id)
	}

	// EnsureVolumeID should return existing
	ensuredID, err := EnsureVolumeID(tempDir)
	if err != nil {
		t.Fatalf("failed ensuring volume ID: %v", err)
	}
	if ensuredID != expectedID {
		t.Errorf("expected %q, got %q", expectedID, ensuredID)
	}

	// In a new directory, EnsureVolumeID creates a new UUID
	newDir := t.TempDir()
	generatedID, err := EnsureVolumeID(newDir)
	if err != nil {
		t.Fatalf("failed creating new volume ID: %v", err)
	}
	if generatedID == "" {
		t.Fatalf("expected non-empty generated ID")
	}

	// Verify it wrote the file
	readBack, err := ReadSidecar(newDir)
	if err != nil {
		t.Fatalf("failed reading back sidecar: %v", err)
	}
	if readBack != generatedID {
		t.Errorf("expected %q, got %q", generatedID, readBack)
	}
}

func TestResolveVolume(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Resolve with default options: should generate UUID and write sidecar
	vol1, err := ResolveVolume(tempDir, VolumeOptions{})
	if err != nil {
		t.Fatalf("failed to resolve volume: %v", err)
	}
	if vol1.VolumeID == "" {
		t.Fatalf("expected non-empty VolumeID")
	}

	// Check if sidecar was written (if tempDir is on mount root or fallback)
	// Even if tempDir is not mount root, let's test explicit options
	customID := "manual-vol-id-789"
	vol2, err := ResolveVolume(tempDir, VolumeOptions{
		VolumeID:    customID,
		Label:       "TestDisk",
		SourceLabel: "Test Lab",
	})
	if err != nil {
		t.Fatalf("failed to resolve volume with custom options: %v", err)
	}
	if vol2.VolumeID != customID {
		t.Errorf("expected %q, got %q", customID, vol2.VolumeID)
	}
	if vol2.Label != "TestDisk" {
		t.Errorf("expected label 'TestDisk', got %q", vol2.Label)
	}
	if vol2.SourceLabel != "Test Lab" {
		t.Errorf("expected source label 'Test Lab', got %q", vol2.SourceLabel)
	}
}

func TestUnescapeOctal(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/media/My\\040Drive", "/media/My Drive"},
		{"/normal/path", "/normal/path"},
		{"/tab\\011here", "/tab\there"},
	}

	for _, tc := range tests {
		actual := unescapeOctal(tc.input)
		if actual != tc.expected {
			t.Errorf("unescapeOctal(%q) = %q, expected %q", tc.input, actual, tc.expected)
		}
	}
}

func TestDiscoverVolume_RealRoot(t *testing.T) {
	// Discover current working directory or /
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get cwd: %v", err)
	}

	disc, err := DiscoverVolume(cwd)
	if err != nil {
		t.Fatalf("DiscoverVolume failed on cwd: %v", err)
	}

	if disc.MountPoint == "" {
		t.Errorf("expected non-empty MountPoint")
	}
	if disc.SourceHost == "" {
		t.Errorf("expected non-empty SourceHost")
	}
	t.Logf("Discovered for cwd: MountPoint=%s, DevicePath=%s, FSType=%s, FSUUID=%s, Label=%s, SourceHost=%s",
		disc.MountPoint, disc.DevicePath, disc.FSType, disc.FSUUID, disc.Label, disc.SourceHost)
}
