package model

import (
	"io/fs"
	"testing"
)

func TestNormalizeRelativePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", "/"},
		{"single dot", ".", "/"},
		{"root slash", "/", "/"},
		{"multiple slashes", "///", "/"},
		{"relative single file", "foo.txt", "/foo.txt"},
		{"relative subpath", "photos/2023/vacation.jpg", "/photos/2023/vacation.jpg"},
		{"windows separators", `photos\2023\vacation.jpg`, "/photos/2023/vacation.jpg"},
		{"with leading slash", "/photos/2023/vacation.jpg", "/photos/2023/vacation.jpg"},
		{"with dot segments", "/photos/./2023/../2024/vacation.jpg", "/photos/2024/vacation.jpg"},
		{"trailing slash stripped", "/photos/2023/", "/photos/2023"},
		{"preserves exact case", "/Photos/MyFolder/Document.PDF", "/Photos/MyFolder/Document.PDF"},
		{"preserves unicode characters", "/Музыка/Альбом/Песня.mp3", "/Музыка/Альбом/Песня.mp3"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := NormalizeRelativePath(tc.input)
			if actual != tc.expected {
				t.Errorf("NormalizeRelativePath(%q) = %q, expected %q", tc.input, actual, tc.expected)
			}
		})
	}
}

func TestToVolumeRelativePath(t *testing.T) {
	tests := []struct {
		name        string
		mount       string
		fullPath    string
		expected    string
		expectError bool
	}{
		{
			name:     "mount root itself",
			mount:    "/mnt/data",
			fullPath: "/mnt/data",
			expected: "/",
		},
		{
			name:     "mount root with trailing slash",
			mount:    "/mnt/data/",
			fullPath: "/mnt/data",
			expected: "/",
		},
		{
			name:     "sub directory",
			mount:    "/mnt/data",
			fullPath: "/mnt/data/Photos/Summer",
			expected: "/Photos/Summer",
		},
		{
			name:     "root mount",
			mount:    "/",
			fullPath: "/etc/hosts",
			expected: "/etc/hosts",
		},
		{
			name:        "outside mount",
			mount:       "/mnt/data",
			fullPath:    "/home/user/file.txt",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ToVolumeRelativePath(tc.mount, tc.fullPath)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil with path %q", actual)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actual != tc.expected {
				t.Errorf("ToVolumeRelativePath(%q, %q) = %q, expected %q", tc.mount, tc.fullPath, actual, tc.expected)
			}
		})
	}
}

func TestEntryTypeFromMode(t *testing.T) {
	if EntryTypeFromMode(0644) != EntryTypeRegular {
		t.Errorf("expected regular, got %v", EntryTypeFromMode(0644))
	}
	if EntryTypeFromMode(fs.ModeDir|0755) != EntryTypeDirectory {
		t.Errorf("expected directory, got %v", EntryTypeFromMode(fs.ModeDir|0755))
	}
	if EntryTypeFromMode(fs.ModeSymlink|0777) != EntryTypeSymlink {
		t.Errorf("expected symlink, got %v", EntryTypeFromMode(fs.ModeSymlink|0777))
	}
	if EntryTypeFromMode(fs.ModeNamedPipe|0600) != EntryTypeOther {
		t.Errorf("expected other, got %v", EntryTypeFromMode(fs.ModeNamedPipe|0600))
	}
}
