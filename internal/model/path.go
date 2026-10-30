package model

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

var (
	// ErrPathOutsideVolume is returned when a target path does not reside within the given mount point.
	ErrPathOutsideVolume = errors.New("path is outside volume root")
)

// NormalizeRelativePath normalizes a volume-relative path according to findex rules:
// - Always uses '/' as the path separator regardless of host OS.
// - Resolves '.' and '..' segments.
// - Ensures volume root is represented as "/".
// - Ensures every relative path starts with "/" (e.g. "/Photos/vacation.jpg").
// - Removes redundant slashes and trailing slashes (except for root "/").
// - Preserves original casing and Unicode byte representations verbatim.
func NormalizeRelativePath(p string) string {
	if p == "" || p == "." || p == "/" {
		return "/"
	}

	// Convert any OS-specific separator (e.g. backslash) to '/'
	p = strings.ReplaceAll(p, "\\", "/")

	// Clean using standard path package (which operates on forward slashes)
	cleaned := path.Clean(p)

	if cleaned == "." || cleaned == "/" {
		return "/"
	}

	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}

	return cleaned
}

// ToVolumeRelativePath converts an absolute host filesystem path into a volume-relative path
// by stripping the mount point prefix and normalizing.
// Both mountPoint and fullPath can be native OS paths.
func ToVolumeRelativePath(mountPoint, fullPath string) (string, error) {
	// Clean both paths using host filepath clean
	cleanMount := filepath.Clean(mountPoint)
	cleanFull := filepath.Clean(fullPath)

	// Evaluate lexical relationship
	rel, err := filepath.Rel(cleanMount, cleanFull)
	if err != nil {
		return "", fmt.Errorf("failed to compute relative path: %w", err)
	}

	// If the path traverses outside the mount point
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: path %q is not inside mount point %q", ErrPathOutsideVolume, fullPath, mountPoint)
	}

	if rel == "." {
		return "/", nil
	}

	return NormalizeRelativePath(rel), nil
}
