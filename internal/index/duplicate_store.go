package index

import (
	"context"
	"fmt"
	"strings"

	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/model"
)

// RawDuplicateGroup holds a group of matching entries retrieved from the database.
type RawDuplicateGroup struct {
	EntryType   model.EntryType
	SizeBytes   int64
	Fingerprint string
	Entries     []*model.DuplicateEntry
}

// QueryDuplicateDirectories retrieves candidate duplicate directory groups across the specified volume IDs.
// Empty directories (matching the empty directory fingerprint) are automatically excluded.
func (d *DB) QueryDuplicateDirectories(ctx context.Context, volumeIDs []string) ([]*RawDuplicateGroup, error) {
	emptyFP, _ := fingerprint.ComputeDirectoryFingerprint(nil)

	var whereVolume string
	var args []any
	if len(volumeIDs) > 0 {
		whereVolume = fmt.Sprintf("e.volume_id IN (%s) AND", makePlaceholders(len(volumeIDs)))
		args = toAnySlice(volumeIDs)
	}

	args = append(args, emptyFP)

	query := fmt.Sprintf(`
		SELECT volume_id, label, source_host, relative_path, entry_type, hex_fp
		FROM (
			SELECT e.volume_id, COALESCE(v.label, '') AS label, COALESCE(v.source_host, '') AS source_host,
			       e.relative_path, e.entry_type, hex(e.directory_fingerprint) AS hex_fp,
			       COUNT(*) OVER (PARTITION BY e.directory_fingerprint) AS dup_count
			FROM entries e
			LEFT JOIN volumes v ON e.volume_id = v.volume_id
			WHERE %s
			      e.entry_type = 2
			  AND e.directory_fingerprint IS NOT NULL
			  AND length(e.directory_fingerprint) > 0
			  AND e.directory_fingerprint != ?
		)
		WHERE dup_count >= 2
		ORDER BY hex_fp, volume_id, relative_path
	`, whereVolume)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duplicate directory groups: %w", err)
	}
	defer rows.Close()

	var groups []*RawDuplicateGroup
	var curGroup *RawDuplicateGroup

	for rows.Next() {
		var e model.DuplicateEntry
		var eType int
		var fpStr string
		if err := rows.Scan(&e.VolumeID, &e.VolumeLabel, &e.SourceHost, &e.RelativePath, &eType, &fpStr); err != nil {
			return nil, fmt.Errorf("failed scanning duplicate directory row: %w", err)
		}
		e.EntryType = model.EntryType(eType)
		e.Fingerprint = strings.ToLower(fpStr)

		if curGroup == nil || curGroup.Fingerprint != e.Fingerprint {
			if curGroup != nil && len(curGroup.Entries) >= 2 {
				groups = append(groups, curGroup)
			}
			curGroup = &RawDuplicateGroup{
				EntryType:   model.EntryTypeDirectory,
				Fingerprint: e.Fingerprint,
				Entries:     []*model.DuplicateEntry{&e},
			}
		} else {
			curGroup.Entries = append(curGroup.Entries, &e)
		}
	}
	if curGroup != nil && len(curGroup.Entries) >= 2 {
		groups = append(groups, curGroup)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate content size for each distinct duplicate directory group using indexed range queries
	for _, group := range groups {
		if len(group.Entries) == 0 {
			continue
		}
		firstEntry := group.Entries[0]
		size, err := d.computeDirectoryContentSize(ctx, firstEntry.VolumeID, firstEntry.RelativePath)
		if err != nil {
			return nil, err
		}
		group.SizeBytes = size
		for _, e := range group.Entries {
			e.SizeBytes = size
		}
	}

	return groups, nil
}

// computeDirectoryContentSize computes the total size of all regular files contained within the given directory.
func (d *DB) computeDirectoryContentSize(ctx context.Context, volumeID, relPath string) (int64, error) {
	if relPath == "/" {
		var total int64
		err := d.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(size_bytes), 0) FROM entries WHERE volume_id = ? AND entry_type = 1`, volumeID).Scan(&total)
		return total, err
	}

	prefix := relPath
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	prefixUpper := prefix[:len(prefix)-1] + "0" // '/' is ASCII 47, '0' is ASCII 48

	var total int64
	err := d.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(size_bytes), 0)
		FROM entries
		WHERE volume_id = ?
		  AND relative_path >= ?
		  AND relative_path < ?
		  AND entry_type = 1
	`, volumeID, prefix, prefixUpper).Scan(&total)
	return total, err
}

// QueryDuplicateFiles retrieves candidate duplicate file groups across the specified volume IDs.
func (d *DB) QueryDuplicateFiles(ctx context.Context, volumeIDs []string, minSizeBytes int64) ([]*RawDuplicateGroup, error) {
	var whereVolume string
	var args []any
	if len(volumeIDs) > 0 {
		whereVolume = fmt.Sprintf("e.volume_id IN (%s) AND", makePlaceholders(len(volumeIDs)))
		args = toAnySlice(volumeIDs)
	}

	args = append(args, minSizeBytes)

	query := fmt.Sprintf(`
		SELECT volume_id, label, source_host, relative_path, entry_type, size_bytes, hex_fp
		FROM (
			SELECT e.volume_id, COALESCE(v.label, '') AS label, COALESCE(v.source_host, '') AS source_host,
			       e.relative_path, e.entry_type, e.size_bytes, hex(e.quick_fingerprint) AS hex_fp,
			       COUNT(*) OVER (PARTITION BY e.quick_fingerprint, e.size_bytes) AS dup_count
			FROM entries e
			LEFT JOIN volumes v ON e.volume_id = v.volume_id
			WHERE %s
			      e.entry_type = 1
			  AND e.size_bytes >= ?
			  AND e.quick_fingerprint IS NOT NULL
			  AND length(e.quick_fingerprint) > 0
		)
		WHERE dup_count >= 2
		ORDER BY size_bytes DESC, hex_fp, volume_id, relative_path
	`, whereVolume)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duplicate file groups: %w", err)
	}
	defer rows.Close()

	var groups []*RawDuplicateGroup
	var curGroup *RawDuplicateGroup

	for rows.Next() {
		var e model.DuplicateEntry
		var eType int
		var fpStr string
		if err := rows.Scan(&e.VolumeID, &e.VolumeLabel, &e.SourceHost, &e.RelativePath, &eType, &e.SizeBytes, &fpStr); err != nil {
			return nil, fmt.Errorf("failed scanning duplicate file row: %w", err)
		}
		e.EntryType = model.EntryType(eType)
		e.Fingerprint = strings.ToLower(fpStr)

		if curGroup == nil || curGroup.Fingerprint != e.Fingerprint || curGroup.SizeBytes != e.SizeBytes {
			if curGroup != nil && len(curGroup.Entries) >= 2 {
				groups = append(groups, curGroup)
			}
			curGroup = &RawDuplicateGroup{
				EntryType:   model.EntryTypeRegular,
				SizeBytes:   e.SizeBytes,
				Fingerprint: e.Fingerprint,
				Entries:     []*model.DuplicateEntry{&e},
			}
		} else {
			curGroup.Entries = append(curGroup.Entries, &e)
		}
	}
	if curGroup != nil && len(curGroup.Entries) >= 2 {
		groups = append(groups, curGroup)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return groups, nil
}
