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
	if len(volumeIDs) == 0 {
		return nil, nil
	}

	emptyFP, _ := fingerprint.ComputeDirectoryFingerprint(nil)

	placeholders := makePlaceholders(len(volumeIDs))
	args := toAnySlice(volumeIDs)

	query := fmt.Sprintf(`
		SELECT directory_fingerprint, COUNT(*)
		FROM entries
		WHERE volume_id IN (%s)
		  AND entry_type = 2
		  AND directory_fingerprint IS NOT NULL
		  AND length(directory_fingerprint) > 0
		GROUP BY directory_fingerprint
		HAVING COUNT(*) >= 2
	`, placeholders)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duplicate directory groups: %w", err)
	}
	defer rows.Close()

	var duplicateFPs [][]byte
	for rows.Next() {
		var fp []byte
		var count int
		if err := rows.Scan(&fp, &count); err != nil {
			return nil, fmt.Errorf("failed scanning duplicate directory group: %w", err)
		}
		if len(emptyFP) > 0 && string(fp) == string(emptyFP) {
			continue
		}
		duplicateFPs = append(duplicateFPs, fp)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	var groups []*RawDuplicateGroup
	for _, fp := range duplicateFPs {
		entriesQuery := fmt.Sprintf(`
			SELECT e.volume_id, COALESCE(v.label, ''), COALESCE(v.source_host, ''),
			       e.relative_path, e.entry_type, hex(e.directory_fingerprint)
			FROM entries e
			LEFT JOIN volumes v ON e.volume_id = v.volume_id
			WHERE e.volume_id IN (%s)
			  AND e.entry_type = 2
			  AND e.directory_fingerprint = ?
			ORDER BY e.volume_id, e.relative_path
		`, placeholders)

		entryArgs := append([]any(nil), args...)
		entryArgs = append(entryArgs, fp)

		eRows, err := d.db.QueryContext(ctx, entriesQuery, entryArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed querying duplicate directory entries: %w", err)
		}

		var entries []*model.DuplicateEntry
		var fpStr string
		for eRows.Next() {
			var e model.DuplicateEntry
			var eType int
			if err := eRows.Scan(&e.VolumeID, &e.VolumeLabel, &e.SourceHost, &e.RelativePath, &eType, &fpStr); err != nil {
				eRows.Close()
				return nil, fmt.Errorf("failed scanning duplicate directory entry: %w", err)
			}
			e.EntryType = model.EntryType(eType)
			e.Fingerprint = strings.ToLower(fpStr)
			entries = append(entries, &e)
		}
		eRows.Close()

		if len(entries) < 2 {
			continue
		}

		// Calculate total content size for one of the matching directories
		firstEntry := entries[0]
		var totalDirSize int64
		var sizeQuery string
		var sizeArgs []any
		if firstEntry.RelativePath == "/" {
			sizeQuery = `SELECT COALESCE(SUM(size_bytes), 0) FROM entries WHERE volume_id = ? AND entry_type = 1`
			sizeArgs = []any{firstEntry.VolumeID}
		} else {
			sizeQuery = `SELECT COALESCE(SUM(size_bytes), 0) FROM entries WHERE volume_id = ? AND (relative_path = ? OR relative_path LIKE ? || '/%') AND entry_type = 1`
			sizeArgs = []any{firstEntry.VolumeID, firstEntry.RelativePath, firstEntry.RelativePath}
		}
		_ = d.db.QueryRowContext(ctx, sizeQuery, sizeArgs...).Scan(&totalDirSize)

		for _, e := range entries {
			e.SizeBytes = totalDirSize
		}

		groups = append(groups, &RawDuplicateGroup{
			EntryType:   model.EntryTypeDirectory,
			SizeBytes:   totalDirSize,
			Fingerprint: strings.ToLower(fpStr),
			Entries:     entries,
		})
	}

	return groups, nil
}

// QueryDuplicateFiles retrieves candidate duplicate file groups across the specified volume IDs.
func (d *DB) QueryDuplicateFiles(ctx context.Context, volumeIDs []string, minSizeBytes int64) ([]*RawDuplicateGroup, error) {
	if len(volumeIDs) == 0 {
		return nil, nil
	}

	placeholders := makePlaceholders(len(volumeIDs))
	args := toAnySlice(volumeIDs)
	args = append(args, minSizeBytes)

	query := fmt.Sprintf(`
		SELECT quick_fingerprint, size_bytes, COUNT(*)
		FROM entries
		WHERE volume_id IN (%s)
		  AND entry_type = 1
		  AND size_bytes >= ?
		  AND quick_fingerprint IS NOT NULL
		  AND length(quick_fingerprint) > 0
		GROUP BY quick_fingerprint, size_bytes
		HAVING COUNT(*) >= 2
		ORDER BY size_bytes DESC
	`, placeholders)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying duplicate file groups: %w", err)
	}
	defer rows.Close()

	type fileGroupKey struct {
		quickFP   []byte
		sizeBytes int64
	}
	var keys []fileGroupKey
	for rows.Next() {
		var k fileGroupKey
		var count int
		if err := rows.Scan(&k.quickFP, &k.sizeBytes, &count); err != nil {
			return nil, fmt.Errorf("failed scanning duplicate file group key: %w", err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	volArgs := toAnySlice(volumeIDs)
	var groups []*RawDuplicateGroup

	for _, k := range keys {
		entriesQuery := fmt.Sprintf(`
			SELECT e.volume_id, COALESCE(v.label, ''), COALESCE(v.source_host, ''),
			       e.relative_path, e.entry_type, e.size_bytes, hex(e.quick_fingerprint)
			FROM entries e
			LEFT JOIN volumes v ON e.volume_id = v.volume_id
			WHERE e.volume_id IN (%s)
			  AND e.entry_type = 1
			  AND e.quick_fingerprint = ?
			  AND e.size_bytes = ?
			ORDER BY e.volume_id, e.relative_path
		`, placeholders)

		entryArgs := append([]any(nil), volArgs...)
		entryArgs = append(entryArgs, k.quickFP, k.sizeBytes)

		eRows, err := d.db.QueryContext(ctx, entriesQuery, entryArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed querying duplicate file entries: %w", err)
		}

		var entries []*model.DuplicateEntry
		var fpStr string
		for eRows.Next() {
			var e model.DuplicateEntry
			var eType int
			if err := eRows.Scan(&e.VolumeID, &e.VolumeLabel, &e.SourceHost, &e.RelativePath, &eType, &e.SizeBytes, &fpStr); err != nil {
				eRows.Close()
				return nil, fmt.Errorf("failed scanning duplicate file entry: %w", err)
			}
			e.EntryType = model.EntryType(eType)
			e.Fingerprint = strings.ToLower(fpStr)
			entries = append(entries, &e)
		}
		eRows.Close()

		if len(entries) < 2 {
			continue
		}

		groups = append(groups, &RawDuplicateGroup{
			EntryType:   model.EntryTypeRegular,
			SizeBytes:   k.sizeBytes,
			Fingerprint: strings.ToLower(fpStr),
			Entries:     entries,
		})
	}

	return groups, nil
}
