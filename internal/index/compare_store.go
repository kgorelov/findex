package index

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/kgorelov/findex/internal/model"
)

// CompareQueryParams defines parameters for volume set comparisons.
type CompareQueryParams struct {
	LeftVolumes    []string
	RightVolumes   []string
	PresentVolumes []string
	AbsentVolumes  []string
	Mode           string // "left", "right", "common", "unique"
	EntryType      model.EntryType
	ShowMatches    bool
	VerifyFullHash bool
}

// QueryComparison executes comparison queries across volume sets using SQLite indexes.
func (d *DB) QueryComparison(ctx context.Context, params CompareQueryParams) (*model.CompareSummary, error) {
	if len(params.PresentVolumes) == 0 {
		return nil, fmt.Errorf("present volumes list cannot be empty")
	}

	leftVols := params.LeftVolumes
	if len(leftVols) == 0 {
		leftVols = params.PresentVolumes
	}
	rightVols := params.RightVolumes
	if len(rightVols) == 0 {
		rightVols = params.AbsentVolumes
	}

	summary := &model.CompareSummary{
		LeftVolumes:    leftVols,
		RightVolumes:   rightVols,
		PresentVolumes: params.PresentVolumes,
		AbsentVolumes:  params.AbsentVolumes,
		Mode:           params.Mode,
	}
	switch params.EntryType {
	case model.EntryTypeRegular:
		summary.EntryTypeFilter = "file"
	case model.EntryTypeDirectory:
		summary.EntryTypeFilter = "dir"
	default:
		summary.EntryTypeFilter = "all"
	}

	presentPlaceholders := makePlaceholders(len(params.PresentVolumes))
	presentArgs := toAnySlice(params.PresentVolumes)

	// 1. Total source counts & bytes
	var typeClause string
	var totalArgs []any
	totalArgs = append(totalArgs, presentArgs...)

	if params.EntryType != 0 {
		typeClause = " AND entry_type = ?"
		totalArgs = append(totalArgs, int(params.EntryType))
	} else {
		typeClause = " AND entry_type IN (1, 2)"
	}

	queryTotal := fmt.Sprintf(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN entry_type = 1 THEN size_bytes ELSE 0 END), 0)
		FROM entries
		WHERE volume_id IN (%s)%s
	`, presentPlaceholders, typeClause)

	err := d.db.QueryRowContext(ctx, queryTotal, totalArgs...).Scan(
		&summary.TotalSourceEntries,
		&summary.TotalSourceBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("failed querying source totals: %w", err)
	}

	// If no absent/target volumes are specified, all source entries are absent
	if len(params.AbsentVolumes) == 0 {
		summary.AbsentEntries = summary.TotalSourceEntries
		summary.AbsentBytes = summary.TotalSourceBytes

		if params.Mode != "common" {
			items, err := d.loadAllSourceItems(ctx, params, presentPlaceholders, presentArgs)
			if err != nil {
				return nil, err
			}
			summary.Items = items
		}
		return summary, nil
	}

	absentPlaceholders := makePlaceholders(len(params.AbsentVolumes))
	absentArgs := toAnySlice(params.AbsentVolumes)

	// Load compared items based on Mode and compute match/absent totals directly from items
	var items []model.CompareItem
	if params.Mode == "common" {
		items, err = d.queryCommonItems(ctx, params, presentPlaceholders, absentPlaceholders, presentArgs, absentArgs)
		if err != nil {
			return nil, err
		}
		var matchingBytes int64
		for _, it := range items {
			if it.EntryType == model.EntryTypeRegular {
				matchingBytes += it.SizeBytes
			}
		}
		summary.MatchingEntries = int64(len(items))
		summary.MatchingBytes = matchingBytes
		summary.AbsentEntries = summary.TotalSourceEntries - summary.MatchingEntries
		summary.AbsentBytes = summary.TotalSourceBytes - summary.MatchingBytes
	} else {
		// "left", "right", or "unique"
		items, err = d.queryAbsentItems(ctx, params, presentPlaceholders, absentPlaceholders, presentArgs, absentArgs)
		if err != nil {
			return nil, err
		}
		var absentBytes int64
		for _, it := range items {
			if it.EntryType == model.EntryTypeRegular {
				absentBytes += it.SizeBytes
			}
		}
		summary.AbsentEntries = int64(len(items))
		summary.AbsentBytes = absentBytes
		summary.MatchingEntries = summary.TotalSourceEntries - summary.AbsentEntries
		summary.MatchingBytes = summary.TotalSourceBytes - summary.AbsentBytes
	}

	summary.Items = items
	return summary, nil
}

func (d *DB) loadAllSourceItems(ctx context.Context, params CompareQueryParams, presentPlaceholders string, presentArgs []any) ([]model.CompareItem, error) {
	var typeClause string
	args := append([]any(nil), presentArgs...)
	if params.EntryType != 0 {
		typeClause = " AND entry_type = ?"
		args = append(args, int(params.EntryType))
	} else {
		typeClause = " AND entry_type IN (1, 2)"
	}

	query := fmt.Sprintf(`
		SELECT volume_id, relative_path, entry_type, size_bytes, quick_fingerprint, full_fingerprint, directory_fingerprint
		FROM entries
		WHERE volume_id IN (%s)%s
		ORDER BY volume_id, relative_path
	`, presentPlaceholders, typeClause)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying all source items: %w", err)
	}
	defer rows.Close()

	var items []model.CompareItem
	for rows.Next() {
		item, _, _, err := scanCompareRow(rows, model.MatchStatusAbsent)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (d *DB) queryAbsentItems(ctx context.Context, params CompareQueryParams, presentPlaceholders, absentPlaceholders string, presentArgs, absentArgs []any) ([]model.CompareItem, error) {
	query, args := buildComparisonQuery(params, "NOT", presentPlaceholders, absentPlaceholders, presentArgs, absentArgs)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying absent items: %w", err)
	}
	defer rows.Close()

	var items []model.CompareItem
	for rows.Next() {
		item, _, _, err := scanCompareRow(rows, model.MatchStatusAbsent)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

func (d *DB) queryCommonItems(ctx context.Context, params CompareQueryParams, presentPlaceholders, absentPlaceholders string, presentArgs, absentArgs []any) ([]model.CompareItem, error) {
	query, args := buildComparisonQuery(params, "", presentPlaceholders, absentPlaceholders, presentArgs, absentArgs)

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying common items: %w", err)
	}
	defer rows.Close()

	type rawItem struct {
		item    *model.CompareItem
		quickFP []byte
		fullFP  []byte
	}

	var rawItems []rawItem
	for rows.Next() {
		item, quickFP, fullFP, err := scanCompareRow(rows, model.MatchStatusPresent)
		if err != nil {
			return nil, err
		}
		rawItems = append(rawItems, rawItem{item: item, quickFP: quickFP, fullFP: fullFP})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close()

	var items []model.CompareItem
	for _, r := range rawItems {
		item := r.item
		if params.ShowMatches || params.VerifyFullHash {
			matches, verified, err := d.lookupMatchingLocations(ctx, params.AbsentVolumes, item.EntryType, item.SizeBytes, r.quickFP, r.fullFP, item.Fingerprint)
			if err != nil {
				return nil, err
			}
			if verified {
				item.Confidence = model.ConfidenceFullHash
			}
			if params.ShowMatches {
				item.MatchingLocations = matches
			}
		}
		items = append(items, *item)
	}
	return items, nil
}

func buildComparisonQuery(params CompareQueryParams, existsOp string, presentPlaceholders, absentPlaceholders string, presentArgs, absentArgs []any) (string, []any) {
	prefix := ""
	if existsOp != "" {
		prefix = existsOp + " "
	}

	switch params.EntryType {
	case model.EntryTypeRegular:
		query := fmt.Sprintf(`
			SELECT a.volume_id, a.relative_path, a.entry_type, a.size_bytes,
			       a.quick_fingerprint, a.full_fingerprint, a.directory_fingerprint
			FROM entries a
			WHERE a.volume_id IN (%s)
			  AND a.entry_type = 1
			  AND %sEXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 1
				  AND b.size_bytes = a.size_bytes
				  AND b.quick_fingerprint = a.quick_fingerprint
			  )
			ORDER BY a.volume_id, a.relative_path
		`, presentPlaceholders, prefix, absentPlaceholders)
		var args []any
		args = append(args, presentArgs...)
		args = append(args, absentArgs...)
		return query, args

	case model.EntryTypeDirectory:
		query := fmt.Sprintf(`
			SELECT a.volume_id, a.relative_path, a.entry_type, a.size_bytes,
			       a.quick_fingerprint, a.full_fingerprint, a.directory_fingerprint
			FROM entries a
			WHERE a.volume_id IN (%s)
			  AND a.entry_type = 2
			  AND a.directory_fingerprint IS NOT NULL
			  AND %sEXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 2
				  AND b.directory_fingerprint = a.directory_fingerprint
			  )
			ORDER BY a.volume_id, a.relative_path
		`, presentPlaceholders, prefix, absentPlaceholders)
		var args []any
		args = append(args, presentArgs...)
		args = append(args, absentArgs...)
		return query, args

	default:
		// UNION ALL splits the scan into files and directories, avoiding slow OR evaluation on every row
		// and allowing SQLite to merge the sorted index streams.
		query := fmt.Sprintf(`
			SELECT a.volume_id, a.relative_path, a.entry_type, a.size_bytes,
			       a.quick_fingerprint, a.full_fingerprint, a.directory_fingerprint
			FROM entries a
			WHERE a.volume_id IN (%s)
			  AND a.entry_type = 1
			  AND %sEXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 1
				  AND b.size_bytes = a.size_bytes
				  AND b.quick_fingerprint = a.quick_fingerprint
			  )
			UNION ALL
			SELECT a.volume_id, a.relative_path, a.entry_type, a.size_bytes,
			       a.quick_fingerprint, a.full_fingerprint, a.directory_fingerprint
			FROM entries a
			WHERE a.volume_id IN (%s)
			  AND a.entry_type = 2
			  AND a.directory_fingerprint IS NOT NULL
			  AND %sEXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 2
				  AND b.directory_fingerprint = a.directory_fingerprint
			  )
			ORDER BY volume_id, relative_path
		`, presentPlaceholders, prefix, absentPlaceholders, presentPlaceholders, prefix, absentPlaceholders)
		var args []any
		args = append(args, presentArgs...)
		args = append(args, absentArgs...)
		args = append(args, presentArgs...)
		args = append(args, absentArgs...)
		return query, args
	}
}

func (d *DB) lookupMatchingLocations(ctx context.Context, targetVolumes []string, entryType model.EntryType, sizeBytes int64, quickFP, fullFP []byte, dirFPStr string) ([]model.LocationRef, bool, error) {
	placeholders := makePlaceholders(len(targetVolumes))
	var args []any
	args = append(args, toAnySlice(targetVolumes)...)

	var query string
	if entryType == model.EntryTypeRegular {
		query = fmt.Sprintf(`
			SELECT volume_id, relative_path, full_fingerprint
			FROM entries
			WHERE volume_id IN (%s)
			  AND +entry_type = 1
			  AND size_bytes = ?
			  AND quick_fingerprint = ?
			ORDER BY volume_id, relative_path
		`, placeholders)
		args = append(args, sizeBytes, quickFP)
	} else {
		dirFP, _ := hex.DecodeString(dirFPStr)
		query = fmt.Sprintf(`
			SELECT volume_id, relative_path, full_fingerprint
			FROM entries
			WHERE volume_id IN (%s)
			  AND +entry_type = 2
			  AND directory_fingerprint = ?
			ORDER BY volume_id, relative_path
		`, placeholders)
		args = append(args, dirFP)
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var locations []model.LocationRef
	verified := false

	for rows.Next() {
		var volID, relPath string
		var targetFullFP []byte
		if err := rows.Scan(&volID, &relPath, &targetFullFP); err != nil {
			return nil, false, err
		}
		locations = append(locations, model.LocationRef{
			VolumeID:     volID,
			RelativePath: relPath,
		})

		if len(fullFP) > 0 && len(targetFullFP) > 0 && bytes.Equal(fullFP, targetFullFP) {
			verified = true
		}
	}

	return locations, verified, rows.Err()
}

func buildMatchCountQuery(params CompareQueryParams, presentPlaceholders, absentPlaceholders string, presentArgs, absentArgs []any) (string, []any) {
	var conditions string
	var args []any
	args = append(args, presentArgs...)

	switch params.EntryType {
	case model.EntryTypeRegular:
		conditions = fmt.Sprintf(`
			a.entry_type = 1 AND EXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 1
				  AND b.size_bytes = a.size_bytes
				  AND b.quick_fingerprint = a.quick_fingerprint
			)
		`, absentPlaceholders)
		args = append(args, absentArgs...)

	case model.EntryTypeDirectory:
		conditions = fmt.Sprintf(`
			a.entry_type = 2 AND a.directory_fingerprint IS NOT NULL AND EXISTS (
				SELECT 1 FROM entries b
				WHERE b.volume_id IN (%s)
				  AND +b.entry_type = 2
				  AND b.directory_fingerprint = a.directory_fingerprint
			)
		`, absentPlaceholders)
		args = append(args, absentArgs...)

	default:
		conditions = fmt.Sprintf(`
			(
				(a.entry_type = 1 AND EXISTS (
					SELECT 1 FROM entries b
					WHERE b.volume_id IN (%s)
					  AND +b.entry_type = 1
					  AND b.size_bytes = a.size_bytes
					  AND b.quick_fingerprint = a.quick_fingerprint
				))
				OR
				(a.entry_type = 2 AND a.directory_fingerprint IS NOT NULL AND EXISTS (
					SELECT 1 FROM entries b
					WHERE b.volume_id IN (%s)
					  AND +b.entry_type = 2
					  AND b.directory_fingerprint = a.directory_fingerprint
				))
			)
		`, absentPlaceholders, absentPlaceholders)
		args = append(args, absentArgs...)
		args = append(args, absentArgs...)
	}

	query := fmt.Sprintf(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN a.entry_type = 1 THEN a.size_bytes ELSE 0 END), 0)
		FROM entries a
		WHERE a.volume_id IN (%s) AND %s
	`, presentPlaceholders, conditions)

	return query, args
}

func scanCompareRow(rows rowScanner, status model.MatchStatus) (*model.CompareItem, []byte, []byte, error) {
	var item model.CompareItem
	var entryType int
	var sizeBytes int64
	var quickFP, fullFP, dirFP []byte

	if err := rows.Scan(
		&item.VolumeID,
		&item.RelativePath,
		&entryType,
		&sizeBytes,
		&quickFP,
		&fullFP,
		&dirFP,
	); err != nil {
		return nil, nil, nil, fmt.Errorf("failed scanning compare row: %w", err)
	}

	item.EntryType = model.EntryType(entryType)
	item.SizeBytes = sizeBytes
	item.Status = status
	item.Confidence = model.ConfidenceQuickFingerprint

	if item.EntryType == model.EntryTypeDirectory {
		if len(dirFP) > 0 {
			item.Fingerprint = hex.EncodeToString(dirFP)
		} else {
			item.Fingerprint = "-"
		}
	} else {
		if len(quickFP) > 0 {
			item.Fingerprint = hex.EncodeToString(quickFP)
		} else {
			item.Fingerprint = "-"
		}
	}

	return &item, quickFP, fullFP, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func makePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

func toAnySlice(strs []string) []any {
	res := make([]any, len(strs))
	for i, s := range strs {
		res[i] = s
	}
	return res
}
