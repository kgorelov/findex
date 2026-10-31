package index

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/kgorelov/findex/internal/model"
)

// LookupFingerprint searches entries across all indexed volumes for matches against
// a full or partial fingerprint (hex string). Matches quick, full, or directory fingerprints.
func (d *DB) LookupFingerprint(ctx context.Context, fpQuery string) ([]*model.LookupMatch, error) {
	cleanQuery := strings.TrimSpace(fpQuery)
	cleanQuery = strings.TrimPrefix(cleanQuery, "0x")
	if cleanQuery == "" {
		return nil, fmt.Errorf("lookup fingerprint query cannot be empty")
	}

	upperQuery := strings.ToUpper(cleanQuery)

	query := `
		SELECT e.volume_id, COALESCE(v.label, ''), COALESCE(v.source_host, ''),
		       e.relative_path, e.entry_type, e.size_bytes, e.mtime_ns,
		       COALESCE(hex(e.quick_fingerprint), ''),
		       COALESCE(hex(e.full_fingerprint), ''),
		       COALESCE(hex(e.directory_fingerprint), '')
		FROM entries e
		LEFT JOIN volumes v ON e.volume_id = v.volume_id
		WHERE hex(e.quick_fingerprint) LIKE ? || '%'
		   OR hex(e.full_fingerprint) LIKE ? || '%'
		   OR hex(e.directory_fingerprint) LIKE ? || '%'
		ORDER BY e.volume_id, e.relative_path
	`

	rows, err := d.db.QueryContext(ctx, query, upperQuery, upperQuery, upperQuery)
	if err != nil {
		return nil, fmt.Errorf("failed querying fingerprint lookup: %w", err)
	}
	defer rows.Close()

	var matches []*model.LookupMatch
	for rows.Next() {
		var (
			volID, label, host, relPath string
			entryType                   int
			sizeBytes, mtimeNs          sql.NullInt64
			hexQuick, hexFull, hexDir   string
		)

		if err := rows.Scan(
			&volID,
			&label,
			&host,
			&relPath,
			&entryType,
			&sizeBytes,
			&mtimeNs,
			&hexQuick,
			&hexFull,
			&hexDir,
		); err != nil {
			return nil, fmt.Errorf("failed scanning lookup match: %w", err)
		}

		matchType := "quick"
		matchedFP := strings.ToLower(hexQuick)

		if hexQuick != "" && strings.HasPrefix(hexQuick, upperQuery) {
			matchType = "quick"
			matchedFP = strings.ToLower(hexQuick)
		} else if hexFull != "" && strings.HasPrefix(hexFull, upperQuery) {
			matchType = "full"
			matchedFP = strings.ToLower(hexFull)
		} else if hexDir != "" && strings.HasPrefix(hexDir, upperQuery) {
			matchType = "dir"
			matchedFP = strings.ToLower(hexDir)
		}

		matches = append(matches, &model.LookupMatch{
			VolumeID:     volID,
			VolumeLabel:  label,
			SourceHost:   host,
			RelativePath: relPath,
			EntryType:    model.EntryType(entryType),
			SizeBytes:    sizeBytes.Int64,
			MTimeNs:      mtimeNs.Int64,
			Fingerprint:  matchedFP,
			MatchType:    matchType,
		})
	}

	return matches, rows.Err()
}
