package index

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// CheckIntegrity performs thorough database integrity and consistency checks.
func (d *DB) CheckIntegrity(ctx context.Context) (*model.CheckSummary, error) {
	summary := &model.CheckSummary{
		CheckedAt:     time.Now().UTC(),
		DatabasePath:  d.path,
		IntegrityOK:   true,
		ForeignKeysOK: true,
		MetadataOK:    true,
	}

	// 1. File size stats
	if d.path != ":memory:" && d.path != "" {
		if fi, err := os.Stat(d.path); err == nil {
			summary.DatabaseBytes = fi.Size()
		}
		if fi, err := os.Stat(d.path + "-wal"); err == nil {
			summary.WALBytes = fi.Size()
		}
	}

	// 2. PRAGMA integrity_check
	rows, err := d.db.QueryContext(ctx, "PRAGMA integrity_check;")
	if err != nil {
		return nil, fmt.Errorf("failed executing integrity_check: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return nil, fmt.Errorf("failed scanning integrity status: %w", err)
		}
		if status != "ok" {
			summary.IntegrityOK = false
			summary.ErrorsCount++
			summary.Issues = append(summary.Issues, model.CheckIssue{
				Severity: model.SeverityError,
				Category: "sqlite_integrity",
				Message:  "SQLite integrity check reported corruption",
				Detail:   status,
			})
		}
	}

	// 3. PRAGMA foreign_key_check
	fkRows, err := d.db.QueryContext(ctx, "PRAGMA foreign_key_check;")
	if err != nil {
		return nil, fmt.Errorf("failed executing foreign_key_check: %w", err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var table, parent string
		var rowid, fkid int64
		if err := fkRows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			return nil, fmt.Errorf("failed scanning foreign_key_check row: %w", err)
		}
		summary.ForeignKeysOK = false
		summary.ErrorsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityError,
			Category: "foreign_keys",
			Message:  fmt.Sprintf("Foreign key violation in table %q (rowid %d) referencing %q", table, rowid, parent),
			Detail:   fmt.Sprintf("fkid=%d", fkid),
		})
	}

	// 4. Counts
	_ = d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM volumes").Scan(&summary.VolumesCount)
	_ = d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM entries").Scan(&summary.EntriesCount)
	_ = d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scan_roots").Scan(&summary.ScanRootsCount)
	_ = d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scan_runs").Scan(&summary.ScanRunsCount)

	// 5. Metadata consistency
	schemaVer, err := d.GetMetadata(ctx, "schema_version")
	if err != nil || schemaVer == "" {
		summary.MetadataOK = false
		summary.ErrorsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityError,
			Category: "metadata",
			Message:  "Missing schema_version metadata",
		})
	}

	fpAlgo, err := d.GetMetadata(ctx, "fingerprint_algorithm")
	if err != nil || fpAlgo == "" {
		summary.MetadataOK = false
		summary.ErrorsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityError,
			Category: "metadata",
			Message:  "Missing fingerprint_algorithm metadata",
		})
	}

	// 6. Volume consistency: duplicate fs_uuid on distinct volumes
	dupUUIDRows, err := d.db.QueryContext(ctx, `
		SELECT fs_uuid, COUNT(*), GROUP_CONCAT(volume_id)
		FROM volumes
		WHERE fs_uuid IS NOT NULL AND fs_uuid != ''
		GROUP BY fs_uuid
		HAVING COUNT(*) > 1
	`)
	if err == nil {
		defer dupUUIDRows.Close()
		for dupUUIDRows.Next() {
			var uuid, volList string
			var count int
			if err := dupUUIDRows.Scan(&uuid, &count, &volList); err == nil {
				summary.WarningsCount++
				summary.Issues = append(summary.Issues, model.CheckIssue{
					Severity: model.SeverityWarning,
					Category: "volumes",
					Message:  fmt.Sprintf("Duplicate filesystem UUID %q shared across %d volumes (potential cloned filesystem)", uuid, count),
					Detail:   "Volumes: " + volList,
				})
			}
		}
	}

	// 7. Orphan entries check
	var orphanEntries int64
	err = d.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM entries
		WHERE volume_id NOT IN (SELECT volume_id FROM volumes)
	`).Scan(&orphanEntries)
	if err == nil && orphanEntries > 0 {
		summary.ErrorsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityError,
			Category: "entries",
			Message:  fmt.Sprintf("Found %d orphan entries with invalid volume_id", orphanEntries),
		})
	}

	// 8. Path normalization check
	var unnormalizedPaths int64
	var samplePath sql.NullString
	err = d.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(relative_path) FROM entries
		WHERE relative_path NOT LIKE '/%'
	`).Scan(&unnormalizedPaths, &samplePath)
	if err == nil && unnormalizedPaths > 0 {
		summary.WarningsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityWarning,
			Category: "entries",
			Message:  fmt.Sprintf("Found %d entries without normalized leading slash", unnormalizedPaths),
			Detail:   "Sample: " + samplePath.String,
		})
	}

	// 9. Regular files without valid 32-byte quick fingerprint
	var invalidFPs int64
	err = d.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM entries
		WHERE entry_type = 1 AND (quick_fingerprint IS NULL OR length(quick_fingerprint) != 32)
	`).Scan(&invalidFPs)
	if err == nil && invalidFPs > 0 {
		summary.ErrorsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityError,
			Category: "entries",
			Message:  fmt.Sprintf("Found %d regular files with missing or invalid 32-byte quick fingerprint", invalidFPs),
		})
	}

	// 10. Interrupted / crashed scan runs
	var runningScans int64
	var sampleScanID sql.NullString
	err = d.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(scan_id) FROM scan_runs WHERE status = 'running'
	`).Scan(&runningScans, &sampleScanID)
	if err == nil && runningScans > 0 {
		summary.WarningsCount++
		summary.Issues = append(summary.Issues, model.CheckIssue{
			Severity: model.SeverityWarning,
			Category: "scan_runs",
			Message:  fmt.Sprintf("Found %d uncompleted scan run(s) with status 'running' (likely interrupted)", runningScans),
			Detail:   "Sample scan ID: " + sampleScanID.String,
		})
	}

	return summary, nil
}
