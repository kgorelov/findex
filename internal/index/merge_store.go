package index

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

// MergeTx wraps a database transaction with prepared statements for merge operations.
type MergeTx struct {
	tx              *sql.Tx
	stmtInsertEntry *sql.Stmt
	stmtUpdateEntry *sql.Stmt
	stmtConflict    *sql.Stmt
}

// BeginMergeTx starts a transaction and prepares statements for high-throughput merging.
func (d *DB) BeginMergeTx(ctx context.Context) (*MergeTx, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin merge transaction: %w", err)
	}

	stmtInsert, err := tx.PrepareContext(ctx, `
		INSERT INTO entries (
			volume_id, relative_path, entry_type, size_bytes, mtime_ns,
			quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to prepare entry insert statement: %w", err)
	}

	stmtUpdate, err := tx.PrepareContext(ctx, `
		UPDATE entries SET
			entry_type = ?,
			size_bytes = ?,
			mtime_ns = ?,
			quick_fingerprint = ?,
			full_fingerprint = COALESCE(?, full_fingerprint),
			directory_fingerprint = ?,
			last_seen_scan_id = ?
		WHERE volume_id = ? AND relative_path = ?
	`)
	if err != nil {
		_ = stmtInsert.Close()
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to prepare entry update statement: %w", err)
	}

	stmtConflict, err := tx.PrepareContext(ctx, `
		INSERT INTO merge_conflicts (
			merge_id, conflict_kind, volume_id, relative_path,
			local_detail, remote_detail, resolution, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = stmtInsert.Close()
		_ = stmtUpdate.Close()
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to prepare merge conflict statement: %w", err)
	}

	return &MergeTx{
		tx:              tx,
		stmtInsertEntry: stmtInsert,
		stmtUpdateEntry: stmtUpdate,
		stmtConflict:    stmtConflict,
	}, nil
}

// closeStatements safely closes all prepared statements.
func (m *MergeTx) closeStatements() {
	if m.stmtInsertEntry != nil {
		_ = m.stmtInsertEntry.Close()
	}
	if m.stmtUpdateEntry != nil {
		_ = m.stmtUpdateEntry.Close()
	}
	if m.stmtConflict != nil {
		_ = m.stmtConflict.Close()
	}
}

// Commit commits the merge transaction.
func (m *MergeTx) Commit() error {
	m.closeStatements()
	return m.tx.Commit()
}

// Rollback rolls back the merge transaction.
func (m *MergeTx) Rollback() error {
	m.closeStatements()
	return m.tx.Rollback()
}

// InsertVolume inserts a new volume record.
func (m *MergeTx) InsertVolume(ctx context.Context, vol *model.Volume) error {
	var lastSeenInt sql.NullInt64
	if vol.LastSeenAt != nil {
		lastSeenInt = sql.NullInt64{Int64: vol.LastSeenAt.Unix(), Valid: true}
	}

	_, err := m.tx.ExecContext(ctx, `
		INSERT INTO volumes (
			volume_id, fs_uuid, fs_type, label, device_id,
			last_mount_point, source_host, source_label, first_seen_at, last_seen_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, vol.VolumeID, vol.FSUUID, vol.FSType, vol.Label, vol.DeviceID,
		vol.LastMountPoint, vol.SourceHost, vol.SourceLabel,
		vol.FirstSeenAt.Unix(), lastSeenInt)
	return err
}

// UpdateVolume updates descriptive metadata for an existing volume.
func (m *MergeTx) UpdateVolume(ctx context.Context, vol *model.Volume) error {
	var lastSeenInt sql.NullInt64
	if vol.LastSeenAt != nil {
		lastSeenInt = sql.NullInt64{Int64: vol.LastSeenAt.Unix(), Valid: true}
	}

	_, err := m.tx.ExecContext(ctx, `
		UPDATE volumes SET
			fs_uuid = ?,
			fs_type = ?,
			label = ?,
			device_id = ?,
			last_mount_point = ?,
			source_host = ?,
			source_label = ?,
			first_seen_at = ?,
			last_seen_at = ?
		WHERE volume_id = ?
	`, vol.FSUUID, vol.FSType, vol.Label, vol.DeviceID,
		vol.LastMountPoint, vol.SourceHost, vol.SourceLabel,
		vol.FirstSeenAt.Unix(), lastSeenInt, vol.VolumeID)
	return err
}

// UpsertScanRun imports or updates a scan run record.
func (m *MergeTx) UpsertScanRun(ctx context.Context, run *model.ScanRun) error {
	var completedAt sql.NullInt64
	if run.CompletedAt != nil {
		completedAt = sql.NullInt64{Int64: run.CompletedAt.Unix(), Valid: true}
	}

	_, err := m.tx.ExecContext(ctx, `
		INSERT INTO scan_runs (
			scan_id, volume_id, root_path, status, started_at,
			completed_at, entries_scanned, errors_count, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(scan_id) DO UPDATE SET
			status = excluded.status,
			completed_at = COALESCE(excluded.completed_at, scan_runs.completed_at),
			entries_scanned = MAX(scan_runs.entries_scanned, excluded.entries_scanned),
			errors_count = excluded.errors_count,
			error_message = excluded.error_message
	`, run.ScanID, run.VolumeID, run.RootPath, string(run.Status),
		run.StartedAt.Unix(), completedAt, run.EntriesScanned,
		run.ErrorsCount, run.ErrorMessage)
	return err
}

// UpsertScanRoot imports or updates a scan root record.
func (m *MergeTx) UpsertScanRoot(ctx context.Context, root *model.ScanRoot) error {
	var scannedAt sql.NullInt64
	if root.LastSuccessfulScanAt != nil {
		scannedAt = sql.NullInt64{Int64: root.LastSuccessfulScanAt.Unix(), Valid: true}
	}

	_, err := m.tx.ExecContext(ctx, `
		INSERT INTO scan_roots (
			volume_id, root_path, last_successful_scan_at, last_scan_id
		) VALUES (?, ?, ?, ?)
		ON CONFLICT(volume_id, root_path) DO UPDATE SET
			last_scan_id = CASE
				WHEN excluded.last_successful_scan_at >= COALESCE(scan_roots.last_successful_scan_at, 0)
				THEN excluded.last_scan_id
				ELSE scan_roots.last_scan_id
			END,
			last_successful_scan_at = MAX(COALESCE(scan_roots.last_successful_scan_at, 0), excluded.last_successful_scan_at)
	`, root.VolumeID, root.RootPath, scannedAt, root.LastScanID)
	return err
}

// InsertEntry inserts a newly discovered entry.
func (m *MergeTx) InsertEntry(ctx context.Context, e *model.Entry) error {
	_, err := m.stmtInsertEntry.ExecContext(ctx,
		e.VolumeID,
		e.RelativePath,
		int(e.EntryType),
		e.SizeBytes,
		e.MTimeNs,
		e.QuickFingerprint,
		e.FullFingerprint,
		e.DirectoryFingerprint,
		e.LastSeenScanID,
	)
	return err
}

// UpdateEntry updates an existing entry.
func (m *MergeTx) UpdateEntry(ctx context.Context, e *model.Entry) error {
	_, err := m.stmtUpdateEntry.ExecContext(ctx,
		int(e.EntryType),
		e.SizeBytes,
		e.MTimeNs,
		e.QuickFingerprint,
		e.FullFingerprint,
		e.DirectoryFingerprint,
		e.LastSeenScanID,
		e.VolumeID,
		e.RelativePath,
	)
	return err
}

// ListEntries returns all entries for a volume within the active transaction.
func (m *MergeTx) ListEntries(ctx context.Context, volumeID string) ([]*model.Entry, error) {
	rows, err := m.tx.QueryContext(ctx, `
		SELECT volume_id, relative_path, entry_type, size_bytes, mtime_ns,
		       quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
		FROM entries
		WHERE volume_id = ?
		ORDER BY relative_path
	`, volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed listing entries in tx: %w", err)
	}
	defer rows.Close()

	var entries []*model.Entry
	for rows.Next() {
		var e model.Entry
		var entryType int
		var sizeBytes, mtimeNs sql.NullInt64
		var quickFP, fullFP, dirFP []byte
		var lastScan sql.NullString

		if err := rows.Scan(
			&e.VolumeID,
			&e.RelativePath,
			&entryType,
			&sizeBytes,
			&mtimeNs,
			&quickFP,
			&fullFP,
			&dirFP,
			&lastScan,
		); err != nil {
			return nil, fmt.Errorf("failed scanning entry in tx: %w", err)
		}

		e.EntryType = model.EntryType(entryType)
		e.SizeBytes = sizeBytes.Int64
		e.MTimeNs = mtimeNs.Int64
		e.QuickFingerprint = quickFP
		e.FullFingerprint = fullFP
		e.DirectoryFingerprint = dirFP
		e.LastSeenScanID = lastScan.String

		entries = append(entries, &e)
	}

	return entries, rows.Err()
}

// RecordMergeRun records summary details of a merge run.
func (m *MergeTx) RecordMergeRun(ctx context.Context, run *model.MergeRun) error {
	var completedAt sql.NullInt64
	if run.CompletedAt != nil {
		completedAt = sql.NullInt64{Int64: run.CompletedAt.Unix(), Valid: true}
	}

	_, err := m.tx.ExecContext(ctx, `
		INSERT INTO merge_runs (
			merge_id, source_path, status, started_at, completed_at,
			volumes_merged, entries_inserted, entries_updated, entries_identical,
			same_content_diff_loc, conflicts_count, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(merge_id) DO UPDATE SET
			status = excluded.status,
			completed_at = excluded.completed_at,
			volumes_merged = excluded.volumes_merged,
			entries_inserted = excluded.entries_inserted,
			entries_updated = excluded.entries_updated,
			entries_identical = excluded.entries_identical,
			same_content_diff_loc = excluded.same_content_diff_loc,
			conflicts_count = excluded.conflicts_count,
			error_message = excluded.error_message
	`, run.MergeID, run.SourcePath, run.Status, run.StartedAt.Unix(), completedAt,
		run.VolumesMerged, run.EntriesInserted, run.EntriesUpdated, run.EntriesIdentical,
		run.SameContentDiffLocation, run.ConflictsCount, run.ErrorMessage)
	return err
}

// RecordMergeConflict records an individual conflict detected during merge.
func (m *MergeTx) RecordMergeConflict(ctx context.Context, mergeID string, c *model.MergeConflict) error {
	_, err := m.stmtConflict.ExecContext(ctx,
		mergeID,
		string(c.ConflictKind),
		c.VolumeID,
		c.RelativePath,
		c.LocalDetail,
		c.RemoteDetail,
		c.Resolution,
		time.Now().Unix(),
	)
	return err
}

// GetAllScanRuns returns all scan run records indexed by scan_id.
func (d *DB) GetAllScanRuns(ctx context.Context) (map[string]*model.ScanRun, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT scan_id, volume_id, root_path, status, started_at, completed_at,
		       entries_scanned, errors_count, error_message
		FROM scan_runs
	`)
	if err != nil {
		return nil, fmt.Errorf("failed querying scan runs: %w", err)
	}
	defer rows.Close()

	results := make(map[string]*model.ScanRun)
	for rows.Next() {
		var r model.ScanRun
		var status string
		var startedAt int64
		var completedAt sql.NullInt64
		var errMsg sql.NullString

		if err := rows.Scan(
			&r.ScanID,
			&r.VolumeID,
			&r.RootPath,
			&status,
			&startedAt,
			&completedAt,
			&r.EntriesScanned,
			&r.ErrorsCount,
			&errMsg,
		); err != nil {
			return nil, fmt.Errorf("failed scanning scan run: %w", err)
		}

		r.Status = model.ScanStatus(status)
		r.StartedAt = time.Unix(startedAt, 0).UTC()
		if completedAt.Valid {
			t := time.Unix(completedAt.Int64, 0).UTC()
			r.CompletedAt = &t
		}
		r.ErrorMessage = errMsg.String

		results[r.ScanID] = &r
	}

	return results, rows.Err()
}

// GetScanRoots returns all scan roots for a volume.
func (d *DB) GetScanRoots(ctx context.Context, volumeID string) ([]*model.ScanRoot, error) {
	var query string
	var args []any
	if volumeID != "" {
		query = `SELECT volume_id, root_path, last_successful_scan_at, last_scan_id FROM scan_roots WHERE volume_id = ? ORDER BY root_path`
		args = []any{volumeID}
	} else {
		query = `SELECT volume_id, root_path, last_successful_scan_at, last_scan_id FROM scan_roots ORDER BY volume_id, root_path`
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying scan roots: %w", err)
	}
	defer rows.Close()

	var results []*model.ScanRoot
	for rows.Next() {
		var r model.ScanRoot
		var scanAt sql.NullInt64
		var scanID sql.NullString

		if err := rows.Scan(&r.VolumeID, &r.RootPath, &scanAt, &scanID); err != nil {
			return nil, fmt.Errorf("failed scanning scan root: %w", err)
		}
		if scanAt.Valid {
			t := time.Unix(scanAt.Int64, 0).UTC()
			r.LastSuccessfulScanAt = &t
		}
		r.LastScanID = scanID.String

		results = append(results, &r)
	}

	return results, rows.Err()
}

// GetAllFingerprints returns a set of all quick and directory fingerprints in the database.
func (d *DB) GetAllFingerprints(ctx context.Context) (map[string]bool, error) {
	fps := make(map[string]bool)

	rows, err := d.db.QueryContext(ctx, `
		SELECT quick_fingerprint FROM entries WHERE quick_fingerprint IS NOT NULL
		UNION
		SELECT directory_fingerprint FROM entries WHERE directory_fingerprint IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("failed querying all fingerprints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fp []byte
		if err := rows.Scan(&fp); err != nil {
			return nil, fmt.Errorf("failed scanning fingerprint: %w", err)
		}
		if len(fp) > 0 {
			fps[string(fp)] = true
		}
	}

	return fps, rows.Err()
}
