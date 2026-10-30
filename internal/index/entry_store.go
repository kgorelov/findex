package index

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

var (
	// ErrEntryNotFound is returned when a requested entry does not exist in the index.
	ErrEntryNotFound = errors.New("entry not found")
)

// CreateScanRun inserts a new scan run record.
func (d *DB) CreateScanRun(ctx context.Context, run *model.ScanRun) error {
	if run == nil || run.ScanID == "" {
		return fmt.Errorf("scan run or scan_id cannot be empty")
	}

	query := `
		INSERT INTO scan_runs (
			scan_id, volume_id, root_path, status, started_at,
			completed_at, entries_scanned, errors_count, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	var completedAt sql.NullInt64
	if run.CompletedAt != nil {
		completedAt = sql.NullInt64{Int64: run.CompletedAt.Unix(), Valid: true}
	}

	_, err := d.db.ExecContext(ctx, query,
		run.ScanID,
		run.VolumeID,
		run.RootPath,
		string(run.Status),
		run.StartedAt.Unix(),
		completedAt,
		run.EntriesScanned,
		run.ErrorsCount,
		run.ErrorMessage,
	)
	if err != nil {
		return fmt.Errorf("failed inserting scan run %q: %w", run.ScanID, err)
	}
	return nil
}

// UpdateScanRun updates the status and statistics of an existing scan run.
func (d *DB) UpdateScanRun(ctx context.Context, run *model.ScanRun) error {
	if run == nil || run.ScanID == "" {
		return fmt.Errorf("scan run or scan_id cannot be empty")
	}

	query := `
		UPDATE scan_runs SET
			status = ?,
			completed_at = ?,
			entries_scanned = ?,
			errors_count = ?,
			error_message = ?
		WHERE scan_id = ?
	`
	var completedAt sql.NullInt64
	if run.CompletedAt != nil {
		completedAt = sql.NullInt64{Int64: run.CompletedAt.Unix(), Valid: true}
	}

	_, err := d.db.ExecContext(ctx, query,
		string(run.Status),
		completedAt,
		run.EntriesScanned,
		run.ErrorsCount,
		run.ErrorMessage,
		run.ScanID,
	)
	if err != nil {
		return fmt.Errorf("failed updating scan run %q: %w", run.ScanID, err)
	}
	return nil
}

// RecordScanRootSuccess records a successful scan for a given volume and root path.
func (d *DB) RecordScanRootSuccess(ctx context.Context, volumeID, rootPath, scanID string, scannedAt time.Time) error {
	query := `
		INSERT INTO scan_roots (volume_id, root_path, last_successful_scan_at, last_scan_id)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(volume_id, root_path) DO UPDATE SET
			last_successful_scan_at = excluded.last_successful_scan_at,
			last_scan_id = excluded.last_scan_id;
	`
	_, err := d.db.ExecContext(ctx, query, volumeID, rootPath, scannedAt.Unix(), scanID)
	if err != nil {
		return fmt.Errorf("failed recording scan root success: %w", err)
	}
	return nil
}

// UpsertEntries inserts or updates a slice of entries in a single transaction.
func (d *DB) UpsertEntries(ctx context.Context, entries []*model.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed beginning transaction for upsert: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO entries (
			volume_id, relative_path, entry_type, size_bytes, mtime_ns,
			quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(volume_id, relative_path) DO UPDATE SET
			entry_type = excluded.entry_type,
			size_bytes = excluded.size_bytes,
			mtime_ns = excluded.mtime_ns,
			quick_fingerprint = excluded.quick_fingerprint,
			full_fingerprint = COALESCE(excluded.full_fingerprint, entries.full_fingerprint),
			directory_fingerprint = excluded.directory_fingerprint,
			last_seen_scan_id = excluded.last_seen_scan_id;
	`)
	if err != nil {
		return fmt.Errorf("failed preparing upsert statement: %w", err)
	}
	defer stmt.Close()

	for _, e := range entries {
		_, err := stmt.ExecContext(ctx,
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
		if err != nil {
			return fmt.Errorf("failed inserting entry %s:%s: %w", e.VolumeID, e.RelativePath, err)
		}
	}

	return tx.Commit()
}

// ReconcileStaleEntries deletes entries under the specified rootPath for volumeID
// that were not observed during scanID.
func (d *DB) ReconcileStaleEntries(ctx context.Context, volumeID, rootPath, scanID string) (int64, error) {
	var query string
	var args []any

	if rootPath == "/" || rootPath == "" {
		query = `
			DELETE FROM entries
			WHERE volume_id = ?
			  AND (last_seen_scan_id != ? OR last_seen_scan_id IS NULL)
		`
		args = []any{volumeID, scanID}
	} else {
		prefix := strings.TrimSuffix(rootPath, "/")
		query = `
			DELETE FROM entries
			WHERE volume_id = ?
			  AND (relative_path = ? OR relative_path LIKE ? || '/%')
			  AND (last_seen_scan_id != ? OR last_seen_scan_id IS NULL)
		`
		args = []any{volumeID, prefix, prefix, scanID}
	}

	res, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed reconciling stale entries under %s: %w", rootPath, err)
	}

	return res.RowsAffected()
}

// EntryMetadata holds cached file attributes and fingerprints for fast change detection.
type EntryMetadata struct {
	EntryType        model.EntryType
	SizeBytes        int64
	MTimeNs          int64
	QuickFingerprint []byte
	FullFingerprint  []byte
}

// GetSubtreeMetadata loads cached metadata for all entries under rootPath for volumeID.
func (d *DB) GetSubtreeMetadata(ctx context.Context, volumeID, rootPath string) (map[string]*EntryMetadata, error) {
	var query string
	var args []any

	if rootPath == "/" || rootPath == "" {
		query = `
			SELECT relative_path, entry_type, size_bytes, mtime_ns, quick_fingerprint, full_fingerprint
			FROM entries
			WHERE volume_id = ?
		`
		args = []any{volumeID}
	} else {
		prefix := strings.TrimSuffix(rootPath, "/")
		query = `
			SELECT relative_path, entry_type, size_bytes, mtime_ns, quick_fingerprint, full_fingerprint
			FROM entries
			WHERE volume_id = ?
			  AND (relative_path = ? OR relative_path LIKE ? || '/%')
		`
		args = []any{volumeID, prefix, prefix}
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed querying subtree metadata under %s: %w", rootPath, err)
	}
	defer rows.Close()

	results := make(map[string]*EntryMetadata)
	for rows.Next() {
		var path string
		var entryType int
		var sizeBytes, mtimeNs sql.NullInt64
		var quickFP, fullFP []byte

		if err := rows.Scan(&path, &entryType, &sizeBytes, &mtimeNs, &quickFP, &fullFP); err != nil {
			return nil, fmt.Errorf("failed scanning subtree metadata row: %w", err)
		}

		results[path] = &EntryMetadata{
			EntryType:        model.EntryType(entryType),
			SizeBytes:        sizeBytes.Int64,
			MTimeNs:          mtimeNs.Int64,
			QuickFingerprint: quickFP,
			FullFingerprint:  fullFP,
		}
	}

	return results, rows.Err()
}

// GetEntry retrieves an entry by volume ID and relative path.
func (d *DB) GetEntry(ctx context.Context, volumeID, relativePath string) (*model.Entry, error) {
	rel := model.NormalizeRelativePath(relativePath)

	var e model.Entry
	var entryType int
	var sizeBytes, mtimeNs sql.NullInt64
	var quickFP, fullFP, dirFP []byte
	var lastScan sql.NullString

	query := `
		SELECT volume_id, relative_path, entry_type, size_bytes, mtime_ns,
		       quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
		FROM entries
		WHERE volume_id = ? AND relative_path = ?
	`
	err := d.db.QueryRowContext(ctx, query, volumeID, rel).Scan(
		&e.VolumeID,
		&e.RelativePath,
		&entryType,
		&sizeBytes,
		&mtimeNs,
		&quickFP,
		&fullFP,
		&dirFP,
		&lastScan,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrEntryNotFound
		}
		return nil, fmt.Errorf("failed getting entry: %w", err)
	}

	e.EntryType = model.EntryType(entryType)
	e.SizeBytes = sizeBytes.Int64
	e.MTimeNs = mtimeNs.Int64
	e.QuickFingerprint = quickFP
	e.FullFingerprint = fullFP
	e.DirectoryFingerprint = dirFP
	e.LastSeenScanID = lastScan.String

	return &e, nil
}

// ListEntries returns entries for a volume matching an optional path prefix.
func (d *DB) ListEntries(ctx context.Context, volumeID, pathPrefix string) ([]*model.Entry, error) {
	var query string
	var args []any

	if pathPrefix == "" || pathPrefix == "/" {
		query = `
			SELECT volume_id, relative_path, entry_type, size_bytes, mtime_ns,
			       quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
			FROM entries
			WHERE volume_id = ?
			ORDER BY relative_path
		`
		args = []any{volumeID}
	} else {
		prefix := model.NormalizeRelativePath(pathPrefix)
		query = `
			SELECT volume_id, relative_path, entry_type, size_bytes, mtime_ns,
			       quick_fingerprint, full_fingerprint, directory_fingerprint, last_seen_scan_id
			FROM entries
			WHERE volume_id = ?
			  AND (relative_path = ? OR relative_path LIKE ? || '/%')
			ORDER BY relative_path
		`
		args = []any{volumeID, prefix, prefix}
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed listing entries: %w", err)
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
			return nil, fmt.Errorf("failed scanning entry: %w", err)
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
