package index

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type migration struct {
	version int
	name    string
	up      func(ctx context.Context, tx *sql.Tx) error
}

var migrations = []migration{
	{
		version: 1,
		name:    "initial_schema",
		up: func(ctx context.Context, tx *sql.Tx) error {
			statements := []string{
				`CREATE TABLE IF NOT EXISTS metadata (
					key         TEXT PRIMARY KEY,
					value       TEXT NOT NULL
				);`,

				`CREATE TABLE IF NOT EXISTS volumes (
					volume_id           TEXT PRIMARY KEY,
					fs_uuid             TEXT,
					fs_type             TEXT,
					label               TEXT,
					device_id           TEXT,
					last_mount_point    TEXT,
					source_host         TEXT,
					source_label        TEXT,
					first_seen_at       INTEGER NOT NULL,
					last_seen_at        INTEGER
				);`,

				`CREATE INDEX IF NOT EXISTS idx_volumes_fs_uuid
					ON volumes(fs_uuid);`,

				`CREATE TABLE IF NOT EXISTS scan_roots (
					volume_id               TEXT NOT NULL,
					root_path               TEXT NOT NULL,
					last_successful_scan_at INTEGER,
					last_scan_id            TEXT,
					PRIMARY KEY (volume_id, root_path),
					FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
				);`,

				`CREATE TABLE IF NOT EXISTS entries (
					volume_id               TEXT NOT NULL,
					relative_path           TEXT NOT NULL,
					entry_type              INTEGER NOT NULL,
					size_bytes              INTEGER,
					mtime_ns                INTEGER,
					quick_fingerprint       BLOB,
					full_fingerprint        BLOB,
					directory_fingerprint   BLOB,
					last_seen_scan_id       TEXT,
					PRIMARY KEY (volume_id, relative_path),
					FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
				);`,

				`CREATE INDEX IF NOT EXISTS idx_entries_quick_fingerprint
					ON entries(quick_fingerprint);`,

				`CREATE INDEX IF NOT EXISTS idx_entries_full_fingerprint
					ON entries(full_fingerprint);`,

				`CREATE INDEX IF NOT EXISTS idx_entries_directory_fingerprint
					ON entries(directory_fingerprint);`,

				`CREATE INDEX IF NOT EXISTS idx_entries_size_type
					ON entries(entry_type, size_bytes);`,

				`CREATE TABLE IF NOT EXISTS scan_runs (
					scan_id             TEXT PRIMARY KEY,
					volume_id           TEXT NOT NULL,
					root_path           TEXT NOT NULL,
					status              TEXT NOT NULL,
					started_at          INTEGER NOT NULL,
					completed_at        INTEGER,
					entries_scanned     INTEGER DEFAULT 0,
					errors_count        INTEGER DEFAULT 0,
					error_message       TEXT,
					FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
				);`,

				`CREATE INDEX IF NOT EXISTS idx_scan_runs_volume
					ON scan_runs(volume_id);`,

				`INSERT OR IGNORE INTO metadata (key, value) VALUES
					('schema_version', '1'),
					('fingerprint_algorithm', 'blake3-sparse'),
					('fingerprint_version', '1');`,
			}

			for _, stmt := range statements {
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("failed executing statement: %w\nSQL: %s", err, stmt)
				}
			}
			return nil
		},
	},
	{
		version: 2,
		name:    "merge_tracking",
		up: func(ctx context.Context, tx *sql.Tx) error {
			statements := []string{
				`CREATE TABLE IF NOT EXISTS merge_runs (
					merge_id            TEXT PRIMARY KEY,
					source_path         TEXT NOT NULL,
					status              TEXT NOT NULL,
					started_at          INTEGER NOT NULL,
					completed_at        INTEGER,
					volumes_merged      INTEGER DEFAULT 0,
					entries_inserted    INTEGER DEFAULT 0,
					entries_updated     INTEGER DEFAULT 0,
					entries_identical   INTEGER DEFAULT 0,
					same_content_diff_loc INTEGER DEFAULT 0,
					conflicts_count     INTEGER DEFAULT 0,
					error_message       TEXT
				);`,

				`CREATE TABLE IF NOT EXISTS merge_conflicts (
					id                  INTEGER PRIMARY KEY AUTOINCREMENT,
					merge_id            TEXT NOT NULL,
					conflict_kind       TEXT NOT NULL,
					volume_id           TEXT NOT NULL,
					relative_path       TEXT,
					local_detail        TEXT,
					remote_detail       TEXT,
					resolution          TEXT,
					created_at          INTEGER NOT NULL,
					FOREIGN KEY (merge_id) REFERENCES merge_runs(merge_id) ON DELETE CASCADE
				);`,

				`CREATE INDEX IF NOT EXISTS idx_merge_conflicts_merge_id
					ON merge_conflicts(merge_id);`,
			}

			for _, stmt := range statements {
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("failed executing statement: %w\nSQL: %s", err, stmt)
				}
			}
			return nil
		},
	},
	{
		version: 3,
		name:    "compare_indexes",
		up: func(ctx context.Context, tx *sql.Tx) error {
			statements := []string{
				`CREATE INDEX IF NOT EXISTS idx_entries_quick_fp_vol_size
					ON entries(quick_fingerprint, volume_id, size_bytes);`,
				`CREATE INDEX IF NOT EXISTS idx_entries_dir_fp_vol
					ON entries(directory_fingerprint, volume_id);`,
				`ANALYZE;`,
			}

			for _, stmt := range statements {
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("failed executing statement: %w\nSQL: %s", err, stmt)
				}
			}
			return nil
		},
	},
}

// Migrate executes pending database migrations in a transaction.
func (d *DB) Migrate(ctx context.Context) error {
	// Create migration tracking table if not present
	_, err := d.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  INTEGER NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	for _, m := range migrations {
		var exists int
		err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, m.version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed checking migration status for version %d: %w", m.version, err)
		}
		if exists > 0 {
			continue
		}

		tx, err := d.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %d: %w", m.version, err)
		}

		if err := m.up(ctx, tx); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed applying migration %d (%s): %w", m.version, m.name, err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO schema_migrations (version, name, applied_at)
			VALUES (?, ?, ?)
		`, m.version, m.name, time.Now().Unix())
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to record migration %d: %w", m.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %d: %w", m.version, err)
		}
	}

	return nil
}

// SchemaVersion returns the latest applied migration version.
func (d *DB) SchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := d.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}
