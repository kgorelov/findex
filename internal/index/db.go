package index

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database connection and provides transaction and schema helpers.
type DB struct {
	db   *sql.DB
	path string
}

// Open opens or creates a SQLite index database at the specified path.
// It sets WAL mode, foreign keys, busy timeout, and durability pragmas.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("database path cannot be empty")
	}

	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create database directory %q: %w", dir, err)
		}
	}

	// modernc.org/sqlite driver name is "sqlite"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database at %q: %w", path, err)
	}

	// Apply essential pragmas for high performance and durability in WAL mode
	pragmas := []string{
		"PRAGMA foreign_keys = ON;",
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA cache_size = -64000;",   // 64 MiB page cache
		"PRAGMA temp_store = MEMORY;",   // In-memory temp tables
		"PRAGMA mmap_size = 268435456;", // 256 MiB memory mapping
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to execute %q: %w", p, err)
		}
	}

	// Restrict connection pool to 1 open connection if necessary for SQLite writes or sensible limits
	db.SetMaxOpenConns(1)

	idxDB := &DB{
		db:   db,
		path: path,
	}

	// Apply migrations
	if err := idxDB.Migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return idxDB, nil
}

// Close closes the underlying database handle.
func (d *DB) Close() error {
	return d.db.Close()
}

// RawDB returns the underlying *sql.DB for advanced queries.
func (d *DB) RawDB() *sql.DB {
	return d.db
}

// Path returns the filesystem path of the database.
func (d *DB) Path() string {
	return d.path
}

// SetMetadata stores or updates a key-value pair in the metadata table.
func (d *DB) SetMetadata(ctx context.Context, key, value string) error {
	query := `
		INSERT INTO metadata (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value;
	`
	_, err := d.db.ExecContext(ctx, query, key, value)
	return err
}

// GetMetadata retrieves a value from the metadata table.
func (d *DB) GetMetadata(ctx context.Context, key string) (string, error) {
	var value string
	err := d.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, key).Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}
