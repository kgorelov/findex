# Findex Database Backup and Restore Guide

This guide details best practices for backing up, maintaining, and restoring the SQLite database used by `findex`.

## 1. Storage Architecture and WAL Mode

`findex` stores all inventory data in a SQLite database configured with Write-Ahead Logging (`PRAGMA journal_mode = WAL;`).

In WAL mode, changes are written sequentially to a companion Write-Ahead Log file before being synced back to the main database file. At any given moment, the database on disk may consist of three files:

- `index.sqlite` — Main database file containing pages and schema.
- `index.sqlite-wal` — Active Write-Ahead Log containing recent committed transactions.
- `index.sqlite-shm` — Shared-memory index used for coordinating concurrent readers and writer.

> [!IMPORTANT]
> Never copy or back up `index.sqlite` alone while an application is actively writing or when `index.sqlite-wal` exists. Doing so will result in a corrupted or incomplete database snapshot missing recent transactions.

---

## 2. Backup Methods

### Method A: Online Backup via `sqlite3` CLI (Recommended)

SQLite provides an atomic online backup API that safely captures consistent snapshots even while scans, queries, or merges are running.

```bash
# Run online backup to a target snapshot file
sqlite3 ~/.findex/index.sqlite ".backup ~/.findex/backups/findex-backup-$(date +%Y%m%d%H%M%S).sqlite"
```

This ensures a clean, single-file backup with all WAL transactions fully integrated.

### Method B: Checkpoint and Cold Copy

If taking a direct file copy, ensure no active `findex` process is running, or checkpoint the WAL first:

1. **Flush the WAL into the main database:**

   ```bash
   sqlite3 ~/.findex/index.sqlite "PRAGMA wal_checkpoint(TRUNCATE);"
   ```

2. **Verify WAL truncation:**
   The `index.sqlite-wal` file will be truncated to 0 bytes or removed.

3. **Copy the database:**

   ```bash
   cp ~/.findex/index.sqlite /path/to/backup/findex.sqlite
   ```

### Method C: Portable Inventory Export

`findex` supports exporting full inventories to portable compressed JSON:

```bash
findex export --compress ~/.findex/backups/inventory-$(date +%Y%m%d).json.gz
```

Portable exports are human-readable, cross-platform, independent of SQLite binary versions, and can be imported into fresh databases using:

```bash
findex import ~/.findex/backups/inventory-20261031.json.gz
```

---

## 3. Restoring from a Backup

To restore an index from a backup snapshot:

1. Stop any running `findex` commands.
2. Replace or restore the SQLite file:

   ```bash
   # Remove stale WAL/SHM files
   rm -f ~/.findex/index.sqlite-wal ~/.findex/index.sqlite-shm

   # Copy backup into place
   cp /path/to/backup/findex-snapshot.sqlite ~/.findex/index.sqlite
   ```

3. Verify database integrity:

   ```bash
   findex check
   ```

---

## 4. Integrity and Consistency Diagnostics

Run `findex check` after any restore or migration to ensure structural integrity and schema consistency:

```bash
findex check
```

Output includes:
- SQLite low-level B-tree integrity check (`PRAGMA integrity_check`)
- Foreign key relationship verification (`PRAGMA foreign_key_check`)
- Inventory schema and algorithm version metadata checks
- Orphan entries and path normalization validation
- Scan run status and abandoned scan detection
