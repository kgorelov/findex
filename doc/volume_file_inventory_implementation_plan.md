# Multi-Volume File Inventory — Implementation Plan

## 1. Project Goal

Implement a Go application that builds and maintains a persistent inventory of files and directories across multiple storage volumes.

The index is stored in SQLite and must support:

- Fast initial indexing of large directory trees.
- Incremental indexing: index one directory now, add another directory later, and continue expanding coverage.
- Re-indexing: update fingerprints and metadata when files change; remove stale entries only within successfully rescanned areas.
- Merging indexes produced on different machines, including remote hosts.
- Comparing arbitrary sets of volumes, for example remote volumes against local volumes.
- Finding files and directories whose content is present in one volume set but absent from another, as well as content unique to a particular volume.
- Keeping volume metadata that helps identify where a volume came from, without making the host part of volume identity.

The application is an inventory and recovery-planning tool. Its primary purpose is to identify content that is not represented in another selected set of volumes.

## 2. Core Data Model and Identity

### 2.1 Volume identity

A volume represents an indexed filesystem.

Use a stable filesystem identifier as `volume_id`. Do not include a host identifier in the primary identity model: a filesystem remains the same volume when moved between machines.

Store descriptive metadata separately:

- filesystem UUID / native filesystem identifier;
- filesystem type;
- volume label;
- device or hardware identifier, when available;
- last known mount point;
- source host name or user-defined location label;
- first-seen and last-seen timestamps.

Mount points and host names are descriptive hints, not identity keys. A mount point can change, and a volume can be attached to another machine.

**Important edge case:** cloned filesystems may have duplicate filesystem UUIDs. Detect and report conflicting volume metadata or simultaneous observations rather than silently treating two distinct devices as one. If a filesystem has no reliable native identifier, assign a persistent application-generated UUID and store it on that filesystem or in a sidecar/registration mechanism. Select the fallback mechanism during Phase 1.

### 2.2 Location identity

A file or directory location is uniquely identified by:

```text
(volume_id, relative_path)
```

`relative_path` is always relative to the filesystem root, not to the directory passed to a particular scan operation. This allows multiple partial scans of the same volume to be combined without path collisions.

Paths are location metadata. They are not part of content fingerprints.

### 2.3 Content identity

Keep content identity separate from location identity:

- File content identity: file size plus a content fingerprint.
- Directory content identity: a recursive fingerprint of its contents.
- Location identity: volume ID plus relative path.

The initial implementation uses a fast sparse file fingerprint. A full-content hash can be added as an optional verification tier.

Directory fingerprints are content-oriented and exclude child names. They include each child's entry kind, relevant size, and fingerprint, sorted deterministically before hashing. This makes a directory's content fingerprint independent of filenames and traversal order.

Preserve duplicate child descriptors. A directory containing two identical files is not equivalent, as a multiset, to a directory containing only one copy. Directory names and paths remain stored in the index so that locations can still be reported and recovered.

This means content comparison and path-layout comparison are distinct operations. The first is in scope for the initial implementation; exact layout comparison can be added later if required.

## 3. SQLite Schema

Use a versioned SQLite schema. Store fingerprints as binary BLOB values, not hexadecimal text.

A starting schema:

```sql
CREATE TABLE volumes (
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
);

CREATE TABLE scan_roots (
    volume_id           TEXT NOT NULL,
    root_path           TEXT NOT NULL,
    last_successful_scan_at INTEGER,
    last_scan_id        TEXT,
    PRIMARY KEY (volume_id, root_path),
    FOREIGN KEY (volume_id) REFERENCES volumes(volume_id)
);

CREATE TABLE entries (
    volume_id           TEXT NOT NULL,
    relative_path       TEXT NOT NULL,
    entry_type          INTEGER NOT NULL,
    size_bytes          INTEGER,
    mtime_ns            INTEGER,
    quick_fingerprint  BLOB,
    full_fingerprint   BLOB,
    directory_fingerprint BLOB,
    last_seen_scan_id   TEXT,
    PRIMARY KEY (volume_id, relative_path),
    FOREIGN KEY (volume_id) REFERENCES volumes(volume_id)
);

CREATE INDEX idx_entries_quick_fingerprint
    ON entries(quick_fingerprint);

CREATE INDEX idx_entries_full_fingerprint
    ON entries(full_fingerprint);

CREATE INDEX idx_entries_directory_fingerprint
    ON entries(directory_fingerprint);

CREATE INDEX idx_entries_size_type
    ON entries(entry_type, size_bytes);
```

The schema is illustrative; refine nullability and constraints during implementation.

Notes:

- `entry_type` distinguishes regular files, directories, symlinks, and other supported entry kinds.
- `size_bytes` is meaningful for regular files. Directory size is not a content measure.
- `mtime_ns` is an optimization hint, not proof that content is unchanged.
- `quick_fingerprint` is the initial sparse fingerprint.
- `full_fingerprint` is optional and may be populated later.
- `directory_fingerprint` is calculated bottom-up from child descriptors.
- `last_seen_scan_id` supports safe reconciliation after a completed scan.
- Consider a `scan_runs` table recording scan start/end time, status, root path, errors, and statistics.
- Enable foreign keys. Use WAL mode for normal operation and transactions for batches of updates. Choose and document an appropriate durability setting.

### 3.1 Path representation

Normalize paths consistently:

- Use `/` as the database separator on every platform.
- Store paths relative to the filesystem root.
- Do not store the current mount point as part of `relative_path`.
- Define explicit handling for case-sensitive and case-insensitive filesystems.
- Preserve original filename spelling.
- Avoid silently applying case or Unicode normalization that could collapse distinct names on a case-sensitive filesystem.

If a volume is mounted at different paths, its entries must still map to the same database keys.

## 4. Indexing and Update Semantics

### 4.1 Partial scans and appendability

The user must be able to index arbitrary subdirectories over time:

```text
Initial scan:  /Music
Later scan:    /Photos
Later scan:    /Projects/Archive
```

Each scan maps its supplied directory to a path relative to the volume root. Upserts affect only entries encountered in that scan.

A scan of `/Music` must not remove entries previously indexed under `/Photos`.

Track indexed scan roots in `scan_roots`. A scan root describes the area for which the application has a successful inventory. Support overlapping roots carefully; a partial scan must not invalidate entries outside its own scope.

### 4.2 Safe reconciliation

Do not delete stale entries while walking the filesystem.

For each scan:

1. Create a scan-run record with status `running`.
2. Walk the requested subtree.
3. Upsert discovered entries and set `last_seen_scan_id` to the current scan ID.
4. Calculate directory fingerprints after their children have been processed.
5. If the scan completed successfully, delete or mark stale entries under that scanned root that were not seen in this scan.
6. Mark the scan run `completed` and update the root's successful scan timestamp.
7. If the scan fails or is interrupted, mark it `failed` and do not perform stale-entry deletion.

Use a transaction strategy that balances safety and database size. For very large scans, avoid holding one enormous write transaction for the entire filesystem walk. A practical approach is batched upserts plus a final reconciliation step that runs only after successful completion. Ensure reconciliation is restricted to the scanned subtree.

### 4.3 Change detection and fast indexing

Use metadata as a fast-path hint:

- If size and reliable modification metadata match the previous record, the implementation may skip recomputing a fingerprint according to a documented policy.
- Provide a stricter mode that recomputes fingerprints regardless of metadata.
- Do not treat modification time alone as proof of unchanged content.
- Handle files changing during reads: compare relevant metadata before and after fingerprinting and retry or report the file as unstable.

Make the trade-off explicit in the CLI, for example `--fast` versus `--verify`.

### 4.4 Concurrency

Use a bounded worker pool for file fingerprinting:

- Directory traversal discovers work.
- Workers read and fingerprint files.
- A controlled writer batches SQLite updates.
- Bound queued work to avoid unbounded memory growth.
- Keep SQLite writes serialized through one writer or a small, explicitly managed write path.
- Make worker count and read-buffer sizes configurable.

Avoid launching one goroutine per file without a bound.

## 5. Fingerprinting

### 5.1 File quick fingerprint

Use BLAKE3-256 for the initial implementation.

Define a versioned sparse fingerprint algorithm. It should incorporate:

- file size;
- bytes from the beginning;
- bytes from one or more deterministic interior offsets;
- bytes from the end;
- unambiguous framing/domain separation so different sample layouts cannot produce ambiguous input streams.

The sampling policy must be deterministic for a given file size and algorithm version. Handle small files by hashing the whole file.

Store the algorithm identifier and version in database metadata. A future algorithm change must not silently mix incompatible fingerprints.

Sparse fingerprints are probabilistic identifiers, not proof that two entire files are identical. Use size as an additional filter and offer optional full BLAKE3 hashing for stronger verification.

### 5.2 Directory content fingerprint

Compute directory fingerprints bottom-up.

For each direct child, construct a deterministic descriptor containing:

- entry kind;
- file size where applicable;
- the child's file fingerprint or directory content fingerprint.

Do not include child names in the content descriptor. Sort descriptors lexicographically before hashing. Preserve repeated descriptors so the directory represents a multiset of child contents.

Use explicit length-prefixing or fixed-width fields to avoid ambiguous concatenation. Include a version/domain tag in the hash input.

Directory fingerprints are content summaries. They intentionally do not detect a rename or a rearrangement that leaves the multiset of child contents unchanged. Stored paths provide location information separately.

### 5.3 Fingerprint versioning

Store the fingerprint algorithm and version in database metadata. If multiple algorithms coexist during migration, store an algorithm/version identifier with each fingerprint or maintain separate columns/tables. Never compare fingerprints from incompatible algorithms as if they were equivalent.

## 6. Index Merge

Indexes produced on different machines must be mergeable without requiring the original volumes to be connected.

Provide a merge operation that imports volume metadata and entries from another SQLite index or a portable export.

Merge rules:

1. Match volumes by `volume_id`.
2. Insert previously unknown volumes.
3. Merge descriptive metadata without treating host name or mount point as identity.
4. For each `(volume_id, relative_path)`, insert a missing entry or reconcile it with the existing entry.
5. Do not blindly overwrite a newer or better-verified fingerprint with an older or weaker one.
6. Detect and report conflicting observations of the same location, especially when timestamps, sizes, or fingerprints disagree.
7. Record merge provenance and conflicts for diagnostics.

A merge must be repeatable: importing the same index more than once should not create duplicate entries or corrupt metadata.

For conflict resolution, distinguish at least:
- same location and same content;
- same location but different content;
- same volume ID with conflicting filesystem metadata;
- same content at different locations.

Do not resolve ambiguous conflicts silently. Provide a report and a documented policy, such as preferring a more recent successful scan only when its provenance and timestamps are trustworthy.

Use SQLite transactions for each import operation. `ATTACH DATABASE` may be used where practical, but merge logic must also work when the source index is transferred as a file.

## 7. Volume and Content Comparison

### 7.1 Volume selection

Provide commands to list volumes and inspect their metadata:

- volume ID;
- filesystem UUID and type;
- label;
- device ID, if available;
- last known mount point;
- source host / user-defined source label;
- last successful scan time;
- indexed roots;
- number of files, directories, and total indexed bytes.

Allow selection by explicit volume IDs and useful metadata filters. Host name is descriptive metadata, not identity.

### 7.2 Comparing sets of volumes

Support comparisons between arbitrary sets:

```text
findex compare --preset local-volume-A,local-volume-B --absent remote-volume-C,remote-volume-D
```

The semantics should be explicit:

- **Present in A, absent from B:** content represented by volumes in set A but not represented by any volume in set B.
- **Unique to one volume:** content represented by a selected volume but not by any other selected volume.
- **Common content:** content represented in both sets.
- **Volume inventory:** entries located on a particular volume, regardless of whether the content exists elsewhere.

For files, use `(size, fingerprint algorithm/version, fingerprint)` as the initial content matching key. Optionally verify candidate matches using full fingerprints.

For directories, compare directory content fingerprints. Because directory fingerprints exclude names, this answers whether equivalent directory contents are represented, not whether the same directory path/layout exists.

### 7.3 Multiplicity and duplicate content

Define two possible comparison semantics:

- **Set semantics:** content is present if at least one matching copy exists in the destination set.
- **Multiset semantics:** the number of copies matters; two source copies require two destination copies.

Use set semantics as the initial default for the recovery use case: a file is not missing if an identical copy exists anywhere in the destination set. Retain all location records so every copy can be reported. Multiset comparison can be added later if needed.

Directory comparison needs separate care for duplicate directory fingerprints. A directory fingerprint does not uniquely identify a directory location.

### 7.4 Efficient comparison

Create indexes that support fingerprint lookups. Avoid loading every entry into Go memory for large inventories.

Use SQL joins, temporary tables, or attached databases to compare selected volume sets. Benchmark representative queries before choosing the final query strategy.

Comparison results should include:
- content identity and size;
- source volume and relative path;
- whether a matching copy exists in the destination set;
- matching destination locations, when requested;
- confidence/verification status (quick fingerprint only or full hash verified).

## 8. Command-Line Interface

A proposed command structure:

```text
findex volume list
findex volume show <volume-id>

findex scan <mount-point-or-directory> [options]
findex scan --volume <volume-id> <directory> [options]

findex merge <other-index.sqlite>

findex compare --preset <volume-set> --absent <volume-set>
findex unique --volume <volume-id> --among <volume-set>

findex files --volume <volume-id> [path-prefix]
findex export <output>
findex check
```

The exact CLI syntax is open to refinement. Important behaviours:

- A scan must identify the volume and map the scan root to a volume-relative path.
- If volume identity cannot be established safely, require explicit registration rather than guessing.
- `volume list` should make volumes easy to distinguish using source host, label, filesystem UUID, and mount-point metadata.
- Comparison commands should accept explicit volume IDs and, eventually, saved named groups.
- Output should support human-readable text and machine-readable JSON/JSONL.
- Long-running operations should report progress and final statistics.

## 9. Go Project Structure

A possible package layout:

```text
cmd/findex/
    main.go

internal/
    model/          # Volume, Entry, ScanRun, fingerprint types
    volume/         # Filesystem identity and metadata discovery
    fingerprint/    # Sparse and full file fingerprints
    tree/           # Traversal and bottom-up directory fingerprints
    index/          # SQLite schema, migrations, queries, upserts
    scan/           # Scan orchestration, workers, reconciliation
    merge/          # Database import and conflict handling
    compare/        # Volume-set and uniqueness queries
    output/         # Text and JSON output
```

Keep SQLite-specific code inside `index` where practical. Other packages should use domain types and interfaces rather than issuing ad hoc SQL.

Use schema migrations from the beginning. Pin and document the SQLite driver and BLAKE3 implementation.

## 10. Implementation Phases

### Phase 1 — Domain Model and SQLite Foundation

- Define volume identity and metadata.
- Define `(volume_id, relative_path)` as the entry primary key.
- Define entry types and path normalization rules.
- Implement SQLite connection management, schema creation, migrations, and metadata.
- Implement volume registration and `volume list` / `volume show`.
- Add tests for duplicate filesystem identifiers and conflicting volume metadata.

**Deliverable:** an empty, versioned SQLite index that can register and list volumes.

### Phase 2 — File Fingerprinting

- Implement versioned BLAKE3 sparse fingerprints.
- Implement full BLAKE3 fingerprints behind an optional mode.
- Handle empty files, small files, large files, read errors, and files modified during hashing.
- Add deterministic test vectors and algorithm-version metadata.

**Deliverable:** tested file fingerprint package and CLI/debug test command.

### Phase 3 — Recursive and Partial Indexing

- Implement filesystem traversal rooted at an arbitrary directory.
- Map every discovered path to a volume-relative path.
- Implement bounded parallel fingerprint workers and batched SQLite writes.
- Upsert entries using `(volume_id, relative_path)`.
- Implement scan-run tracking and safe stale-entry reconciliation limited to the successfully scanned subtree.
- Implement bottom-up directory content fingerprints.
- Support adding a second or subsequent directory to an already indexed volume without disturbing previously indexed areas.

**Deliverable:** reliable initial and incremental scans of multiple roots on one or more volumes.

### Phase 4 — Re-scanning and Performance

- Implement fast-path change detection using size and modification metadata.
- Add a strict re-fingerprint mode.
- Handle unstable files and partial failures explicitly.
- Benchmark traversal, hashing, database writes, and directory fingerprint calculation.
- Tune worker count, batch size, SQLite pragmas, and indexes.
- Add progress reporting and scan statistics.

**Deliverable:** fast repeatable indexing with documented correctness/performance trade-offs.

### Phase 5 — Merge

- Import another SQLite index transactionally.
- Merge volume metadata and entries using stable volume IDs.
- Make repeated imports idempotent.
- Detect and report conflicting volume identities and conflicting observations of locations.
- Preserve provenance and scan timestamps.
- Add merge tests using indexes created independently on different machines.

**Deliverable:** indexes from remote and local hosts can be combined safely.

### Phase 6 — Volume-Set Comparison

- List and filter volumes using metadata.
- Implement file content comparison across arbitrary volume sets.
- Implement directory content comparison using directory fingerprints.
- Implement per-volume uniqueness queries.
- Report source locations and matching destination locations.
- Add set semantics as the initial default and test duplicate-content cases.
- Add optional full-hash verification of candidate matches.

**Deliverable:** commands that identify content present in one volume set and absent from another, and content unique to a selected volume.

### Phase 7 — Export, Diagnostics, and Hardening

- Add portable export/import if needed; SQLite remains the canonical working format.
- Add database integrity checks and index consistency checks.
- Add structured logs and machine-readable reports.
- Test interrupted scans, interrupted merges, unreadable directories, disappearing volumes, permission errors, symlinks, and unusual filenames.
- Document backup and restore of the SQLite index.
- Document fingerprint limitations and volume-identity collision handling.

**Deliverable:** a robust tool suitable for maintaining and merging long-lived inventories.

## 11. Key Design Decisions to Preserve

1. SQLite is the canonical index format.
2. A volume is identified by a stable filesystem ID, not by its host or mount point.
3. Host name, mount point, label, and device information are descriptive metadata only.
4. The primary key for a location is `(volume_id, relative_path)`.
5. Partial scans are appendable and must not invalidate entries outside their scan root.
6. Stale entries are removed only after a successful scan of the relevant subtree.
7. File and directory content fingerprints do not include filenames or paths.
8. Directory fingerprints are deterministic, order-independent, and preserve duplicate child descriptors.
9. Index merging is idempotent and reports conflicts rather than silently discarding evidence.
10. Content presence across volume sets is distinct from location and directory-layout equality.
11. Sparse fingerprints are fast probabilistic identifiers; full hashing is available for stronger verification.
12. Algorithm and schema versions are explicit and migration-safe.
