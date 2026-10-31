# Fingerprinting and Volume Identity Model

This document outlines the design, implementation, trade-offs, and operational limitations of file/directory fingerprinting and volume identity in `findex`.

---

## 1. Volume Identity Model

### 1.1 Philosophy: Filesystem as Volume

A volume represents a persistent, indexed filesystem.

- The primary key is `volume_id`.
- Host names, network aliases, and mount paths are **descriptive metadata hints**, not identity keys. A filesystem remains the same volume when remounted at a different mount point or connected to another physical host.

### 1.2 Resolution Strategy

When scanning or registering a volume, `findex` establishes identity using a prioritized resolution cascade:

1. **Explicit Identifier:** User-specified `--id` flag.
2. **Native Filesystem UUID:** Stable filesystem UUID queried from OS block device subsystem (e.g., `blkid`, `udev`, filesystem superblock).
3. **Persistent Sidecar:** Stored in `.findex-volume-id` at the filesystem root for filesystems without reliable native UUIDs (FAT, exFAT, network shares).
4. **Auto-Generated UUID:** If no identifier exists and sidecar creation is permitted, generates and persists a random UUID v4 sidecar.

### 1.3 Collision Handling (Cloned Disks & Duplicate UUIDs)

A critical edge case is disk cloning (e.g. `dd if=/dev/sda of=/dev/sdb`), which duplicates native filesystem UUIDs:

- `findex` detects when an incoming volume presents a filesystem UUID already associated with a different `volume_id`.
- By default, `findex` rejects registration and skips conflicting volumes during merge with a `duplicate_fs_uuid` error to prevent silent data corruption.
- Users can override using `--force` or assign distinct explicit identifiers using `findex volume register --id <unique-id>`.

---

## 2. File Fingerprinting

### 2.1 Sparse Quick Fingerprint (BLAKE3-256)

To index large multi-terabyte volumes with millions of files at wire speed, `findex` uses a versioned sparse sampling fingerprint:

- **Small files (< 192 KB by default):** Full file is read and hashed using BLAKE3-256.
- **Large files (>= 192 KB):** Samples three deterministic chunks:
  1. Initial 64 KB chunk starting at byte 0.
  2. Interior 64 KB chunk centered at `(file_size - chunk_size) / 2`.
  3. Tail 64 KB chunk ending at `file_size`.
- **Domain Framing:** Hash input uses explicit length-prefixed headers containing algorithm version (`v1`), file size in bytes, and sample boundaries to eliminate ambiguous concatenation.

### 2.2 Probabilistic Nature & Limitations

> [!WARNING]
> Sparse fingerprints are fast probabilistic content identifiers, not mathematical proof of whole-file identity. Two files with identical sampled offsets but differences in unsampled interior regions will produce identical sparse fingerprints.

### 2.3 Verification Tier (`--full` / `--verify`)

For workflows requiring cryptographic certainty:

- `findex scan --full`: Computes full-file BLAKE3-256 hashes alongside sparse fingerprints.
- `findex compare --verify`: Verifies candidate matches by comparing full BLAKE3 hashes whenever available.
- Merge operations protect verification levels: an entry with an existing full fingerprint will never be degraded or overwritten by an incoming entry lacking one if the sampled fingerprints match.

---

## 3. Directory Content Fingerprints

### 3.1 Bottom-Up Recursive Hashing

Directory fingerprints summarize recursive directory content without depending on file names or directory layout:

1. Direct children are processed:
   - Regular files provide `(size, quick_fingerprint)`.
   - Child subdirectories provide their computed `directory_fingerprint`.
   - Symlinks provide target hash.
2. Descriptors are sorted lexicographically before hashing.
3. Descriptors preserve duplicates (multiset semantics: a directory containing two identical files produces a distinct fingerprint from one containing one copy).
4. Child names and filesystem paths are intentionally excluded from the descriptor multiset.

### 3.2 Implications

- Renaming a directory or renaming files inside it does not change its content fingerprint.
- Moving identical directory trees to different paths or volumes produces identical directory fingerprints.
- `findex compare` can detect identical directory contents even when folder structures or layouts differ.
