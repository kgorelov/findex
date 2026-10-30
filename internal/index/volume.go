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
	// ErrVolumeNotFound is returned when a requested volume is not present in the index.
	ErrVolumeNotFound = errors.New("volume not found")
)

// RegisterVolume registers or updates a volume in the index.
// If force is false, it validates metadata consistency and returns a VolumeConflictError upon mismatch.
func (d *DB) RegisterVolume(ctx context.Context, vol *model.Volume, force bool) error {
	if vol == nil || vol.VolumeID == "" {
		return fmt.Errorf("volume_id cannot be empty")
	}

	now := time.Now().UTC()
	if vol.FirstSeenAt.IsZero() {
		vol.FirstSeenAt = now
	}
	if vol.LastSeenAt == nil || vol.LastSeenAt.IsZero() {
		vol.LastSeenAt = &now
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 1. Check for duplicate native filesystem UUID under a different volume_id (e.g. cloned filesystem)
	if vol.FSUUID != "" {
		var existingVolID string
		err := tx.QueryRowContext(ctx, `
			SELECT volume_id FROM volumes WHERE fs_uuid = ? AND volume_id != ? LIMIT 1
		`, vol.FSUUID, vol.VolumeID).Scan(&existingVolID)
		if err == nil {
			return &model.VolumeConflictError{
				Type:          model.ConflictTypeDuplicateFSUUID,
				VolumeID:      vol.VolumeID,
				ExistingValue: existingVolID,
				IncomingValue: vol.VolumeID,
				Message: fmt.Sprintf("filesystem UUID %q is already associated with volume %q; potential cloned filesystem",
					vol.FSUUID, existingVolID),
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("failed checking duplicate fs_uuid: %w", err)
		}
	}

	// 2. Check if volume_id already exists
	var existing model.Volume
	var firstSeenInt, lastSeenInt sql.NullInt64
	var fsUUID, fsType, label, deviceID, lastMount, sourceHost, sourceLabel sql.NullString

	err = tx.QueryRowContext(ctx, `
		SELECT volume_id, fs_uuid, fs_type, label, device_id, last_mount_point, source_host, source_label, first_seen_at, last_seen_at
		FROM volumes WHERE volume_id = ?
	`, vol.VolumeID).Scan(
		&existing.VolumeID,
		&fsUUID,
		&fsType,
		&label,
		&deviceID,
		&lastMount,
		&sourceHost,
		&sourceLabel,
		&firstSeenInt,
		&lastSeenInt,
	)

	if err == nil {
		// Existing volume found: populate fields
		existing.FSUUID = fsUUID.String
		existing.FSType = fsType.String
		existing.Label = label.String
		existing.DeviceID = deviceID.String
		existing.LastMountPoint = lastMount.String
		existing.SourceHost = sourceHost.String
		existing.SourceLabel = sourceLabel.String

		if !force {
			// Validate against conflicting metadata
			if existing.FSUUID != "" && vol.FSUUID != "" && existing.FSUUID != vol.FSUUID {
				return &model.VolumeConflictError{
					Type:          model.ConflictTypeFSUUIDMismatch,
					VolumeID:      vol.VolumeID,
					ExistingValue: existing.FSUUID,
					IncomingValue: vol.FSUUID,
					Message: fmt.Sprintf("volume %q has conflicting filesystem UUID: existing=%q, incoming=%q",
						vol.VolumeID, existing.FSUUID, vol.FSUUID),
				}
			}

			if existing.FSType != "" && vol.FSType != "" && !strings.EqualFold(existing.FSType, vol.FSType) {
				return &model.VolumeConflictError{
					Type:          model.ConflictTypeFSTypeMismatch,
					VolumeID:      vol.VolumeID,
					ExistingValue: existing.FSType,
					IncomingValue: vol.FSType,
					Message: fmt.Sprintf("volume %q has conflicting filesystem type: existing=%q, incoming=%q",
						vol.VolumeID, existing.FSType, vol.FSType),
				}
			}

			// If observed on same host, device ID must match if both present
			if existing.SourceHost != "" && vol.SourceHost != "" &&
				strings.EqualFold(existing.SourceHost, vol.SourceHost) &&
				existing.DeviceID != "" && vol.DeviceID != "" &&
				existing.DeviceID != vol.DeviceID {
				return &model.VolumeConflictError{
					Type:          model.ConflictTypeDeviceMismatch,
					VolumeID:      vol.VolumeID,
					ExistingValue: existing.DeviceID,
					IncomingValue: vol.DeviceID,
					Message: fmt.Sprintf("volume %q observed on same host %q with conflicting device ID: existing=%q, incoming=%q",
						vol.VolumeID, vol.SourceHost, existing.DeviceID, vol.DeviceID),
				}
			}
		}

		// Update fields
		targetMount := existing.LastMountPoint
		if vol.LastMountPoint != "" {
			targetMount = vol.LastMountPoint
		}
		targetLabel := existing.Label
		if vol.Label != "" {
			targetLabel = vol.Label
		}
		targetSourceHost := existing.SourceHost
		if vol.SourceHost != "" {
			targetSourceHost = vol.SourceHost
		}
		targetSourceLabel := existing.SourceLabel
		if vol.SourceLabel != "" {
			targetSourceLabel = vol.SourceLabel
		}
		targetFSUUID := existing.FSUUID
		if vol.FSUUID != "" {
			targetFSUUID = vol.FSUUID
		}
		targetFSType := existing.FSType
		if vol.FSType != "" {
			targetFSType = vol.FSType
		}
		targetDeviceID := existing.DeviceID
		if vol.DeviceID != "" {
			targetDeviceID = vol.DeviceID
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE volumes SET
				fs_uuid = ?,
				fs_type = ?,
				label = ?,
				device_id = ?,
				last_mount_point = ?,
				source_host = ?,
				source_label = ?,
				last_seen_at = ?
			WHERE volume_id = ?
		`, targetFSUUID, targetFSType, targetLabel, targetDeviceID, targetMount,
			targetSourceHost, targetSourceLabel, vol.LastSeenAt.Unix(), vol.VolumeID)
		if err != nil {
			return fmt.Errorf("failed to update volume %q: %w", vol.VolumeID, err)
		}

	} else if errors.Is(err, sql.ErrNoRows) {
		// New volume
		_, err = tx.ExecContext(ctx, `
			INSERT INTO volumes (
				volume_id, fs_uuid, fs_type, label, device_id,
				last_mount_point, source_host, source_label, first_seen_at, last_seen_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, vol.VolumeID, vol.FSUUID, vol.FSType, vol.Label, vol.DeviceID,
			vol.LastMountPoint, vol.SourceHost, vol.SourceLabel,
			vol.FirstSeenAt.Unix(), vol.LastSeenAt.Unix())
		if err != nil {
			return fmt.Errorf("failed to insert new volume %q: %w", vol.VolumeID, err)
		}
	} else {
		return fmt.Errorf("failed querying volume %q: %w", vol.VolumeID, err)
	}

	return tx.Commit()
}

// GetVolume retrieves a single volume by ID.
func (d *DB) GetVolume(ctx context.Context, volumeID string) (*model.Volume, error) {
	var vol model.Volume
	var firstSeenInt, lastSeenInt sql.NullInt64
	var fsUUID, fsType, label, deviceID, lastMount, sourceHost, sourceLabel sql.NullString

	err := d.db.QueryRowContext(ctx, `
		SELECT volume_id, fs_uuid, fs_type, label, device_id, last_mount_point, source_host, source_label, first_seen_at, last_seen_at
		FROM volumes WHERE volume_id = ?
	`, volumeID).Scan(
		&vol.VolumeID,
		&fsUUID,
		&fsType,
		&label,
		&deviceID,
		&lastMount,
		&sourceHost,
		&sourceLabel,
		&firstSeenInt,
		&lastSeenInt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVolumeNotFound
		}
		return nil, fmt.Errorf("failed to get volume %q: %w", volumeID, err)
	}

	vol.FSUUID = fsUUID.String
	vol.FSType = fsType.String
	vol.Label = label.String
	vol.DeviceID = deviceID.String
	vol.LastMountPoint = lastMount.String
	vol.SourceHost = sourceHost.String
	vol.SourceLabel = sourceLabel.String

	if firstSeenInt.Valid {
		vol.FirstSeenAt = time.Unix(firstSeenInt.Int64, 0).UTC()
	}
	if lastSeenInt.Valid {
		t := time.Unix(lastSeenInt.Int64, 0).UTC()
		vol.LastSeenAt = &t
	}

	return &vol, nil
}

// ListVolumes retrieves all volumes matching the filter, ordered by first_seen_at DESC.
func (d *DB) ListVolumes(ctx context.Context, filter model.VolumeFilter) ([]*model.Volume, error) {
	var conditions []string
	var args []any

	if filter.VolumeID != "" {
		conditions = append(conditions, "volume_id = ?")
		args = append(args, filter.VolumeID)
	}
	if filter.FSUUID != "" {
		conditions = append(conditions, "fs_uuid = ?")
		args = append(args, filter.FSUUID)
	}
	if filter.Label != "" {
		conditions = append(conditions, "label LIKE ?")
		args = append(args, "%"+filter.Label+"%")
	}
	if filter.SourceHost != "" {
		conditions = append(conditions, "source_host = ?")
		args = append(args, filter.SourceHost)
	}
	if filter.SourceLabel != "" {
		conditions = append(conditions, "source_label LIKE ?")
		args = append(args, "%"+filter.SourceLabel+"%")
	}

	query := `
		SELECT volume_id, fs_uuid, fs_type, label, device_id, last_mount_point, source_host, source_label, first_seen_at, last_seen_at
		FROM volumes
	`
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY first_seen_at DESC"

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list volumes: %w", err)
	}
	defer rows.Close()

	var results []*model.Volume
	for rows.Next() {
		var vol model.Volume
		var firstSeenInt, lastSeenInt sql.NullInt64
		var fsUUID, fsType, label, deviceID, lastMount, sourceHost, sourceLabel sql.NullString

		if err := rows.Scan(
			&vol.VolumeID,
			&fsUUID,
			&fsType,
			&label,
			&deviceID,
			&lastMount,
			&sourceHost,
			&sourceLabel,
			&firstSeenInt,
			&lastSeenInt,
		); err != nil {
			return nil, fmt.Errorf("failed scanning volume row: %w", err)
		}

		vol.FSUUID = fsUUID.String
		vol.FSType = fsType.String
		vol.Label = label.String
		vol.DeviceID = deviceID.String
		vol.LastMountPoint = lastMount.String
		vol.SourceHost = sourceHost.String
		vol.SourceLabel = sourceLabel.String

		if firstSeenInt.Valid {
			vol.FirstSeenAt = time.Unix(firstSeenInt.Int64, 0).UTC()
		}
		if lastSeenInt.Valid {
			t := time.Unix(lastSeenInt.Int64, 0).UTC()
			vol.LastSeenAt = &t
		}

		results = append(results, &vol)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading volume rows: %w", err)
	}

	return results, nil
}

// GetVolumeStats retrieves detailed statistics for a volume.
func (d *DB) GetVolumeStats(ctx context.Context, volumeID string) (*model.VolumeStats, error) {
	vol, err := d.GetVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	stats := &model.VolumeStats{
		Volume: *vol,
	}

	// 1. Scan roots
	rows, err := d.db.QueryContext(ctx, `
		SELECT root_path, last_successful_scan_at
		FROM scan_roots
		WHERE volume_id = ?
		ORDER BY root_path
	`, volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed querying scan roots: %w", err)
	}
	defer rows.Close()

	var latestScanAt int64
	for rows.Next() {
		var root string
		var scanAt sql.NullInt64
		if err := rows.Scan(&root, &scanAt); err != nil {
			return nil, err
		}
		stats.ScanRoots = append(stats.ScanRoots, root)
		if scanAt.Valid && scanAt.Int64 > latestScanAt {
			latestScanAt = scanAt.Int64
		}
	}
	stats.ScanRootsCount = int64(len(stats.ScanRoots))
	if latestScanAt > 0 {
		t := time.Unix(latestScanAt, 0).UTC()
		stats.LastSuccessfulAt = &t
	}

	// 2. Entries breakdown by type & total bytes
	entryRows, err := d.db.QueryContext(ctx, `
		SELECT entry_type, COUNT(*), COALESCE(SUM(size_bytes), 0)
		FROM entries
		WHERE volume_id = ?
		GROUP BY entry_type
	`, volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed querying entries breakdown: %w", err)
	}
	defer entryRows.Close()

	for entryRows.Next() {
		var entryType int
		var count, totalBytes int64
		if err := entryRows.Scan(&entryType, &count, &totalBytes); err != nil {
			return nil, err
		}
		switch model.EntryType(entryType) {
		case model.EntryTypeRegular:
			stats.FileCount = count
			stats.TotalBytes = totalBytes
		case model.EntryTypeDirectory:
			stats.DirectoryCount = count
		case model.EntryTypeSymlink:
			stats.SymlinkCount = count
		case model.EntryTypeOther:
			stats.OtherCount = count
		}
	}

	return stats, nil
}
