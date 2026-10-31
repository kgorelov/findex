package merge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// Options configures the merge behavior.
type Options struct {
	Policy ConflictPolicy
	Force  bool
	DryRun bool
}

// ConflictPolicy re-exports the model policy type for convenience.
type ConflictPolicy = model.ConflictPolicy

const (
	PolicyPreferNewer    = model.PolicyPreferNewer
	PolicyKeepLocal      = model.PolicyKeepLocal
	PolicyKeepRemote     = model.PolicyKeepRemote
	PolicyFailOnConflict = model.PolicyFailOnConflict
)

// Merge imports volume metadata and entries from an external SQLite index file.
func Merge(ctx context.Context, destDB *index.DB, sourcePath string, opts Options) (*model.MergeResult, error) {
	if destDB == nil {
		return nil, fmt.Errorf("destination database cannot be nil")
	}
	if sourcePath == "" {
		return nil, fmt.Errorf("source database path cannot be empty")
	}

	sourceDB, err := index.OpenReadOnly(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open source database at %q: %w", sourcePath, err)
	}
	defer sourceDB.Close()

	return MergeDB(ctx, destDB, sourceDB, sourcePath, opts)
}

// MergeDB performs the merge operation between two initialized DB instances.
func MergeDB(ctx context.Context, destDB *index.DB, sourceDB *index.DB, sourcePath string, opts Options) (*model.MergeResult, error) {
	if destDB == nil || sourceDB == nil {
		return nil, fmt.Errorf("destination and source databases cannot be nil")
	}

	if opts.Policy == "" {
		opts.Policy = PolicyPreferNewer
	}

	startTime := time.Now().UTC()
	mergeID := newUUID()

	result := &model.MergeResult{
		MergeID:    mergeID,
		SourcePath: sourcePath,
		StartedAt:  startTime,
	}

	// 1. Verify fingerprint algorithm and version compatibility
	if err := checkAlgorithmCompatibility(ctx, destDB, sourceDB); err != nil {
		return nil, err
	}

	// 2. Load context: scan runs, existing fingerprints, local volumes
	localScanRuns, err := destDB.GetAllScanRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed loading local scan runs: %w", err)
	}
	remoteScanRuns, err := sourceDB.GetAllScanRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed loading remote scan runs: %w", err)
	}

	existingFingerprints, err := destDB.GetAllFingerprints(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed loading existing fingerprints: %w", err)
	}

	localVolumesList, err := destDB.ListVolumes(ctx, model.VolumeFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed loading local volumes: %w", err)
	}
	localVolumesByID := make(map[string]*model.Volume, len(localVolumesList))
	localVolumesByFSUUID := make(map[string]*model.Volume, len(localVolumesList))
	for _, v := range localVolumesList {
		localVolumesByID[v.VolumeID] = v
		if v.FSUUID != "" {
			localVolumesByFSUUID[v.FSUUID] = v
		}
	}

	remoteVolumesList, err := sourceDB.ListVolumes(ctx, model.VolumeFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed loading remote volumes: %w", err)
	}

	// 3. Begin transaction on destination DB
	tx, err := destDB.BeginMergeTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed starting merge transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Record initial merge run to satisfy foreign key constraints
	initRun := &model.MergeRun{
		MergeID:    mergeID,
		SourcePath: sourcePath,
		Status:     "running",
		StartedAt:  startTime,
	}
	if err := tx.RecordMergeRun(ctx, initRun); err != nil {
		return nil, fmt.Errorf("failed recording initial merge run: %w", err)
	}

	// 4. Import all scan runs from source (preserves scan history and timestamps)
	for _, run := range remoteScanRuns {
		if err := tx.UpsertScanRun(ctx, run); err != nil {
			return nil, fmt.Errorf("failed importing scan run %q: %w", run.ScanID, err)
		}
	}

	// 5. Merge each remote volume
	for _, remoteVol := range remoteVolumesList {
		localVol, exists := localVolumesByID[remoteVol.VolumeID]

		if !exists {
			// Check for duplicate fs_uuid on a different volume_id (e.g. cloned filesystem)
			if remoteVol.FSUUID != "" {
				if dupVol, hasDup := localVolumesByFSUUID[remoteVol.FSUUID]; hasDup && dupVol.VolumeID != remoteVol.VolumeID {
					conflict := model.MergeConflict{
						ConflictKind: model.ConflictKindDuplicateFSUUID,
						VolumeID:     remoteVol.VolumeID,
						LocalDetail:  fmt.Sprintf("volume %q has fs_uuid %q", dupVol.VolumeID, dupVol.FSUUID),
						RemoteDetail: fmt.Sprintf("incoming volume %q has same fs_uuid %q", remoteVol.VolumeID, remoteVol.FSUUID),
					}
					result.ConflictsCount++

					if !opts.Force {
						if opts.Policy == PolicyFailOnConflict {
							conflict.Resolution = "aborted"
							result.Conflicts = append(result.Conflicts, conflict)
							_ = tx.RecordMergeConflict(ctx, mergeID, &conflict)
							return nil, fmt.Errorf("merge aborted: duplicate fs_uuid %q on volume %q conflicts with existing volume %q",
								remoteVol.FSUUID, remoteVol.VolumeID, dupVol.VolumeID)
						}
						conflict.Resolution = "skipped_due_to_duplicate_fs_uuid"
						result.Conflicts = append(result.Conflicts, conflict)
						_ = tx.RecordMergeConflict(ctx, mergeID, &conflict)
						continue
					}
					conflict.Resolution = "forced"
					result.Conflicts = append(result.Conflicts, conflict)
					_ = tx.RecordMergeConflict(ctx, mergeID, &conflict)
				}
			}

			// Insert new volume
			if err := tx.InsertVolume(ctx, remoteVol); err != nil {
				return nil, fmt.Errorf("failed inserting volume %q: %w", remoteVol.VolumeID, err)
			}
			result.VolumesMerged++
			localVolumesByID[remoteVol.VolumeID] = remoteVol
			if remoteVol.FSUUID != "" {
				localVolumesByFSUUID[remoteVol.FSUUID] = remoteVol
			}

		} else {
			// Volume already exists locally: validate metadata consistency
			var volConflict *model.MergeConflict
			if localVol.FSUUID != "" && remoteVol.FSUUID != "" && localVol.FSUUID != remoteVol.FSUUID {
				volConflict = &model.MergeConflict{
					ConflictKind: model.ConflictKindVolumeMetadata,
					VolumeID:     remoteVol.VolumeID,
					LocalDetail:  "fs_uuid=" + localVol.FSUUID,
					RemoteDetail: "fs_uuid=" + remoteVol.FSUUID,
				}
			} else if localVol.FSType != "" && remoteVol.FSType != "" && !strings.EqualFold(localVol.FSType, remoteVol.FSType) {
				volConflict = &model.MergeConflict{
					ConflictKind: model.ConflictKindVolumeMetadata,
					VolumeID:     remoteVol.VolumeID,
					LocalDetail:  "fs_type=" + localVol.FSType,
					RemoteDetail: "fs_type=" + remoteVol.FSType,
				}
			} else if localVol.SourceHost != "" && remoteVol.SourceHost != "" &&
				strings.EqualFold(localVol.SourceHost, remoteVol.SourceHost) &&
				localVol.DeviceID != "" && remoteVol.DeviceID != "" &&
				localVol.DeviceID != remoteVol.DeviceID {
				volConflict = &model.MergeConflict{
					ConflictKind: model.ConflictKindVolumeMetadata,
					VolumeID:     remoteVol.VolumeID,
					LocalDetail:  "device_id=" + localVol.DeviceID,
					RemoteDetail: "device_id=" + remoteVol.DeviceID,
				}
			}

			if volConflict != nil {
				result.ConflictsCount++
				if !opts.Force {
					if opts.Policy == PolicyFailOnConflict {
						volConflict.Resolution = "aborted"
						result.Conflicts = append(result.Conflicts, *volConflict)
						_ = tx.RecordMergeConflict(ctx, mergeID, volConflict)
						return nil, fmt.Errorf("merge aborted: metadata conflict for volume %q: local (%s) vs remote (%s)",
							remoteVol.VolumeID, volConflict.LocalDetail, volConflict.RemoteDetail)
					}
					volConflict.Resolution = "skipped_due_to_volume_conflict"
					result.Conflicts = append(result.Conflicts, *volConflict)
					_ = tx.RecordMergeConflict(ctx, mergeID, volConflict)
					continue
				}
				volConflict.Resolution = "forced"
				result.Conflicts = append(result.Conflicts, *volConflict)
				_ = tx.RecordMergeConflict(ctx, mergeID, volConflict)
			}

			// Merge descriptive metadata
			mergedVol := *localVol
			if remoteVol.FirstSeenAt.Before(mergedVol.FirstSeenAt) && !remoteVol.FirstSeenAt.IsZero() {
				mergedVol.FirstSeenAt = remoteVol.FirstSeenAt
			}
			remoteIsNewer := false
			if remoteVol.LastSeenAt != nil {
				if mergedVol.LastSeenAt == nil || remoteVol.LastSeenAt.After(*mergedVol.LastSeenAt) {
					mergedVol.LastSeenAt = remoteVol.LastSeenAt
					remoteIsNewer = true
				}
			}
			if mergedVol.Label == "" || (remoteIsNewer && remoteVol.Label != "") {
				mergedVol.Label = remoteVol.Label
			}
			if mergedVol.LastMountPoint == "" || (remoteIsNewer && remoteVol.LastMountPoint != "") {
				mergedVol.LastMountPoint = remoteVol.LastMountPoint
			}
			if mergedVol.SourceHost == "" || (remoteIsNewer && remoteVol.SourceHost != "") {
				mergedVol.SourceHost = remoteVol.SourceHost
			}
			if mergedVol.SourceLabel == "" || (remoteIsNewer && remoteVol.SourceLabel != "") {
				mergedVol.SourceLabel = remoteVol.SourceLabel
			}
			if mergedVol.FSUUID == "" && remoteVol.FSUUID != "" {
				mergedVol.FSUUID = remoteVol.FSUUID
			}
			if mergedVol.FSType == "" && remoteVol.FSType != "" {
				mergedVol.FSType = remoteVol.FSType
			}
			if mergedVol.DeviceID == "" && remoteVol.DeviceID != "" {
				mergedVol.DeviceID = remoteVol.DeviceID
			}

			if err := tx.UpdateVolume(ctx, &mergedVol); err != nil {
				return nil, fmt.Errorf("failed updating volume %q: %w", remoteVol.VolumeID, err)
			}
			result.VolumesMerged++
			localVolumesByID[remoteVol.VolumeID] = &mergedVol
		}

		// Import scan roots for this volume
		remoteRoots, err := sourceDB.GetScanRoots(ctx, remoteVol.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("failed loading scan roots for volume %q: %w", remoteVol.VolumeID, err)
		}
		for _, root := range remoteRoots {
			if err := tx.UpsertScanRoot(ctx, root); err != nil {
				return nil, fmt.Errorf("failed upserting scan root %s:%s: %w", root.VolumeID, root.RootPath, err)
			}
		}

		// 6. Merge entries for this volume
		localEntriesList, err := tx.ListEntries(ctx, remoteVol.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("failed listing local entries for volume %q: %w", remoteVol.VolumeID, err)
		}
		localEntriesMap := make(map[string]*model.Entry, len(localEntriesList))
		for _, e := range localEntriesList {
			e.RelativePath = model.NormalizeRelativePath(e.RelativePath)
			localEntriesMap[e.RelativePath] = e
		}

		remoteEntriesList, err := sourceDB.ListEntries(ctx, remoteVol.VolumeID, "")
		if err != nil {
			return nil, fmt.Errorf("failed listing remote entries for volume %q: %w", remoteVol.VolumeID, err)
		}

		for _, remoteEntry := range remoteEntriesList {
			remoteEntry.RelativePath = model.NormalizeRelativePath(remoteEntry.RelativePath)
			localEntry, exists := localEntriesMap[remoteEntry.RelativePath]

			if !exists {
				// Entry is missing locally
				contentFP := remoteEntry.QuickFingerprint
				if remoteEntry.EntryType == model.EntryTypeDirectory {
					contentFP = remoteEntry.DirectoryFingerprint
				}
				if len(contentFP) > 0 && existingFingerprints[string(contentFP)] {
					result.SameContentDiffLocation++
				} else if len(contentFP) > 0 {
					existingFingerprints[string(contentFP)] = true
				}

				if err := tx.InsertEntry(ctx, remoteEntry); err != nil {
					return nil, fmt.Errorf("failed inserting entry %s:%s: %w", remoteEntry.VolumeID, remoteEntry.RelativePath, err)
				}
				result.EntriesInserted++
				localEntriesMap[remoteEntry.RelativePath] = remoteEntry
				continue
			}

			// Entry exists locally: check content equivalence
			isSameContent := (localEntry.EntryType == remoteEntry.EntryType)
			if isSameContent {
				switch localEntry.EntryType {
				case model.EntryTypeRegular:
					isSameContent = (localEntry.SizeBytes == remoteEntry.SizeBytes &&
						bytes.Equal(localEntry.QuickFingerprint, remoteEntry.QuickFingerprint))
				case model.EntryTypeDirectory:
					isSameContent = bytes.Equal(localEntry.DirectoryFingerprint, remoteEntry.DirectoryFingerprint)
				default:
					isSameContent = (localEntry.SizeBytes == remoteEntry.SizeBytes)
				}
			}

			if isSameContent {
				// Same location and same content
				result.EntriesIdentical++

				needsUpdate := false
				// Verification preservation: do not overwrite a better-verified fingerprint
				if len(localEntry.FullFingerprint) == 0 && len(remoteEntry.FullFingerprint) > 0 {
					localEntry.FullFingerprint = remoteEntry.FullFingerprint
					needsUpdate = true
				}

				if isRemoteObservationNewer(localEntry, remoteEntry, localScanRuns, remoteScanRuns) {
					localEntry.LastSeenScanID = remoteEntry.LastSeenScanID
					localEntry.MTimeNs = remoteEntry.MTimeNs
					needsUpdate = true
				}

				if needsUpdate {
					if err := tx.UpdateEntry(ctx, localEntry); err != nil {
						return nil, fmt.Errorf("failed updating verified entry %s:%s: %w", localEntry.VolumeID, localEntry.RelativePath, err)
					}
				}
				continue
			}

			// Content differs: same location but different content conflict
			result.ConflictsCount++
			remoteNewer := isRemoteObservationNewer(localEntry, remoteEntry, localScanRuns, remoteScanRuns)

			conflict := model.MergeConflict{
				ConflictKind: model.ConflictKindSameLocationDifferentContent,
				VolumeID:     remoteEntry.VolumeID,
				RelativePath: remoteEntry.RelativePath,
				LocalDetail:  formatEntryDetail(localEntry, localScanRuns),
				RemoteDetail: formatEntryDetail(remoteEntry, remoteScanRuns),
			}

			switch opts.Policy {
			case PolicyFailOnConflict:
				conflict.Resolution = "aborted"
				result.Conflicts = append(result.Conflicts, conflict)
				_ = tx.RecordMergeConflict(ctx, mergeID, &conflict)
				return nil, fmt.Errorf("merge aborted on conflict at %s:%s: local (%s) vs remote (%s)",
					remoteEntry.VolumeID, remoteEntry.RelativePath, conflict.LocalDetail, conflict.RemoteDetail)

			case PolicyKeepLocal:
				conflict.Resolution = "kept_local"

			case PolicyKeepRemote:
				conflict.Resolution = "updated_to_remote"
				if err := tx.UpdateEntry(ctx, remoteEntry); err != nil {
					return nil, fmt.Errorf("failed updating entry on conflict: %w", err)
				}
				result.EntriesUpdated++
				localEntriesMap[remoteEntry.RelativePath] = remoteEntry

			case PolicyPreferNewer:
				if remoteNewer {
					conflict.Resolution = "updated_to_remote_newer"
					if err := tx.UpdateEntry(ctx, remoteEntry); err != nil {
						return nil, fmt.Errorf("failed updating entry on conflict: %w", err)
					}
					result.EntriesUpdated++
					localEntriesMap[remoteEntry.RelativePath] = remoteEntry
				} else {
					conflict.Resolution = "kept_local_newer"
				}

			default:
				if remoteNewer {
					conflict.Resolution = "updated_to_remote_newer"
					if err := tx.UpdateEntry(ctx, remoteEntry); err != nil {
						return nil, fmt.Errorf("failed updating entry on conflict: %w", err)
					}
					result.EntriesUpdated++
					localEntriesMap[remoteEntry.RelativePath] = remoteEntry
				} else {
					conflict.Resolution = "kept_local_newer"
				}
			}

			result.Conflicts = append(result.Conflicts, conflict)
			if err := tx.RecordMergeConflict(ctx, mergeID, &conflict); err != nil {
				return nil, fmt.Errorf("failed recording merge conflict: %w", err)
			}
		}
	}

	completedTime := time.Now().UTC()
	result.CompletedAt = completedTime
	result.Duration = completedTime.Sub(startTime)
	result.Status = "completed"

	// 7. Record merge run summary
	mergeRun := &model.MergeRun{
		MergeID:                 mergeID,
		SourcePath:              sourcePath,
		Status:                  "completed",
		StartedAt:               startTime,
		CompletedAt:             &completedTime,
		VolumesMerged:           result.VolumesMerged,
		EntriesInserted:         result.EntriesInserted,
		EntriesUpdated:          result.EntriesUpdated,
		EntriesIdentical:        result.EntriesIdentical,
		SameContentDiffLocation: result.SameContentDiffLocation,
		ConflictsCount:          result.ConflictsCount,
	}
	if err := tx.RecordMergeRun(ctx, mergeRun); err != nil {
		return nil, fmt.Errorf("failed recording merge run: %w", err)
	}

	// 8. Commit or rollback on dry run
	if opts.DryRun {
		_ = tx.Rollback()
	} else {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed committing merge transaction: %w", err)
		}
	}

	return result, nil
}

func checkAlgorithmCompatibility(ctx context.Context, destDB, sourceDB *index.DB) error {
	destAlgo, _ := destDB.GetMetadata(ctx, "fingerprint_algorithm")
	destVer, _ := destDB.GetMetadata(ctx, "fingerprint_version")
	sourceAlgo, _ := sourceDB.GetMetadata(ctx, "fingerprint_algorithm")
	sourceVer, _ := sourceDB.GetMetadata(ctx, "fingerprint_version")

	if destAlgo != "" && sourceAlgo != "" && destAlgo != sourceAlgo {
		return fmt.Errorf("cannot merge index: fingerprint algorithm mismatch (dest=%q, source=%q)", destAlgo, sourceAlgo)
	}
	if destVer != "" && sourceVer != "" && destVer != sourceVer {
		return fmt.Errorf("cannot merge index: fingerprint version mismatch (dest=%q, source=%q)", destVer, sourceVer)
	}
	return nil
}

func isRemoteObservationNewer(local, remote *model.Entry, localRuns, remoteRuns map[string]*model.ScanRun) bool {
	var localTime time.Time
	if localRun, ok := localRuns[local.LastSeenScanID]; ok && localRun != nil {
		if localRun.CompletedAt != nil {
			localTime = *localRun.CompletedAt
		} else {
			localTime = localRun.StartedAt
		}
	}

	var remoteTime time.Time
	if remoteRun, ok := remoteRuns[remote.LastSeenScanID]; ok && remoteRun != nil {
		if remoteRun.CompletedAt != nil {
			remoteTime = *remoteRun.CompletedAt
		} else {
			remoteTime = remoteRun.StartedAt
		}
	}

	if !remoteTime.IsZero() && !localTime.IsZero() {
		if remoteTime.After(localTime) {
			return true
		}
		if localTime.After(remoteTime) {
			return false
		}
	} else if !remoteTime.IsZero() && localTime.IsZero() {
		return true
	} else if remoteTime.IsZero() && !localTime.IsZero() {
		return false
	}

	// Fallback to mtime comparison
	return remote.MTimeNs > local.MTimeNs
}

func formatEntryDetail(e *model.Entry, runs map[string]*model.ScanRun) string {
	var scanTimeStr string
	if run, ok := runs[e.LastSeenScanID]; ok && run != nil {
		t := run.StartedAt
		if run.CompletedAt != nil {
			t = *run.CompletedAt
		}
		scanTimeStr = t.Format(time.RFC3339)
	}
	var fpStr string
	if e.EntryType == model.EntryTypeDirectory && len(e.DirectoryFingerprint) > 0 {
		fpStr = hex.EncodeToString(e.DirectoryFingerprint[:min(8, len(e.DirectoryFingerprint))])
	} else if len(e.QuickFingerprint) > 0 {
		fpStr = hex.EncodeToString(e.QuickFingerprint[:min(8, len(e.QuickFingerprint))])
	}

	return fmt.Sprintf("type=%s size=%d mtime_ns=%d scan_time=%s fp=%s",
		e.EntryType.String(), e.SizeBytes, e.MTimeNs, scanTimeStr, fpStr)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
