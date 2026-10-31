package index

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func newTestDB(t testing.TB) *DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func TestDB_PragmasAndMigrations(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	// Check foreign keys
	var fk int
	if err := db.RawDB().QueryRow("PRAGMA foreign_keys;").Scan(&fk); err != nil {
		t.Fatalf("failed querying foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("expected foreign_keys = 1, got %d", fk)
	}

	// Check schema version
	ver, err := db.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("failed checking schema version: %v", err)
	}
	if ver != 2 {
		t.Errorf("expected schema version 2, got %d", ver)
	}

	// Check metadata
	algo, err := db.GetMetadata(ctx, "fingerprint_algorithm")
	if err != nil {
		t.Fatalf("failed getting algorithm metadata: %v", err)
	}
	if algo != "blake3-sparse" {
		t.Errorf("expected algorithm blake3-sparse, got %q", algo)
	}

	// Test metadata set/get
	if err := db.SetMetadata(ctx, "custom_key", "custom_value"); err != nil {
		t.Fatalf("failed setting metadata: %v", err)
	}
	val, err := db.GetMetadata(ctx, "custom_key")
	if err != nil {
		t.Fatalf("failed getting custom metadata: %v", err)
	}
	if val != "custom_value" {
		t.Errorf("expected custom_value, got %q", val)
	}

	// Test migration re-run is idempotent
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("re-running migrations failed: %v", err)
	}
}

func TestDB_RegisterAndGetVolume(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol := &model.Volume{
		VolumeID:       "vol-123",
		FSUUID:         "uuid-abc",
		FSType:         "ext4",
		Label:          "BackupDrive",
		DeviceID:       "/dev/sda1",
		LastMountPoint: "/media/backup",
		SourceHost:     "nas01",
		SourceLabel:    "Primary Backup",
		FirstSeenAt:    time.Now().Add(-time.Hour).UTC(),
	}

	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("failed to register volume: %v", err)
	}

	got, err := db.GetVolume(ctx, "vol-123")
	if err != nil {
		t.Fatalf("failed to get volume: %v", err)
	}

	if got.VolumeID != vol.VolumeID {
		t.Errorf("expected VolumeID %q, got %q", vol.VolumeID, got.VolumeID)
	}
	if got.FSUUID != vol.FSUUID {
		t.Errorf("expected FSUUID %q, got %q", vol.FSUUID, got.FSUUID)
	}
	if got.FSType != vol.FSType {
		t.Errorf("expected FSType %q, got %q", vol.FSType, got.FSType)
	}
	if got.Label != vol.Label {
		t.Errorf("expected Label %q, got %q", vol.Label, got.Label)
	}
	if got.LastMountPoint != vol.LastMountPoint {
		t.Errorf("expected LastMountPoint %q, got %q", vol.LastMountPoint, got.LastMountPoint)
	}
	if got.SourceHost != vol.SourceHost {
		t.Errorf("expected SourceHost %q, got %q", vol.SourceHost, got.SourceHost)
	}
}

func TestDB_RegisterVolume_CompatibleUpdate(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol := &model.Volume{
		VolumeID:       "vol-update",
		FSUUID:         "uuid-update",
		FSType:         "ext4",
		Label:          "Drive1",
		LastMountPoint: "/mnt/old",
		SourceHost:     "server1",
	}

	if err := db.RegisterVolume(ctx, vol, false); err != nil {
		t.Fatalf("initial register failed: %v", err)
	}

	// Update mount point and label
	updateVol := &model.Volume{
		VolumeID:       "vol-update",
		FSUUID:         "uuid-update",
		FSType:         "ext4",
		Label:          "Drive1-Renamed",
		LastMountPoint: "/mnt/new",
		SourceHost:     "server1",
	}

	if err := db.RegisterVolume(ctx, updateVol, false); err != nil {
		t.Fatalf("update register failed: %v", err)
	}

	got, err := db.GetVolume(ctx, "vol-update")
	if err != nil {
		t.Fatalf("get volume failed: %v", err)
	}
	if got.Label != "Drive1-Renamed" {
		t.Errorf("expected updated label 'Drive1-Renamed', got %q", got.Label)
	}
	if got.LastMountPoint != "/mnt/new" {
		t.Errorf("expected updated mount point '/mnt/new', got %q", got.LastMountPoint)
	}
}

func TestDB_RegisterVolume_Conflicts(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	baseVol := &model.Volume{
		VolumeID:   "vol-conflict",
		FSUUID:     "uuid-base",
		FSType:     "btrfs",
		DeviceID:   "/dev/nvme0n1p2",
		SourceHost: "desktop",
	}
	if err := db.RegisterVolume(ctx, baseVol, false); err != nil {
		t.Fatalf("base register failed: %v", err)
	}

	// 1. Conflicting FSUUID
	conflictUUID := &model.Volume{
		VolumeID:   "vol-conflict",
		FSUUID:     "uuid-different",
		FSType:     "btrfs",
		DeviceID:   "/dev/nvme0n1p2",
		SourceHost: "desktop",
	}
	err := db.RegisterVolume(ctx, conflictUUID, false)
	var conflictErr *model.VolumeConflictError
	if !errors.As(err, &conflictErr) || conflictErr.Type != model.ConflictTypeFSUUIDMismatch {
		t.Fatalf("expected ConflictTypeFSUUIDMismatch, got %v", err)
	}

	// 2. Conflicting FSType
	conflictFSType := &model.Volume{
		VolumeID:   "vol-conflict",
		FSUUID:     "uuid-base",
		FSType:     "ext4",
		DeviceID:   "/dev/nvme0n1p2",
		SourceHost: "desktop",
	}
	err = db.RegisterVolume(ctx, conflictFSType, false)
	if !errors.As(err, &conflictErr) || conflictErr.Type != model.ConflictTypeFSTypeMismatch {
		t.Fatalf("expected ConflictTypeFSTypeMismatch, got %v", err)
	}

	// 3. Changed Device on same host (e.g. removable drive plugged in as /dev/sdb1) should succeed and update DeviceID
	changedDevice := &model.Volume{
		VolumeID:   "vol-conflict",
		FSUUID:     "uuid-base",
		FSType:     "btrfs",
		DeviceID:   "/dev/sdb1",
		SourceHost: "desktop",
	}
	if err := db.RegisterVolume(ctx, changedDevice, false); err != nil {
		t.Fatalf("expected device change to succeed on removable media, got %v", err)
	}
	updated, err := db.GetVolume(ctx, "vol-conflict")
	if err != nil {
		t.Fatalf("failed getting updated volume: %v", err)
	}
	if updated.DeviceID != "/dev/sdb1" {
		t.Errorf("expected updated DeviceID /dev/sdb1, got %q", updated.DeviceID)
	}

	// 4. Force should override conflict
	if err := db.RegisterVolume(ctx, conflictFSType, true); err != nil {
		t.Fatalf("expected force to succeed, got %v", err)
	}
}

func TestDB_RegisterVolume_DuplicateFSUUID(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vol1 := &model.Volume{
		VolumeID: "vol-alpha",
		FSUUID:   "uuid-cloned",
		FSType:   "ext4",
	}
	if err := db.RegisterVolume(ctx, vol1, false); err != nil {
		t.Fatalf("vol1 register failed: %v", err)
	}

	// Cloned volume with different VolumeID but duplicate FSUUID
	vol2 := &model.Volume{
		VolumeID: "vol-beta",
		FSUUID:   "uuid-cloned",
		FSType:   "ext4",
	}
	err := db.RegisterVolume(ctx, vol2, false)
	var conflictErr *model.VolumeConflictError
	if !errors.As(err, &conflictErr) || conflictErr.Type != model.ConflictTypeDuplicateFSUUID {
		t.Fatalf("expected ConflictTypeDuplicateFSUUID, got %v", err)
	}
}

func TestDB_ListVolumesAndStats(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	vols := []*model.Volume{
		{VolumeID: "vol-1", Label: "Photos", SourceHost: "macbook"},
		{VolumeID: "vol-2", Label: "Music", SourceHost: "macbook"},
		{VolumeID: "vol-3", Label: "Archives", SourceHost: "linux-server"},
	}
	for _, v := range vols {
		if err := db.RegisterVolume(ctx, v, false); err != nil {
			t.Fatalf("failed registering volume: %v", err)
		}
	}

	// List all
	all, err := db.ListVolumes(ctx, model.VolumeFilter{})
	if err != nil {
		t.Fatalf("failed listing volumes: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 volumes, got %d", len(all))
	}

	// Filter by host
	macVols, err := db.ListVolumes(ctx, model.VolumeFilter{SourceHost: "macbook"})
	if err != nil {
		t.Fatalf("failed filtering volumes: %v", err)
	}
	if len(macVols) != 2 {
		t.Errorf("expected 2 macbook volumes, got %d", len(macVols))
	}

	// Insert some sample entries & scan roots into vol-1 to test stats
	_, err = db.RawDB().Exec(`
		INSERT INTO scan_roots (volume_id, root_path, last_successful_scan_at)
		VALUES ('vol-1', '/Photos', 1700000000);

		INSERT INTO entries (volume_id, relative_path, entry_type, size_bytes)
		VALUES
			('vol-1', '/Photos', 2, 0),
			('vol-1', '/Photos/a.jpg', 1, 5000),
			('vol-1', '/Photos/b.jpg', 1, 10000),
			('vol-1', '/Photos/link', 3, 0);
	`)
	if err != nil {
		t.Fatalf("failed inserting test entries: %v", err)
	}

	stats, err := db.GetVolumeStats(ctx, "vol-1")
	if err != nil {
		t.Fatalf("failed getting stats: %v", err)
	}
	if stats.ScanRootsCount != 1 {
		t.Errorf("expected 1 scan root, got %d", stats.ScanRootsCount)
	}
	if stats.FileCount != 2 {
		t.Errorf("expected 2 files, got %d", stats.FileCount)
	}
	if stats.DirectoryCount != 1 {
		t.Errorf("expected 1 directory, got %d", stats.DirectoryCount)
	}
	if stats.SymlinkCount != 1 {
		t.Errorf("expected 1 symlink, got %d", stats.SymlinkCount)
	}
	if stats.TotalBytes != 15000 {
		t.Errorf("expected 15000 bytes, got %d", stats.TotalBytes)
	}
}
