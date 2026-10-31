package export

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

func createTestDB(t *testing.T, filename string) (*index.DB, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), filename)
	db, err := index.Open(dbPath)
	if err != nil {
		t.Fatalf("failed creating test db: %v", err)
	}
	return db, dbPath
}

func TestExportAndImport_Uncompressed(t *testing.T) {
	ctx := context.Background()
	srcDB, _ := createTestDB(t, "src.sqlite")
	defer srcDB.Close()

	now := time.Now().UTC()
	vol := &model.Volume{
		VolumeID:       "vol-export-1",
		Label:          "Photos",
		FSUUID:         "uuid-photos",
		FSType:         "ext4",
		SourceHost:     "main-host",
		LastMountPoint: "/mnt/photos",
		FirstSeenAt:    now,
	}
	_ = srcDB.RegisterVolume(ctx, vol, false)

	_ = srcDB.CreateScanRun(ctx, &model.ScanRun{
		ScanID:      "scan-100",
		VolumeID:    "vol-export-1",
		RootPath:    "/",
		Status:      model.ScanStatusCompleted,
		StartedAt:   now,
		CompletedAt: &now,
	})

	_ = srcDB.RecordScanRootSuccess(ctx, "vol-export-1", "/", "scan-100", now)

	_ = srcDB.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-export-1",
			RelativePath:     "/2026/img1.jpg",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1024,
			QuickFingerprint: []byte("fp-img1"),
			LastSeenScanID:   "scan-100",
		},
		{
			VolumeID:             "vol-export-1",
			RelativePath:         "/2026",
			EntryType:            model.EntryTypeDirectory,
			DirectoryFingerprint: []byte("fp-dir-2026"),
			LastSeenScanID:       "scan-100",
		},
	})

	exportFile := filepath.Join(t.TempDir(), "export.json")
	summary, err := Export(ctx, srcDB, exportFile, ExportOptions{IndentJSON: true})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if summary.VolumesCount != 1 || summary.EntriesCount != 2 || summary.ScanRootsCount != 1 {
		t.Errorf("unexpected export summary: %+v", summary)
	}
	if summary.Compressed {
		t.Errorf("expected uncompressed export")
	}

	// Import into clean database
	destDB, _ := createTestDB(t, "dest.sqlite")
	defer destDB.Close()

	impSummary, err := Import(ctx, destDB, exportFile)
	if err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	if impSummary.VolumesImported != 1 {
		t.Errorf("expected 1 volume imported, got %d", impSummary.VolumesImported)
	}
	if impSummary.EntriesInserted != 2 {
		t.Errorf("expected 2 entries inserted, got %d", impSummary.EntriesInserted)
	}

	// Verify imported data
	v, err := destDB.GetVolume(ctx, "vol-export-1")
	if err != nil {
		t.Fatalf("failed getting imported volume: %v", err)
	}
	if v.Label != "Photos" || v.FSUUID != "uuid-photos" {
		t.Errorf("unexpected volume data: %+v", v)
	}

	entries, err := destDB.ListEntries(ctx, "vol-export-1", "")
	if err != nil {
		t.Fatalf("failed listing entries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in destDB, got %d", len(entries))
	}
}

func TestExportAndImport_GzipCompressed(t *testing.T) {
	ctx := context.Background()
	srcDB, _ := createTestDB(t, "src_gz.sqlite")
	defer srcDB.Close()

	now := time.Now().UTC()
	vol := &model.Volume{VolumeID: "volGZ", FirstSeenAt: now}
	_ = srcDB.RegisterVolume(ctx, vol, false)
	_ = srcDB.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "volGZ",
			RelativePath:     "/file.txt",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        500,
			QuickFingerprint: []byte("fp-file-gz"),
		},
	})

	exportFile := filepath.Join(t.TempDir(), "inventory.json.gz")
	summary, err := Export(ctx, srcDB, exportFile, ExportOptions{})
	if err != nil {
		t.Fatalf("Export failed: %v", err)
	}

	if !summary.Compressed {
		t.Errorf("expected compressed flag in summary")
	}

	destDB, _ := createTestDB(t, "dest_gz.sqlite")
	defer destDB.Close()

	impSummary, err := Import(ctx, destDB, exportFile)
	if err != nil {
		t.Fatalf("Import from gzip failed: %v", err)
	}

	if impSummary.EntriesInserted != 1 {
		t.Errorf("expected 1 entry inserted, got %d", impSummary.EntriesInserted)
	}

	e, err := destDB.GetEntry(ctx, "volGZ", "/file.txt")
	if err != nil {
		t.Fatalf("failed getting imported entry: %v", err)
	}
	if e.SizeBytes != 500 {
		t.Errorf("expected size 500, got %d", e.SizeBytes)
	}
}
