package index

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/kgorelov/findex/internal/model"
)

func TestLookupFingerprint(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_lookup.sqlite")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	err = db.RegisterVolume(ctx, &model.Volume{
		VolumeID:   "vol-media",
		Label:      "MediaDrive",
		SourceHost: "home-server",
		FirstSeenAt: now,
	}, false)
	if err != nil {
		t.Fatalf("failed registering volume: %v", err)
	}

	quickBytes, _ := hex.DecodeString("aabbccdd11223344")
	fullBytes, _ := hex.DecodeString("11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff")
	dirBytes, _ := hex.DecodeString("cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe")

	err = db.UpsertEntries(ctx, []*model.Entry{
		{
			VolumeID:         "vol-media",
			RelativePath:     "/movies/intro.mp4",
			EntryType:        model.EntryTypeRegular,
			SizeBytes:        1024,
			QuickFingerprint: quickBytes,
			FullFingerprint:  fullBytes,
		},
		{
			VolumeID:             "vol-media",
			RelativePath:         "/movies",
			EntryType:            model.EntryTypeDirectory,
			SizeBytes:            0,
			DirectoryFingerprint: dirBytes,
		},
	})
	if err != nil {
		t.Fatalf("failed inserting entries: %v", err)
	}

	// 1. Partial quick fingerprint lookup
	matches, err := db.LookupFingerprint(ctx, "aabb")
	if err != nil {
		t.Fatalf("lookup aabb failed: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match for 'aabb', got %d", len(matches))
	}
	if matches[0].RelativePath != "/movies/intro.mp4" || matches[0].MatchType != "quick" {
		t.Errorf("unexpected match: %+v", matches[0])
	}
	if matches[0].VolumeLabel != "MediaDrive" || matches[0].SourceHost != "home-server" {
		t.Errorf("unexpected volume metadata: %+v", matches[0])
	}

	// 2. Full fingerprint lookup
	matches, err = db.LookupFingerprint(ctx, "112233445566")
	if err != nil {
		t.Fatalf("lookup full fp failed: %v", err)
	}
	if len(matches) != 1 || matches[0].MatchType != "full" {
		t.Errorf("expected 1 full match, got %+v", matches)
	}

	// 3. Directory fingerprint lookup (with 0x prefix and lowercase)
	matches, err = db.LookupFingerprint(ctx, "0xcafebabe")
	if err != nil {
		t.Fatalf("lookup dir fp failed: %v", err)
	}
	if len(matches) != 1 || matches[0].MatchType != "dir" || matches[0].RelativePath != "/movies" {
		t.Errorf("expected 1 dir match, got %+v", matches)
	}

	// 4. Non-matching query
	matches, err = db.LookupFingerprint(ctx, "deadbeef")
	if err != nil {
		t.Fatalf("lookup non-matching failed: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}
