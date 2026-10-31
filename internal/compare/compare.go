package compare

import (
	"context"
	"fmt"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// Options configures a volume comparison operation.
type Options struct {
	Present        []string
	Absent         []string
	Mode           string // "absent", "common", "unique"
	EntryType      model.EntryType
	ShowMatches    bool
	VerifyFullHash bool
}

// Compare compares entries between a source volume set and target volume set.
func Compare(ctx context.Context, db *index.DB, opts Options) (*model.CompareSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	presentIDs, err := ResolveVolumes(ctx, db, opts.Present)
	if err != nil {
		return nil, fmt.Errorf("failed resolving present volumes: %w", err)
	}
	if len(presentIDs) == 0 {
		return nil, fmt.Errorf("source volume set (--present) cannot be empty")
	}

	absentIDs, err := ResolveVolumes(ctx, db, opts.Absent)
	if err != nil {
		return nil, fmt.Errorf("failed resolving absent volumes: %w", err)
	}

	// Validate sets are disjoint
	absentSet := make(map[string]bool, len(absentIDs))
	for _, id := range absentIDs {
		absentSet[id] = true
	}
	for _, id := range presentIDs {
		if absentSet[id] {
			return nil, fmt.Errorf("volume %q cannot be in both present and absent sets", id)
		}
	}

	mode := opts.Mode
	if mode == "" {
		mode = "absent"
	}

	return db.QueryComparison(ctx, index.CompareQueryParams{
		PresentVolumes: presentIDs,
		AbsentVolumes:  absentIDs,
		Mode:           mode,
		EntryType:      opts.EntryType,
		ShowMatches:    opts.ShowMatches,
		VerifyFullHash: opts.VerifyFullHash,
	})
}

// Unique identifies content present on a selected volume but absent from all other volumes in the set.
func Unique(ctx context.Context, db *index.DB, volumeSelector string, amongSelectors []string, opts Options) (*model.CompareSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	if volumeSelector == "" {
		return nil, fmt.Errorf("volume selector cannot be empty")
	}

	targetIDs, err := ResolveVolumes(ctx, db, []string{volumeSelector})
	if err != nil {
		return nil, fmt.Errorf("failed resolving target volume: %w", err)
	}
	if len(targetIDs) != 1 {
		return nil, fmt.Errorf("unique requires exactly one target volume, found %d", len(targetIDs))
	}
	targetID := targetIDs[0]

	var otherIDs []string
	if len(amongSelectors) > 0 {
		resolvedAmong, err := ResolveVolumes(ctx, db, amongSelectors)
		if err != nil {
			return nil, fmt.Errorf("failed resolving among volumes: %w", err)
		}
		for _, id := range resolvedAmong {
			if id != targetID {
				otherIDs = append(otherIDs, id)
			}
		}
	} else {
		// Default to all other registered volumes
		allVols, err := db.ListVolumes(ctx, model.VolumeFilter{})
		if err != nil {
			return nil, fmt.Errorf("failed listing all volumes: %w", err)
		}
		for _, v := range allVols {
			if v.VolumeID != targetID {
				otherIDs = append(otherIDs, v.VolumeID)
			}
		}
	}

	opts.Present = []string{targetID}
	opts.Absent = otherIDs
	opts.Mode = "unique"

	return Compare(ctx, db, opts)
}
