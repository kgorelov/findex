package compare

import (
	"context"
	"fmt"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// Options configures a volume comparison operation.
type Options struct {
	LeftVolumes    []string
	RightVolumes   []string
	Mode           string // "left", "right", "common", "unique"
	EntryType      model.EntryType
	ShowMatches    bool
	VerifyFullHash bool
}

// Compare compares entries between two volume sets (LeftVolumes and RightVolumes)
// using the mandatory mode: "left", "right", or "common".
func Compare(ctx context.Context, db *index.DB, opts Options) (*model.CompareSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	switch opts.Mode {
	case "left", "right", "common", "unique":
	default:
		return nil, fmt.Errorf("invalid comparison mode %q: must be 'left', 'right', or 'common'", opts.Mode)
	}

	leftIDs, err := ResolveVolumes(ctx, db, opts.LeftVolumes)
	if err != nil {
		return nil, fmt.Errorf("failed resolving left volumes: %w", err)
	}
	if len(leftIDs) == 0 {
		return nil, fmt.Errorf("left volume set cannot be empty")
	}

	rightIDs, err := ResolveVolumes(ctx, db, opts.RightVolumes)
	if err != nil {
		return nil, fmt.Errorf("failed resolving right volumes: %w", err)
	}
	if len(rightIDs) == 0 && opts.Mode != "unique" {
		return nil, fmt.Errorf("right volume set cannot be empty")
	}

	// Validate sets are disjoint
	rightSet := make(map[string]bool, len(rightIDs))
	for _, id := range rightIDs {
		rightSet[id] = true
	}
	for _, id := range leftIDs {
		if rightSet[id] {
			return nil, fmt.Errorf("volume %q cannot be in both left and right sets", id)
		}
	}

	var presentIDs, absentIDs []string
	if opts.Mode == "right" {
		presentIDs = rightIDs
		absentIDs = leftIDs
	} else {
		// "left", "common", "unique"
		presentIDs = leftIDs
		absentIDs = rightIDs
	}

	return db.QueryComparison(ctx, index.CompareQueryParams{
		LeftVolumes:    leftIDs,
		RightVolumes:   rightIDs,
		PresentVolumes: presentIDs,
		AbsentVolumes:  absentIDs,
		Mode:           opts.Mode,
		EntryType:      opts.EntryType,
		ShowMatches:    opts.ShowMatches,
		VerifyFullHash: opts.VerifyFullHash,
	})
}

// Unique identifies content present in volumesSelector but absent from all other volumes.
// This is syntax sugar for: findex compare --left volumes_set all_other_volumes.
func Unique(ctx context.Context, db *index.DB, volumeSelector string, amongSelectors []string, opts Options) (*model.CompareSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}
	if volumeSelector == "" {
		return nil, fmt.Errorf("volume selector cannot be empty")
	}

	targetIDs, err := ResolveVolumes(ctx, db, []string{volumeSelector})
	if err != nil {
		return nil, fmt.Errorf("failed resolving target volumes: %w", err)
	}
	if len(targetIDs) == 0 {
		return nil, fmt.Errorf("unique requires at least one target volume")
	}

	targetSet := make(map[string]bool, len(targetIDs))
	for _, id := range targetIDs {
		targetSet[id] = true
	}

	var otherIDs []string
	if len(amongSelectors) > 0 {
		resolvedAmong, err := ResolveVolumes(ctx, db, amongSelectors)
		if err != nil {
			return nil, fmt.Errorf("failed resolving among volumes: %w", err)
		}
		for _, id := range resolvedAmong {
			if !targetSet[id] {
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
			if !targetSet[v.VolumeID] {
				otherIDs = append(otherIDs, v.VolumeID)
			}
		}
	}

	opts.LeftVolumes = targetIDs
	opts.RightVolumes = otherIDs
	opts.Mode = "unique"

	return Compare(ctx, db, opts)
}
