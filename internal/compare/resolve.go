package compare

import (
	"context"
	"fmt"
	"strings"

	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// ResolveVolumes resolves user-specified volume selectors (IDs, host:X, label:X, uuid:X)
// into a deduplicated slice of valid volume IDs from the database.
func ResolveVolumes(ctx context.Context, db *index.DB, selectors []string) ([]string, error) {
	if len(selectors) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool)
	var resolved []string

	for _, raw := range selectors {
		tokens := strings.Split(raw, ",")
		for _, tok := range tokens {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}

			ids, err := resolveToken(ctx, db, tok)
			if err != nil {
				return nil, err
			}

			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					resolved = append(resolved, id)
				}
			}
		}
	}

	return resolved, nil
}

func resolveToken(ctx context.Context, db *index.DB, token string) ([]string, error) {
	lower := strings.ToLower(token)

	if strings.HasPrefix(lower, "host:") {
		hostVal := strings.TrimPrefix(token, token[:5])
		vols, err := db.ListVolumes(ctx, model.VolumeFilter{SourceHost: hostVal})
		if err != nil {
			return nil, fmt.Errorf("failed listing volumes for host %q: %w", hostVal, err)
		}
		if len(vols) == 0 {
			return nil, fmt.Errorf("no volumes found for host filter %q", token)
		}
		return extractVolumeIDs(vols), nil
	}

	if strings.HasPrefix(lower, "label:") {
		labelVal := strings.TrimPrefix(token, token[:6])
		vols, err := db.ListVolumes(ctx, model.VolumeFilter{Label: labelVal})
		if err != nil {
			return nil, fmt.Errorf("failed listing volumes for label %q: %w", labelVal, err)
		}
		if len(vols) == 0 {
			return nil, fmt.Errorf("no volumes found for label filter %q", token)
		}
		return extractVolumeIDs(vols), nil
	}

	if strings.HasPrefix(lower, "uuid:") {
		uuidVal := strings.TrimPrefix(token, token[:5])
		vols, err := db.ListVolumes(ctx, model.VolumeFilter{FSUUID: uuidVal})
		if err != nil {
			return nil, fmt.Errorf("failed listing volumes for fs_uuid %q: %w", uuidVal, err)
		}
		if len(vols) == 0 {
			return nil, fmt.Errorf("no volumes found for uuid filter %q", token)
		}
		return extractVolumeIDs(vols), nil
	}

	// Direct volume ID match
	vol, err := db.GetVolume(ctx, token)
	if err == nil {
		return []string{vol.VolumeID}, nil
	}

	// Fallback: match by label or fs_uuid if volume ID lookup failed
	byLabel, _ := db.ListVolumes(ctx, model.VolumeFilter{Label: token})
	if len(byLabel) > 0 {
		return extractVolumeIDs(byLabel), nil
	}

	byUUID, _ := db.ListVolumes(ctx, model.VolumeFilter{FSUUID: token})
	if len(byUUID) > 0 {
		return extractVolumeIDs(byUUID), nil
	}

	return nil, fmt.Errorf("volume %q not found in index", token)
}

func extractVolumeIDs(vols []*model.Volume) []string {
	ids := make([]string, len(vols))
	for i, v := range vols {
		ids[i] = v.VolumeID
	}
	return ids
}
