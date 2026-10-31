package duplicate

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/kgorelov/findex/internal/compare"
	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/model"
)

// Options configures a duplicate search operation.
type Options struct {
	MinSizeBytes int64
	EntryType    model.EntryType
}

// FindDuplicates searches for duplicate directories and files across the selected volumes.
// If an entire directory is a duplicate, the directory is returned and its children are pruned.
func FindDuplicates(ctx context.Context, db *index.DB, selectors []string, opts Options) (*model.DuplicateSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("database cannot be nil")
	}

	var volIDs []string
	if len(selectors) == 0 {
		allVols, err := db.ListVolumes(ctx, model.VolumeFilter{})
		if err != nil {
			return nil, fmt.Errorf("failed listing volumes: %w", err)
		}
		for _, v := range allVols {
			volIDs = append(volIDs, v.VolumeID)
		}
	} else {
		var err error
		volIDs, err = compare.ResolveVolumes(ctx, db, selectors)
		if err != nil {
			return nil, fmt.Errorf("failed resolving volumes: %w", err)
		}
	}

	if len(volIDs) == 0 {
		return &model.DuplicateSummary{
			Volumes: volIDs,
		}, nil
	}

	var acceptedDirGroups []*model.DuplicateGroup
	type coveredPrefix struct {
		volumeID string
		prefix   string
	}
	var covered []coveredPrefix

	// 1. Process duplicate directories if not filtering to files only
	if opts.EntryType == 0 || opts.EntryType == model.EntryTypeDirectory {
		rawDirGroups, err := db.QueryDuplicateDirectories(ctx, volIDs)
		if err != nil {
			return nil, err
		}

		// Sort candidate directory groups by path depth ascending (top-down)
		sort.Slice(rawDirGroups, func(i, j int) bool {
			return len(rawDirGroups[i].Entries[0].RelativePath) < len(rawDirGroups[j].Entries[0].RelativePath)
		})

		for _, raw := range rawDirGroups {
			// Check if all members are inside an already accepted directory group
			allSubsumed := true
			for _, e := range raw.Entries {
				subsumed := false
				for _, c := range covered {
					if e.VolumeID == c.volumeID && strings.HasPrefix(e.RelativePath, c.prefix) {
						subsumed = true
						break
					}
				}
				if !subsumed {
					allSubsumed = false
					break
				}
			}

			if allSubsumed {
				// Entire group is inside parent duplicate directories
				continue
			}

			group := &model.DuplicateGroup{
				EntryType:   model.EntryTypeDirectory,
				SizeBytes:   raw.SizeBytes,
				Fingerprint: raw.Fingerprint,
				Entries:     raw.Entries,
			}
			acceptedDirGroups = append(acceptedDirGroups, group)

			// Add covered prefixes for all entries in this group
			for _, e := range raw.Entries {
				p := e.RelativePath
				if p == "/" {
					p = "/"
				} else if !strings.HasSuffix(p, "/") {
					p += "/"
				}
				covered = append(covered, coveredPrefix{
					volumeID: e.VolumeID,
					prefix:   p,
				})
			}
		}
	}

	// 2. Process duplicate files if not filtering to directories only
	var acceptedFileGroups []*model.DuplicateGroup
	if opts.EntryType == 0 || opts.EntryType == model.EntryTypeRegular {
		rawFileGroups, err := db.QueryDuplicateFiles(ctx, volIDs, opts.MinSizeBytes)
		if err != nil {
			return nil, err
		}

		for _, raw := range rawFileGroups {
			var remaining []*model.DuplicateEntry
			for _, e := range raw.Entries {
				isCovered := false
				for _, c := range covered {
					if e.VolumeID == c.volumeID {
						if c.prefix == "/" || strings.HasPrefix(e.RelativePath, c.prefix) {
							isCovered = true
							break
						}
					}
				}
				if !isCovered {
					remaining = append(remaining, e)
				}
			}

			if len(remaining) >= 2 {
				acceptedFileGroups = append(acceptedFileGroups, &model.DuplicateGroup{
					EntryType:   model.EntryTypeRegular,
					SizeBytes:   raw.SizeBytes,
					Fingerprint: raw.Fingerprint,
					Entries:     remaining,
				})
			}
		}
	}

	// 3. Combine and sort duplicate groups (largest size first)
	allGroups := append(acceptedDirGroups, acceptedFileGroups...)
	sort.Slice(allGroups, func(i, j int) bool {
		if allGroups[i].SizeBytes != allGroups[j].SizeBytes {
			return allGroups[i].SizeBytes > allGroups[j].SizeBytes
		}
		return allGroups[i].Entries[0].RelativePath < allGroups[j].Entries[0].RelativePath
	})

	var totalEntries int
	var wastedBytes int64
	for _, g := range allGroups {
		totalEntries += len(g.Entries)
		if len(g.Entries) > 1 {
			wastedBytes += int64(len(g.Entries)-1) * g.SizeBytes
		}
	}

	return &model.DuplicateSummary{
		Volumes:      volIDs,
		TotalGroups:  len(allGroups),
		TotalEntries: totalEntries,
		WastedBytes:  wastedBytes,
		Groups:       allGroups,
	}, nil
}
