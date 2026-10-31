package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kgorelov/findex/internal/compare"
	"github.com/kgorelov/findex/internal/export"
	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/index"
	"github.com/kgorelov/findex/internal/lookup"
	"github.com/kgorelov/findex/internal/merge"
	"github.com/kgorelov/findex/internal/model"
	"github.com/kgorelov/findex/internal/output"
	"github.com/kgorelov/findex/internal/scan"
	"github.com/kgorelov/findex/internal/volume"
	"github.com/spf13/cobra"
)

var (
	dbPath  string
	jsonOut bool
)

func defaultDBPath() string {
	if dbEnv := os.Getenv("FINDEX_DB"); dbEnv != "" {
		return dbEnv
	}
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		return filepath.Join(home, ".findex", "index.sqlite")
	}
	return "findex.sqlite"
}

func getDB() (*index.DB, error) {
	return index.Open(dbPath)
}

func buildRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "findex",
		Short: "Multi-Volume File Inventory and recovery-planning tool",
		Long: `findex builds and maintains a persistent inventory of files and directories
across multiple storage volumes in SQLite.`,
	}

	rootCmd.PersistentFlags().StringVar(&dbPath, "db", defaultDBPath(), "Path to SQLite index database")
	rootCmd.PersistentFlags().BoolVar(&jsonOut, "json", false, "Output results in JSON format")

	// volume command group
	volumeCmd := &cobra.Command{
		Use:   "volume",
		Short: "Manage and inspect indexed storage volumes",
	}

	// volume list
	var listHost string
	var listLabel string
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List registered storage volumes",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			volumes, err := db.ListVolumes(context.Background(), model.VolumeFilter{
				SourceHost: listHost,
				Label:      listLabel,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintJSON(os.Stdout, volumes)
			}
			return output.PrintVolumesTable(os.Stdout, volumes)
		},
	}
	listCmd.Flags().StringVar(&listHost, "host", "", "Filter volumes by source host")
	listCmd.Flags().StringVar(&listLabel, "label", "", "Filter volumes by label substring")

	// volume show
	showCmd := &cobra.Command{
		Use:   "show <volume-id>",
		Short: "Show detailed information and statistics for a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			volumeID := args[0]
			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			stats, err := db.GetVolumeStats(context.Background(), volumeID)
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintJSON(os.Stdout, stats)
			}
			return output.PrintVolumeStats(os.Stdout, stats)
		},
	}

	// volume register
	var regID string
	var regLabel string
	var regSourceLabel string
	var regHost string
	var regNoSidecar bool
	var regForce bool

	registerCmd := &cobra.Command{
		Use:   "register <path>",
		Short: "Register a storage volume for a filesystem mount or directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetPath := args[0]

			vol, err := volume.ResolveVolume(targetPath, volume.VolumeOptions{
				VolumeID:       regID,
				Label:          regLabel,
				SourceHost:     regHost,
				SourceLabel:    regSourceLabel,
				DisableSidecar: regNoSidecar,
			})
			if err != nil {
				return fmt.Errorf("failed resolving volume for %q: %w", targetPath, err)
			}

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			if err := db.RegisterVolume(context.Background(), vol, regForce); err != nil {
				return fmt.Errorf("failed registering volume: %w", err)
			}

			if jsonOut {
				return output.PrintJSON(os.Stdout, vol)
			}

			fmt.Printf("Successfully registered volume %q\n", vol.VolumeID)
			if vol.Label != "" {
				fmt.Printf("  Label:       %s\n", vol.Label)
			}
			if vol.FSUUID != "" {
				fmt.Printf("  FS UUID:     %s\n", vol.FSUUID)
			}
			if vol.FSType != "" {
				fmt.Printf("  FS Type:     %s\n", vol.FSType)
			}
			if vol.LastMountPoint != "" {
				fmt.Printf("  Mount Point: %s\n", vol.LastMountPoint)
			}
			if vol.SourceHost != "" {
				fmt.Printf("  Host:        %s\n", vol.SourceHost)
			}
			return nil
		},
	}
	registerCmd.Flags().StringVar(&regID, "id", "", "Explicit volume identifier override")
	registerCmd.Flags().StringVar(&regLabel, "label", "", "Descriptive volume label")
	registerCmd.Flags().StringVar(&regSourceLabel, "source-label", "", "User-defined location or alias")
	registerCmd.Flags().StringVar(&regHost, "host", "", "Source host override")
	registerCmd.Flags().BoolVar(&regNoSidecar, "no-sidecar", false, "Disable writing .findex-volume-id fallback")
	registerCmd.Flags().BoolVar(&regForce, "force", false, "Force registration even if metadata conflicts")

	volumeCmd.AddCommand(listCmd, showCmd, registerCmd)
	rootCmd.AddCommand(volumeCmd)

	// fingerprint / hash debug command
	var fpFull bool
	var fpChunkSize int64
	fpCmd := &cobra.Command{
		Use:     "fingerprint <file-path>",
		Aliases: []string{"hash"},
		Short:   "Compute quick sparse fingerprint (and optional full hash) of a file",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			res, err := fingerprint.FingerprintFile(filePath, fingerprint.Options{
				IncludeFull: fpFull,
				ChunkSize:   fpChunkSize,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				return output.PrintFingerprintJSON(os.Stdout, res)
			}
			return output.PrintFingerprint(os.Stdout, res)
		},
	}
	fpCmd.Flags().BoolVar(&fpFull, "full", false, "Compute full BLAKE3-256 fingerprint in addition to quick sparse fingerprint")
	fpCmd.Flags().Int64Var(&fpChunkSize, "chunk-size", fingerprint.DefaultChunkSize, "Chunk size in bytes for sparse sampling")
	rootCmd.AddCommand(fpCmd)

	// scan command
	var scanVolID string
	var scanWorkers int
	var scanBatchSize int
	var scanFull bool
	var scanFast bool
	var scanVerify bool
	var scanDryRun bool

	scanCmd := &cobra.Command{
		Use:   "scan <mount-point-or-directory>",
		Short: "Scan and index a directory tree on a volume",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			targetDir := args[0]

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			var progressFn func(scan.Progress)
			if !jsonOut {
				var lastReport time.Time
				progressFn = func(p scan.Progress) {
					now := time.Now()
					if p.Percent < 100 && now.Sub(lastReport) < 80*time.Millisecond {
						return
					}
					lastReport = now

					if p.Phase == scan.PhaseDiscovering {
						fmt.Fprintf(os.Stderr, "\rScanning: %d entries (%d files, %d dirs) ...",
							p.EntriesScanned, p.FilesScanned, p.DirsScanned)
						return
					}

					barWidth := 25
					completedBars := int((p.Percent / 100.0) * float64(barWidth))
					if completedBars > barWidth {
						completedBars = barWidth
					}
					if completedBars < 0 {
						completedBars = 0
					}

					bar := strings.Repeat("=", completedBars)
					if completedBars < barWidth {
						bar += ">" + strings.Repeat(" ", barWidth-completedBars-1)
					}

					etaStr := "--"
					if p.EstimatedRemain > 0 {
						etaStr = formatETA(p.EstimatedRemain)
					}

					fmt.Fprintf(os.Stderr, "\r[%s] %5.1f%% (%d/%d files, ETA: %s) \033[K",
						bar, p.Percent, p.FilesCompleted, p.TotalFiles, etaStr)
				}
			}

			scanner := scan.New(db)
			summary, err := scanner.Run(context.Background(), scan.Options{
				VolumeID:     scanVolID,
				TargetDir:    targetDir,
				Workers:      scanWorkers,
				BatchSize:    scanBatchSize,
				IncludeFull:  scanFull,
				FastMode:     scanFast,
				StrictVerify: scanVerify,
				DryRun:       scanDryRun,
				ProgressFn:   progressFn,
			})

			if !jsonOut {
				// Clear the progress line
				fmt.Fprint(os.Stderr, "\r\033[K")
			}

			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintScanSummaryJSON(os.Stdout, summary)
			}
			return output.PrintScanSummary(os.Stdout, summary)
		},
	}
	scanCmd.Flags().StringVar(&scanVolID, "volume", "", "Explicit volume ID (auto-discovered if omitted)")
	scanCmd.Flags().IntVar(&scanWorkers, "workers", runtime.NumCPU(), "Number of parallel fingerprint workers")
	scanCmd.Flags().IntVar(&scanBatchSize, "batch-size", 500, "SQLite write batch size")
	scanCmd.Flags().BoolVar(&scanFull, "full", false, "Compute full BLAKE3-256 fingerprints in addition to sparse fingerprints")
	scanCmd.Flags().BoolVar(&scanFast, "fast", true, "Fast mode: reuse fingerprints if size & mtime match previous record")
	scanCmd.Flags().BoolVar(&scanVerify, "verify", false, "Strict verify mode: force re-computing fingerprints regardless of metadata")
	scanCmd.Flags().BoolVar(&scanDryRun, "dry-run", false, "Traverse and fingerprint without writing to database")
	rootCmd.AddCommand(scanCmd)

	// files command
	var filesVolID string
	filesCmd := &cobra.Command{
		Use:   "files [path-prefix]",
		Short: "List indexed files and directories for a volume",
		RunE: func(cmd *cobra.Command, args []string) error {
			if filesVolID == "" {
				return fmt.Errorf("--volume <volume-id> is required")
			}

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			var prefix string
			if len(args) > 0 {
				prefix = args[0]
			}

			entries, err := db.ListEntries(context.Background(), filesVolID, prefix)
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintJSON(os.Stdout, entries)
			}
			return output.PrintEntriesTable(os.Stdout, entries)
		},
	}
	filesCmd.Flags().StringVar(&filesVolID, "volume", "", "Volume ID to list entries from (required)")
	rootCmd.AddCommand(filesCmd)

	// merge command
	var mergePolicy string
	var mergeForce bool
	var mergeDryRun bool

	mergeCmd := &cobra.Command{
		Use:   "merge <other-index.sqlite>",
		Short: "Merge volume metadata and entries from another SQLite index database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sourcePath := args[0]

			policy := model.ConflictPolicy(mergePolicy)
			switch policy {
			case model.PolicyPreferNewer, model.PolicyKeepLocal, model.PolicyKeepRemote, model.PolicyFailOnConflict:
			default:
				return fmt.Errorf("invalid conflict policy %q: must be one of 'newer', 'keep-local', 'keep-remote', 'fail'", mergePolicy)
			}

			destDB, err := getDB()
			if err != nil {
				return err
			}
			defer destDB.Close()

			res, err := merge.Merge(context.Background(), destDB, sourcePath, merge.Options{
				Policy: policy,
				Force:  mergeForce,
				DryRun: mergeDryRun,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintMergeSummaryJSON(os.Stdout, res)
			}
			return output.PrintMergeSummary(os.Stdout, res)
		},
	}
	mergeCmd.Flags().StringVar(&mergePolicy, "policy", "newer", "Conflict resolution policy (newer, keep-local, keep-remote, fail)")
	mergeCmd.Flags().BoolVar(&mergeForce, "force", false, "Force merge even if volume metadata conflicts")
	mergeCmd.Flags().BoolVar(&mergeDryRun, "dry-run", false, "Simulate merge without committing database changes")
	rootCmd.AddCommand(mergeCmd)

	// compare command
	var compLeft bool
	var compRight bool
	var compCommon bool
	var compType string
	var compShowMatches bool
	var compVerify bool

	compareCmd := &cobra.Command{
		Use:   "compare [--left | --right | --common] <left_volumes> <right_volumes>",
		Short: "Compare content between two volume sets",
		Long: `Compare content between two volume sets.
Specifying one of the comparison options is mandatory (no defaults):
  --left   - display files/directories which exist only in the left volumes set
  --right  - display files/directories which exist only in the right volumes set
  --common - display files/directories which exist in both left and right volumes sets`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("compare requires 2 arguments: <left_volumes> <right_volumes> (e.g. 'findex compare --left vol1 vol2')")
			}

			modeCount := 0
			if compLeft {
				modeCount++
			}
			if compRight {
				modeCount++
			}
			if compCommon {
				modeCount++
			}

			if modeCount == 0 {
				return fmt.Errorf("a comparison option is mandatory: specify exactly one of --left, --right, or --common")
			}
			if modeCount > 1 {
				return fmt.Errorf("cannot combine comparison options: specify exactly one of --left, --right, or --common")
			}

			var mode string
			if compLeft {
				mode = "left"
			} else if compRight {
				mode = "right"
			} else {
				mode = "common"
			}

			entryType, err := parseEntryType(compType)
			if err != nil {
				return err
			}

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := compare.Compare(context.Background(), db, compare.Options{
				LeftVolumes:    []string{args[0]},
				RightVolumes:   []string{args[1]},
				Mode:           mode,
				EntryType:      entryType,
				ShowMatches:    compShowMatches,
				VerifyFullHash: compVerify,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintCompareSummaryJSON(os.Stdout, summary)
			}
			return output.PrintCompareSummary(os.Stdout, summary)
		},
	}
	compareCmd.Flags().BoolVar(&compLeft, "left", false, "Display files/directories which exist only in the left volumes set")
	compareCmd.Flags().BoolVar(&compRight, "right", false, "Display files/directories which exist only in the right volumes set")
	compareCmd.Flags().BoolVar(&compCommon, "common", false, "Display files/directories which exist in both left and right volumes sets")
	compareCmd.MarkFlagsMutuallyExclusive("left", "right", "common")

	compareCmd.Flags().StringVar(&compType, "type", "all", "Filter entries by type (all, file, dir)")
	compareCmd.Flags().BoolVar(&compShowMatches, "show-matches", false, "Include matching destination locations in report")
	compareCmd.Flags().BoolVar(&compVerify, "verify", false, "Verify candidate matches using full fingerprints")
	rootCmd.AddCommand(compareCmd)

	// uniq command (alias: unique)
	var uniqVol string
	var uniqAmong []string
	var uniqType string
	var uniqShowMatches bool
	var uniqVerify bool

	uniqCmd := &cobra.Command{
		Use:     "uniq <volumes_set>",
		Aliases: []string{"unique"},
		Short:   "Display files which exist only in this volumes set",
		Long: `Display files which exist only in this volumes set.
This command is syntax sugar for: findex compare --left volumes_set all_other_volumes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var volSet string
			if len(args) == 1 {
				volSet = args[0]
			} else if len(args) == 0 && uniqVol != "" {
				volSet = uniqVol
			} else {
				return fmt.Errorf("uniq requires 1 argument: volumes_set (e.g. 'findex uniq vol1')")
			}

			entryType, err := parseEntryType(uniqType)
			if err != nil {
				return err
			}

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := compare.Unique(context.Background(), db, volSet, uniqAmong, compare.Options{
				EntryType:      entryType,
				ShowMatches:    uniqShowMatches,
				VerifyFullHash: uniqVerify,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintCompareSummaryJSON(os.Stdout, summary)
			}
			return output.PrintCompareSummary(os.Stdout, summary)
		},
	}
	uniqCmd.Flags().StringVar(&uniqVol, "volume", "", "Volume or volume set (legacy flag)")
	_ = uniqCmd.Flags().MarkHidden("volume")
	uniqCmd.Flags().StringSliceVar(&uniqAmong, "among", nil, "Volumes to compare against (defaults to all other volumes)")
	uniqCmd.Flags().StringVar(&uniqType, "type", "all", "Filter entries by type (all, file, dir)")
	uniqCmd.Flags().BoolVar(&uniqShowMatches, "show-matches", false, "Include matching destination locations in report")
	uniqCmd.Flags().BoolVar(&uniqVerify, "verify", false, "Verify candidate matches using full fingerprints")
	rootCmd.AddCommand(uniqCmd)

	// lookup command
	lookupCmd := &cobra.Command{
		Use:   "lookup <fingerprint | filepath>",
		Short: "Lookup a fingerprint or calculate fingerprint for a file/directory and search database",
		Long: `Lookup searches the index database for matching entries.
- If a partial or full fingerprint is given, it searches entries matching that fingerprint.
- If a filepath is given (file or directory), it computes the fingerprint on-the-fly and looks it up in the database.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := args[0]

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := lookup.Lookup(context.Background(), db, target)
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintLookupSummaryJSON(os.Stdout, summary)
			}
			return output.PrintLookupSummary(os.Stdout, summary)
		},
	}
	rootCmd.AddCommand(lookupCmd)

	// check command
	checkCmd := &cobra.Command{
		Use:   "check",
		Short: "Run database integrity and inventory consistency checks",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := db.CheckIntegrity(context.Background())
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintCheckSummaryJSON(os.Stdout, summary)
			}
			return output.PrintCheckSummary(os.Stdout, summary)
		},
	}
	rootCmd.AddCommand(checkCmd)

	// export command
	var expVol string
	var expCompress bool
	var expPretty bool

	exportCmd := &cobra.Command{
		Use:   "export <output-file>",
		Short: "Export inventory metadata and entries to a portable JSON or gzip file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			outputPath := args[0]

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := export.Export(context.Background(), db, outputPath, export.ExportOptions{
				VolumeID:   expVol,
				Compress:   expCompress,
				IndentJSON: expPretty,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintExportSummaryJSON(os.Stdout, summary)
			}
			return output.PrintExportSummary(os.Stdout, summary)
		},
	}
	exportCmd.Flags().StringVar(&expVol, "volume", "", "Filter export to a specific volume ID")
	exportCmd.Flags().BoolVar(&expCompress, "compress", false, "Compress export with gzip (auto-detected if output ends with .gz)")
	exportCmd.Flags().BoolVar(&expPretty, "pretty", true, "Indent JSON output for human readability")
	rootCmd.AddCommand(exportCmd)

	// import command
	importCmd := &cobra.Command{
		Use:   "import <input-file>",
		Short: "Import inventory metadata and entries from a portable JSON or gzip file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputPath := args[0]

			db, err := getDB()
			if err != nil {
				return err
			}
			defer db.Close()

			summary, err := export.Import(context.Background(), db, inputPath)
			if err != nil {
				return err
			}

			if jsonOut {
				return output.PrintImportSummaryJSON(os.Stdout, summary)
			}
			return output.PrintImportSummary(os.Stdout, summary)
		},
	}
	rootCmd.AddCommand(importCmd)

	return rootCmd
}

func parseEntryType(typeStr string) (model.EntryType, error) {
	switch strings.ToLower(typeStr) {
	case "file", "files", "regular":
		return model.EntryTypeRegular, nil
	case "dir", "dirs", "directory", "directories":
		return model.EntryTypeDirectory, nil
	case "all", "":
		return 0, nil
	default:
		return 0, fmt.Errorf("invalid entry type %q: must be one of 'all', 'file', 'dir'", typeStr)
	}
}

func formatETA(d time.Duration) string {
	d = d.Round(time.Second)
	s := int(d.Seconds())
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	m := s / 60
	s = s % 60
	if m < 60 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	h := m / 60
	m = m % 60
	return fmt.Sprintf("%dh%02dm", h, m)
}

func main() {
	if err := buildRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
