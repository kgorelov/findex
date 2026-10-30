package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/kgorelov/findex/internal/fingerprint"
	"github.com/kgorelov/findex/internal/index"
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

func main() {
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

			scanner := scan.New(db)
			summary, err := scanner.Run(context.Background(), scan.Options{
				VolumeID:    scanVolID,
				TargetDir:   targetDir,
				Workers:     scanWorkers,
				BatchSize:   scanBatchSize,
				IncludeFull: scanFull,
				DryRun:      scanDryRun,
			})
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

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
