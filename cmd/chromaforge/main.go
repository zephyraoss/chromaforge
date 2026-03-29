package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/zephyraoss/chromaforge/internal/build"
	"github.com/zephyraoss/chromaforge/internal/match"
	"github.com/zephyraoss/chromaforge/internal/validate"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	root := &cobra.Command{
		Use:           "chromaforge",
		Short:         "Build and validate the Chromakopia AcoustID fingerprint database",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(newBuildCmd(), newBackfillMetadataCmd(), newValidateCmd(), newMatchCmd(), newVersionCmd())

	if err := root.Execute(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func newBuildCmd() *cobra.Command {
	cfg := build.Config{
		DBPath:              "/mnt/nvme/chromakopia.db",
		OutputPath:          "",
		Workers:             0,
		DecodeWorkers:       0,
		BatchSize:           500,
		CacheDir:            "",
		TempDir:             "",
		BaseURL:             "https://data.acoustid.org",
		GoMaxProcs:          0,
		SoftHeapLimit:       -1,
		CacheSizeBytes:      0,
		MmapSizeBytes:       0,
		IndexCacheSizeBytes: 0,
		IndexMmapSizeBytes:  0,
		DownloadWorkers:     4,
		SkipValidate:        false,
	}

	cmd := &cobra.Command{
		Use:   "build",
		Short: "Reconstruct the full fingerprint database from the AcoustID archive",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			stopCh := make(chan struct{})
			sigCh := make(chan os.Signal, 2)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigCh)

			go func() {
				stopping := false
				for {
					select {
					case <-ctx.Done():
						return
					case sig := <-sigCh:
						if !stopping {
							stopping = true
							log.Printf("received %s, stopping after the current day and saving resume progress; press Ctrl+C again to abort immediately", sig)
							close(stopCh)
							continue
						}
						log.Printf("received %s again, aborting immediately", sig)
						cancel()
						return
					}
				}
			}()

			cfg.GracefulStop = stopCh
			return build.Run(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Output database path")
	cmd.Flags().StringVar(&cfg.OutputPath, "output", cfg.OutputPath, "Optional rsync destination after build")
	cmd.Flags().IntVar(&cfg.GoMaxProcs, "gomaxprocs", cfg.GoMaxProcs, "Go scheduler CPU parallelism (defaults to runtime auto-detect)")
	cmd.Flags().IntVar(&cfg.Workers, "workers", cfg.Workers, "SQLite index build threads (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.DecodeWorkers, "decode-workers", cfg.DecodeWorkers, "Parallel fingerprint decode workers (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "Records per insert batch")
	cmd.Flags().BoolVar(&cfg.SelfDeallocate, "self-deallocate", false, "Trigger Azure VM self-deallocation after a successful build")
	cmd.Flags().IntVar(&cfg.StartYear, "start-year", 0, "Replay archive from this year (defaults to earliest available)")
	cmd.Flags().StringVar(&cfg.EndDate, "end-date", "", "Replay archive through this date (YYYY-MM-DD, defaults to latest available)")
	cmd.Flags().StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "Directory for downloaded archive files (defaults to a cache directory beside --db)")
	cmd.Flags().StringVar(&cfg.TempDir, "temp-dir", cfg.TempDir, "Directory for SQLite temp files during index build (defaults under --cache-dir)")
	cmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Background archive download workers")
	cmd.Flags().Int64Var(&cfg.CacheSizeBytes, "cache-size", cfg.CacheSizeBytes, "SQLite replay/write page cache target in bytes; 0 keeps the phase default")
	cmd.Flags().Int64Var(&cfg.MmapSizeBytes, "mmap-size", cfg.MmapSizeBytes, "SQLite replay/write mmap_size in bytes; 0 keeps the phase default")
	cmd.Flags().Int64Var(&cfg.IndexCacheSizeBytes, "index-cache-size", cfg.IndexCacheSizeBytes, "SQLite index-build page cache target in bytes; 0 keeps the phase default")
	cmd.Flags().Int64Var(&cfg.IndexMmapSizeBytes, "index-mmap-size", cfg.IndexMmapSizeBytes, "SQLite index-build mmap_size in bytes; 0 keeps the phase default")
	cmd.Flags().BoolVar(&cfg.SkipValidate, "skip-validate", cfg.SkipValidate, "Skip post-build validation so it can be run later with the validate command")
	cmd.Flags().Int64Var(&cfg.SoftHeapLimit, "soft-heap-limit", cfg.SoftHeapLimit, "SQLite soft heap limit in bytes; use 0 to disable, negative to leave unchanged")
	_ = cmd.Flags().MarkHidden("start-year")
	_ = cmd.Flags().MarkHidden("end-date")

	return cmd
}

func newValidateCmd() *cobra.Command {
	cfg := validate.Config{
		DBPath:          "/mnt/disk/chromakopia.db",
		SoftHeapLimit:   -1,
		SampleCount:     5,
		ReadConnections: 1,
	}
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the fingerprint database",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			_, err := validate.Run(ctx, cfg)
			return err
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Database to validate")
	cmd.Flags().BoolVar(&cfg.QuickCheck, "quick-check", cfg.QuickCheck, "Run PRAGMA quick_check after the fast validation pass")
	cmd.Flags().BoolVar(&cfg.FullIntegrityCheck, "full-integrity-check", cfg.FullIntegrityCheck, "Run the slower full PRAGMA integrity_check instead of quick_check")
	cmd.Flags().BoolVar(&cfg.CountRows, "count-rows", cfg.CountRows, "Run exact COUNT(*) scans for fingerprints and sub_fingerprints")
	cmd.Flags().IntVar(&cfg.SampleCount, "sample-count", cfg.SampleCount, "Sample lookups per table")
	cmd.Flags().IntVar(&cfg.ReadConnections, "read-conns", cfg.ReadConnections, "SQLite read connections for standalone validation")
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Validation timeout; 0 disables the timeout")
	cmd.Flags().Int64Var(&cfg.SoftHeapLimit, "soft-heap-limit", cfg.SoftHeapLimit, "SQLite soft heap limit in bytes; use 0 to disable, negative to leave unchanged")
	return cmd
}

func newBackfillMetadataCmd() *cobra.Command {
	cfg := build.MetadataBackfillConfig{
		DBPath:          "/mnt/nvme/chromakopia.db",
		CacheDir:        "",
		BaseURL:         "https://data.acoustid.org",
		GoMaxProcs:      0,
		DecodeWorkers:   0,
		DownloadWorkers: 4,
		SoftHeapLimit:   -1,
	}

	cmd := &cobra.Command{
		Use:   "backfill-metadata",
		Short: "Replay archive metadata into an existing database without rebuilding fingerprints",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			stopCh := make(chan struct{})
			sigCh := make(chan os.Signal, 2)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigCh)

			go func() {
				stopping := false
				for {
					select {
					case <-ctx.Done():
						return
					case sig := <-sigCh:
						if !stopping {
							stopping = true
							log.Printf("received %s, stopping after the current day and saving metadata backfill progress; press Ctrl+C again to abort immediately", sig)
							close(stopCh)
							continue
						}
						log.Printf("received %s again, aborting immediately", sig)
						cancel()
						return
					}
				}
			}()

			cfg.GracefulStop = stopCh
			return build.RunMetadataBackfill(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Existing database path to update in place")
	cmd.Flags().StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "Directory for downloaded archive files (defaults to a cache directory beside --db)")
	cmd.Flags().IntVar(&cfg.GoMaxProcs, "gomaxprocs", cfg.GoMaxProcs, "Go scheduler CPU parallelism (defaults to runtime auto-detect)")
	cmd.Flags().IntVar(&cfg.DecodeWorkers, "decode-workers", cfg.DecodeWorkers, "Parallel metadata decode workers (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.StartYear, "start-year", cfg.StartYear, "Replay archive from this year (defaults to earliest available)")
	cmd.Flags().StringVar(&cfg.EndDate, "end-date", cfg.EndDate, "Replay archive through this date (YYYY-MM-DD, defaults to latest available)")
	cmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Background archive download workers")
	cmd.Flags().Int64Var(&cfg.SoftHeapLimit, "soft-heap-limit", cfg.SoftHeapLimit, "SQLite soft heap limit in bytes; use 0 to disable, negative to leave unchanged")
	_ = cmd.Flags().MarkHidden("start-year")
	_ = cmd.Flags().MarkHidden("end-date")

	return cmd
}

func newMatchCmd() *cobra.Command {
	cfg := match.Config{
		DBPath:          "/mnt/disk/chromakopia.db",
		DurationWindow:  0,
		Limit:           10,
		MinHits:         0,
		SoftHeapLimit:   -1,
		ReadConnections: 1,
	}

	cmd := &cobra.Command{
		Use:   "match",
		Short: "Match a Chromaprint fingerprint against the local database",
		RunE: func(cmd *cobra.Command, args []string) error {
			result, err := match.Run(cmd.Context(), cfg)
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if result.QueryDuration > 0 {
				fmt.Fprintf(out, "query_duration=%d query_sub_fingerprints=%d\n", result.QueryDuration, result.QuerySubFingerprintCount)
			} else {
				fmt.Fprintf(out, "query_sub_fingerprints=%d\n", result.QuerySubFingerprintCount)
			}
			if len(result.Candidates) == 0 {
				fmt.Fprintln(out, "no matches found")
				return nil
			}

			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "RANK\tHITS\tCOVERAGE\tDELTA\tDURATION\tACOUSTID\tARTIST\tTITLE")
			for i, candidate := range result.Candidates {
				fmt.Fprintf(tw, "%d\t%d\t%.1f%%\t%d\t%d\t%s\t%s\t%s\n",
					i+1,
					candidate.Hits,
					candidate.Coverage,
					candidate.Delta,
					candidate.Duration,
					candidate.AcoustID,
					candidate.Artist,
					candidate.Title,
				)
			}
			return tw.Flush()
		},
	}

	cmd.Flags().StringVar(&cfg.DBPath, "db", cfg.DBPath, "Database to query")
	cmd.Flags().StringVar(&cfg.Fingerprint, "fingerprint", cfg.Fingerprint, "Raw Chromaprint fingerprint values as a comma-separated list")
	cmd.Flags().StringVar(&cfg.FingerprintFile, "fingerprint-file", cfg.FingerprintFile, "Path to a file containing raw fingerprint values or fpcalc -raw output; use - for stdin")
	cmd.Flags().IntVar(&cfg.Duration, "duration", cfg.Duration, "Query duration in seconds; overrides any DURATION= value from --fingerprint-file")
	cmd.Flags().IntVar(&cfg.DurationWindow, "duration-window", cfg.DurationWindow, "Duration tolerance in seconds; 0 auto-selects a small default when duration is known, negative is invalid")
	cmd.Flags().IntVar(&cfg.Limit, "limit", cfg.Limit, "Maximum matches to return")
	cmd.Flags().IntVar(&cfg.MinHits, "min-hits", cfg.MinHits, "Minimum aligned sub-fingerprint hits required; 0 auto-selects a small threshold")
	cmd.Flags().IntVar(&cfg.ReadConnections, "read-conns", cfg.ReadConnections, "SQLite read connections for matching")
	cmd.Flags().Int64Var(&cfg.SoftHeapLimit, "soft-heap-limit", cfg.SoftHeapLimit, "SQLite soft heap limit in bytes; use 0 to disable, negative to leave unchanged")
	return cmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("version=%s commit=%s date=%s\n", version, commit, date)
		},
	}
}
