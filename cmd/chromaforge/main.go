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

	root.AddCommand(newBuildCmd(), newBuildCKAFCmd(), newSyncCmd(), newRefreshCmd(), newBackfillMetadataCmd(), newValidateCmd(), newMatchCmd(), newVersionCmd())

	if err := root.Execute(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func gracefulStopChannel(ctx context.Context, cancel context.CancelFunc, progressNoun string) <-chan struct{} {
	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		defer signal.Stop(sigCh)
		stopping := false
		for {
			select {
			case <-ctx.Done():
				return
			case sig := <-sigCh:
				if !stopping {
					stopping = true
					log.Printf("received %s, stopping after the current day and saving %s; press Ctrl+C again to abort immediately", sig, progressNoun)
					close(stopCh)
					continue
				}
				log.Printf("received %s again, aborting immediately", sig)
				cancel()
				return
			}
		}
	}()

	return stopCh
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

			cfg.GracefulStop = gracefulStopChannel(ctx, cancel, "resume progress")
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

func newBuildCKAFCmd() *cobra.Command {
	cfg := build.CKAFConfig{
		OutputPrefix:    "/mnt/nvme/chromakopia",
		CacheDir:        "",
		BaseURL:         "https://data.acoustid.org",
		GoMaxProcs:      0,
		DecodeWorkers:   0,
		DownloadWorkers: 4,
	}

	cmd := &cobra.Command{
		Use:   "build-ckaf",
		Short: "Replay the AcoustID archive into a CKAF dataset (.ckd/.cki/.ckm), skipping SQLite",
		Long: `Replay the AcoustID archive and emit a CKAF dataset directly:

  <prefix>.ckd  every fingerprint, PFOR-compressed
  <prefix>.cki  sampled posting index (stride 8, 2-bit quantization) for
                fingerprints whose track has an enabled MBID mapping
  <prefix>.ckm  fingerprint -> track/MBID map for every fingerprint

Keeping unmapped fingerprints in the .ckd and .ckm lets them be promoted
into the posting index later when new MBID mappings arrive.

RAM: replay state (track gid, track->mbid, fingerprint->track, acoustid
dedup) is held in memory like the SQLite build — on the order of 10-20 GB
for the full 92.6M-fingerprint corpus. Final assembly spills builder state
to scratch files under --spill-dir, keeping only compact per-record tables
in memory (~2-3 GB at full scale), so a full-corpus build fits in roughly
16-32 GB of RAM. Disk: decoded fingerprints are spooled beside the output
(roughly the final .ckd size) so an interrupted build can resume, and
assembly needs scratch space under --spill-dir roughly the size of the
final dataset — put it on NVMe.

On first Ctrl+C the build stops after the current day and saves resume
progress beside the output prefix; a second Ctrl+C aborts immediately.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			cfg.GracefulStop = gracefulStopChannel(ctx, cancel, "resume progress")
			return build.RunCKAF(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.OutputPrefix, "output", cfg.OutputPrefix, "Output dataset path prefix (writes <prefix>.ckd, <prefix>.cki, <prefix>.ckm)")
	cmd.Flags().IntVar(&cfg.GoMaxProcs, "gomaxprocs", cfg.GoMaxProcs, "Go scheduler CPU parallelism (defaults to runtime auto-detect)")
	cmd.Flags().IntVar(&cfg.DecodeWorkers, "decode-workers", cfg.DecodeWorkers, "Parallel fingerprint decode workers (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.StartYear, "start-year", 0, "Replay archive from this year (defaults to earliest available)")
	cmd.Flags().StringVar(&cfg.EndDate, "end-date", "", "Replay archive through this date (YYYY-MM-DD, defaults to latest available)")
	cmd.Flags().StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "Directory for downloaded archive files (defaults to a cache directory beside --output)")
	cmd.Flags().StringVar(&cfg.SpillDir, "spill-dir", cfg.SpillDir, "Directory for assembly scratch files, needs roughly the final dataset size free (defaults to the directory of --output)")
	cmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Background archive download workers")
	_ = cmd.Flags().MarkHidden("start-year")
	_ = cmd.Flags().MarkHidden("end-date")

	return cmd
}

func newSyncCmd() *cobra.Command {
	cfg := build.SyncConfig{
		DatasetPrefix:   "/mnt/nvme/chromakopia",
		CacheDir:        "",
		BaseURL:         "https://data.acoustid.org",
		GoMaxProcs:      0,
		DecodeWorkers:   0,
		DownloadWorkers: 4,
	}

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Apply daily AcoustID archive deltas to an existing CKAF dataset",
		Long: `Apply daily AcoustID archive deltas to a CKAF dataset built by build-ckaf.

Sync discovers archive days newer than the dataset's progress file
(<prefix>.sync-progress.json; first run falls back to the source date
stamped by build-ckaf), downloads them (~60 MB/day) and applies them:

  new fingerprints      appended to the .ckd/.ckm overflow regions, with
                        .cki postings when the track has an enabled MBID
  new MBID mappings     promote a previously-unmapped track's existing
                        fingerprints into the .ckm and .cki
  duplicate submissions merge a missing duration into the recorded
                        fingerprint, mirroring the build's dedup rule

CKAF files carry a single overflow region, so sync keeps an append-only
journal (<prefix>.sync-journal) of everything added since the last
compaction and rewrites the region from it each run; re-running a day is
detected via the journal and progress file and does not duplicate records.
When the overflow region exceeds 10% of the main records, sync warns, or
folds it into fresh main files when run with --compact.

Serving-node mode (--serving, or auto-detected when <prefix>.ckd does not
exist): the node holds only the .cki and .ckm. New MBID-mapped fingerprints
get postings and mappings appended using values straight from the day
files; work that needs the canonical .ckd — promoting previously-unmapped
fingerprints that gained an MBID, and duration backfills of main records —
is skipped and logged as deferred counts, to be folded in by the next
'chromaforge refresh'. This mode requires the .sync-progress.json that
refresh emits alongside the artifacts, and --compact is unavailable.

RAM: sync rebuilds its state (fingerprint->track, track->fingerprints,
track->MBID) by scanning the .ckm each run — on the order of 10 GB at the
full 92.6M-fingerprint corpus. The dataset files must be local paths.

On Ctrl+C sync stops after the current day, applies what it ingested and
saves progress; a second Ctrl+C aborts immediately.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			cfg.GracefulStop = gracefulStopChannel(ctx, cancel, "sync progress")
			return build.RunSync(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DatasetPrefix, "dataset", cfg.DatasetPrefix, "Dataset path prefix (reads and updates <prefix>.ckd, <prefix>.cki, <prefix>.ckm)")
	cmd.Flags().StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "Directory for downloaded archive files (defaults to a cache directory beside --dataset)")
	cmd.Flags().BoolVar(&cfg.Compact, "compact", cfg.Compact, "Compact the dataset when the overflow region exceeds the threshold instead of only warning")
	cmd.Flags().BoolVar(&cfg.Serving, "serving", cfg.Serving, "Serving-node mode: update only <prefix>.cki and <prefix>.ckm, deferring .ckd-dependent work to the next refresh (auto-detected when <prefix>.ckd is absent)")
	cmd.Flags().IntVar(&cfg.GoMaxProcs, "gomaxprocs", cfg.GoMaxProcs, "Go scheduler CPU parallelism (defaults to runtime auto-detect)")
	cmd.Flags().IntVar(&cfg.DecodeWorkers, "decode-workers", cfg.DecodeWorkers, "Parallel fingerprint decode workers (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Background archive download workers")

	return cmd
}

func newRefreshCmd() *cobra.Command {
	cfg := build.SyncConfig{
		DatasetPrefix:   "/mnt/nvme/chromakopia",
		CacheDir:        "",
		BaseURL:         "https://data.acoustid.org",
		GoMaxProcs:      0,
		DecodeWorkers:   0,
		DownloadWorkers: 4,
	}

	cmd := &cobra.Command{
		Use:   "refresh",
		Short: "Re-baseline a CKAF dataset: full sync against the canonical .ckd, all deferred promotions, forced compaction, fresh artifacts",
		Long: `Refresh a CKAF dataset on a machine that holds the canonical .ckd (for the
two-tier setup: a throwaway VM after downloading <prefix>.ckd, .cki, .ckm
and .sync-progress.json from object storage).

Refresh applies every archive day since the dataset's last-synced day like
a full sync — including all promotions the serving nodes had to defer,
whose fingerprint values are read from the local .ckd — then always folds
the overflow regions into fresh main files (rebuilding the .cki so it keeps
covering only MBID-mapped fingerprints) and writes .sync-progress.json with
a new artifact generation.

The resulting artifact sets:

  object storage    <prefix>.ckd  + <prefix>.sync-progress.json
  serving nodes     <prefix>.cki + <prefix>.ckm + <prefix>.sync-progress.json

Deploy the progress file together with the .cki/.ckm: its new generation is
what tells the serving node's next sync that its journal predates the fresh
artifacts and must be discarded. Uploading is left to the operator (aws s3
cp, rclone, rsync — see the README runbook).

RAM: like sync, state is rebuilt by scanning the .ckm (~10 GB at the full
corpus); the pending overflow content (all fingerprints added since the
last refresh, with values) is additionally held in memory while it is
rewritten and compacted.

On Ctrl+C refresh stops after the current day, applies what it ingested and
saves progress; a second Ctrl+C aborts immediately. Re-running continues
where it left off.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			cfg.GracefulStop = gracefulStopChannel(ctx, cancel, "refresh progress")
			return build.RunRefresh(ctx, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.DatasetPrefix, "dataset", cfg.DatasetPrefix, "Dataset path prefix (reads and rewrites <prefix>.ckd, <prefix>.cki, <prefix>.ckm)")
	cmd.Flags().StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "Directory for downloaded archive files (defaults to a cache directory beside --dataset)")
	cmd.Flags().IntVar(&cfg.GoMaxProcs, "gomaxprocs", cfg.GoMaxProcs, "Go scheduler CPU parallelism (defaults to runtime auto-detect)")
	cmd.Flags().IntVar(&cfg.DecodeWorkers, "decode-workers", cfg.DecodeWorkers, "Parallel fingerprint decode workers (defaults to GOMAXPROCS)")
	cmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Background archive download workers")

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

			cfg.GracefulStop = gracefulStopChannel(ctx, cancel, "metadata backfill progress")
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
			fmt.Fprintln(tw, "RANK\tHITS\tCOVERAGE\tDELTA\tDURATION\tACOUSTID\tMBID")
			for i, candidate := range result.Candidates {
				fmt.Fprintf(tw, "%d\t%d\t%.1f%%\t%d\t%d\t%s\t%s\n",
					i+1,
					candidate.Hits,
					candidate.Coverage,
					candidate.Delta,
					candidate.Duration,
					candidate.AcoustID,
					candidate.MBID,
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
