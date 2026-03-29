package build

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/zephyraoss/chromaforge/internal/azure"
	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
	"github.com/zephyraoss/chromaforge/internal/validate"
)

type Config struct {
	DBPath              string
	OutputPath          string
	Workers             int
	DecodeWorkers       int
	BatchSize           int
	SelfDeallocate      bool
	StartYear           int
	EndDate             string
	CacheDir            string
	BaseURL             string
	GoMaxProcs          int
	SoftHeapLimit       int64
	CacheSizeBytes      int64
	MmapSizeBytes       int64
	IndexCacheSizeBytes int64
	IndexMmapSizeBytes  int64
	DownloadWorkers     int
	HTTPClient          *http.Client
	GracefulStop        <-chan struct{}
}

type downloader struct {
	client *http.Client
}

func (d downloader) Ensure(ctx context.Context, file dump.ArchiveFile, dst string) error {
	return dump.DownloadFile(ctx, d.client, file.URL, dst, file.Size)
}

func Run(ctx context.Context, cfg Config) error {
	start := time.Now()
	if cfg.GoMaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.GoMaxProcs)
	}
	effectiveGoMaxProcs := runtime.GOMAXPROCS(0)
	if cfg.Workers <= 0 {
		cfg.Workers = effectiveGoMaxProcs
	}
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = effectiveGoMaxProcs
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://data.acoustid.org"
	}
	if cfg.DownloadWorkers <= 0 {
		cfg.DownloadWorkers = 4
	}
	if cfg.CacheSizeBytes < 0 {
		return fmt.Errorf("cache size must be >= 0, got %d", cfg.CacheSizeBytes)
	}
	if cfg.MmapSizeBytes < 0 {
		return fmt.Errorf("mmap size must be >= 0, got %d", cfg.MmapSizeBytes)
	}
	if cfg.IndexCacheSizeBytes < 0 {
		return fmt.Errorf("index cache size must be >= 0, got %d", cfg.IndexCacheSizeBytes)
	}
	if cfg.IndexMmapSizeBytes < 0 {
		return fmt.Errorf("index mmap size must be >= 0, got %d", cfg.IndexMmapSizeBytes)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = defaultHTTPClient(cfg.DownloadWorkers)
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(filepath.Dir(cfg.DBPath), ".chromaforge-cache")
	}

	log.Printf("build started db=%s output=%s cache_dir=%s gomaxprocs=%d workers=%d decode_workers=%d batch_size=%d cache_size=%d mmap_size=%d index_cache_size=%d index_mmap_size=%d", cfg.DBPath, cfg.OutputPath, cfg.CacheDir, effectiveGoMaxProcs, cfg.Workers, cfg.DecodeWorkers, cfg.BatchSize, cfg.CacheSizeBytes, cfg.MmapSizeBytes, cfg.IndexCacheSizeBytes, cfg.IndexMmapSizeBytes)

	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return err
	}

	progress, hasProgress, err := loadBuildProgress(cfg.DBPath)
	if err != nil {
		return err
	}
	dbExists := fileExists(cfg.DBPath)
	resumeFromDay := ""
	if hasProgress && dbExists {
		resumeFromDay = progress.LastCompletedDay
		log.Printf("resume detected last_completed_day=%s", resumeFromDay)
	} else {
		if hasProgress && !dbExists {
			log.Printf("resume progress found without database, starting fresh")
			if err := clearBuildProgress(cfg.DBPath); err != nil {
				return err
			}
		}
		cleanupBuildArtifacts(cfg.DBPath)
	}

	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if applied, err := libsqlutil.ApplySoftHeapLimit(ctx, db, cfg.SoftHeapLimit); err != nil {
		return err
	} else if cfg.SoftHeapLimit >= 0 {
		log.Printf("sqlite soft_heap_limit=%d", applied)
	}
	if err := ApplyBuildPragmas(ctx, db, cfg.CacheSizeBytes, cfg.MmapSizeBytes); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		return err
	}

	mode := txModeBeginExclusive
	log.Printf("transaction mode: %s", mode)
	if cfg.Workers > 1 {
		log.Printf("sqlite bulk-load path uses a single exclusive writer; keeping decode/index parallelism and batching writes through one insert worker")
	}

	log.Printf("archive discovery started")
	days, err := dump.DiscoverArchive(ctx, cfg.HTTPClient, cfg.BaseURL, cfg.StartYear, cfg.EndDate)
	if err != nil {
		return err
	}
	if len(days) == 0 {
		return errors.New("no archive days discovered")
	}
	log.Printf("archive discovery completed: %d days", len(days))

	state := NewReplayState()
	startIdx, hasResume, err := findReplayStartIndex(days, resumeFromDay)
	if err != nil {
		return err
	}
	if hasResume {
		if startIdx == len(days) {
			log.Printf("resume detected all archive days already completed; skipping state rebuild")
		} else {
			dl := downloader{client: cfg.HTTPClient}
			for i := 0; i < startIdx; i++ {
				day := days[i]
				log.Printf("resume state rebuild day=%s", day.Day.Format("2006-01-02"))
				if err := ReplayStateDay(ctx, dl, cfg, day, state); err != nil {
					return err
				}
			}
			log.Printf("resume ready remaining_days=%d", len(days)-startIdx)
		}
	}
	remainingDays := days[startIdx:]
	var totalEstimate int64
	for _, day := range remainingDays {
		if file, ok := day.Files[dump.FileTypeFingerprint]; ok {
			totalEstimate += max(1, file.Size/100)
		}
	}

	stats := &Stats{start: start}
	if len(remainingDays) > 0 {
		dl := downloader{client: cfg.HTTPClient}
		writeSession, err := newWriteSession(ctx, db)
		if err != nil {
			return err
		}
		defer func() {
			if writeSession != nil {
				if err := writeSession.Close(); err != nil {
					log.Printf("writer session close warning: %v", err)
				}
			}
		}()
		prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
		pdl := newPrefetchDownloader(prefetchCtx, dl, cfg.DownloadWorkers)
		defer func() {
			cancelPrefetch()
			pdl.Close()
		}()

		log.Printf("download prefetch window_days=%d workers=%d", prefetchWindowDays, cfg.DownloadWorkers)
		initialPrefetch := prefetchWindowDays + 1
		if initialPrefetch > len(remainingDays) {
			initialPrefetch = len(remainingDays)
		}
		for i := 1; i < initialPrefetch; i++ {
			pdl.PrefetchDay(remainingDays[i], cfg.CacheDir)
		}

		for i, day := range remainingDays {
			next := i + prefetchWindowDays + 1
			if next < len(remainingDays) {
				pdl.PrefetchDay(remainingDays[next], cfg.CacheDir)
			}

			log.Printf("replaying day %s", day.Day.Format("2006-01-02"))
			if err := ReplayDay(ctx, writeSession, pdl, cfg, day, state, stats, mode, totalEstimate); err != nil {
				return err
			}
			if err := saveBuildProgress(cfg.DBPath, day.Day); err != nil {
				return err
			}
			if stopRequested(cfg.GracefulStop) {
				log.Printf("graceful stop completed at day=%s rerun the same build command to resume", day.Day.Format("2006-01-02"))
				return nil
			}
		}

		if err := validateBadRecordRate(stats); err != nil {
			return err
		}
		if err := writeSession.Close(); err != nil {
			return err
		}
	} else {
		log.Printf("resume skipping replay: no remaining archive days")
	}

	log.Printf("index build started")
	indexStart := time.Now()
	if err := ApplyIndexPragmas(ctx, db, cfg.Workers, cfg.IndexCacheSizeBytes, cfg.IndexMmapSizeBytes); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.CreateHashIndex); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.AnalyzeSQL); err != nil {
		return err
	}
	log.Printf("index build completed elapsed=%s", time.Since(indexStart).Round(time.Second))

	if err := ApplyFinalizePragmas(ctx, db); err != nil {
		return err
	}

	log.Printf("validate started")
	result, err := validate.RunDB(ctx, db)
	if err != nil {
		return err
	}
	log.Printf("validate completed ok=%t fingerprints=%d sub_fingerprints=%d", result.OK, result.FingerprintCount, result.SubFingerprintCount)

	if err := CheckpointWAL(ctx, db); err != nil {
		return err
	}
	if err := clearBuildProgress(cfg.DBPath); err != nil {
		return err
	}

	if cfg.OutputPath != "" {
		log.Printf("rsync started src=%s dst=%s", cfg.DBPath, cfg.OutputPath)
		if err := RsyncDB(cfg.DBPath, cfg.OutputPath); err != nil {
			return fmt.Errorf("rsync failed: %w", err)
		}
		log.Printf("rsync completed")
	} else {
		log.Printf("rsync skipped: no --output provided")
	}

	if cfg.SelfDeallocate {
		if err := azure.SelfDeallocate(ctx); err != nil {
			log.Printf("self-deallocate warning: %v", err)
		}
	}

	log.Printf("build summary days=%d fingerprints=%d sub_fingerprints=%d skipped=%d db=%s elapsed=%s",
		len(days),
		stats.insertedFingerprints.Load(),
		stats.insertedSubFPs.Load(),
		stats.skipped.Load(),
		cfg.DBPath,
		time.Since(start).Round(time.Second),
	)
	return nil
}

func findReplayStartIndex(days []dump.DayFiles, resumeFromDay string) (int, bool, error) {
	if resumeFromDay == "" {
		return 0, false, nil
	}

	cutoff, err := time.Parse("2006-01-02", resumeFromDay)
	if err != nil {
		return 0, false, err
	}

	for i, day := range days {
		if day.Day.After(cutoff) {
			return i, true, nil
		}
	}
	return len(days), true, nil
}

func stopRequested(ch <-chan struct{}) bool {
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func defaultHTTPClient(downloadWorkers int) *http.Client {
	maxIdleConns := maxInt(32, downloadWorkers*4)
	maxIdleConnsPerHost := maxInt(8, downloadWorkers)
	maxConnsPerHost := maxInt(16, downloadWorkers*2)

	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          maxIdleConns,
			MaxIdleConnsPerHost:   maxIdleConnsPerHost,
			MaxConnsPerHost:       maxConnsPerHost,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		},
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func cleanupBuildArtifacts(dbPath string) {
	for _, suffix := range []string{"", "-wal", "-log", "-shm", "-journal"} {
		path := dbPath + suffix
		if suffix == "" {
			path = dbPath
		}
		if err := os.Remove(path); err == nil {
			log.Printf("removed stale build artifact: %s", path)
		}
	}
}
