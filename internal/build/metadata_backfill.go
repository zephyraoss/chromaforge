package build

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

type MetadataBackfillConfig struct {
	DBPath          string
	CacheDir        string
	BaseURL         string
	GoMaxProcs      int
	StartYear       int
	EndDate         string
	DownloadWorkers int
	SoftHeapLimit   int64
	GracefulStop    <-chan struct{}
}

type metadataBackfillStats struct {
	processed atomic.Int64
	updated   atomic.Int64
}

type metadataBackfillSession struct {
	conn       *sql.Conn
	insertStmt *sql.Stmt
}

type fingerprintMetadataUpdate struct {
	ID     int64 `json:"id"`
	Length int   `json:"length"`
}

const (
	createCandidateMetadataTempTable = `
CREATE TEMP TABLE IF NOT EXISTS candidate_metadata (
	acoustid TEXT PRIMARY KEY,
	mb_id TEXT,
	title TEXT,
	artist TEXT,
	duration INTEGER
) WITHOUT ROWID
`
	insertCandidateMetadataSQL = `
INSERT INTO temp.candidate_metadata (acoustid, mb_id, title, artist, duration)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(acoustid) DO UPDATE SET
	mb_id = COALESCE(NULLIF(candidate_metadata.mb_id, ''), NULLIF(excluded.mb_id, '')),
	title = COALESCE(NULLIF(candidate_metadata.title, ''), NULLIF(excluded.title, '')),
	artist = COALESCE(NULLIF(candidate_metadata.artist, ''), NULLIF(excluded.artist, '')),
	duration = CASE
		WHEN COALESCE(candidate_metadata.duration, 0) > 0 THEN candidate_metadata.duration
		WHEN COALESCE(excluded.duration, 0) > 0 THEN excluded.duration
		ELSE candidate_metadata.duration
	END
`
	applyCandidateMetadataSQL = `
UPDATE fingerprints
SET
	mb_id = CASE
		WHEN COALESCE(mb_id, '') = '' THEN NULLIF((SELECT cm.mb_id FROM temp.candidate_metadata cm WHERE cm.acoustid = fingerprints.acoustid), '')
		ELSE mb_id
	END,
	title = CASE
		WHEN COALESCE(title, '') = '' THEN NULLIF((SELECT cm.title FROM temp.candidate_metadata cm WHERE cm.acoustid = fingerprints.acoustid), '')
		ELSE title
	END,
	artist = CASE
		WHEN COALESCE(artist, '') = '' THEN NULLIF((SELECT cm.artist FROM temp.candidate_metadata cm WHERE cm.acoustid = fingerprints.acoustid), '')
		ELSE artist
	END,
	duration = CASE
		WHEN COALESCE(duration, 0) <= 0 THEN COALESCE((SELECT cm.duration FROM temp.candidate_metadata cm WHERE cm.acoustid = fingerprints.acoustid), duration)
		ELSE duration
	END
WHERE acoustid IN (SELECT acoustid FROM temp.candidate_metadata)
`
	clearCandidateMetadataSQL = `DELETE FROM temp.candidate_metadata`
)

func RunMetadataBackfill(ctx context.Context, cfg MetadataBackfillConfig) error {
	start := time.Now()
	if cfg.GoMaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.GoMaxProcs)
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://data.acoustid.org"
	}
	if cfg.DownloadWorkers <= 0 {
		cfg.DownloadWorkers = 4
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(filepath.Dir(cfg.DBPath), ".chromaforge-cache")
	}

	if !fileExists(cfg.DBPath) {
		return fmt.Errorf("database does not exist: %s", cfg.DBPath)
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return err
	}

	log.Printf("metadata backfill started db=%s cache_dir=%s gomaxprocs=%d download_workers=%d", cfg.DBPath, cfg.CacheDir, runtime.GOMAXPROCS(0), cfg.DownloadWorkers)

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
	if err := ApplyFinalizePragmas(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		return err
	}

	progress, hasProgress, err := loadMetadataBackfillProgress(cfg.DBPath)
	if err != nil {
		return err
	}
	resumeFromDay := ""
	if hasProgress {
		resumeFromDay = progress.LastCompletedDay
		log.Printf("metadata backfill resume detected last_completed_day=%s", resumeFromDay)
	}

	client := defaultHTTPClient(cfg.DownloadWorkers)
	log.Printf("archive discovery started")
	days, err := dump.DiscoverArchive(ctx, client, cfg.BaseURL, cfg.StartYear, cfg.EndDate)
	if err != nil {
		return err
	}
	if len(days) == 0 {
		return fmt.Errorf("no archive days discovered")
	}
	log.Printf("archive discovery completed: %d days", len(days))

	state := NewReplayState()
	startIdx, hasResume, err := findReplayStartIndex(days, resumeFromDay)
	if err != nil {
		return err
	}
	if hasResume {
		if startIdx == len(days) {
			log.Printf("metadata backfill resume detected all archive days already completed")
		} else {
			dl := downloader{client: client}
			for i := 0; i < startIdx; i++ {
				day := days[i]
				log.Printf("metadata backfill resume state rebuild day=%s", day.Day.Format("2006-01-02"))
				if err := ReplayStateDay(ctx, dl, Config{CacheDir: cfg.CacheDir}, day, state); err != nil {
					return err
				}
			}
		}
	}

	session, err := newMetadataBackfillSession(ctx, db)
	if err != nil {
		return err
	}
	defer session.Close()

	stats := &metadataBackfillStats{}
	remainingDays := days[startIdx:]
	if len(remainingDays) == 0 {
		log.Printf("metadata backfill skipping replay: no remaining archive days")
	} else {
		dl := downloader{client: client}
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

			log.Printf("metadata backfill day=%s", day.Day.Format("2006-01-02"))
			if err := BackfillMetadataDay(ctx, session, pdl, cfg.CacheDir, day, state, stats); err != nil {
				return err
			}
			if err := saveMetadataBackfillProgress(cfg.DBPath, day.Day); err != nil {
				return err
			}
			if stopRequested(cfg.GracefulStop) {
				log.Printf("metadata backfill graceful stop completed at day=%s rerun the same command to resume", day.Day.Format("2006-01-02"))
				return nil
			}
		}
	}

	if err := CheckpointWAL(ctx, db); err != nil {
		return err
	}
	if err := clearMetadataBackfillProgress(cfg.DBPath); err != nil {
		return err
	}
	log.Printf("metadata backfill completed processed=%d updated=%d db=%s elapsed=%s", stats.processed.Load(), stats.updated.Load(), cfg.DBPath, time.Since(start).Round(time.Second))
	return nil
}

func newMetadataBackfillSession(ctx context.Context, db *sql.DB) (*metadataBackfillSession, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if err := execConnPragma(ctx, conn, `PRAGMA temp_store = MEMORY;`); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, createCandidateMetadataTempTable); err != nil {
		_ = conn.Close()
		return nil, err
	}
	insertStmt, err := conn.PrepareContext(ctx, insertCandidateMetadataSQL)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &metadataBackfillSession{
		conn:       conn,
		insertStmt: insertStmt,
	}, nil
}

func (s *metadataBackfillSession) Close() error {
	var firstErr error
	if s.insertStmt != nil {
		if err := s.insertStmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func BackfillMetadataDay(ctx context.Context, session *metadataBackfillSession, client DownloadClient, cacheDir string, day dump.DayFiles, state *ReplayState, stats *metadataBackfillStats) error {
	for _, file := range day.OrderedFiles() {
		localPath := filepath.Join(cacheDir, day.Day.Format("2006-01"), file.Name)
		if err := client.Ensure(ctx, file, localPath); err != nil {
			return err
		}
		switch file.Type {
		case dump.FileTypeTrack:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				state.ApplyTrack(v)
				return nil
			}); err != nil {
				return err
			}
		case dump.FileTypeTrackMeta:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackMetaUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				state.ApplyTrackMeta(v)
				return nil
			}); err != nil {
				return err
			}
		case dump.FileTypeTrackMBID:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackMBIDUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				state.ApplyTrackMBID(v)
				return nil
			}); err != nil {
				return err
			}
		case dump.FileTypeTrackFingerprint:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackFingerprintUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				state.ApplyTrackFingerprint(v)
				return nil
			}); err != nil {
				return err
			}
		case dump.FileTypeFingerprint:
			if err := backfillMetadataFile(ctx, session, localPath, state, stats); err != nil {
				return err
			}
		case dump.FileTypeMeta:
			continue
		}
	}
	return nil
}

func backfillMetadataFile(ctx context.Context, session *metadataBackfillSession, path string, state *ReplayState, stats *metadataBackfillStats) error {
	if _, err := session.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = session.conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	if _, err := session.conn.ExecContext(ctx, clearCandidateMetadataSQL); err != nil {
		return err
	}

	if err := dump.ScanGzipLines(ctx, path, func(line []byte) error {
		var payload fingerprintMetadataUpdate
		if err := json.Unmarshal(line, &payload); err != nil {
			return err
		}
		acoustID, mbid, title, artist, ok := state.ResolveFingerprint(payload.ID)
		if !ok {
			return nil
		}
		record := Record{
			AcoustID: acoustID,
			MBID:     mbid,
			Title:    title,
			Artist:   artist,
			Duration: payload.Length,
		}
		if _, err := session.insertStmt.ExecContext(ctx,
			record.AcoustID,
			nullIfEmpty(record.MBID),
			nullIfEmpty(record.Title),
			nullIfEmpty(record.Artist),
			record.Duration,
		); err != nil {
			return err
		}
		stats.processed.Add(1)
		return nil
	}); err != nil {
		return err
	}

	res, err := session.conn.ExecContext(ctx, applyCandidateMetadataSQL)
	if err != nil {
		return err
	}
	if rows, err := res.RowsAffected(); err == nil {
		stats.updated.Add(rows)
	}
	if _, err := session.conn.ExecContext(ctx, clearCandidateMetadataSQL); err != nil {
		return err
	}
	if _, err := session.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}
