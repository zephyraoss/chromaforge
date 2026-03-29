package build

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
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
	DecodeWorkers   int
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

type metadataReplayState struct {
	mu                     sync.RWMutex
	trackGID               map[int64]string
	trackMeta              map[int64]trackMeta
	trackMBID              map[int64]string
	fingerprintTrack       map[int64]int64
	targetAcoustIDs        *compactAcoustIDSet
	unresolvedFingerprints map[int64]struct{}
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

const (
	metadataRawLineBuffer       = 20000
	metadataRecordBuffer        = 20000
	metadataTargetProgressEvery = 1_000_000
)

type compactAcoustIDSet struct {
	uuids  [][16]byte
	extras map[string]struct{}
}

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
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = runtime.GOMAXPROCS(0)
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

	log.Printf("metadata backfill started db=%s cache_dir=%s gomaxprocs=%d decode_workers=%d download_workers=%d", cfg.DBPath, cfg.CacheDir, runtime.GOMAXPROCS(0), cfg.DecodeWorkers, cfg.DownloadWorkers)

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

	targetAcoustIDs, err := loadIncompleteMetadataAcoustIDs(ctx, db)
	if err != nil {
		return err
	}
	if targetAcoustIDs.Len() == 0 {
		log.Printf("metadata backfill skipped: no fingerprints have missing metadata")
		return nil
	}
	log.Printf("metadata backfill target_acoustids=%d", targetAcoustIDs.Len())

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

	state := newMetadataReplayState(targetAcoustIDs)
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
				if err := ReplayMetadataStateDay(ctx, dl, cfg.CacheDir, day, state); err != nil {
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
			if err := BackfillMetadataDay(ctx, session, pdl, cfg.CacheDir, day, state, stats, cfg.DecodeWorkers); err != nil {
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

func newMetadataReplayState(targetAcoustIDs *compactAcoustIDSet) *metadataReplayState {
	return &metadataReplayState{
		trackGID:               map[int64]string{},
		trackMeta:              map[int64]trackMeta{},
		trackMBID:              map[int64]string{},
		fingerprintTrack:       map[int64]int64{},
		targetAcoustIDs:        targetAcoustIDs,
		unresolvedFingerprints: map[int64]struct{}{},
	}
}

func loadIncompleteMetadataAcoustIDs(ctx context.Context, db *sql.DB) (*compactAcoustIDSet, error) {
	rows, err := db.QueryContext(ctx, `
SELECT acoustid
FROM fingerprints
WHERE
	COALESCE(mb_id, '') = ''
	OR COALESCE(title, '') = ''
	OR COALESCE(artist, '') = ''
	OR COALESCE(duration, 0) <= 0
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &compactAcoustIDSet{
		uuids:  make([][16]byte, 0, 1024),
		extras: make(map[string]struct{}),
	}
	var count int64
	for rows.Next() {
		var acoustid string
		if err := rows.Scan(&acoustid); err != nil {
			return nil, err
		}
		if id, ok := parseUUIDString(acoustid); ok {
			out.uuids = append(out.uuids, id)
		} else {
			out.extras[acoustid] = struct{}{}
		}
		count++
		if count%metadataTargetProgressEvery == 0 {
			log.Printf("metadata backfill target load loaded=%d", count)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out.uuids, func(i, j int) bool {
		return compareUUID(out.uuids[i], out.uuids[j]) < 0
	})
	log.Printf("metadata backfill target load completed total=%d parsed_uuids=%d extras=%d", count, len(out.uuids), len(out.extras))
	return out, nil
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

func (s *metadataReplayState) ApplyTrack(v dump.TrackUpdate) {
	if v.GID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.targetAcoustIDs.Contains(v.GID) {
		return
	}
	if _, ok := s.trackGID[v.ID]; !ok {
		s.trackGID[v.ID] = v.GID
	}
}

func (s *metadataReplayState) ApplyTrackMeta(v dump.TrackMetaUpdate) {
	if v.TrackID == 0 || (v.Track == "" && v.Artist == "") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.trackGID[v.TrackID]; !ok {
		return
	}
	current := s.trackMeta[v.TrackID]
	if current.title == "" && v.Track != "" {
		current.title = v.Track
	}
	if current.artist == "" && v.Artist != "" {
		current.artist = v.Artist
	}
	if current.title != "" || current.artist != "" {
		s.trackMeta[v.TrackID] = current
	}
}

func (s *metadataReplayState) ApplyTrackMBID(v dump.TrackMBIDUpdate) {
	if v.TrackID == 0 || v.MBID == "" || v.Disabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.trackGID[v.TrackID]; !ok {
		return
	}
	if s.trackMBID[v.TrackID] == "" {
		s.trackMBID[v.TrackID] = v.MBID
	}
}

func (s *metadataReplayState) ApplyTrackFingerprint(v dump.TrackFingerprintUpdate) {
	if v.TrackID == 0 || v.FingerprintID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.trackGID[v.TrackID]; !ok {
		return
	}
	if _, ok := s.fingerprintTrack[v.FingerprintID]; !ok {
		s.fingerprintTrack[v.FingerprintID] = v.TrackID
		s.unresolvedFingerprints[v.FingerprintID] = struct{}{}
	}
}

func (s *metadataReplayState) ResolveFingerprint(id int64) (acoustID, mbid, title, artist string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	trackID, ok := s.fingerprintTrack[id]
	if !ok {
		return "", "", "", "", false
	}
	acoustID, ok = s.trackGID[trackID]
	if !ok || acoustID == "" {
		return "", "", "", "", false
	}
	meta := s.trackMeta[trackID]
	mbid = s.trackMBID[trackID]
	return acoustID, mbid, meta.title, meta.artist, true
}

func (s *metadataReplayState) HasPendingFingerprints() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.unresolvedFingerprints) > 0
}

func (s *metadataReplayState) NeedsFingerprint(id int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.unresolvedFingerprints[id]
	return ok
}

func (s *metadataReplayState) ResolveFingerprintMetadata(id int64) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.unresolvedFingerprints[id]; !ok {
		return Record{}, false
	}

	trackID, ok := s.fingerprintTrack[id]
	if !ok {
		return Record{}, false
	}
	acoustID, ok := s.trackGID[trackID]
	if !ok {
		return Record{}, false
	}
	meta := s.trackMeta[trackID]
	mbid := s.trackMBID[trackID]
	delete(s.unresolvedFingerprints, id)
	return Record{
		AcoustID: acoustID,
		MBID:     mbid,
		Title:    meta.title,
		Artist:   meta.artist,
	}, true
}

func ReplayMetadataStateDay(ctx context.Context, client DownloadClient, cacheDir string, day dump.DayFiles, state *metadataReplayState) error {
	for _, file := range day.OrderedFiles() {
		switch file.Type {
		case dump.FileTypeMeta, dump.FileTypeFingerprint:
			continue
		}

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
		}
	}
	return nil
}

func BackfillMetadataDay(ctx context.Context, session *metadataBackfillSession, client DownloadClient, cacheDir string, day dump.DayFiles, state *metadataReplayState, stats *metadataBackfillStats, decodeWorkers int) error {
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
			if !state.HasPendingFingerprints() {
				continue
			}
			if err := backfillMetadataFile(ctx, session, localPath, state, stats, decodeWorkers); err != nil {
				return err
			}
		case dump.FileTypeMeta:
			continue
		}
	}
	return nil
}

func backfillMetadataFile(ctx context.Context, session *metadataBackfillSession, path string, state *metadataReplayState, stats *metadataBackfillStats, decodeWorkers int) error {
	if decodeWorkers <= 0 {
		decodeWorkers = 1
	}

	rawLines := make(chan []byte, metadataRawLineBuffer)
	records := make(chan Record, metadataRecordBuffer)
	errCh := make(chan error, 1)

	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go func() {
		defer close(rawLines)
		if err := dump.ScanGzipLines(ctx, path, func(line []byte) error {
			select {
			case rawLines <- line:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			reportErr(err)
		}
	}()

	var decodeWG sync.WaitGroup
	for i := 0; i < decodeWorkers; i++ {
		decodeWG.Add(1)
		go func() {
			defer decodeWG.Done()
			for line := range rawLines {
				var payload fingerprintMetadataUpdate
				if err := json.Unmarshal(line, &payload); err != nil {
					reportErr(err)
					return
				}
				record, ok := state.ResolveFingerprintMetadata(payload.ID)
				if !ok {
					continue
				}
				record.Duration = payload.Length
				select {
				case records <- record:
					stats.processed.Add(1)
				case <-ctx.Done():
					reportErr(ctx.Err())
					return
				}
			}
		}()
	}

	go func() {
		decodeWG.Wait()
		close(records)
	}()

	candidates := make(map[string]Record)
	for record := range records {
		candidates[record.AcoustID] = mergeMetadataRecord(candidates[record.AcoustID], record)
	}

	select {
	case err := <-errCh:
		return err
	default:
	}

	if len(candidates) == 0 {
		return nil
	}

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

	for _, record := range candidates {
		if _, err := session.insertStmt.ExecContext(ctx,
			record.AcoustID,
			nullIfEmpty(record.MBID),
			nullIfEmpty(record.Title),
			nullIfEmpty(record.Artist),
			record.Duration,
		); err != nil {
			return err
		}
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

func mergeMetadataRecord(dst, src Record) Record {
	if dst.AcoustID == "" {
		return src
	}
	if dst.MBID == "" && src.MBID != "" {
		dst.MBID = src.MBID
	}
	if dst.Title == "" && src.Title != "" {
		dst.Title = src.Title
	}
	if dst.Artist == "" && src.Artist != "" {
		dst.Artist = src.Artist
	}
	if dst.Duration <= 0 && src.Duration > 0 {
		dst.Duration = src.Duration
	}
	return dst
}

func (s *compactAcoustIDSet) Contains(acoustid string) bool {
	if s == nil {
		return false
	}
	if id, ok := parseUUIDString(acoustid); ok {
		idx := sort.Search(len(s.uuids), func(i int) bool {
			return compareUUID(s.uuids[i], id) >= 0
		})
		return idx < len(s.uuids) && s.uuids[idx] == id
	}
	_, ok := s.extras[acoustid]
	return ok
}

func (s *compactAcoustIDSet) Len() int {
	if s == nil {
		return 0
	}
	return len(s.uuids) + len(s.extras)
}

func parseUUIDString(s string) ([16]byte, bool) {
	var out [16]byte
	if len(s) != 36 {
		return out, false
	}

	j := 0
	for i := 0; i < len(s); {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return out, false
			}
			i++
			continue
		}

		if i+1 >= len(s) || j >= len(out) {
			return out, false
		}
		hi := fromHex(s[i])
		lo := fromHex(s[i+1])
		if hi < 0 || lo < 0 {
			return out, false
		}
		out[j] = byte((hi << 4) | lo)
		j++
		i += 2
	}
	return out, j == len(out)
}

func compareUUID(a, b [16]byte) int {
	return bytes.Compare(a[:], b[:])
}

func fromHex(b byte) int {
	switch {
	case b >= '0' && b <= '9':
		return int(b - '0')
	case b >= 'a' && b <= 'f':
		return int(b-'a') + 10
	case b >= 'A' && b <= 'F':
		return int(b-'A') + 10
	default:
		return -1
	}
}
