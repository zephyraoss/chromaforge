package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	chroma "github.com/zephyraoss/libchroma/v2"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

const (
	ckiStride       = 8
	ckiQBits        = 2
	ckiSkipInterval = 64
	ckiMaxOrdinal   = 255
)

type CKAFConfig struct {
	OutputPrefix        string
	SpillDir            string
	CacheDir            string
	BaseURL             string
	GoMaxProcs          int
	DecodeWorkers       int
	AssemblyConcurrency int
	StartYear           int
	EndDate             string
	DownloadWorkers     int
	HTTPClient          *http.Client
	GracefulStop        <-chan struct{}
}

func (s *ReplayState) FingerprintTrack(id int64) (trackID int64, mbid string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	trackID, ok = s.fingerprintTrack[id]
	if !ok {
		return 0, "", false
	}
	return trackID, s.trackMBID[trackID], true
}

type ckafDedup struct {
	seenAcoustIDs  map[string]uint32
	pendingZeroDur map[uint32]struct{}
}

func newCKAFDedup() *ckafDedup {
	return &ckafDedup{
		seenAcoustIDs:  map[string]uint32{},
		pendingZeroDur: map[uint32]struct{}{},
	}
}

func RunCKAF(ctx context.Context, cfg CKAFConfig) error {
	if cfg.OutputPrefix == "" {
		return errors.New("output prefix is required")
	}
	if cfg.GoMaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.GoMaxProcs)
	}
	effectiveGoMaxProcs := runtime.GOMAXPROCS(0)
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = effectiveGoMaxProcs
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://data.acoustid.org"
	}
	if cfg.DownloadWorkers <= 0 {
		cfg.DownloadWorkers = 4
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = defaultHTTPClient(cfg.DownloadWorkers)
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(filepath.Dir(cfg.OutputPrefix), ".chromaforge-cache")
	}

	log.Printf("ckaf build started output=%s cache_dir=%s gomaxprocs=%d decode_workers=%d", cfg.OutputPrefix, cfg.CacheDir, effectiveGoMaxProcs, cfg.DecodeWorkers)

	if err := os.MkdirAll(filepath.Dir(cfg.OutputPrefix), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return err
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

	return runCKAFDays(ctx, cfg, downloader{client: cfg.HTTPClient}, days)
}

func runCKAFDays(ctx context.Context, cfg CKAFConfig, client DownloadClient, days []dump.DayFiles) error {
	start := time.Now()
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = runtime.GOMAXPROCS(0)
	}
	if cfg.AssemblyConcurrency <= 0 {
		cfg.AssemblyConcurrency = runtime.GOMAXPROCS(0)
	}
	if cfg.DownloadWorkers <= 0 {
		cfg.DownloadWorkers = 4
	}
	if cfg.SpillDir == "" {
		cfg.SpillDir = filepath.Dir(cfg.OutputPrefix)
	}
	if err := os.MkdirAll(cfg.SpillDir, 0o755); err != nil {
		return err
	}
	spoolPath := ckafSpoolPath(cfg.OutputPrefix)

	progress, hasProgress, err := loadCKAFProgress(cfg.OutputPrefix)
	if err != nil {
		return err
	}
	resumeFromDay := ""
	if hasProgress && fileExists(spoolPath) {
		resumeFromDay = progress.LastCompletedDay
		log.Printf("ckaf resume detected last_completed_day=%s spool_bytes=%d", resumeFromDay, progress.SpoolBytes)
	} else {
		if hasProgress {
			log.Printf("ckaf resume progress found without spool, starting fresh")
			if err := clearCKAFProgress(cfg.OutputPrefix); err != nil {
				return err
			}
		}
		if err := os.Remove(spoolPath); err == nil {
			log.Printf("removed stale ckaf spool: %s", spoolPath)
		}
	}

	startIdx, hasResume, err := findReplayStartIndex(days, resumeFromDay)
	if err != nil {
		return err
	}

	state := NewReplayState()
	dedup := newCKAFDedup()
	stats := &Stats{start: start}
	stateCfg := Config{CacheDir: cfg.CacheDir}

	var spool *ckafSpoolWriter
	if hasResume {
		for i := 0; i < startIdx; i++ {
			day := days[i]
			log.Printf("ckaf resume state rebuild day=%s", day.Day.Format("2006-01-02"))
			if err := ReplayStateDay(ctx, client, stateCfg, day, state); err != nil {
				return err
			}
		}
		spool, err = openCKAFSpoolForAppend(spoolPath, progress.SpoolBytes)
		if err != nil {
			return err
		}
		var spooled int64
		if err := scanCKAFSpool(spoolPath, false, func(rec ckafSpoolRecord) error {
			switch rec.Kind {
			case ckafSpoolKindFingerprint:
				spooled++
				if acoustID, _, ok := state.ResolveFingerprint(int64(rec.ID)); ok {
					dedup.seenAcoustIDs[acoustID] = rec.ID
				}
				if rec.DurationMs == 0 {
					dedup.pendingZeroDur[rec.ID] = struct{}{}
				}
			case ckafSpoolKindMerge:
				delete(dedup.pendingZeroDur, rec.ID)
			}
			return nil
		}); err != nil {
			_ = spool.Close()
			return err
		}
		stats.insertedFingerprints.Store(spooled)
		log.Printf("ckaf resume ready spooled_fingerprints=%d remaining_days=%d", spooled, len(days)-startIdx)
	} else {
		spool, err = createCKAFSpool(spoolPath)
		if err != nil {
			return err
		}
	}
	defer spool.Close()

	remainingDays := days[startIdx:]
	tracker := newProgressTracker(totalReplayableBytes(remainingDays), time.Now())

	if len(remainingDays) > 0 {
		prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
		pdl := newPrefetchDownloader(prefetchCtx, client, cfg.DownloadWorkers)
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
			if err := replayCKAFDay(ctx, spool, dedup, pdl, cfg, stateCfg, day, state, stats, tracker); err != nil {
				return err
			}
			if err := spool.Sync(); err != nil {
				return err
			}
			if err := saveCKAFProgress(cfg.OutputPrefix, day.Day, spool.Offset()); err != nil {
				return err
			}
			if stopRequested(cfg.GracefulStop) {
				log.Printf("graceful stop completed at day=%s rerun the same build-ckaf command to resume", day.Day.Format("2006-01-02"))
				return nil
			}
		}

		if err := validateBadRecordRate(stats); err != nil {
			return err
		}
	} else {
		log.Printf("ckaf resume skipping replay: no remaining archive days")
	}

	if err := spool.Close(); err != nil {
		return err
	}

	summary, err := assembleCKAF(ctx, cfg.OutputPrefix, cfg.SpillDir, cfg.AssemblyConcurrency, state, spoolPath, days[len(days)-1].Day)
	if err != nil {
		return err
	}

	if err := os.Remove(spoolPath); err != nil {
		log.Printf("ckaf spool cleanup warning: %v", err)
	}
	if err := clearCKAFProgress(cfg.OutputPrefix); err != nil {
		return err
	}

	log.Printf("ckaf build summary days=%d fingerprints=%d mbid_mapped=%d postings=%d skipped=%d oversized=%d invalid_mbids=%d ckd_bytes=%d cki_bytes=%d ckm_bytes=%d bytes_per_posting=%.2f elapsed=%s",
		len(days),
		summary.fingerprints,
		summary.mapped,
		summary.postings,
		stats.skipped.Load(),
		summary.oversized,
		summary.invalidMBIDs,
		summary.ckdBytes,
		summary.ckiBytes,
		summary.ckmBytes,
		summary.bytesPerPosting(),
		time.Since(start).Round(time.Second),
	)
	return nil
}

func replayCKAFDay(ctx context.Context, spool *ckafSpoolWriter, dedup *ckafDedup, client DownloadClient, cfg CKAFConfig, stateCfg Config, day dump.DayFiles, state *ReplayState, stats *Stats, tracker *progressTracker) error {
	if err := ReplayStateDay(ctx, client, stateCfg, day, state); err != nil {
		return err
	}
	for _, ft := range replayedFileTypes {
		if ft == dump.FileTypeFingerprint {
			continue
		}
		if file, ok := day.Files[ft]; ok {
			tracker.FileCompleted(file.Size)
		}
	}
	file, ok := day.Files[dump.FileTypeFingerprint]
	if !ok {
		return nil
	}
	localPath := filepath.Join(cfg.CacheDir, day.Day.Format("2006-01"), file.Name)
	if err := client.Ensure(ctx, file, localPath); err != nil {
		return err
	}
	if err := spoolCKAFFingerprintFile(ctx, spool, dedup, localPath, state, stats, cfg.DecodeWorkers, tracker); err != nil {
		return err
	}
	tracker.FileCompleted(file.Size)
	return nil
}

type ckafDecoded struct {
	id         uint32
	acoustID   string
	durationMs uint32
	rawCount   uint16
	blob       []byte
}

func spoolCKAFFingerprintFile(ctx context.Context, spool *ckafSpoolWriter, dedup *ckafDedup, path string, state *ReplayState, stats *Stats, decodeWorkers int, tracker *progressTracker) error {
	rawLines := make(chan []byte, rawLineBuffer)
	records := make(chan ckafDecoded, rawLineBuffer)
	errCh := make(chan error, 1)

	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	go func() {
		defer close(rawLines)
		if err := dump.ScanGzipLinesCounted(ctx, path, tracker.CurrentFileBytes(), func(line []byte) error {
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
				rec, ok := decodeCKAFFingerprintLine(line, state)
				stats.processed.Add(1)
				if !ok {
					stats.skipped.Add(1)
					stats.maybeLogProgress(tracker)
					if stats.overBadRecordThreshold() {
						reportErr(fmt.Errorf("malformed record threshold exceeded"))
						return
					}
					continue
				}
				stats.maybeLogProgress(tracker)
				select {
				case records <- rec:
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

	for rec := range records {
		existingID, seen := dedup.seenAcoustIDs[rec.acoustID]
		if seen {
			if rec.durationMs > 0 {
				if _, pending := dedup.pendingZeroDur[existingID]; pending {
					if err := spool.WriteMerge(existingID, rec.durationMs); err != nil {
						return err
					}
					delete(dedup.pendingZeroDur, existingID)
				}
			}
			continue
		}
		dedup.seenAcoustIDs[rec.acoustID] = rec.id
		if rec.durationMs == 0 {
			dedup.pendingZeroDur[rec.id] = struct{}{}
		}
		if err := spool.WriteFingerprint(rec.id, rec.durationMs, rec.rawCount, rec.blob); err != nil {
			return err
		}
		stats.insertedFingerprints.Add(1)
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func decodeFingerprintPayload(line []byte) (ckafDecoded, bool) {
	var payload dump.FingerprintUpdate
	if err := json.Unmarshal(line, &payload); err != nil {
		return ckafDecoded{}, false
	}
	if payload.ID <= 0 || payload.ID > math.MaxUint32 {
		return ckafDecoded{}, false
	}
	if len(payload.Fingerprint) == 0 || len(payload.Fingerprint) > 0xFFFF {
		return ckafDecoded{}, false
	}
	values, err := dump.NormalizeFingerprint(payload.Fingerprint)
	if err != nil {
		return ckafDecoded{}, false
	}
	var durationMs uint32
	if payload.Length > 0 && payload.Length <= math.MaxUint32/1000 {
		durationMs = uint32(payload.Length) * 1000
	}
	return ckafDecoded{
		id:         uint32(payload.ID),
		durationMs: durationMs,
		rawCount:   uint16(len(values)),
		blob:       chroma.CompressFingerprint(values),
	}, true
}

func decodeCKAFFingerprintLine(line []byte, state *ReplayState) (ckafDecoded, bool) {
	rec, ok := decodeFingerprintPayload(line)
	if !ok {
		return ckafDecoded{}, false
	}
	acoustID, _, ok := state.ResolveFingerprint(int64(rec.id))
	if !ok {
		return ckafDecoded{}, false
	}
	rec.acoustID = acoustID
	return rec, true
}

type ckafSummary struct {
	fingerprints int64
	mapped       int64
	postings     int64
	oversized    int64
	invalidMBIDs int64
	ckdBytes     int64
	ckiBytes     int64
	ckmBytes     int64
}

func (s ckafSummary) bytesPerPosting() float64 {
	if s.postings == 0 {
		return 0
	}
	return float64(s.ckiBytes) / float64(s.postings)
}

func assembleCKAF(ctx context.Context, outputPrefix, spillDir string, concurrency int, state *ReplayState, spoolPath string, sourceDate time.Time) (ckafSummary, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	log.Printf("ckaf assembly started spill_dir=%s concurrency=%d", spillDir, concurrency)
	assemblyStart := time.Now()
	stageStart := time.Now()

	mergedDurations := map[uint32]uint32{}
	if err := scanCKAFSpool(spoolPath, false, func(rec ckafSpoolRecord) error {
		if rec.Kind == ckafSpoolKindMerge {
			mergedDurations[rec.ID] = rec.DurationMs
		}
		return nil
	}); err != nil {
		return ckafSummary{}, err
	}
	log.Printf("ckaf assembly stage=merged-durations merges=%d elapsed=%s", len(mergedDurations), time.Since(stageStart).Round(time.Second))

	spillOpts := chroma.BuilderOptions{SpillDir: spillDir, Concurrency: concurrency, Logf: log.Printf}
	ds, err := chroma.NewDataStoreBuilderWithOptions(outputPrefix+".ckd", chroma.CompressPFOR, spillOpts)
	if err != nil {
		return ckafSummary{}, err
	}
	pi, err := chroma.NewPostingIndexBuilderWithOptions(outputPrefix+".cki", spillOpts)
	if err != nil {
		return ckafSummary{}, err
	}
	mm, err := chroma.NewMetadataMapBuilder(outputPrefix+".ckm", false)
	if err != nil {
		return ckafSummary{}, err
	}

	datasetID := uuid.New()
	ds.SetDatasetID(datasetID)
	pi.SetDatasetID(datasetID)
	mm.SetDatasetID(datasetID)
	ds.SetSourceDate(uint64(sourceDate.UTC().Unix()))
	pi.SetTuningConfig(chroma.TuningConfig{Stride: ckiStride, QBits: ckiQBits, SkipInterval: ckiSkipInterval})

	var summary ckafSummary
	stageStart = time.Now()
	apply := func(res ckafAssembled) error {
		if summary.fingerprints%1_000_000 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if res.fatalErr != nil {
			return res.fatalErr
		}
		addErr := res.compressErr
		if addErr == nil {
			addErr = ds.AddPrecompressed(res.id, res.durationMs, res.compressed, res.rawCount)
		}
		if addErr != nil {
			log.Printf("ckaf assembly skipping oversized fingerprint %d: %v", res.id, addErr)
			summary.oversized++
			return nil
		}
		summary.fingerprints++
		if res.invalidMBID {
			summary.invalidMBIDs++
		}
		if err := mm.Add(res.id, res.mbid, res.trackID, nil); err != nil {
			return err
		}
		if res.mbid == uuid.Nil {
			return nil
		}
		if err := pi.Add(res.id, res.hashes, res.ordinals); err != nil {
			return err
		}
		summary.mapped++
		summary.postings += int64(len(res.hashes))
		return nil
	}
	if err := runCKAFAssemblyPipeline(spoolPath, concurrency, state, mergedDurations, apply); err != nil {
		return ckafSummary{}, err
	}
	log.Printf("ckaf assembly stage=spool-pass fingerprints=%d postings=%d elapsed=%s", summary.fingerprints, summary.postings, time.Since(stageStart).Round(time.Second))

	stageStart = time.Now()
	if err := ds.Finish(); err != nil {
		return ckafSummary{}, fmt.Errorf("finish .ckd: %w", err)
	}
	log.Printf("ckaf assembly stage=finish-ckd elapsed=%s", time.Since(stageStart).Round(time.Second))

	stageStart = time.Now()
	if err := pi.Finish(); err != nil {
		return ckafSummary{}, fmt.Errorf("finish .cki: %w", err)
	}
	log.Printf("ckaf assembly stage=finish-cki elapsed=%s", time.Since(stageStart).Round(time.Second))

	stageStart = time.Now()
	if err := mm.Finish(); err != nil {
		return ckafSummary{}, fmt.Errorf("finish .ckm: %w", err)
	}
	log.Printf("ckaf assembly stage=finish-ckm elapsed=%s", time.Since(stageStart).Round(time.Second))

	for suffix, dst := range map[string]*int64{".ckd": &summary.ckdBytes, ".cki": &summary.ckiBytes, ".ckm": &summary.ckmBytes} {
		info, err := os.Stat(outputPrefix + suffix)
		if err != nil {
			return ckafSummary{}, err
		}
		*dst = info.Size()
	}

	log.Printf("ckaf assembly completed dataset_id=%s elapsed=%s", datasetID, time.Since(assemblyStart).Round(time.Second))
	return summary, nil
}

type ckafAssembled struct {
	id          uint32
	rawCount    uint16
	durationMs  uint32
	compressed  []byte
	compressErr error
	fatalErr    error
	trackID     uint32
	mbid        uuid.UUID
	invalidMBID bool
	hashes      []uint32
	ordinals    []uint8
}

func assembleCKAFRecord(rec ckafSpoolRecord, state *ReplayState, mergedDurations map[uint32]uint32) ckafAssembled {
	res := ckafAssembled{id: rec.ID, rawCount: rec.RawCount, durationMs: rec.DurationMs}
	values, err := chroma.DecompressFingerprint(rec.Blob, int(rec.RawCount))
	if err != nil {
		res.fatalErr = fmt.Errorf("ckaf spool fingerprint %d: %w", rec.ID, err)
		return res
	}
	if merged, ok := mergedDurations[rec.ID]; ok {
		res.durationMs = merged
	}
	res.compressed, res.compressErr = chroma.CompressFingerprintPFOR(values)

	trackID, mbid, ok := state.FingerprintTrack(int64(rec.ID))
	if ok && trackID > 0 && trackID <= math.MaxUint32 {
		res.trackID = uint32(trackID)
	}
	if mbid != "" {
		parsed, err := uuid.Parse(mbid)
		if err != nil {
			res.invalidMBID = true
		} else {
			res.mbid = parsed
		}
	}
	if res.mbid != uuid.Nil && res.compressErr == nil {
		res.hashes, res.ordinals = sampleForPostingIndex(values)
	}
	return res
}

var errCKAFAssemblyAborted = errors.New("ckaf assembly aborted")

func runCKAFAssemblyPipeline(spoolPath string, workers int, state *ReplayState, mergedDurations map[uint32]uint32, apply func(ckafAssembled) error) error {
	if workers < 1 {
		workers = 1
	}
	type job struct {
		rec ckafSpoolRecord
		out chan ckafAssembled
	}
	jobs := make(chan job)
	pending := make(chan chan ckafAssembled, workers*64)
	abort := make(chan struct{})
	var abortOnce sync.Once
	stop := func() { abortOnce.Do(func() { close(abort) }) }

	var scanErr error
	go func() {
		defer close(pending)
		defer close(jobs)
		scanErr = scanCKAFSpool(spoolPath, true, func(rec ckafSpoolRecord) error {
			if rec.Kind != ckafSpoolKindFingerprint {
				return nil
			}
			out := make(chan ckafAssembled, 1)
			select {
			case jobs <- job{rec: rec, out: out}:
			case <-abort:
				return errCKAFAssemblyAborted
			}
			select {
			case pending <- out:
			case <-abort:
				return errCKAFAssemblyAborted
			}
			return nil
		})
	}()

	for i := 0; i < workers; i++ {
		go func() {
			for j := range jobs {
				j.out <- assembleCKAFRecord(j.rec, state, mergedDurations)
			}
		}()
	}

	var applyErr error
	for out := range pending {
		res := <-out
		if applyErr != nil {
			continue
		}
		if err := apply(res); err != nil {
			applyErr = err
			stop()
		}
	}
	if applyErr != nil {
		return applyErr
	}
	return scanErr
}

func sampleForPostingIndex(values []uint32) ([]uint32, []uint8) {
	n := (len(values) + ckiStride - 1) / ckiStride
	if n > ckiMaxOrdinal+1 {
		n = ckiMaxOrdinal + 1
	}
	hashes := make([]uint32, 0, n)
	ordinals := make([]uint8, 0, n)
	for i := 0; i < len(values) && i/ckiStride <= ckiMaxOrdinal; i += ckiStride {
		hashes = append(hashes, values[i])
		ordinals = append(ordinals, uint8(i/ckiStride))
	}
	return hashes, ordinals
}
