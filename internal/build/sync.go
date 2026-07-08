package build

import (
	"context"
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
	chroma "github.com/zephyraoss/libchroma"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

type SyncConfig struct {
	DatasetPrefix          string
	CacheDir               string
	BaseURL                string
	GoMaxProcs             int
	DecodeWorkers          int
	Compact                bool
	CompactionThresholdPct float64
	Serving                bool
	DownloadWorkers        int
	HTTPClient             *http.Client
	GracefulStop           <-chan struct{}

	mode syncMode
}

type syncMode int

const (
	syncModeFull syncMode = iota
	syncModeServing
	syncModeRefresh
)

func (m syncMode) String() string {
	switch m {
	case syncModeServing:
		return "serving"
	case syncModeRefresh:
		return "refresh"
	default:
		return "full"
	}
}

type syncSummary struct {
	ingestedDays       int
	newFingerprints    int64
	mappedNew          int64
	promoted           int64
	promotionsDeferred int64
	backfillsDeferred  int64
}

func syncDatasetPaths(prefix string) (ckd, cki, ckm string) {
	return prefix + ".ckd", prefix + ".cki", prefix + ".ckm"
}

func RunSync(ctx context.Context, cfg SyncConfig) error {
	if cfg.DatasetPrefix == "" {
		return errors.New("dataset prefix is required")
	}
	ckd, cki, ckm := syncDatasetPaths(cfg.DatasetPrefix)
	if cfg.mode != syncModeRefresh {
		switch {
		case cfg.Serving:
			cfg.mode = syncModeServing
		case !fileExists(ckd):
			if !fileExists(cki) && !fileExists(ckm) {
				return fmt.Errorf("no dataset files found at prefix %s", cfg.DatasetPrefix)
			}
			log.Printf("sync: %s not found; running in serving-node mode (.cki/.ckm only)", ckd)
			cfg.mode = syncModeServing
		}
	}
	required := []string{ckd, cki, ckm}
	if cfg.mode == syncModeServing {
		if cfg.Compact {
			return errors.New("--compact requires the .ckd and is not available in serving mode; run refresh instead")
		}
		required = []string{cki, ckm}
	}
	for _, path := range required {
		if !fileExists(path) {
			return fmt.Errorf("dataset file %s not found", path)
		}
	}
	if cfg.GoMaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.GoMaxProcs)
	}
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = runtime.GOMAXPROCS(0)
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
		cfg.CacheDir = filepath.Join(filepath.Dir(cfg.DatasetPrefix), ".chromaforge-cache")
	}
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return err
	}

	lastSynced, err := syncStartDay(cfg.DatasetPrefix, cfg.mode)
	if err != nil {
		return err
	}
	log.Printf("sync started dataset=%s mode=%s last_synced_day=%s cache_dir=%s", cfg.DatasetPrefix, cfg.mode, lastSynced, cfg.CacheDir)

	startDay, err := time.Parse("2006-01-02", lastSynced)
	if err != nil {
		return fmt.Errorf("parse last synced day %q: %w", lastSynced, err)
	}

	log.Printf("archive discovery started")
	days, err := dump.DiscoverArchive(ctx, cfg.HTTPClient, cfg.BaseURL, startDay.Year(), "")
	if err != nil {
		return err
	}
	log.Printf("archive discovery completed: %d days", len(days))

	_, err = runSyncDays(ctx, cfg, downloader{client: cfg.HTTPClient}, days, lastSynced)
	return err
}

func RunRefresh(ctx context.Context, cfg SyncConfig) error {
	cfg.mode = syncModeRefresh
	cfg.Serving = false
	cfg.Compact = true
	return RunSync(ctx, cfg)
}

func syncStartDay(datasetPrefix string, mode syncMode) (string, error) {
	progress, ok, err := loadSyncProgress(datasetPrefix)
	if err != nil {
		return "", err
	}
	if ok {
		return progress.LastSyncedDay, nil
	}
	if mode == syncModeServing {
		return "", fmt.Errorf("serving-mode sync needs %s deployed alongside the .cki/.ckm artifacts (refresh writes it); without a .ckd there is no source date to start from", syncProgressPath(datasetPrefix))
	}
	ds, err := chroma.OpenDataStore(datasetPrefix + ".ckd")
	if err != nil {
		return "", fmt.Errorf("open datastore for source date: %w", err)
	}
	defer ds.Close()
	if ds.Header.SourceDate == 0 {
		return "", errors.New("dataset has no source date and no sync progress file")
	}
	return time.Unix(int64(ds.Header.SourceDate), 0).UTC().Format("2006-01-02"), nil
}

func runSyncDays(ctx context.Context, cfg SyncConfig, client DownloadClient, days []dump.DayFiles, lastSyncedDay string) (syncSummary, error) {
	start := time.Now()
	var summary syncSummary
	if cfg.DecodeWorkers <= 0 {
		cfg.DecodeWorkers = runtime.GOMAXPROCS(0)
	}
	if cfg.CompactionThresholdPct <= 0 {
		cfg.CompactionThresholdPct = 10.0
	}
	serving := cfg.mode == syncModeServing
	ckd, cki, ckm := syncDatasetPaths(cfg.DatasetPrefix)

	progress, hasProgress, err := loadSyncProgress(cfg.DatasetPrefix)
	if err != nil {
		return summary, err
	}
	if !hasProgress {
		progress = syncProgress{LastSyncedDay: lastSyncedDay}
		if err := saveSyncProgress(cfg.DatasetPrefix, lastSyncedDay, ""); err != nil {
			return summary, err
		}
	}

	journal, hasJournal, err := loadSyncJournal(cfg.DatasetPrefix)
	if err != nil {
		return summary, err
	}
	if hasJournal && journal.generation != progress.Generation {
		if !serving {
			journal.Close()
			return summary, fmt.Errorf("sync journal generation %q does not match progress generation %q: the dataset files were replaced underneath the journal; remove %s if the current files are correct", journal.generation, progress.Generation, journal.path)
		}
		log.Printf("sync journal generation %q predates artifact generation %q (refresh deployed); discarding stale journal", journal.generation, progress.Generation)
		if err := journal.Close(); err != nil {
			return summary, err
		}
		if err := os.Remove(journal.path); err != nil {
			return summary, err
		}
		hasJournal = false
	}
	if hasJournal {
		if serving && journal.baseCKD != syncJournalNoCKD {
			journal.Close()
			return summary, fmt.Errorf("sync journal %s was created with a .ckd present; refusing to use it in serving mode", journal.path)
		}
		if !serving && journal.baseCKD == syncJournalNoCKD {
			journal.Close()
			return summary, fmt.Errorf("sync journal %s was created by a serving-mode sync (no .ckd); refusing to use it against a full dataset", journal.path)
		}
	}
	if !hasJournal {
		journal, err = createFreshSyncJournal(cfg, progress.Generation)
		if err != nil {
			return summary, err
		}
	}
	defer journal.Close()

	var pending []dump.DayFiles
	for _, day := range days {
		key := day.Day.Format("2006-01-02")
		if key <= lastSyncedDay {
			continue
		}
		if _, done := journal.days[key]; done {
			continue
		}
		pending = append(pending, day)
	}

	if len(pending) == 0 && !journal.dirty {
		log.Printf("sync up to date last_synced_day=%s", maxDayString(lastSyncedDay, journal.lastDay))
		return summary, finishSyncMaintenance(cfg, journal, maxDayString(lastSyncedDay, journal.lastDay))
	}

	targets := []struct {
		path string
		base int64
	}{{cki, journal.baseCKI}, {ckm, journal.baseCKM}}
	if !serving {
		targets = append(targets, struct {
			path string
			base int64
		}{ckd, journal.baseCKD})
	}
	for _, target := range targets {
		if err := chroma.TruncateOverflow(target.path, target.base); err != nil {
			return summary, fmt.Errorf("reset overflow of %s: %w", target.path, err)
		}
	}

	var ds *chroma.DataStore
	if !serving {
		ds, err = chroma.OpenDataStore(ckd)
		if err != nil {
			return summary, err
		}
	}
	dsClosed := false
	defer func() {
		if ds != nil && !dsClosed {
			_ = ds.Close()
		}
	}()

	state := newSyncState()
	if err := func() error {
		mm, err := chroma.OpenMetadataMap(ckm)
		if err != nil {
			return err
		}
		defer mm.Close()
		pi, err := chroma.OpenPostingIndex(cki)
		if err != nil {
			return err
		}
		defer pi.Close()
		if mm.Header.DatasetID != pi.Header.DatasetID {
			return fmt.Errorf("dataset id mismatch across %s files", cfg.DatasetPrefix)
		}
		if ds != nil && mm.Header.DatasetID != ds.Header.DatasetID {
			return fmt.Errorf("dataset id mismatch across %s files", cfg.DatasetPrefix)
		}
		log.Printf("sync state scan started mode=%s", cfg.mode)
		return state.scanDataset(mm)
	}(); err != nil {
		return summary, err
	}
	state.applyJournal(journal)
	log.Printf("sync state ready tracks=%d journal_days=%d journal_fingerprints=%d pending_days=%d",
		len(state.trackFPs), len(journal.days), len(journal.newFPs), len(pending))

	stats := &Stats{start: start}
	for _, day := range pending {
		log.Printf("syncing day %s", day.Day.Format("2006-01-02"))
		if err := ingestSyncDay(ctx, cfg, client, day, ds, state, journal, stats, &summary); err != nil {
			return summary, err
		}
		if err := journal.WriteDayDone(day.Day.Format("2006-01-02")); err != nil {
			return summary, err
		}
		summary.ingestedDays++
		if stopRequested(cfg.GracefulStop) {
			log.Printf("graceful stop requested, applying synced days through %s", day.Day.Format("2006-01-02"))
			break
		}
	}
	if err := validateBadRecordRate(stats); err != nil {
		return summary, err
	}

	content, err := buildSyncOverflow(ds, state, journal)
	if err != nil {
		return summary, err
	}
	if ds != nil {
		dsClosed = true
		if err := ds.Close(); err != nil {
			return summary, err
		}
	}
	if err := applySyncOverflow(cfg.DatasetPrefix, journal, content, serving); err != nil {
		return summary, err
	}

	if journal.lastDay > lastSyncedDay {
		if err := saveSyncProgress(cfg.DatasetPrefix, journal.lastDay, progress.Generation); err != nil {
			return summary, err
		}
	}

	summary.newFingerprints = stats.insertedFingerprints.Load()
	summary.mappedNew = content.mappedNew
	summary.promoted = content.promoted
	summary.promotionsDeferred = content.promotionsDeferred

	log.Printf("sync summary mode=%s days=%d new_fingerprints=%d mapped=%d promoted=%d backfilled_durations=%d skipped=%d overflow_fingerprints=%d elapsed=%s",
		cfg.mode,
		summary.ingestedDays,
		summary.newFingerprints,
		content.mappedNew,
		content.promoted,
		len(journal.backfills),
		stats.skipped.Load(),
		len(content.ckd),
		time.Since(start).Round(time.Second),
	)
	if serving {
		log.Printf("sync deferred to next refresh: promotions=%d (fingerprints in the canonical .ckd whose track gained an MBID) duration_backfills=%d", content.promotionsDeferred, summary.backfillsDeferred)
	}

	return summary, finishSyncMaintenance(cfg, journal, maxDayString(lastSyncedDay, journal.lastDay))
}

func createFreshSyncJournal(cfg SyncConfig, generation string) (*syncJournal, error) {
	ckd, cki, ckm := syncDatasetPaths(cfg.DatasetPrefix)
	serving := cfg.mode == syncModeServing

	hasOverflow := false
	if !serving {
		ds, err := chroma.OpenDataStore(ckd)
		if err != nil {
			return nil, fmt.Errorf("open datastore: %w (if a --compact run was interrupted, re-run with --compact)", err)
		}
		hasOverflow = ds.HasOvfl
		ds.Close()
	}
	if !hasOverflow {
		pi, err := chroma.OpenPostingIndex(cki)
		if err != nil {
			return nil, err
		}
		hasOverflow = pi.HasOvfl
		pi.Close()
	}
	if !hasOverflow {
		mm, err := chroma.OpenMetadataMap(ckm)
		if err != nil {
			return nil, err
		}
		hasOverflow = mm.HasOvfl
		mm.Close()
	}
	if hasOverflow {
		if serving {
			return nil, errors.New("dataset files carry overflow regions but no matching sync journal; redeploy the freshly compacted artifacts (with their progress file) from the last refresh")
		}
		return nil, errors.New("dataset has overflow regions but no sync journal; run sync --compact to fold them into the main files first")
	}

	baseCKD := syncJournalNoCKD
	if !serving {
		info, err := os.Stat(ckd)
		if err != nil {
			return nil, err
		}
		baseCKD = info.Size()
	}
	sizes := make([]int64, 2)
	for i, path := range []string{cki, ckm} {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		sizes[i] = info.Size()
	}
	return createSyncJournal(cfg.DatasetPrefix, baseCKD, sizes[0], sizes[1], generation)
}

func ingestSyncDay(ctx context.Context, cfg SyncConfig, client DownloadClient, day dump.DayFiles, ds *chroma.DataStore, state *syncState, journal *syncJournal, stats *Stats, summary *syncSummary) error {
	dayLinks := map[uint32]uint32{}

	for _, file := range day.OrderedFiles() {
		switch file.Type {
		case dump.FileTypeTrack, dump.FileTypeTrackMeta, dump.FileTypeMeta:
			continue
		}
		localPath := filepath.Join(cfg.CacheDir, day.Day.Format("2006-01"), file.Name)
		if err := client.Ensure(ctx, file, localPath); err != nil {
			return err
		}

		switch file.Type {
		case dump.FileTypeTrackMBID:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackMBIDUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				return applySyncTrackMBID(v, state, journal)
			}); err != nil {
				return err
			}
		case dump.FileTypeTrackFingerprint:
			if err := dump.ScanGzipLines(ctx, localPath, func(line []byte) error {
				var v dump.TrackFingerprintUpdate
				if err := dump.DecodeJSONLine(line, &v); err != nil {
					return err
				}
				if v.TrackID <= 0 || v.TrackID > math.MaxUint32 || v.FingerprintID <= 0 || v.FingerprintID > math.MaxUint32 {
					return nil
				}
				fp, track := uint32(v.FingerprintID), uint32(v.TrackID)
				if _, ok := state.fpTrack[fp]; ok {
					return nil
				}
				if _, ok := journal.orphanLinks[fp]; ok {
					return nil
				}
				if _, ok := dayLinks[fp]; !ok {
					dayLinks[fp] = track
				}
				return nil
			}); err != nil {
				return err
			}
		case dump.FileTypeFingerprint:
			if err := ingestSyncFingerprintFile(ctx, localPath, ds, state, journal, dayLinks, stats, summary, cfg.DecodeWorkers); err != nil {
				return err
			}
		}
	}

	for fp, track := range dayLinks {
		if _, ok := state.fpTrack[fp]; ok {
			continue
		}
		if err := journal.WriteOrphanLink(fp, track); err != nil {
			return err
		}
	}
	return nil
}

func applySyncTrackMBID(v dump.TrackMBIDUpdate, state *syncState, journal *syncJournal) error {
	if v.TrackID <= 0 || v.TrackID > math.MaxUint32 || v.MBID == "" || v.Disabled {
		return nil
	}
	track := uint32(v.TrackID)
	if _, ok := state.trackMBID[track]; ok {
		return nil
	}
	mbid, err := uuid.Parse(v.MBID)
	if err != nil || mbid == uuid.Nil {
		return nil
	}
	state.trackMBID[track] = mbid
	return journal.WriteTrackMBID(track, mbid)
}

func ingestSyncFingerprintFile(ctx context.Context, path string, ds *chroma.DataStore, state *syncState, journal *syncJournal, dayLinks map[uint32]uint32, stats *Stats, summary *syncSummary, decodeWorkers int) error {
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
				rec, ok := decodeFingerprintPayload(line)
				stats.processed.Add(1)
				if !ok {
					stats.skipped.Add(1)
					if stats.overBadRecordThreshold() {
						reportErr(fmt.Errorf("malformed record threshold exceeded"))
						return
					}
					continue
				}
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
		if err := ingestSyncFingerprint(rec, ds, state, journal, dayLinks, stats, summary); err != nil {
			return err
		}
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func ingestSyncFingerprint(rec ckafDecoded, ds *chroma.DataStore, state *syncState, journal *syncJournal, dayLinks map[uint32]uint32, stats *Stats, summary *syncSummary) error {
	if _, exists := state.fpTrack[rec.id]; exists {
		return fillSyncDuration(rec.id, rec.durationMs, ds, journal, summary)
	}

	track, ok := journal.orphanLinks[rec.id]
	if !ok {
		track, ok = dayLinks[rec.id]
	}
	if !ok {
		stats.skipped.Add(1)
		return nil
	}

	if existing := state.trackFPs[track]; len(existing) > 0 {
		return fillSyncDuration(existing[0], rec.durationMs, ds, journal, summary)
	}

	fp := &syncNewFP{
		id:         rec.id,
		durationMs: rec.durationMs,
		trackID:    track,
		mbid:       state.trackMBID[track],
		rawCount:   rec.rawCount,
		blob:       rec.blob,
	}
	if err := journal.WriteNewFP(fp); err != nil {
		return err
	}
	state.fpTrack[rec.id] = track
	state.trackFPs[track] = append(state.trackFPs[track], rec.id)
	stats.insertedFingerprints.Add(1)
	return nil
}

func fillSyncDuration(target, durationMs uint32, ds *chroma.DataStore, journal *syncJournal, summary *syncSummary) error {
	if durationMs == 0 {
		return nil
	}
	if fp, ok := journal.newFPByID[target]; ok {
		if fp.durationMs != 0 {
			return nil
		}
		return journal.WriteMerge(target, durationMs)
	}
	if ds == nil {
		summary.backfillsDeferred++
		return nil
	}
	if existing, ok := journal.backfills[target]; ok && existing != 0 {
		return nil
	}
	baseRec, err := ds.Lookup(target)
	if err != nil {
		log.Printf("sync warning: duration merge target %d not in datastore: %v", target, err)
		return nil
	}
	if baseRec.DurationMs != 0 {
		return nil
	}
	return journal.WriteBackfill(target, durationMs)
}

type syncOverflowContent struct {
	ckd                []chroma.OverflowRecord
	ckm                []chroma.OverflowMappingRecord
	cki                []chroma.OverflowRecord
	mappedNew          int64
	promoted           int64
	promotionsDeferred int64
}

func buildSyncOverflow(ds *chroma.DataStore, state *syncState, journal *syncJournal) (syncOverflowContent, error) {
	var content syncOverflowContent

	for _, fp := range journal.newFPs {
		values, err := chroma.DecompressFingerprint(fp.blob, int(fp.rawCount))
		if err != nil {
			return content, fmt.Errorf("sync journal fingerprint %d: %w", fp.id, err)
		}
		if ds != nil {
			content.ckd = append(content.ckd, chroma.OverflowRecord{
				FingerprintID: fp.id,
				DurationMs:    fp.durationMs,
				Values:        values,
			})
		}
		mbid := state.trackMBID[fp.trackID]
		content.ckm = append(content.ckm, chroma.OverflowMappingRecord{
			FingerprintID: fp.id,
			MBID:          mbid,
			TrackID:       fp.trackID,
		})
		if mbid != uuid.Nil {
			content.cki = append(content.cki, chroma.OverflowRecord{FingerprintID: fp.id, Values: values})
			content.mappedNew++
		}
	}

	for target, durationMs := range journal.backfills {
		rec, err := ds.Lookup(target)
		if err != nil {
			log.Printf("sync warning: dropping duration backfill for missing fingerprint %d: %v", target, err)
			continue
		}
		fp, err := ds.ReadFingerprint(rec)
		if err != nil {
			return content, fmt.Errorf("read backfill fingerprint %d: %w", target, err)
		}
		content.ckd = append(content.ckd, chroma.OverflowRecord{
			FingerprintID: target,
			DurationMs:    durationMs,
			Values:        fp.Values,
		})
	}

	for _, tm := range journal.trackMBIDs {
		for _, fpID := range state.trackFPs[tm.trackID] {
			if _, isJournalFP := journal.newFPByID[fpID]; isJournalFP {
				continue
			}
			if ds == nil {
				content.promotionsDeferred++
				continue
			}
			rec, err := ds.Lookup(fpID)
			if err != nil {
				return content, fmt.Errorf("promotion fingerprint %d: %w", fpID, err)
			}
			fp, err := ds.ReadFingerprint(rec)
			if err != nil {
				return content, fmt.Errorf("promotion fingerprint %d: %w", fpID, err)
			}
			content.ckm = append(content.ckm, chroma.OverflowMappingRecord{
				FingerprintID: fpID,
				MBID:          tm.mbid,
				TrackID:       tm.trackID,
			})
			content.cki = append(content.cki, chroma.OverflowRecord{FingerprintID: fpID, Values: fp.Values})
			content.promoted++
		}
	}

	return content, nil
}

func applySyncOverflow(prefix string, journal *syncJournal, content syncOverflowContent, serving bool) error {
	ckd, cki, ckm := syncDatasetPaths(prefix)

	if !serving && len(content.ckd) > 0 {
		if err := chroma.AppendDataStoreOverflow(ckd, content.ckd); err != nil {
			return fmt.Errorf("append datastore overflow: %w", err)
		}
	}
	if len(content.ckm) > 0 {
		if err := chroma.AppendMetadataOverflow(ckm, content.ckm); err != nil {
			return fmt.Errorf("append metadata overflow: %w", err)
		}
	}
	if len(content.cki) > 0 {
		if err := chroma.AppendPostingIndexOverflowValues(cki, content.cki); err != nil {
			return fmt.Errorf("append posting index overflow: %w", err)
		}
	}

	return journal.WriteApplied()
}

func finishSyncMaintenance(cfg SyncConfig, journal *syncJournal, lastSyncedDay string) error {
	if cfg.mode == syncModeServing {
		return nil
	}

	threshold := cfg.CompactionThresholdPct
	if threshold <= 0 {
		threshold = 10.0
	}

	ds, err := chroma.OpenDataStore(cfg.DatasetPrefix + ".ckd")
	if err != nil {
		return err
	}
	mainCount := ds.RecordCount()
	overflowCount := ds.OverflowCount
	hasOverflow := ds.HasOvfl
	ds.Close()

	needsCompaction := false
	switch {
	case cfg.mode == syncModeRefresh:
		needsCompaction = hasOverflow || journal.hasRecords()
	case mainCount == 0:
		needsCompaction = hasOverflow
	default:
		needsCompaction = float64(overflowCount)/float64(mainCount) >= threshold/100.0
	}
	if !needsCompaction {
		return nil
	}
	ratio := 100 * float64(overflowCount) / float64(maxUint64(mainCount, 1))
	if !cfg.Compact {
		log.Printf("sync warning: overflow holds %d records (%.1f%% of %d main records, threshold %.1f%%); run sync with --compact to fold them into the main files", overflowCount, ratio, mainCount, threshold)
		return nil
	}

	log.Printf("sync compaction started overflow=%d main=%d (%.1f%%)", overflowCount, mainCount, ratio)
	compactStart := time.Now()
	if err := journal.Close(); err != nil {
		return err
	}
	if err := os.Remove(syncJournalPath(cfg.DatasetPrefix)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := compactSyncDataset(cfg.DatasetPrefix); err != nil {
		return err
	}
	if err := saveSyncProgress(cfg.DatasetPrefix, lastSyncedDay, uuid.NewString()); err != nil {
		return err
	}
	log.Printf("sync compaction completed elapsed=%s", time.Since(compactStart).Round(time.Second))
	return nil
}

func compactSyncDataset(prefix string) error {
	ckd, cki, ckm := syncDatasetPaths(prefix)

	if err := chroma.CompactDataStore(ckd, ckd+".tmp"); err != nil {
		return fmt.Errorf("compacting datastore: %w", err)
	}
	if err := chroma.CompactMetadata(ckm, ckm+".tmp"); err != nil {
		return fmt.Errorf("compacting metadata: %w", err)
	}

	newDS, err := chroma.OpenDataStore(ckd + ".tmp")
	if err != nil {
		return fmt.Errorf("opening compacted datastore: %w", err)
	}
	err = rebuildMappedPostingIndex(cki, cki+".tmp", ckm+".tmp", newDS)
	if err == nil && fileExists(prefix+".ckx") {
		err = chroma.CompactSearchIndex(prefix+".ckx", prefix+".ckx.tmp", newDS)
	}
	newDS.Close()
	if err != nil {
		return err
	}

	for _, path := range []string{ckd, ckm, cki, prefix + ".ckx"} {
		if !fileExists(path + ".tmp") {
			continue
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			return fmt.Errorf("renaming compacted %s: %w", path, err)
		}
	}
	return nil
}

func rebuildMappedPostingIndex(srcCKI, dstCKI, ckmPath string, ds *chroma.DataStore) error {
	src, err := chroma.OpenPostingIndex(srcCKI)
	if err != nil {
		return fmt.Errorf("opening source posting index: %w", err)
	}
	tuning := src.Tuning
	datasetID := src.Header.DatasetID
	src.Close()

	mm, err := chroma.OpenMetadataMap(ckmPath)
	if err != nil {
		return fmt.Errorf("opening metadata for posting rebuild: %w", err)
	}
	defer mm.Close()

	builder, err := chroma.NewPostingIndexBuilder(dstCKI)
	if err != nil {
		return fmt.Errorf("creating posting index builder: %w", err)
	}
	builder.SetDatasetID(datasetID)
	builder.SetTuningConfig(tuning)

	stride := int(tuning.Stride)
	if err := mm.IterateMappings(func(rec *chroma.MappingRecord) error {
		if rec.MBID == uuid.Nil {
			return nil
		}
		dsRec, err := ds.Lookup(rec.FingerprintID)
		if err != nil {
			return fmt.Errorf("posting rebuild: fingerprint %d: %w", rec.FingerprintID, err)
		}
		fp, err := ds.ReadFingerprint(dsRec)
		if err != nil {
			return fmt.Errorf("posting rebuild: fingerprint %d: %w", rec.FingerprintID, err)
		}
		hashes, ordinals := sampleValuesForPostings(fp.Values, stride)
		return builder.Add(rec.FingerprintID, hashes, ordinals)
	}); err != nil {
		return err
	}

	return builder.Finish()
}

func sampleValuesForPostings(values []uint32, stride int) ([]uint32, []uint8) {
	if stride <= 0 {
		stride = ckiStride
	}
	n := (len(values) + stride - 1) / stride
	if n > ckiMaxOrdinal+1 {
		n = ckiMaxOrdinal + 1
	}
	hashes := make([]uint32, 0, n)
	ordinals := make([]uint8, 0, n)
	for i := 0; i < len(values) && i/stride <= ckiMaxOrdinal; i += stride {
		hashes = append(hashes, values[i])
		ordinals = append(ordinals, uint8(i/stride))
	}
	return hashes, ordinals
}

func maxDayString(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
