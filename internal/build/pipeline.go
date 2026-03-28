package build

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

const (
	maxBadRecords     = 10000
	maxBadRecordPct   = 0.001
	progressEvery     = 1_000_000
	rawLineBuffer     = 10000
	recordBatchBuffer = 50
)

type ReplayState struct {
	mu               sync.RWMutex
	trackGID         map[int64]string
	trackMeta        map[int64]trackMeta
	trackMBID        map[int64]string
	fingerprintTrack map[int64]int64
}

type trackMeta struct {
	title  string
	artist string
}

func NewReplayState() *ReplayState {
	return &ReplayState{
		trackGID:         map[int64]string{},
		trackMeta:        map[int64]trackMeta{},
		trackMBID:        map[int64]string{},
		fingerprintTrack: map[int64]int64{},
	}
}

func (s *ReplayState) ApplyTrack(v dump.TrackUpdate) {
	if v.GID == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.trackGID[v.ID]; !ok {
		s.trackGID[v.ID] = v.GID
	}
	s.mu.Unlock()
}

func (s *ReplayState) ApplyTrackMeta(v dump.TrackMetaUpdate) {
	if v.TrackID == 0 || (v.Track == "" && v.Artist == "") {
		return
	}
	s.mu.Lock()
	if _, ok := s.trackMeta[v.TrackID]; !ok {
		s.trackMeta[v.TrackID] = trackMeta{title: v.Track, artist: v.Artist}
	}
	s.mu.Unlock()
}

func (s *ReplayState) ApplyTrackMBID(v dump.TrackMBIDUpdate) {
	if v.TrackID == 0 || v.MBID == "" || v.Disabled {
		return
	}
	s.mu.Lock()
	if _, ok := s.trackMBID[v.TrackID]; !ok {
		s.trackMBID[v.TrackID] = v.MBID
	}
	s.mu.Unlock()
}

func (s *ReplayState) ApplyTrackFingerprint(v dump.TrackFingerprintUpdate) {
	if v.TrackID == 0 || v.FingerprintID == 0 {
		return
	}
	s.mu.Lock()
	if _, ok := s.fingerprintTrack[v.FingerprintID]; !ok {
		s.fingerprintTrack[v.FingerprintID] = v.TrackID
	}
	s.mu.Unlock()
}

func (s *ReplayState) ResolveFingerprint(id int64) (acoustID, mbid, title, artist string, ok bool) {
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

type Stats struct {
	start                time.Time
	processed            atomic.Int64
	skipped              atomic.Int64
	insertedFingerprints atomic.Int64
	insertedSubFPs       atomic.Int64
}

func (s *Stats) maybeLogProgress(totalEstimate int64) {
	processed := s.processed.Load()
	if processed == 0 || processed%progressEvery != 0 {
		return
	}
	elapsed := time.Since(s.start)
	rate := float64(processed) / elapsed.Seconds()
	remaining := "unknown"
	if totalEstimate > 0 && processed < totalEstimate && rate > 0 {
		eta := time.Duration(float64(totalEstimate-processed)/rate) * time.Second
		remaining = eta.String()
	}
	log.Printf("processed=%d skipped=%d elapsed=%s rate=%.0f records/sec eta=%s", processed, s.skipped.Load(), elapsed.Round(time.Second), rate, remaining)
}

func (s *Stats) overBadRecordThreshold() bool {
	processed := s.processed.Load()
	skipped := s.skipped.Load()
	if skipped > maxBadRecords {
		return true
	}
	if processed == 0 {
		return false
	}
	return float64(skipped)/float64(processed) > maxBadRecordPct
}

func ReplayDay(ctx context.Context, writer *writeSession, client DownloadClient, cfg Config, day dump.DayFiles, state *ReplayState, stats *Stats, mode txMode, totalEstimate int64) error {
	for _, file := range day.OrderedFiles() {
		localPath := filepath.Join(cfg.CacheDir, day.Day.Format("2006-01"), file.Name)
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
			if err := replayFingerprintFile(ctx, writer, localPath, state, stats, cfg, mode, totalEstimate); err != nil {
				return err
			}
		case dump.FileTypeMeta:
			continue
		}
	}
	return nil
}

func ReplayStateDay(ctx context.Context, client DownloadClient, cfg Config, day dump.DayFiles, state *ReplayState) error {
	for _, file := range day.OrderedFiles() {
		switch file.Type {
		case dump.FileTypeMeta, dump.FileTypeFingerprint:
			continue
		}

		localPath := filepath.Join(cfg.CacheDir, day.Day.Format("2006-01"), file.Name)
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

type DownloadClient interface {
	Ensure(ctx context.Context, file dump.ArchiveFile, dst string) error
}

func replayFingerprintFile(ctx context.Context, writer *writeSession, path string, state *ReplayState, stats *Stats, cfg Config, mode txMode, totalEstimate int64) error {
	rawLines := make(chan []byte, rawLineBuffer)
	records := make(chan Record, cfg.BatchSize*recordBatchBuffer)
	batches := make(chan []Record, recordBatchBuffer)
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
	for i := 0; i < cfg.DecodeWorkers; i++ {
		decodeWG.Add(1)
		go func() {
			defer decodeWG.Done()
			for line := range rawLines {
				var payload dump.FingerprintUpdate
				if err := json.Unmarshal(line, &payload); err != nil {
					stats.skipped.Add(1)
					stats.processed.Add(1)
					stats.maybeLogProgress(totalEstimate)
					if stats.overBadRecordThreshold() {
						reportErr(fmt.Errorf("malformed record threshold exceeded"))
						return
					}
					continue
				}
				fp, err := dump.NormalizeFingerprint(payload.Fingerprint)
				if err != nil {
					stats.skipped.Add(1)
					stats.processed.Add(1)
					stats.maybeLogProgress(totalEstimate)
					if stats.overBadRecordThreshold() {
						reportErr(fmt.Errorf("malformed record threshold exceeded"))
						return
					}
					continue
				}
				acoustID, mbid, title, artist, ok := state.ResolveFingerprint(payload.ID)
				if !ok {
					stats.skipped.Add(1)
					stats.processed.Add(1)
					stats.maybeLogProgress(totalEstimate)
					if stats.overBadRecordThreshold() {
						reportErr(fmt.Errorf("malformed record threshold exceeded"))
						return
					}
					continue
				}
				rec := Record{
					AcoustID: acoustID,
					MBID:     mbid,
					Title:    title,
					Artist:   artist,
					Duration: payload.Length,
					SubFPs:   convertSubFPs(dump.ExtractSubFingerprints(fp)),
				}
				select {
				case records <- rec:
				case <-ctx.Done():
					reportErr(ctx.Err())
					return
				}
				stats.processed.Add(1)
				stats.maybeLogProgress(totalEstimate)
			}
		}()
	}

	go func() {
		decodeWG.Wait()
		close(records)
	}()

	go func() {
		defer close(batches)
		batch := make([]Record, 0, cfg.BatchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			out := append([]Record(nil), batch...)
			select {
			case batches <- out:
			case <-ctx.Done():
				reportErr(ctx.Err())
			}
			batch = batch[:0]
		}
		for rec := range records {
			batch = append(batch, rec)
			if len(batch) >= cfg.BatchSize {
				flush()
			}
		}
		flush()
	}()

	var insertWG sync.WaitGroup
	for i := 0; i < 1; i++ {
		insertWG.Add(1)
		go func() {
			defer insertWG.Done()
			batchWriter, err := writer.NewBatchWriter(ctx, mode)
			if err != nil {
				reportErr(err)
				return
			}
			defer func() {
				if err := batchWriter.Close(); err != nil {
					reportErr(err)
				}
			}()

			for batch := range batches {
				insertedFPs, insertedSubs, err := batchWriter.InsertBatch(ctx, batch)
				if err != nil {
					reportErr(err)
					return
				}
				stats.insertedFingerprints.Add(insertedFPs)
				stats.insertedSubFPs.Add(insertedSubs)
			}
			if err := batchWriter.Commit(ctx); err != nil {
				reportErr(err)
				return
			}
		}()
	}

	insertWG.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func convertSubFPs(in []dump.SubFingerprint) []SubFP {
	out := make([]SubFP, len(in))
	for i, sub := range in {
		out[i] = SubFP{Hash: sub.Hash, Position: sub.Position}
	}
	return out
}

func validateBadRecordRate(stats *Stats) error {
	if !stats.overBadRecordThreshold() {
		return nil
	}
	return errors.New("malformed record threshold exceeded")
}
