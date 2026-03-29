package build

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
	"github.com/zephyraoss/chromaforge/internal/validate"
)

func TestReplayStateFirstSeenWins(t *testing.T) {
	state := NewReplayState()
	state.ApplyTrack(dump.TrackUpdate{ID: 10, GID: "gid-1"})
	state.ApplyTrack(dump.TrackUpdate{ID: 10, GID: "gid-2"})
	state.ApplyTrackMBID(dump.TrackMBIDUpdate{TrackID: 10, MBID: "mbid-1"})
	state.ApplyTrackMBID(dump.TrackMBIDUpdate{TrackID: 10, MBID: "mbid-2"})
	state.ApplyTrackFingerprint(dump.TrackFingerprintUpdate{FingerprintID: 99, TrackID: 10})
	state.ApplyTrackFingerprint(dump.TrackFingerprintUpdate{FingerprintID: 99, TrackID: 11})

	acoustID, mbid, ok := state.ResolveFingerprint(99)
	if !ok || acoustID != "gid-1" || mbid != "mbid-1" {
		t.Fatalf("unexpected resolution: ok=%t acoustid=%q mbid=%q", ok, acoustID, mbid)
	}
}

func TestReplayStateFillsMissingMBIDFromLaterUpdates(t *testing.T) {
	state := NewReplayState()
	state.ApplyTrack(dump.TrackUpdate{ID: 10, GID: "gid-1"})
	state.ApplyTrackMBID(dump.TrackMBIDUpdate{TrackID: 10, MBID: "mbid-1"})
	state.ApplyTrackFingerprint(dump.TrackFingerprintUpdate{FingerprintID: 99, TrackID: 10})

	acoustID, mbid, ok := state.ResolveFingerprint(99)
	if !ok || acoustID != "gid-1" || mbid != "mbid-1" {
		t.Fatalf("unexpected resolution: ok=%t acoustid=%q mbid=%q", ok, acoustID, mbid)
	}
}

func TestValidateBadRecordRate(t *testing.T) {
	stats := &Stats{start: time.Now()}
	stats.processed.Store(1000)
	stats.skipped.Store(0)
	if err := validateBadRecordRate(stats); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats.skipped.Store(1)
	if err := validateBadRecordRate(stats); err != nil {
		t.Fatalf("unexpected error at low skip rate: %v", err)
	}

	stats.skipped.Store(10001)
	if err := validateBadRecordRate(stats); err == nil {
		t.Fatal("expected threshold error")
	}
}

func TestReplayDayAndValidate(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chromakopia.db")
	db, err := libsqlutil.OpenLocal(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := ApplyBuildPragmas(ctx, db, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}

	cacheDir := filepath.Join(dir, "cache")
	files := makeTestArchiveFiles(t, cacheDir)
	day := dump.DayFiles{
		Day:   mustDay("2026-03-26"),
		Files: files,
	}
	cfg := Config{
		Workers:       2,
		DecodeWorkers: 2,
		BatchSize:     2,
		CacheDir:      cacheDir,
	}
	state := NewReplayState()
	stats := &Stats{start: time.Now()}
	writer, err := newWriteSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	if err := ReplayDay(ctx, writer, staticDownloader{}, cfg, day, state, stats, txModeBeginExclusive, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateHashIndex); err != nil {
		t.Fatal(err)
	}
	if err := ApplyFinalizePragmas(ctx, db); err != nil {
		t.Fatal(err)
	}

	result, err := validate.RunDB(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatalf("unexpected validation result: %#v", result)
	}
}

func TestReplayDayResumeDedupeUsesTempSeenTable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chromakopia.db")
	db, err := libsqlutil.OpenLocal(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if err := ApplyBuildPragmas(ctx, db, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}

	cacheDir := filepath.Join(dir, "cache")
	dayOne := makeTestDayFiles(t, cacheDir, "2026-03-26", map[dump.FileType][]any{
		dump.FileTypeTrack: {
			dump.TrackUpdate{ID: 1, GID: "acoustid-1"},
		},
		dump.FileTypeTrackMBID: {
			dump.TrackMBIDUpdate{TrackID: 1, MBID: "mbid-1"},
		},
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 10, TrackID: 1},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 10, Length: 30, Fingerprint: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		},
	})
	dayTwo := makeTestDayFiles(t, cacheDir, "2026-03-27", map[dump.FileType][]any{
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 11, TrackID: 1},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 11, Length: 31, Fingerprint: []int64{9, 8, 7, 6, 5, 4, 3, 2, 1}},
		},
	})
	cfg := Config{
		Workers:       2,
		DecodeWorkers: 2,
		BatchSize:     2,
		CacheDir:      cacheDir,
	}
	state := NewReplayState()
	stats := &Stats{start: time.Now()}

	writer, err := newWriteSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReplayDay(ctx, writer, staticDownloader{}, cfg, dayOne, state, stats, txModeBeginExclusive, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	writer, err = newWriteSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReplayDay(ctx, writer, staticDownloader{}, cfg, dayTwo, state, stats, txModeBeginExclusive, 0); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	var fingerprintCount int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fingerprints`).Scan(&fingerprintCount); err != nil {
		t.Fatal(err)
	}
	if fingerprintCount != 1 {
		t.Fatalf("fingerprint count = %d, want 1", fingerprintCount)
	}
}

type staticDownloader struct{}

func (staticDownloader) Ensure(ctx context.Context, file dump.ArchiveFile, dst string) error {
	return nil
}
