package build

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func TestBackfillMetadataDayUpdatesExistingFingerprintRow(t *testing.T) {
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if err := ApplyFinalizePragmas(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO fingerprints (acoustid, duration) VALUES ('acoustid-1', 0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES (123, 1, 0)`); err != nil {
		t.Fatal(err)
	}

	cacheDir := filepath.Join(t.TempDir(), "cache")
	day := makeTestDayFiles(t, cacheDir, "2026-03-26", map[dump.FileType][]any{
		dump.FileTypeTrack: {
			dump.TrackUpdate{ID: 1, GID: "acoustid-1"},
		},
		dump.FileTypeTrackMeta: {
			dump.TrackMetaUpdate{TrackID: 1, Track: "Track One", Artist: "Artist One"},
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

	session, err := newMetadataBackfillSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	state := newMetadataReplayState(map[string]struct{}{"acoustid-1": {}})
	stats := &metadataBackfillStats{}
	if err := BackfillMetadataDay(ctx, session, staticDownloader{}, cacheDir, day, state, stats, 4); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	var mbid, title, artist string
	var duration int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(mb_id, ''), COALESCE(title, ''), COALESCE(artist, ''), COALESCE(duration, 0) FROM fingerprints WHERE acoustid = 'acoustid-1'`).Scan(&mbid, &title, &artist, &duration); err != nil {
		t.Fatal(err)
	}
	if mbid != "mbid-1" || title != "Track One" || artist != "Artist One" || duration != 30 {
		t.Fatalf("unexpected metadata backfill result: mbid=%q title=%q artist=%q duration=%d", mbid, title, artist, duration)
	}

	var subFingerprintCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sub_fingerprints`).Scan(&subFingerprintCount); err != nil {
		t.Fatal(err)
	}
	if subFingerprintCount != 1 {
		t.Fatalf("sub_fingerprint count = %d, want 1", subFingerprintCount)
	}
	if stats.updated.Load() != 1 {
		t.Fatalf("updated rows = %d, want 1", stats.updated.Load())
	}
}

func TestMetadataBackfillProgressRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "chromakopia.db")
	day := time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC)

	if err := saveMetadataBackfillProgress(dbPath, day); err != nil {
		t.Fatal(err)
	}

	progress, ok, err := loadMetadataBackfillProgress(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected metadata progress file")
	}
	if progress.LastCompletedDay != "2026-03-26" {
		t.Fatalf("last_completed_day = %q, want 2026-03-26", progress.LastCompletedDay)
	}
	if err := clearMetadataBackfillProgress(dbPath); err != nil {
		t.Fatal(err)
	}
}
