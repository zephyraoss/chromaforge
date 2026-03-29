package build

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func TestFindReplayStartIndexSkipsReplayWhenResumeReachedEnd(t *testing.T) {
	days := []dump.DayFiles{
		{Day: mustDay("2026-03-26")},
		{Day: mustDay("2026-03-27")},
		{Day: mustDay("2026-03-28")},
	}

	startIdx, hasResume, err := findReplayStartIndex(days, "2026-03-28")
	if err != nil {
		t.Fatal(err)
	}
	if !hasResume {
		t.Fatal("expected resume marker")
	}
	if startIdx != len(days) {
		t.Fatalf("startIdx = %d, want %d", startIdx, len(days))
	}
}

func TestFindReplayStartIndexReturnsFirstRemainingDay(t *testing.T) {
	days := []dump.DayFiles{
		{Day: mustDay("2026-03-26")},
		{Day: mustDay("2026-03-27")},
		{Day: mustDay("2026-03-28")},
	}

	startIdx, hasResume, err := findReplayStartIndex(days, "2026-03-27")
	if err != nil {
		t.Fatal(err)
	}
	if !hasResume {
		t.Fatal("expected resume marker")
	}
	if startIdx != 2 {
		t.Fatalf("startIdx = %d, want 2", startIdx)
	}
}

func TestConfigureProcessTempDirSetsEnvironment(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sqlite-temp")

	for _, key := range []string{"SQLITE_TMPDIR", "TMPDIR", "TMP", "TEMP"} {
		t.Setenv(key, "")
	}

	if err := configureProcessTempDir(dir); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"SQLITE_TMPDIR", "TMPDIR", "TMP", "TEMP"} {
		if got := os.Getenv(key); got != dir {
			t.Fatalf("%s = %q, want %q", key, got, dir)
		}
	}
}

func TestIndexStageStatusDetectsCompletedIndexesAndStats(t *testing.T) {
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO fingerprints (acoustid, mb_id, title, artist, duration) VALUES ('acoustid-1', 'mbid-1', 'title-1', 'artist-1', 30)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES (123, 1, 0), (123, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateHashIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.AnalyzeSQL); err != nil {
		t.Fatal(err)
	}

	indexReady, analyzeReady, err := indexStageStatus(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !indexReady {
		t.Fatal("expected completed indexes")
	}
	if !analyzeReady {
		t.Fatal("expected sqlite_stat1 entries")
	}
}
