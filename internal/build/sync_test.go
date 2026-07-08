package build

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	chroma "github.com/zephyraoss/libchroma"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

const (
	testMBIDTrack4 = "44444444-4444-4444-4444-444444444444"
	testMBIDTrack6 = "66666666-6666-6666-6666-666666666666"
)

func makeSyncTestDays(t *testing.T, cacheDir string) []dump.DayFiles {
	t.Helper()
	days := makeCKAFTestDays(t, cacheDir)
	dayThree := makeTestDayFiles(t, cacheDir, "2026-03-28", map[dump.FileType][]any{
		dump.FileTypeTrack: {
			dump.TrackUpdate{ID: 4, GID: "gid-4"},
			dump.TrackUpdate{ID: 5, GID: "gid-5"},
		},
		dump.FileTypeTrackMBID: {
			dump.TrackMBIDUpdate{TrackID: 4, MBID: testMBIDTrack4},
		},
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 40, TrackID: 4},
			dump.TrackFingerprintUpdate{FingerprintID: 50, TrackID: 5},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 40, Length: 130, Fingerprint: testFingerprintValues(40, 160)},
			dump.FingerprintUpdate{ID: 50, Length: 70, Fingerprint: testFingerprintValues(50, 128)},
			dump.FingerprintUpdate{ID: 20, Length: 95, Fingerprint: testFingerprintValues(20, 128)},
		},
	})
	return append(days, dayThree)
}

func buildSyncBaseDataset(t *testing.T, cacheDir, prefix string, days []dump.DayFiles) {
	t.Helper()
	cfg := CKAFConfig{
		OutputPrefix:  prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
	}
	if err := runCKAFDays(context.Background(), cfg, staticDownloader{}, days[:1]); err != nil {
		t.Fatal(err)
	}
}

func queryFirstFP(t *testing.T, pi *chroma.PostingIndex, seed uint32, n int) (uint32, bool) {
	t.Helper()
	hits, err := pi.QueryFull(normalizedTestValues(t, seed, n), &chroma.PostingQueryOptions{MinHits: 3, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 {
		return 0, false
	}
	return hits[0].FingerprintID, true
}

func containsFP(t *testing.T, pi *chroma.PostingIndex, seed uint32, n int, fpID uint32) bool {
	t.Helper()
	hits, err := pi.QueryFull(normalizedTestValues(t, seed, n), &chroma.PostingQueryOptions{MinHits: 3, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range hits {
		if hit.FingerprintID == fpID {
			return true
		}
	}
	return false
}

func assertSyncedDataset(t *testing.T, prefix string) {
	t.Helper()

	ds, err := chroma.OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	mm, err := chroma.OpenMetadataMap(prefix + ".ckm")
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()
	pi, err := chroma.OpenPostingIndex(prefix + ".cki")
	if err != nil {
		t.Fatal(err)
	}
	defer pi.Close()

	rec10, err := ds.Lookup(10)
	if err != nil {
		t.Fatal(err)
	}
	if rec10.DurationMs != 120_000 {
		t.Fatalf("fp 10 duration = %d ms, want 120000 (backfilled from duplicate)", rec10.DurationMs)
	}
	if _, err := ds.Lookup(11); err == nil {
		t.Fatal("duplicate submission fp 11 must not be in the datastore")
	}

	rec40, err := ds.Lookup(40)
	if err != nil {
		t.Fatalf("synced fp 40 missing: %v", err)
	}
	if rec40.DurationMs != 130_000 {
		t.Fatalf("fp 40 duration = %d ms, want 130000", rec40.DurationMs)
	}
	fp40, err := ds.ReadFingerprint(rec40)
	if err != nil {
		t.Fatal(err)
	}
	want40 := normalizedTestValues(t, 40, 160)
	if len(fp40.Values) != len(want40) {
		t.Fatalf("fp 40 has %d values, want %d", len(fp40.Values), len(want40))
	}
	for i := range want40 {
		if fp40.Values[i] != want40[i] {
			t.Fatalf("fp 40 value %d = %d, want %d", i, fp40.Values[i], want40[i])
		}
	}
	if _, err := ds.Lookup(50); err != nil {
		t.Fatalf("synced fp 50 missing: %v", err)
	}

	map20, err := mm.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	if map20.MBID != uuid.MustParse(testMBIDTrack2) || map20.TrackID != 2 {
		t.Fatalf("fp 20 mapping = (%s, track %d), want (%s, track 2)", map20.MBID, map20.TrackID, testMBIDTrack2)
	}
	if got, ok := queryFirstFP(t, pi, 20, 128); !ok || got != 20 {
		t.Fatalf("full query for promoted fp 20 returned (%d, %t), want fp 20", got, ok)
	}

	map40, err := mm.Lookup(40)
	if err != nil {
		t.Fatal(err)
	}
	if map40.MBID != uuid.MustParse(testMBIDTrack4) || map40.TrackID != 4 {
		t.Fatalf("fp 40 mapping = (%s, track %d), want (%s, track 4)", map40.MBID, map40.TrackID, testMBIDTrack4)
	}
	if got, ok := queryFirstFP(t, pi, 40, 160); !ok || got != 40 {
		t.Fatalf("full query for synced fp 40 returned (%d, %t), want fp 40", got, ok)
	}

	map50, err := mm.Lookup(50)
	if err != nil {
		t.Fatal(err)
	}
	if map50.MBID != uuid.Nil || map50.TrackID != 5 {
		t.Fatalf("fp 50 mapping = (%s, track %d), want (nil MBID, track 5)", map50.MBID, map50.TrackID)
	}
	if containsFP(t, pi, 50, 128, 50) {
		t.Fatal("unmapped fp 50 must not be in the posting index")
	}

	map30, err := mm.Lookup(30)
	if err != nil {
		t.Fatal(err)
	}
	if map30.MBID != uuid.Nil {
		t.Fatalf("fp 30 must stay unmapped, got %s", map30.MBID)
	}
	if containsFP(t, pi, 30, 96, 30) {
		t.Fatal("fp 30 (disabled MBID mapping) must not be in the posting index")
	}
	if got, ok := queryFirstFP(t, pi, 10, 160); !ok || got != 10 {
		t.Fatalf("full query for fp 10 returned (%d, %t), want fp 10", got, ok)
	}
}

func datasetFileSizes(t *testing.T, prefix string) map[string]int64 {
	t.Helper()
	sizes := map[string]int64{}
	for _, suffix := range []string{".ckd", ".cki", ".ckm"} {
		info, err := os.Stat(prefix + suffix)
		if err != nil {
			t.Fatal(err)
		}
		sizes[suffix] = info.Size()
	}
	return sizes
}

func TestRunSyncDaysAppliesDeltas(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	cfg := SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	assertSyncedDataset(t, prefix)

	ds, err := chroma.OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	if got := ds.RecordCount(); got != 3 {
		t.Fatalf("main record count = %d, want 3", got)
	}
	if ds.OverflowCount != 3 {
		t.Fatalf("overflow count = %d, want 3 (fp 40, fp 50, fp 10 backfill)", ds.OverflowCount)
	}
	ds.Close()

	progress, ok, err := loadSyncProgress(prefix)
	if err != nil || !ok {
		t.Fatalf("sync progress missing after sync (err=%v)", err)
	}
	if progress.LastSyncedDay != "2026-03-28" {
		t.Fatalf("last_synced_day = %s, want 2026-03-28", progress.LastSyncedDay)
	}
	if !fileExists(syncJournalPath(prefix)) {
		t.Fatal("sync journal missing after sync")
	}
}

func TestRunSyncDaysIsIdempotent(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	cfg := SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}
	sizes := datasetFileSizes(t, prefix)

	progress, _, err := loadSyncProgress(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, progress.LastSyncedDay); err != nil {
		t.Fatal(err)
	}
	if again := datasetFileSizes(t, prefix); again[".ckd"] != sizes[".ckd"] || again[".cki"] != sizes[".cki"] || again[".ckm"] != sizes[".ckm"] {
		t.Fatalf("file sizes changed on idempotent re-run: %v -> %v", sizes, again)
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}
	assertSyncedDataset(t, prefix)

	ds, err := chroma.OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	if ds.OverflowCount != 3 {
		t.Fatalf("overflow count after re-runs = %d, want 3", ds.OverflowCount)
	}
	ds.Close()
}

func TestRunSyncDaysCompaction(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	cfg := SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
		Compact:       true,
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	ds, err := chroma.OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	if ds.HasOvfl {
		t.Fatal("expected no overflow after compaction")
	}
	if got := ds.RecordCount(); got != 5 {
		t.Fatalf("main record count after compaction = %d, want 5", got)
	}
	ds.Close()
	if fileExists(syncJournalPath(prefix)) {
		t.Fatal("sync journal must be removed by compaction")
	}

	assertSyncedDataset(t, prefix)

	dayFour := makeTestDayFiles(t, cacheDir, "2026-03-29", map[dump.FileType][]any{
		dump.FileTypeTrack: {
			dump.TrackUpdate{ID: 6, GID: "gid-6"},
		},
		dump.FileTypeTrackMBID: {
			dump.TrackMBIDUpdate{TrackID: 6, MBID: testMBIDTrack6},
		},
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 60, TrackID: 6},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 60, Length: 45, Fingerprint: testFingerprintValues(60, 96)},
		},
	})
	cfg.Compact = false
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, append(days, dayFour), "2026-03-28"); err != nil {
		t.Fatal(err)
	}

	ds, err = chroma.OpenDataStore(prefix + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if _, err := ds.Lookup(60); err != nil {
		t.Fatalf("fp 60 missing after post-compaction sync: %v", err)
	}
	mm, err := chroma.OpenMetadataMap(prefix + ".ckm")
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()
	map60, err := mm.Lookup(60)
	if err != nil {
		t.Fatal(err)
	}
	if map60.MBID != uuid.MustParse(testMBIDTrack6) || map60.TrackID != 6 {
		t.Fatalf("fp 60 mapping = (%s, track %d), want (%s, track 6)", map60.MBID, map60.TrackID, testMBIDTrack6)
	}
	pi, err := chroma.OpenPostingIndex(prefix + ".cki")
	if err != nil {
		t.Fatal(err)
	}
	defer pi.Close()
	if got, ok := queryFirstFP(t, pi, 60, 96); !ok || got != 60 {
		t.Fatalf("full query for fp 60 returned (%d, %t), want fp 60", got, ok)
	}
}

func TestRunSyncDaysRefusesOverflowWithoutJournal(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	cfg := SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(syncJournalPath(prefix)); err != nil {
		t.Fatal(err)
	}
	_, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26")
	if err == nil {
		t.Fatal("sync must refuse a dataset with overflow but no journal")
	}
}

func TestRunSyncDaysGracefulStop(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	stopCh := make(chan struct{})
	close(stopCh)
	cfg := SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
		GracefulStop:  stopCh,
	}
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	progress, ok, err := loadSyncProgress(prefix)
	if err != nil || !ok {
		t.Fatalf("sync progress missing after graceful stop (err=%v)", err)
	}
	if progress.LastSyncedDay != "2026-03-27" {
		t.Fatalf("last_synced_day = %s, want 2026-03-27", progress.LastSyncedDay)
	}

	mm, err := chroma.OpenMetadataMap(prefix + ".ckm")
	if err != nil {
		t.Fatal(err)
	}
	map20, err := mm.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	mm.Close()
	if map20.MBID != uuid.MustParse(testMBIDTrack2) {
		t.Fatalf("fp 20 not promoted after graceful stop, mbid=%s", map20.MBID)
	}

	cfg.GracefulStop = nil
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, progress.LastSyncedDay); err != nil {
		t.Fatal(err)
	}
	assertSyncedDataset(t, prefix)
}
