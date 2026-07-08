package build

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/google/uuid"
	chroma "github.com/zephyraoss/libchroma"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

func copyDatasetFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

func deployServingArtifacts(t *testing.T, srcPrefix, dstPrefix string) {
	t.Helper()
	for _, suffix := range []string{".cki", ".ckm", ".sync-progress.json"} {
		copyDatasetFile(t, srcPrefix+suffix, dstPrefix+suffix)
	}
}

func servingSyncConfig(prefix, cacheDir string) SyncConfig {
	return SyncConfig{
		DatasetPrefix: prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
		mode:          syncModeServing,
	}
}

func TestRunSyncDaysServingMode(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	servingPrefix := t.TempDir() + "/serving"
	deployServingArtifactsWithProgress(t, prefix, servingPrefix, "2026-03-26", "gen-initial")

	cfg := servingSyncConfig(servingPrefix, cacheDir)
	summary, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26")
	if err != nil {
		t.Fatal(err)
	}

	if fileExists(servingPrefix + ".ckd") {
		t.Fatal("serving-mode sync must not create a .ckd")
	}
	if summary.newFingerprints != 2 {
		t.Fatalf("new fingerprints = %d, want 2 (fp 40, fp 50)", summary.newFingerprints)
	}
	if summary.mappedNew != 1 {
		t.Fatalf("mapped new fingerprints = %d, want 1 (fp 40)", summary.mappedNew)
	}
	if summary.promoted != 0 {
		t.Fatalf("promoted = %d, want 0 in serving mode", summary.promoted)
	}
	if summary.promotionsDeferred != 1 {
		t.Fatalf("promotions deferred = %d, want 1 (fp 20 lives only in the canonical .ckd)", summary.promotionsDeferred)
	}
	if summary.backfillsDeferred != 2 {
		t.Fatalf("backfills deferred = %d, want 2", summary.backfillsDeferred)
	}

	mm, err := chroma.OpenMetadataMap(servingPrefix + ".ckm")
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()
	pi, err := chroma.OpenPostingIndex(servingPrefix + ".cki")
	if err != nil {
		t.Fatal(err)
	}
	defer pi.Close()

	map40, err := mm.Lookup(40)
	if err != nil {
		t.Fatalf("synced fp 40 mapping missing: %v", err)
	}
	if map40.MBID != uuid.MustParse(testMBIDTrack4) || map40.TrackID != 4 {
		t.Fatalf("fp 40 mapping = (%s, track %d), want (%s, track 4)", map40.MBID, map40.TrackID, testMBIDTrack4)
	}
	if got, ok := queryFirstFP(t, pi, 40, 160); !ok || got != 40 {
		t.Fatalf("full query for synced fp 40 returned (%d, %t), want fp 40", got, ok)
	}

	map50, err := mm.Lookup(50)
	if err != nil {
		t.Fatalf("synced fp 50 mapping missing: %v", err)
	}
	if map50.MBID != uuid.Nil || map50.TrackID != 5 {
		t.Fatalf("fp 50 mapping = (%s, track %d), want (nil MBID, track 5)", map50.MBID, map50.TrackID)
	}
	if containsFP(t, pi, 50, 128, 50) {
		t.Fatal("unmapped fp 50 must not be in the posting index")
	}

	map20, err := mm.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	if map20.MBID != uuid.Nil {
		t.Fatalf("fp 20 must stay unmapped on the serving node until refresh, got %s", map20.MBID)
	}
	if containsFP(t, pi, 20, 128, 20) {
		t.Fatal("deferred fp 20 must not be in the serving posting index")
	}

	if got, ok := queryFirstFP(t, pi, 10, 160); !ok || got != 10 {
		t.Fatalf("full query for fp 10 returned (%d, %t), want fp 10", got, ok)
	}

	progress, ok, err := loadSyncProgress(servingPrefix)
	if err != nil || !ok {
		t.Fatalf("serving progress missing after sync (err=%v)", err)
	}
	if progress.LastSyncedDay != "2026-03-28" {
		t.Fatalf("last_synced_day = %s, want 2026-03-28", progress.LastSyncedDay)
	}
	if progress.Generation != "gen-initial" {
		t.Fatalf("serving sync must keep the artifact generation, got %q", progress.Generation)
	}
}

func deployServingArtifactsWithProgress(t *testing.T, srcPrefix, dstPrefix, lastSyncedDay, generation string) {
	t.Helper()
	for _, suffix := range []string{".cki", ".ckm"} {
		copyDatasetFile(t, srcPrefix+suffix, dstPrefix+suffix)
	}
	if err := saveSyncProgress(dstPrefix, lastSyncedDay, generation); err != nil {
		t.Fatal(err)
	}
}

func TestRunSyncDaysServingModeIsIdempotent(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := t.TempDir() + "/dataset"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, prefix, days)

	servingPrefix := t.TempDir() + "/serving"
	deployServingArtifactsWithProgress(t, prefix, servingPrefix, "2026-03-26", "gen-initial")

	cfg := servingSyncConfig(servingPrefix, cacheDir)
	if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	sizes := map[string]int64{}
	for _, suffix := range []string{".cki", ".ckm"} {
		info, err := os.Stat(servingPrefix + suffix)
		if err != nil {
			t.Fatal(err)
		}
		sizes[suffix] = info.Size()
	}

	for _, lastSynced := range []string{"2026-03-28", "2026-03-26"} {
		if _, err := runSyncDays(context.Background(), cfg, staticDownloader{}, days, lastSynced); err != nil {
			t.Fatal(err)
		}
	}
	for _, suffix := range []string{".cki", ".ckm"} {
		info, err := os.Stat(servingPrefix + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != sizes[suffix] {
			t.Fatalf("%s size changed on idempotent re-run: %d -> %d", suffix, sizes[suffix], info.Size())
		}
	}
}

func TestSyncStartDayServingRequiresProgress(t *testing.T) {
	prefix := t.TempDir() + "/serving"
	if _, err := syncStartDay(prefix, syncModeServing); err == nil {
		t.Fatal("serving mode without a progress file must fail (no .ckd to derive a start day from)")
	}
}

func TestRefreshRoundTrip(t *testing.T) {
	cacheDir := t.TempDir()
	canonical := t.TempDir() + "/canonical"
	days := makeSyncTestDays(t, cacheDir)
	buildSyncBaseDataset(t, cacheDir, canonical, days)

	refreshCfg := SyncConfig{
		DatasetPrefix: canonical,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
		Compact:       true,
		mode:          syncModeRefresh,
	}
	if _, err := runSyncDays(context.Background(), refreshCfg, staticDownloader{}, days[:1], "2026-03-26"); err != nil {
		t.Fatal(err)
	}

	servingPrefix := t.TempDir() + "/serving"
	deployServingArtifacts(t, canonical, servingPrefix)

	servingCfg := servingSyncConfig(servingPrefix, cacheDir)
	summary, err := runSyncDays(context.Background(), servingCfg, staticDownloader{}, days, "2026-03-26")
	if err != nil {
		t.Fatal(err)
	}
	if summary.promotionsDeferred != 1 {
		t.Fatalf("promotions deferred on serving node = %d, want 1", summary.promotionsDeferred)
	}

	summary, err = runSyncDays(context.Background(), refreshCfg, staticDownloader{}, days, "2026-03-26")
	if err != nil {
		t.Fatal(err)
	}
	if summary.promoted != 1 {
		t.Fatalf("refresh promoted = %d, want 1 (fp 20 deferred by the serving node)", summary.promoted)
	}
	if summary.promotionsDeferred != 0 {
		t.Fatalf("refresh must not defer promotions, got %d", summary.promotionsDeferred)
	}
	assertSyncedDataset(t, canonical)
	ds, err := chroma.OpenDataStore(canonical + ".ckd")
	if err != nil {
		t.Fatal(err)
	}
	hasOverflow := ds.HasOvfl
	ds.Close()
	if hasOverflow {
		t.Fatal("refresh must compact the overflow into the main files")
	}
	if fileExists(syncJournalPath(canonical)) {
		t.Fatal("refresh compaction must remove the canonical journal")
	}
	refreshedProgress, ok, err := loadSyncProgress(canonical)
	if err != nil || !ok {
		t.Fatalf("refresh progress missing (err=%v)", err)
	}
	if refreshedProgress.LastSyncedDay != "2026-03-28" {
		t.Fatalf("refresh last_synced_day = %s, want 2026-03-28", refreshedProgress.LastSyncedDay)
	}
	if refreshedProgress.Generation == "" {
		t.Fatal("refresh compaction must rotate the artifact generation")
	}

	deployServingArtifacts(t, canonical, servingPrefix)
	if !fileExists(syncJournalPath(servingPrefix)) {
		t.Fatal("test setup: stale serving journal should still exist after deployment")
	}

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
	allDays := append(days, dayFour)
	summary, err = runSyncDays(context.Background(), servingCfg, staticDownloader{}, allDays, refreshedProgress.LastSyncedDay)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ingestedDays != 1 {
		t.Fatalf("serving sync after redeploy ingested %d days, want 1 (only day four)", summary.ingestedDays)
	}

	journal, hasJournal, err := loadSyncJournal(servingPrefix)
	if err != nil || !hasJournal {
		t.Fatalf("serving journal missing after redeploy sync (err=%v)", err)
	}
	journalGen := journal.generation
	journal.Close()
	if journalGen != refreshedProgress.Generation {
		t.Fatalf("serving journal generation = %q, want refreshed generation %q", journalGen, refreshedProgress.Generation)
	}

	mm, err := chroma.OpenMetadataMap(servingPrefix + ".ckm")
	if err != nil {
		t.Fatal(err)
	}
	defer mm.Close()
	pi, err := chroma.OpenPostingIndex(servingPrefix + ".cki")
	if err != nil {
		t.Fatal(err)
	}
	defer pi.Close()

	map20, err := mm.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	if map20.MBID != uuid.MustParse(testMBIDTrack2) || map20.TrackID != 2 {
		t.Fatalf("fp 20 mapping after refresh deploy = (%s, track %d), want (%s, track 2)", map20.MBID, map20.TrackID, testMBIDTrack2)
	}
	if got, ok := queryFirstFP(t, pi, 20, 128); !ok || got != 20 {
		t.Fatalf("full query for promoted fp 20 returned (%d, %t), want fp 20", got, ok)
	}

	map60, err := mm.Lookup(60)
	if err != nil {
		t.Fatalf("fp 60 mapping missing after redeploy sync: %v", err)
	}
	if map60.MBID != uuid.MustParse(testMBIDTrack6) || map60.TrackID != 6 {
		t.Fatalf("fp 60 mapping = (%s, track %d), want (%s, track 6)", map60.MBID, map60.TrackID, testMBIDTrack6)
	}
	if got, ok := queryFirstFP(t, pi, 60, 96); !ok || got != 60 {
		t.Fatalf("full query for fp 60 returned (%d, %t), want fp 60", got, ok)
	}
	if got, ok := queryFirstFP(t, pi, 40, 160); !ok || got != 40 {
		t.Fatalf("full query for fp 40 returned (%d, %t), want fp 40", got, ok)
	}
}
