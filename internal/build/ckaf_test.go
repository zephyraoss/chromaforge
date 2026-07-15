package build

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	chroma "github.com/zephyraoss/libchroma/v2"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

const (
	testMBIDTrack1   = "11111111-2222-3333-4444-555555555555"
	testMBIDTrack2   = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	testMBIDDisabled = "99999999-8888-7777-6666-555555555555"
)

func testFingerprintValues(seed uint32, n int) []int64 {
	out := make([]int64, n)
	x := seed
	for i := range out {
		x = x*1664525 + 1013904223
		out[i] = int64(int32(x))
	}
	return out
}

func normalizedTestValues(t *testing.T, seed uint32, n int) []uint32 {
	t.Helper()
	values, err := dump.NormalizeFingerprint(testFingerprintValues(seed, n))
	if err != nil {
		t.Fatal(err)
	}
	return values
}

func makeCKAFTestDays(t *testing.T, cacheDir string) []dump.DayFiles {
	t.Helper()
	dayOne := makeTestDayFiles(t, cacheDir, "2026-03-26", map[dump.FileType][]any{
		dump.FileTypeTrack: {
			dump.TrackUpdate{ID: 1, GID: "gid-1"},
			dump.TrackUpdate{ID: 2, GID: "gid-2"},
			dump.TrackUpdate{ID: 3, GID: "gid-3"},
		},
		dump.FileTypeTrackMBID: {
			dump.TrackMBIDUpdate{TrackID: 1, MBID: testMBIDTrack1},
			dump.TrackMBIDUpdate{TrackID: 3, MBID: testMBIDDisabled, Disabled: true},
		},
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 10, TrackID: 1},
			dump.TrackFingerprintUpdate{FingerprintID: 20, TrackID: 2},
			dump.TrackFingerprintUpdate{FingerprintID: 30, TrackID: 3},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 10, Length: 0, Fingerprint: testFingerprintValues(10, 160)},
			dump.FingerprintUpdate{ID: 20, Length: 95, Fingerprint: testFingerprintValues(20, 128)},
			dump.FingerprintUpdate{ID: 30, Length: 60, Fingerprint: testFingerprintValues(30, 96)},
		},
	})
	dayTwo := makeTestDayFiles(t, cacheDir, "2026-03-27", map[dump.FileType][]any{
		dump.FileTypeTrackMBID: {
			dump.TrackMBIDUpdate{TrackID: 2, MBID: testMBIDTrack2},
		},
		dump.FileTypeTrackFingerprint: {
			dump.TrackFingerprintUpdate{FingerprintID: 11, TrackID: 1},
		},
		dump.FileTypeFingerprint: {
			dump.FingerprintUpdate{ID: 10, Length: 0, Fingerprint: testFingerprintValues(10, 160)},
			dump.FingerprintUpdate{ID: 11, Length: 120, Fingerprint: testFingerprintValues(11, 160)},
		},
	})
	return []dump.DayFiles{dayOne, dayTwo}
}

func assertCKAFDataset(t *testing.T, prefix string) {
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

	if ds.Header.DatasetID == uuid.Nil {
		t.Fatal("datastore has zero dataset id")
	}
	if mm.Header.DatasetID != ds.Header.DatasetID {
		t.Fatalf("metadata dataset id %s != datastore %s", mm.Header.DatasetID, ds.Header.DatasetID)
	}
	if pi.Header.DatasetID != ds.Header.DatasetID {
		t.Fatalf("posting index dataset id %s != datastore %s", pi.Header.DatasetID, ds.Header.DatasetID)
	}

	if got := ds.RecordCount(); got != 3 {
		t.Fatalf("datastore record count = %d, want 3", got)
	}
	if _, err := ds.Lookup(11); err == nil {
		t.Fatal("duplicate submission fp 11 must not be in the datastore")
	}

	rec10, err := ds.Lookup(10)
	if err != nil {
		t.Fatal(err)
	}
	if rec10.DurationMs != 120_000 {
		t.Fatalf("fp 10 duration = %d ms, want 120000 (merged from duplicate)", rec10.DurationMs)
	}
	fp10, err := ds.ReadFingerprint(rec10)
	if err != nil {
		t.Fatal(err)
	}
	want10 := normalizedTestValues(t, 10, 160)
	if len(fp10.Values) != len(want10) {
		t.Fatalf("fp 10 has %d values, want %d", len(fp10.Values), len(want10))
	}
	for i := range want10 {
		if fp10.Values[i] != want10[i] {
			t.Fatalf("fp 10 value %d = %d, want %d", i, fp10.Values[i], want10[i])
		}
	}
	rec20, err := ds.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	if rec20.DurationMs != 95_000 {
		t.Fatalf("fp 20 duration = %d ms, want 95000", rec20.DurationMs)
	}

	map10, err := mm.Lookup(10)
	if err != nil {
		t.Fatal(err)
	}
	if map10.MBID != uuid.MustParse(testMBIDTrack1) || map10.TrackID != 1 {
		t.Fatalf("fp 10 mapping = (%s, track %d), want (%s, track 1)", map10.MBID, map10.TrackID, testMBIDTrack1)
	}
	map20, err := mm.Lookup(20)
	if err != nil {
		t.Fatal(err)
	}
	if map20.MBID != uuid.MustParse(testMBIDTrack2) || map20.TrackID != 2 {
		t.Fatalf("fp 20 mapping = (%s, track %d), want (%s, track 2)", map20.MBID, map20.TrackID, testMBIDTrack2)
	}
	map30, err := mm.Lookup(30)
	if err != nil {
		t.Fatal(err)
	}
	if map30.MBID != uuid.Nil || map30.TrackID != 3 {
		t.Fatalf("fp 30 mapping = (%s, track %d), want (nil MBID, track 3)", map30.MBID, map30.TrackID)
	}

	queryOpts := &chroma.PostingQueryOptions{MinHits: 3, TopK: 10}
	hits10, err := pi.QueryFull(want10, queryOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits10) == 0 || hits10[0].FingerprintID != 10 {
		t.Fatalf("full query for fp 10 values returned %v, want fp 10 first", hits10)
	}
	hits20, err := pi.QueryFull(normalizedTestValues(t, 20, 128), queryOpts)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits20) == 0 || hits20[0].FingerprintID != 20 {
		t.Fatalf("full query for fp 20 values returned %v, want fp 20 first", hits20)
	}
	hits30, err := pi.QueryFull(normalizedTestValues(t, 30, 96), queryOpts)
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range hits30 {
		if hit.FingerprintID == 30 {
			t.Fatal("fp 30 (disabled MBID mapping) must not be in the posting index")
		}
	}
}

func TestRunCKAFDaysBuildsDataset(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := filepath.Join(t.TempDir(), "dataset")
	days := makeCKAFTestDays(t, cacheDir)

	cfg := CKAFConfig{
		OutputPrefix:  prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
	}
	if err := runCKAFDays(context.Background(), cfg, staticDownloader{}, days); err != nil {
		t.Fatal(err)
	}

	assertCKAFDataset(t, prefix)

	if fileExists(ckafSpoolPath(prefix)) {
		t.Fatal("spool file should be removed after a successful build")
	}
	if fileExists(ckafProgressPath(prefix)) {
		t.Fatal("progress file should be removed after a successful build")
	}
}

func readCKAFMaskedFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 0x38 {
		t.Fatalf("file %s too small for header: %d bytes", path, len(data))
	}
	for i := 0x18; i < 0x20; i++ {
		data[i] = 0
	}
	for i := 0x28; i < 0x38; i++ {
		data[i] = 0
	}
	return data
}

func TestRunCKAFDaysAssemblyConcurrencyEquivalence(t *testing.T) {
	cacheDir := t.TempDir()
	days := makeCKAFTestDays(t, cacheDir)

	buildWith := func(concurrency int) string {
		t.Helper()
		prefix := filepath.Join(t.TempDir(), "dataset")
		cfg := CKAFConfig{
			OutputPrefix:        prefix,
			CacheDir:            cacheDir,
			DecodeWorkers:       2,
			AssemblyConcurrency: concurrency,
		}
		if err := runCKAFDays(context.Background(), cfg, staticDownloader{}, days); err != nil {
			t.Fatal(err)
		}
		return prefix
	}

	serialPrefix := buildWith(1)
	parallelPrefix := buildWith(8)

	for _, suffix := range []string{".ckd", ".cki", ".ckm"} {
		serial := readCKAFMaskedFile(t, serialPrefix+suffix)
		parallel := readCKAFMaskedFile(t, parallelPrefix+suffix)
		if !bytes.Equal(serial, parallel) {
			t.Errorf("parallel-assembled %s differs from serially-assembled %s", suffix, suffix)
		}
	}
	assertCKAFDataset(t, parallelPrefix)
}

func TestRunCKAFDaysGracefulStopAndResume(t *testing.T) {
	cacheDir := t.TempDir()
	prefix := filepath.Join(t.TempDir(), "dataset")
	days := makeCKAFTestDays(t, cacheDir)

	stopCh := make(chan struct{})
	close(stopCh)
	cfg := CKAFConfig{
		OutputPrefix:  prefix,
		CacheDir:      cacheDir,
		DecodeWorkers: 2,
		GracefulStop:  stopCh,
	}
	if err := runCKAFDays(context.Background(), cfg, staticDownloader{}, days); err != nil {
		t.Fatal(err)
	}

	progress, hasProgress, err := loadCKAFProgress(prefix)
	if err != nil {
		t.Fatal(err)
	}
	if !hasProgress || progress.LastCompletedDay != "2026-03-26" {
		t.Fatalf("progress = %+v (present=%t), want last completed day 2026-03-26", progress, hasProgress)
	}
	if !fileExists(ckafSpoolPath(prefix)) {
		t.Fatal("graceful stop must leave the spool for resume")
	}
	if _, err := os.Stat(prefix + ".ckd"); err == nil {
		t.Fatal("graceful stop must not emit a partial dataset")
	}

	cfg.GracefulStop = nil
	if err := runCKAFDays(context.Background(), cfg, staticDownloader{}, days); err != nil {
		t.Fatal(err)
	}

	assertCKAFDataset(t, prefix)

	if fileExists(ckafSpoolPath(prefix)) || fileExists(ckafProgressPath(prefix)) {
		t.Fatal("resume artifacts should be removed after a successful build")
	}
}
