package build

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

func makeTestArchiveFiles(t *testing.T, cacheDir string) map[dump.FileType]dump.ArchiveFile {
	return map[dump.FileType]dump.ArchiveFile{
		dump.FileTypeTrack: writeTestArchiveFile(t, cacheDir, "2026-03-26-track-update.jsonl.gz", []any{
			dump.TrackUpdate{ID: 1, GID: "acoustid-1"},
		}),
		dump.FileTypeTrackMeta: writeTestArchiveFile(t, cacheDir, "2026-03-26-track_meta-update.jsonl.gz", []any{
			dump.TrackMetaUpdate{TrackID: 1, Track: "Track One", Artist: "Artist One"},
		}),
		dump.FileTypeTrackMBID: writeTestArchiveFile(t, cacheDir, "2026-03-26-track_mbid-update.jsonl.gz", []any{
			dump.TrackMBIDUpdate{TrackID: 1, MBID: "mbid-1"},
		}),
		dump.FileTypeTrackFingerprint: writeTestArchiveFile(t, cacheDir, "2026-03-26-track_fingerprint-update.jsonl.gz", []any{
			dump.TrackFingerprintUpdate{FingerprintID: 10, TrackID: 1},
		}),
		dump.FileTypeFingerprint: writeTestArchiveFile(t, cacheDir, "2026-03-26-fingerprint-update.jsonl.gz", []any{
			dump.FingerprintUpdate{ID: 10, Length: 30, Fingerprint: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		}),
	}
}

func writeTestArchiveFile(t *testing.T, cacheDir, name string, lines []any) dump.ArchiveFile {
	t.Helper()
	monthDir := filepath.Join(cacheDir, name[:7])
	if err := os.MkdirAll(monthDir, 0o755); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(monthDir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
	for _, line := range lines {
		if err := enc.Encode(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	ft, day, ok := dump.ClassifyArchiveFileForTest(name)
	if !ok {
		t.Fatalf("could not classify %s", name)
	}
	return dump.ArchiveFile{Name: name, Size: info.Size(), Type: ft, Day: day, URL: path}
}

func makeTestDayFiles(t *testing.T, cacheDir, day string, lines map[dump.FileType][]any) dump.DayFiles {
	t.Helper()

	files := map[dump.FileType]dump.ArchiveFile{}
	suffixes := map[dump.FileType]string{
		dump.FileTypeTrack:            "track",
		dump.FileTypeTrackMeta:        "track_meta",
		dump.FileTypeMeta:             "meta",
		dump.FileTypeTrackMBID:        "track_mbid",
		dump.FileTypeTrackFingerprint: "track_fingerprint",
		dump.FileTypeFingerprint:      "fingerprint",
	}
	for ft, entries := range lines {
		suffix, ok := suffixes[ft]
		if !ok {
			t.Fatalf("unsupported file type %q", ft)
		}
		name := fmt.Sprintf("%s-%s-update.jsonl.gz", day, suffix)
		files[ft] = writeTestArchiveFile(t, cacheDir, name, entries)
	}

	return dump.DayFiles{
		Day:   mustDay(day),
		Files: files,
	}
}

func mustDay(value string) time.Time {
	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		panic(err)
	}
	return t
}
