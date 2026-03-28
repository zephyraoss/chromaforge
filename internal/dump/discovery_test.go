package dump

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClassifyArchiveFile(t *testing.T) {
	tests := []struct {
		name string
		ft   FileType
		ok   bool
	}{
		{"2026-03-26-track-update.jsonl.gz", FileTypeTrack, true},
		{"2026-03-26-track_meta-update.jsonl.gz", FileTypeTrackMeta, true},
		{"2026-03-26-meta-update.jsonl.gz", FileTypeMeta, true},
		{"2026-03-26-track_mbid-update.jsonl.gz", FileTypeTrackMBID, true},
		{"2026-03-26-track_fingerprint-update.jsonl.gz", FileTypeTrackFingerprint, true},
		{"2026-03-26-fingerprint-update.jsonl.gz", FileTypeFingerprint, true},
		{"ignore-me.txt", "", false},
	}

	for _, tt := range tests {
		ft, _, ok := classifyArchiveFile(tt.name)
		if ok != tt.ok || ft != tt.ft {
			t.Fatalf("classifyArchiveFile(%q) = (%q, %t), want (%q, %t)", tt.name, ft, ok, tt.ft, tt.ok)
		}
	}
}

func TestDiscoverArchive(t *testing.T) {
	mux := http.NewServeMux()
	writeJSON := func(path string, payload any) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(payload)
		})
	}

	writeJSON("/index.json", []ArchiveIndexEntry{{Name: "2025/"}, {Name: "2026/"}})
	writeJSON("/2026/index.json", []ArchiveIndexEntry{{Name: "2026-03/"}})
	writeJSON("/2026/2026-03/index.json", []ArchiveIndexEntry{
		{Name: "2026-03-25-track-update.jsonl.gz", Size: 10},
		{Name: "2026-03-25-track_meta-update.jsonl.gz", Size: 10},
		{Name: "2026-03-25-track_mbid-update.jsonl.gz", Size: 10},
		{Name: "2026-03-25-track_fingerprint-update.jsonl.gz", Size: 10},
		{Name: "2026-03-25-fingerprint-update.jsonl.gz", Size: 10},
		{Name: "2026-03-26-fingerprint-update.jsonl.gz", Size: 10},
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	days, err := DiscoverArchive(context.Background(), srv.Client(), srv.URL, 2026, "2026-03-25")
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 1 {
		t.Fatalf("got %d days, want 1", len(days))
	}
	got := days[0].OrderedFiles()
	if len(got) != 5 {
		t.Fatalf("got %d ordered files, want 5", len(got))
	}
	if got[0].Type != FileTypeTrack || got[len(got)-1].Type != FileTypeFingerprint {
		t.Fatalf("unexpected file order: first=%s last=%s", got[0].Type, got[len(got)-1].Type)
	}
}
