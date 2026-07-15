package build

import (
	"testing"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

func TestProgressTrackerSnapshotDerivesETAFromByteRate(t *testing.T) {
	start := time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC)
	tracker := newProgressTracker(1000, start)

	if _, _, ok := tracker.snapshot(start.Add(time.Minute)); ok {
		t.Fatal("expected no snapshot before any bytes are consumed")
	}

	tracker.CurrentFileBytes().Store(250)
	percent, eta, ok := tracker.snapshot(start.Add(time.Minute))
	if !ok {
		t.Fatal("expected snapshot after bytes are consumed")
	}
	if percent != 25.0 {
		t.Fatalf("percent = %v, want 25.0", percent)
	}
	if got := eta.Round(time.Second); got != 3*time.Minute {
		t.Fatalf("eta = %v, want 3m0s", got)
	}

	tracker.FileCompleted(400)
	percent, _, ok = tracker.snapshot(start.Add(time.Minute))
	if !ok || percent != 40.0 {
		t.Fatalf("percent after FileCompleted = %v ok=%t, want 40.0", percent, ok)
	}
	if got := tracker.CurrentFileBytes().Load(); got != 0 {
		t.Fatalf("current file bytes after FileCompleted = %d, want 0", got)
	}
}

func TestProgressTrackerSnapshotClampsOverconsumption(t *testing.T) {
	start := time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC)
	tracker := newProgressTracker(1000, start)
	tracker.FileCompleted(1200)

	percent, eta, ok := tracker.snapshot(start.Add(time.Minute))
	if !ok {
		t.Fatal("expected snapshot")
	}
	if percent != 100.0 {
		t.Fatalf("percent = %v, want 100.0", percent)
	}
	if eta != 0 {
		t.Fatalf("eta = %v, want 0", eta)
	}
}

func TestProgressTrackerNilSafe(t *testing.T) {
	var tracker *progressTracker
	if tracker.CurrentFileBytes() != nil {
		t.Fatal("expected nil counter from nil tracker")
	}
	tracker.FileCompleted(10)
	if _, _, ok := tracker.snapshot(time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC)); ok {
		t.Fatal("expected no snapshot from nil tracker")
	}
}

func TestProgressTrackerZeroTotalBytes(t *testing.T) {
	start := time.Date(2026, 3, 26, 0, 0, 0, 0, time.UTC)
	tracker := newProgressTracker(0, start)
	tracker.FileCompleted(10)
	if _, _, ok := tracker.snapshot(start.Add(time.Minute)); ok {
		t.Fatal("expected no snapshot with zero total bytes")
	}
}

func TestFormatETA(t *testing.T) {
	cases := []struct {
		eta  time.Duration
		want string
	}{
		{30 * time.Second, "<1m"},
		{90 * time.Second, "2m"},
		{52 * time.Minute, "52m"},
		{60 * time.Minute, "1h0m"},
		{3*time.Hour + 20*time.Minute, "3h20m"},
		{129*time.Hour + 40*time.Minute + 39*time.Second, "129h41m"},
	}
	for _, tc := range cases {
		if got := formatETA(tc.eta); got != tc.want {
			t.Errorf("formatETA(%v) = %q, want %q", tc.eta, got, tc.want)
		}
	}
}

func TestReplayableBytesExcludesMetaFiles(t *testing.T) {
	day := dump.DayFiles{
		Day: mustDay("2026-03-26"),
		Files: map[dump.FileType]dump.ArchiveFile{
			dump.FileTypeTrack:            {Size: 10},
			dump.FileTypeTrackMeta:        {Size: 1000},
			dump.FileTypeMeta:             {Size: 2000},
			dump.FileTypeTrackMBID:        {Size: 20},
			dump.FileTypeTrackFingerprint: {Size: 30},
			dump.FileTypeFingerprint:      {Size: 4000},
		},
	}
	if got := replayableBytes(day); got != 4060 {
		t.Fatalf("replayableBytes = %d, want 4060", got)
	}
	if got := totalReplayableBytes([]dump.DayFiles{day, day}); got != 8120 {
		t.Fatalf("totalReplayableBytes = %d, want 8120", got)
	}
}
