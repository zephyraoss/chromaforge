package build

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/dump"
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
