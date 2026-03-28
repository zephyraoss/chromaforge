package build

import (
	"path/filepath"
	"testing"
)

func TestBuildProgressRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "chromakopia.db")
	day := mustDay("2026-03-26")

	if err := saveBuildProgress(dbPath, day); err != nil {
		t.Fatal(err)
	}

	progress, ok, err := loadBuildProgress(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected progress file to exist")
	}
	if progress.LastCompletedDay != "2026-03-26" {
		t.Fatalf("last_completed_day = %q, want 2026-03-26", progress.LastCompletedDay)
	}

	if err := clearBuildProgress(dbPath); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := loadBuildProgress(dbPath); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected progress file to be removed")
	}
}
