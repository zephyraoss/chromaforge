package build

import (
	"context"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
)

func TestApplyBuildPragmasBulkLoadModeThenFinalizeToWAL(t *testing.T) {
	db, err := libsqlutil.OpenLocal(t.TempDir() + "/chromakopia.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := ApplyBuildPragmas(ctx, db, 0, 0); err != nil {
		t.Fatal(err)
	}

	var mode string
	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode;`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "off" {
		t.Fatalf("journal_mode = %q, want off", mode)
	}

	var tempStore int
	if err := db.QueryRowContext(ctx, `PRAGMA temp_store;`).Scan(&tempStore); err != nil {
		t.Fatal(err)
	}
	if tempStore != 2 {
		t.Fatalf("temp_store = %d, want 2 (MEMORY)", tempStore)
	}

	var lockingMode string
	if err := db.QueryRowContext(ctx, `PRAGMA locking_mode;`).Scan(&lockingMode); err != nil {
		t.Fatal(err)
	}
	if lockingMode != "exclusive" {
		t.Fatalf("locking_mode = %q, want exclusive", lockingMode)
	}

	if err := ApplyIndexPragmas(ctx, db, 4, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := ApplyFinalizePragmas(ctx, db); err != nil {
		t.Fatal(err)
	}

	if err := db.QueryRowContext(ctx, `PRAGMA journal_mode;`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("final journal_mode = %q, want wal", mode)
	}

	if err := CheckpointWAL(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func TestApplyBuildPragmasCustomCacheAndMmapSizes(t *testing.T) {
	db, err := libsqlutil.OpenLocal(t.TempDir() + "/chromakopia.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		cacheSizeBytes = 32*1024*1024 + 123
		mmapSizeBytes  = 64 * 1024 * 1024
	)

	ctx := context.Background()
	if err := ApplyBuildPragmas(ctx, db, cacheSizeBytes, mmapSizeBytes); err != nil {
		t.Fatal(err)
	}

	var cacheSize int64
	if err := db.QueryRowContext(ctx, `PRAGMA cache_size;`).Scan(&cacheSize); err != nil {
		t.Fatal(err)
	}
	wantCacheSize := -int64((cacheSizeBytes + 1023) / 1024)
	if cacheSize != wantCacheSize {
		t.Fatalf("cache_size = %d, want %d", cacheSize, wantCacheSize)
	}

	var mmapSize int64
	if err := db.QueryRowContext(ctx, `PRAGMA mmap_size;`).Scan(&mmapSize); err != nil {
		t.Fatal(err)
	}
	if mmapSize != mmapSizeBytes {
		t.Fatalf("mmap_size = %d, want %d", mmapSize, mmapSizeBytes)
	}
}
