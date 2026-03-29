package validate

import (
	"context"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func TestSampleTargets(t *testing.T) {
	got := sampleTargets(100, 5)
	want := []int64{17, 34, 50, 67, 83}
	if len(got) != len(want) {
		t.Fatalf("len(sampleTargets) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sampleTargets[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestRunDBDefaultFastValidation(t *testing.T) {
	db, err := libsqlutil.OpenLocal(t.TempDir() + "/chromakopia.db")
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

	result, err := RunDB(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatalf("expected validation OK, got %#v", result)
	}
}

func TestRunDBQuickCheckWhenRequested(t *testing.T) {
	db, err := libsqlutil.OpenLocal(t.TempDir() + "/chromakopia.db")
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

	result, err := RunDBWithConfig(ctx, db, Config{QuickCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.OK {
		t.Fatalf("expected validation OK, got %#v", result)
	}
}
