package match

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func TestParseFingerprintTextParsesFPCalcOutput(t *testing.T) {
	raw, duration, err := parseFingerprintText("FILE=test.mp3\nDURATION=12.4\nFINGERPRINT=-1,0,1\n")
	if err != nil {
		t.Fatal(err)
	}
	if duration != 12 {
		t.Fatalf("duration = %d, want 12", duration)
	}
	want := []int64{-1, 0, 1}
	if len(raw) != len(want) {
		t.Fatalf("len(raw) = %d, want %d", len(raw), len(want))
	}
	for i := range want {
		if raw[i] != want[i] {
			t.Fatalf("raw[%d] = %d, want %d", i, raw[i], want[i])
		}
	}
}

func TestRunDBWithFingerprintReturnsBestCandidate(t *testing.T) {
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
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
	if _, err := db.ExecContext(ctx, `INSERT INTO fingerprints (id, acoustid, mb_id, title, artist, duration) VALUES
		(1, 'acoustid-1', 'mbid-1', 'Track One', 'Artist One', 30),
		(2, 'acoustid-2', 'mbid-2', 'Track Two', 'Artist Two', 30)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES
		(11, 1, 0),
		(22, 1, 8),
		(33, 1, 16),
		(11, 2, 0),
		(55, 2, 8),
		(66, 2, 16)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateHashIndex); err != nil {
		t.Fatal(err)
	}

	rawFingerprint := []int64{
		11, 1, 1, 1, 1, 1, 1, 1,
		22, 1, 1, 1, 1, 1, 1, 1,
		33,
	}

	result, err := RunDBWithFingerprint(ctx, db, Config{Limit: 5}, rawFingerprint, 30)
	if err != nil {
		t.Fatal(err)
	}
	if result.QuerySubFingerprintCount != 3 {
		t.Fatalf("query sub-fingerprint count = %d, want 3", result.QuerySubFingerprintCount)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(result.Candidates))
	}
	if got := result.Candidates[0].AcoustID; got != "acoustid-1" {
		t.Fatalf("best acoustid = %q, want acoustid-1", got)
	}
	if got := result.Candidates[0].Hits; got != 3 {
		t.Fatalf("best hits = %d, want 3", got)
	}
}

func TestRunDBWithWrappedUint32FingerprintReturnsBestCandidate(t *testing.T) {
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
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
	if _, err := db.ExecContext(ctx, `INSERT INTO fingerprints (id, acoustid, mb_id, title, artist, duration) VALUES
		(1, 'acoustid-1', 'mbid-1', 'Track One', 'Artist One', 30)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES
		(4046453422, 1, 0),
		(4046453423, 1, 8)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateHashIndex); err != nil {
		t.Fatal(err)
	}

	rawFingerprint := []int64{
		4046453422, 1, 1, 1, 1, 1, 1, 1,
		4046453423,
	}

	result, err := RunDBWithFingerprint(ctx, db, Config{Limit: 5}, rawFingerprint, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(result.Candidates))
	}
	if got := result.Candidates[0].AcoustID; got != "acoustid-1" {
		t.Fatalf("best acoustid = %q, want acoustid-1", got)
	}
}
