package build

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func TestBuildSubFingerprintInsert(t *testing.T) {
	query, args := buildSubFingerprintInsert(42, []SubFP{
		{Hash: 11, Position: 0},
		{Hash: 22, Position: 8},
	})

	wantQuery := "INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES (?,?,?),(?,?,?)"
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	if len(args) != 6 {
		t.Fatalf("len(args) = %d, want 6", len(args))
	}
}

func TestInsertBatchBackfillsMissingMetadataForDuplicateAcoustid(t *testing.T) {
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
		t.Fatal(err)
	}

	writer, err := newWriteSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	batchWriter, err := writer.NewBatchWriter(ctx, txModeBeginExclusive)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := batchWriter.InsertBatch(ctx, []Record{{AcoustID: "acoustid-1"}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := batchWriter.InsertBatch(ctx, []Record{{
		AcoustID: "acoustid-1",
		MBID:     "mbid-1",
		Duration: 30,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := batchWriter.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := batchWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	var mbid string
	var duration int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(mb_id, ''), COALESCE(duration, 0) FROM fingerprints WHERE acoustid = 'acoustid-1'`).Scan(&mbid, &duration); err != nil {
		t.Fatal(err)
	}
	if mbid != "mbid-1" || duration != 30 {
		t.Fatalf("unexpected metadata backfill result: mbid=%q duration=%d", mbid, duration)
	}
}
