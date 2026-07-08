package build

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
	"github.com/zephyraoss/chromaforge/internal/schema"
)

func openBulkLoadDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := libsqlutil.OpenLocal(filepath.Join(t.TempDir(), "chromakopia.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, schema.CreateFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema.CreateSubFingerprintsTable); err != nil {
		t.Fatal(err)
	}
	return db
}

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
	db := openBulkLoadDB(t)

	ctx := context.Background()
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

	if _, err := db.ExecContext(ctx, schema.CreateAcoustIDIndex); err != nil {
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

func TestInsertBatchDuplicateDoesNotOverwriteExistingMetadata(t *testing.T) {
	db := openBulkLoadDB(t)

	ctx := context.Background()
	writer, err := newWriteSession(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	batchWriter, err := writer.NewBatchWriter(ctx, txModeBeginExclusive)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := batchWriter.InsertBatch(ctx, []Record{{
		AcoustID: "acoustid-1",
		MBID:     "mbid-1",
		Duration: 30,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := batchWriter.InsertBatch(ctx, []Record{{
		AcoustID: "acoustid-1",
		MBID:     "mbid-2",
		Duration: 99,
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
		t.Fatalf("duplicate overwrote existing metadata: mbid=%q duration=%d", mbid, duration)
	}
}

func TestWriteSessionResumeSeedsFingerprintRowids(t *testing.T) {
	db := openBulkLoadDB(t)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
INSERT INTO fingerprints (id, acoustid, mb_id, duration) VALUES
	(1, 'acoustid-1', 'mbid-1', 10),
	(2, 'acoustid-2', NULL, 0)
`); err != nil {
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
	insertedFPs, _, err := batchWriter.InsertBatch(ctx, []Record{
		{AcoustID: "acoustid-2", MBID: "mbid-2", Duration: 20},
		{AcoustID: "acoustid-3", MBID: "mbid-3", Duration: 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	if insertedFPs != 1 {
		t.Fatalf("insertedFPs = %d, want 1", insertedFPs)
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

	rows, err := db.QueryContext(ctx, `SELECT id, acoustid, COALESCE(mb_id, ''), COALESCE(duration, 0) FROM fingerprints ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type fpRow struct {
		id       int64
		acoustid string
		mbid     string
		duration int
	}
	var got []fpRow
	for rows.Next() {
		var r fpRow
		if err := rows.Scan(&r.id, &r.acoustid, &r.mbid, &r.duration); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	want := []fpRow{
		{id: 1, acoustid: "acoustid-1", mbid: "mbid-1", duration: 10},
		{id: 2, acoustid: "acoustid-2", mbid: "mbid-2", duration: 20},
		{id: 3, acoustid: "acoustid-3", mbid: "mbid-3", duration: 30},
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (%+v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func BenchmarkInsertBatchDuplicateMerge(b *testing.B) {
	for _, tableSize := range []int{10_000, 50_000, 250_000} {
		b.Run(fmt.Sprintf("table_%d", tableSize), func(b *testing.B) {
			db := openBulkLoadDB(b)
			ctx := context.Background()

			writer, err := newWriteSession(ctx, db)
			if err != nil {
				b.Fatal(err)
			}
			defer writer.Close()
			batchWriter, err := writer.NewBatchWriter(ctx, txModeBeginExclusive)
			if err != nil {
				b.Fatal(err)
			}
			defer batchWriter.Close()

			const preloadBatch = 1000
			batch := make([]Record, 0, preloadBatch)
			for i := 0; i < tableSize; i += preloadBatch {
				batch = batch[:0]
				for j := i; j < i+preloadBatch && j < tableSize; j++ {
					batch = append(batch, Record{AcoustID: fmt.Sprintf("acoustid-%d", j)})
				}
				if _, _, err := batchWriter.InsertBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dup := Record{
					AcoustID: fmt.Sprintf("acoustid-%d", i%tableSize),
					MBID:     "mbid-1",
					Duration: 30,
				}
				if _, _, err := batchWriter.InsertBatch(ctx, []Record{dup}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
