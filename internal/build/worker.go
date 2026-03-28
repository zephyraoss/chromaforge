package build

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type txMode string

const (
	txModeBeginExclusive    txMode = "BEGIN EXCLUSIVE"
	subFingerprintChunkSize        = 8192
)

const (
	createSeenAcoustIDsTempTable = `
CREATE TEMP TABLE IF NOT EXISTS seen_acoustids (
	acoustid TEXT PRIMARY KEY
) WITHOUT ROWID
`
	seedSeenAcoustIDsTempTable = `
INSERT OR IGNORE INTO temp.seen_acoustids(acoustid)
SELECT acoustid FROM main.fingerprints
`
)

type Record struct {
	AcoustID string
	MBID     string
	Title    string
	Artist   string
	Duration int
	SubFPs   []SubFP
}

type SubFP struct {
	Hash     uint32
	Position int
}

type writeSession struct {
	conn     *sql.Conn
	seenStmt *sql.Stmt
}

type batchWriter struct {
	conn      *sql.Conn
	seenStmt  *sql.Stmt
	fpStmt    *sql.Stmt
	committed bool
}

func newWriteSession(ctx context.Context, db *sql.DB) (*writeSession, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if err := execConnPragma(ctx, conn, `PRAGMA temp_store = MEMORY;`); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, createSeenAcoustIDsTempTable); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, seedSeenAcoustIDsTempTable); err != nil {
		_ = conn.Close()
		return nil, err
	}
	seenStmt, err := conn.PrepareContext(ctx, `INSERT OR IGNORE INTO temp.seen_acoustids(acoustid) VALUES (?)`)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &writeSession{
		conn:     conn,
		seenStmt: seenStmt,
	}, nil
}

func (s *writeSession) NewBatchWriter(ctx context.Context, mode txMode) (*batchWriter, error) {
	if _, err := s.conn.ExecContext(ctx, string(mode)); err != nil {
		return nil, err
	}

	fpStmt, err := s.conn.PrepareContext(ctx, `
INSERT INTO fingerprints (acoustid, mb_id, title, artist, duration)
VALUES (?, ?, ?, ?, ?)
`)
	if err != nil {
		_, _ = s.conn.ExecContext(context.Background(), "ROLLBACK")
		return nil, err
	}

	return &batchWriter{
		conn:     s.conn,
		seenStmt: s.seenStmt,
		fpStmt:   fpStmt,
	}, nil
}

func (s *writeSession) Close() error {
	var firstErr error
	if s.seenStmt != nil {
		if err := s.seenStmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.conn != nil {
		if err := s.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *batchWriter) InsertBatch(ctx context.Context, batch []Record) (int64, int64, error) {
	var insertedFPs int64
	var insertedSubs int64
	for _, r := range batch {
		seenRes, err := w.seenStmt.ExecContext(ctx, r.AcoustID)
		if err != nil {
			return insertedFPs, insertedSubs, err
		}
		seenRows, err := seenRes.RowsAffected()
		if err != nil {
			return insertedFPs, insertedSubs, err
		}
		if seenRows == 0 {
			continue
		}
		res, err := w.fpStmt.ExecContext(ctx, r.AcoustID, nullIfEmpty(r.MBID), nullIfEmpty(r.Title), nullIfEmpty(r.Artist), r.Duration)
		if err != nil {
			return insertedFPs, insertedSubs, err
		}
		fpID, err := res.LastInsertId()
		if err != nil {
			return insertedFPs, insertedSubs, err
		}
		insertedFPs++
		if len(r.SubFPs) > 0 {
			n, err := insertSubFingerprints(ctx, w.conn, fpID, r.SubFPs)
			if err != nil {
				return insertedFPs, insertedSubs, err
			}
			insertedSubs += n
		}
	}

	return insertedFPs, insertedSubs, nil
}

func (w *batchWriter) Commit(ctx context.Context) error {
	if _, err := w.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	w.committed = true
	return nil
}

func (w *batchWriter) Close() error {
	var firstErr error
	if w.fpStmt != nil {
		if err := w.fpStmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.conn != nil {
		if !w.committed {
			if _, err := w.conn.ExecContext(context.Background(), "ROLLBACK"); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func insertSubFingerprints(ctx context.Context, conn *sql.Conn, fpID int64, subfps []SubFP) (int64, error) {
	var inserted int64
	for start := 0; start < len(subfps); start += subFingerprintChunkSize {
		end := start + subFingerprintChunkSize
		if end > len(subfps) {
			end = len(subfps)
		}
		query, args := buildSubFingerprintInsert(fpID, subfps[start:end])
		if _, err := conn.ExecContext(ctx, query, args...); err != nil {
			return inserted, err
		}
		inserted += int64(end - start)
	}
	return inserted, nil
}

func buildSubFingerprintInsert(fpID int64, subfps []SubFP) (string, []any) {
	var b strings.Builder
	b.WriteString("INSERT INTO sub_fingerprints (hash, fingerprint_id, position) VALUES ")
	args := make([]any, 0, len(subfps)*3)
	for i, s := range subfps {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?)")
		args = append(args, int64(s.Hash), fpID, s.Position)
	}
	return b.String(), args
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func itoa(v int) string {
	return fmt.Sprintf("%d", v)
}

func execConnPragma(ctx context.Context, conn *sql.Conn, stmt string) error {
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	return discardRows(rows)
}
