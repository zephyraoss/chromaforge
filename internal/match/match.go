package match

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/zephyraoss/chromaforge/internal/dump"
	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
)

type Config struct {
	DBPath          string
	Fingerprint     string
	FingerprintFile string
	Duration        int
	DurationWindow  int
	Limit           int
	MinHits         int
	SoftHeapLimit   int64
	ReadConnections int
}

type Result struct {
	QueryDuration            int
	QuerySubFingerprintCount int
	Candidates               []Candidate
}

type Candidate struct {
	AcoustID string
	MBID     string
	Duration int
	Delta    int
	Hits     int64
	Coverage float64
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if cfg.ReadConnections <= 0 {
		cfg.ReadConnections = 1
	}
	db.SetMaxOpenConns(cfg.ReadConnections)
	db.SetMaxIdleConns(cfg.ReadConnections)
	if applied, err := libsqlutil.ApplySoftHeapLimit(ctx, db, cfg.SoftHeapLimit); err != nil {
		return Result{}, err
	} else if cfg.SoftHeapLimit >= 0 {
		log.Printf("sqlite soft_heap_limit=%d", applied)
	}

	rawFingerprint, parsedDuration, err := loadFingerprintInput(cfg)
	if err != nil {
		return Result{}, err
	}
	return RunDBWithFingerprint(ctx, db, cfg, rawFingerprint, parsedDuration)
}

func RunDBWithFingerprint(ctx context.Context, db *sql.DB, cfg Config, rawFingerprint []int64, parsedDuration int) (Result, error) {
	if cfg.Limit <= 0 {
		cfg.Limit = 10
	}

	normalized, err := normalizeQueryFingerprint(rawFingerprint)
	if err != nil {
		return Result{}, err
	}
	subFingerprints := dump.ExtractSubFingerprints(normalized)
	if len(subFingerprints) == 0 {
		return Result{}, fmt.Errorf("fingerprint produced no sub-fingerprints")
	}

	if cfg.MinHits <= 0 {
		cfg.MinHits = min(3, len(subFingerprints))
	}
	if cfg.MinHits > len(subFingerprints) {
		return Result{}, fmt.Errorf("min hits %d exceeds query sub-fingerprint count %d", cfg.MinHits, len(subFingerprints))
	}

	queryDuration := parsedDuration
	if cfg.Duration > 0 {
		queryDuration = cfg.Duration
	}
	if cfg.DurationWindow < 0 {
		return Result{}, fmt.Errorf("duration window must be >= 0, got %d", cfg.DurationWindow)
	}
	if queryDuration > 0 && cfg.DurationWindow == 0 {
		cfg.DurationWindow = 5
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()

	if err := prepareQuerySubFingerprints(ctx, conn, subFingerprints); err != nil {
		return Result{}, err
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS temp.query_sub_fingerprints`)
	}()

	candidates, err := queryCandidates(ctx, conn, queryDuration, cfg.DurationWindow, len(subFingerprints), cfg.MinHits, cfg.Limit)
	if err != nil {
		return Result{}, err
	}

	return Result{
		QueryDuration:            queryDuration,
		QuerySubFingerprintCount: len(subFingerprints),
		Candidates:               candidates,
	}, nil
}

func prepareQuerySubFingerprints(ctx context.Context, conn *sql.Conn, subFingerprints []dump.SubFingerprint) error {
	if err := execConnPragma(ctx, conn, `PRAGMA temp_store = MEMORY;`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS temp.query_sub_fingerprints`); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `
CREATE TEMP TABLE temp.query_sub_fingerprints (
	hash INTEGER NOT NULL,
	position INTEGER NOT NULL
)
`); err != nil {
		return err
	}

	stmt, err := conn.PrepareContext(ctx, `INSERT INTO temp.query_sub_fingerprints(hash, position) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, sub := range subFingerprints {
		if _, err := stmt.ExecContext(ctx, int64(sub.Hash), sub.Position); err != nil {
			return err
		}
	}
	return nil
}

func queryCandidates(ctx context.Context, conn *sql.Conn, queryDuration int, durationWindow int, querySubFingerprintCount int, minHits int, limit int) ([]Candidate, error) {
	minDuration := queryDuration - durationWindow
	if minDuration < 0 {
		minDuration = 0
	}
	maxDuration := queryDuration + durationWindow

	rows, err := conn.QueryContext(ctx, `
WITH grouped AS (
	SELECT
		sf.fingerprint_id AS fingerprint_id,
		sf.position - q.position AS delta,
		COUNT(*) AS hits
	FROM temp.query_sub_fingerprints q
	JOIN sub_fingerprints sf ON sf.hash = q.hash
	GROUP BY sf.fingerprint_id, delta
),
ranked AS (
	SELECT
		fingerprint_id,
		delta,
		hits,
		ROW_NUMBER() OVER (
			PARTITION BY fingerprint_id
			ORDER BY hits DESC, ABS(delta) ASC
		) AS rn
	FROM grouped
),
best AS (
	SELECT fingerprint_id, delta, hits
	FROM ranked
	WHERE rn = 1 AND hits >= ?
)
SELECT
	f.acoustid,
	COALESCE(f.mb_id, ''),
	COALESCE(f.duration, 0),
	b.delta,
	b.hits
FROM best b
JOIN fingerprints f ON f.id = b.fingerprint_id
WHERE
	? <= 0 OR ? <= 0 OR COALESCE(f.duration, 0) BETWEEN ? AND ?
ORDER BY
	b.hits DESC,
	CASE WHEN ? > 0 THEN ABS(COALESCE(f.duration, 0) - ?) ELSE 0 END ASC,
	ABS(b.delta) ASC,
	f.id ASC
LIMIT ?
`, minHits, queryDuration, durationWindow, minDuration, maxDuration, queryDuration, queryDuration, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]Candidate, 0, limit)
	for rows.Next() {
		var candidate Candidate
		if err := rows.Scan(&candidate.AcoustID, &candidate.MBID, &candidate.Duration, &candidate.Delta, &candidate.Hits); err != nil {
			return nil, err
		}
		candidate.Coverage = (float64(candidate.Hits) / float64(querySubFingerprintCount)) * 100
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func loadFingerprintInput(cfg Config) ([]int64, int, error) {
	if cfg.Fingerprint != "" && cfg.FingerprintFile != "" {
		return nil, 0, fmt.Errorf("use either --fingerprint or --fingerprint-file, not both")
	}

	switch {
	case cfg.Fingerprint != "":
		return parseFingerprintText(cfg.Fingerprint)
	case cfg.FingerprintFile != "":
		data, err := readFingerprintFile(cfg.FingerprintFile)
		if err != nil {
			return nil, 0, err
		}
		return parseFingerprintText(string(data))
	default:
		return nil, 0, fmt.Errorf("missing fingerprint input; use --fingerprint or --fingerprint-file")
	}
}

func readFingerprintFile(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func parseFingerprintText(text string) ([]int64, int, error) {
	fingerprintText := strings.TrimSpace(text)
	duration := 0

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(key)) {
		case "FINGERPRINT":
			fingerprintText = strings.TrimSpace(value)
		case "DURATION":
			if duration != 0 {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil {
				return nil, 0, fmt.Errorf("parse duration: %w", err)
			}
			duration = int(math.Round(parsed))
		}
	}

	raw, err := parseInt64List(fingerprintText)
	if err != nil {
		return nil, 0, err
	}
	return raw, duration, nil
}

func parseInt64List(s string) ([]int64, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '[' || r == ']' || unicode.IsSpace(r)
	})
	if len(fields) == 0 {
		return nil, fmt.Errorf("fingerprint input is empty")
	}

	values := make([]int64, 0, len(fields))
	for _, field := range fields {
		v, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse fingerprint value %q: %w", field, err)
		}
		values = append(values, v)
	}
	return values, nil
}

func execConnPragma(ctx context.Context, conn *sql.Conn, stmt string) error {
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
	}
	return rows.Err()
}

func normalizeQueryFingerprint(raw []int64) ([]uint32, error) {
	const maxUint32 = int64(^uint32(0))

	out := make([]uint32, 0, len(raw))
	for _, v := range raw {
		switch {
		case v >= math.MinInt32 && v <= math.MaxInt32:
			out = append(out, uint32(int32(v)))
		case v >= 0 && v <= maxUint32:
			out = append(out, uint32(v))
		default:
			return nil, fmt.Errorf("fingerprint value %d outside int32/uint32 range", v)
		}
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
