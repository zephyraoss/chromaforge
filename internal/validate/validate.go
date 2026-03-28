package validate

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
)

type Config struct {
	DBPath        string
	SoftHeapLimit int64
}

type Result struct {
	OK                  bool
	FingerprintCount    int64
	SubFingerprintCount int64
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	db, err := libsqlutil.OpenLocal(cfg.DBPath)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if applied, err := libsqlutil.ApplySoftHeapLimit(ctx, db, cfg.SoftHeapLimit); err != nil {
		return Result{}, err
	} else if cfg.SoftHeapLimit >= 0 {
		log.Printf("sqlite soft_heap_limit=%d", applied)
	}

	return RunDB(ctx, db)
}

func RunDB(ctx context.Context, db *sql.DB) (Result, error) {
	var res Result
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fingerprints`).Scan(&res.FingerprintCount); err != nil {
		return res, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sub_fingerprints`).Scan(&res.SubFingerprintCount); err != nil {
		return res, err
	}

	checks := []struct {
		name string
		ok   bool
		info string
	}{
		{"fingerprints_count", res.FingerprintCount > 0, fmt.Sprintf("%d", res.FingerprintCount)},
		{"subfingerprints_count", res.SubFingerprintCount > res.FingerprintCount, fmt.Sprintf("%d", res.SubFingerprintCount)},
	}

	if err := verifyRandomLookups(ctx, db, `SELECT acoustid FROM fingerprints ORDER BY RANDOM() LIMIT 5`, `SELECT COUNT(*) FROM fingerprints WHERE acoustid = ?`, &checks, "acoustid_lookup"); err != nil {
		return res, err
	}
	if err := verifyRandomLookups(ctx, db, `SELECT hash FROM sub_fingerprints ORDER BY RANDOM() LIMIT 5`, `SELECT COUNT(*) FROM sub_fingerprints WHERE hash = ?`, &checks, "hash_lookup"); err != nil {
		return res, err
	}

	var integrity string
	if err := queryIntegrityCheck(ctx, db, &integrity); err != nil {
		return res, err
	}
	checks = append(checks, struct {
		name string
		ok   bool
		info string
	}{"integrity_check", integrity == "ok", integrity})

	res.OK = true
	for _, check := range checks {
		log.Printf("check=%s pass=%t info=%s", check.name, check.ok, check.info)
		if !check.ok {
			res.OK = false
		}
	}
	if !res.OK {
		return res, fmt.Errorf("validation failed")
	}
	return res, nil
}

func queryIntegrityCheck(ctx context.Context, db *sql.DB, out *string) error {
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(out); err == nil {
		return nil
	}

	rows, err := db.QueryContext(ctx, `SELECT * FROM pragma_integrity_check`)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(out); err != nil {
			return err
		}
		return rows.Err()
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return fmt.Errorf("integrity check returned no rows")
}

func verifyRandomLookups(ctx context.Context, db *sql.DB, sampleQuery, lookupQuery string, checks *[]struct {
	name string
	ok   bool
	info string
}, name string) error {
	rows, err := db.QueryContext(ctx, sampleQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var samples []any
	for rows.Next() {
		var value any
		if err := rows.Scan(&value); err != nil {
			return err
		}
		samples = append(samples, value)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	count := len(samples)
	found := 0
	for _, value := range samples {
		var hits int64
		if err := db.QueryRowContext(ctx, lookupQuery, value).Scan(&hits); err != nil {
			return err
		}
		if hits > 0 {
			found++
		}
	}
	*checks = append(*checks, struct {
		name string
		ok   bool
		info string
	}{name, count == found, fmt.Sprintf("%d/%d", found, count)})
	return nil
}
