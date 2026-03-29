package validate

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/zephyraoss/chromaforge/internal/libsqlutil"
)

type Config struct {
	DBPath             string
	SoftHeapLimit      int64
	QuickCheck         bool
	FullIntegrityCheck bool
	CountRows          bool
	SampleCount        int
	ReadConnections    int
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

	return RunDBWithConfig(ctx, db, cfg)
}

func RunDB(ctx context.Context, db *sql.DB) (Result, error) {
	return RunDBWithConfig(ctx, db, Config{})
}

func RunDBWithConfig(ctx context.Context, db *sql.DB, cfg Config) (Result, error) {
	if cfg.SampleCount <= 0 {
		cfg.SampleCount = 5
	}
	if cfg.QuickCheck && cfg.FullIntegrityCheck {
		return Result{}, fmt.Errorf("quick_check and full integrity_check are mutually exclusive")
	}

	var (
		res  = Result{FingerprintCount: -1, SubFingerprintCount: -1}
		logs []validationCheck
	)

	fingerprintsPresent, err := tableHasRow(ctx, db, `SELECT 1 FROM fingerprints LIMIT 1`)
	if err != nil {
		return res, err
	}
	logs = append(logs, validationCheck{
		name: "fingerprints_present",
		ok:   fingerprintsPresent,
		info: boolInfo(fingerprintsPresent),
	})

	subFingerprintsPresent, err := tableHasRow(ctx, db, `SELECT 1 FROM sub_fingerprints LIMIT 1`)
	if err != nil {
		return res, err
	}
	logs = append(logs, validationCheck{
		name: "subfingerprints_present",
		ok:   subFingerprintsPresent,
		info: boolInfo(subFingerprintsPresent),
	})

	acoustidIndexExists, err := indexExists(ctx, db, "idx_fingerprints_acoustid")
	if err != nil {
		return res, err
	}
	logs = append(logs, validationCheck{
		name: "idx_fingerprints_acoustid_present",
		ok:   acoustidIndexExists,
		info: boolInfo(acoustidIndexExists),
	})

	hashIndexExists, err := indexExists(ctx, db, "idx_hash")
	if err != nil {
		return res, err
	}
	logs = append(logs, validationCheck{
		name: "idx_hash_present",
		ok:   hashIndexExists,
		info: boolInfo(hashIndexExists),
	})

	if cfg.CountRows {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fingerprints`).Scan(&res.FingerprintCount); err != nil {
			return res, err
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sub_fingerprints`).Scan(&res.SubFingerprintCount); err != nil {
			return res, err
		}
		logs = append(logs,
			validationCheck{name: "fingerprints_count", ok: res.FingerprintCount > 0, info: fmt.Sprintf("%d", res.FingerprintCount)},
			validationCheck{name: "subfingerprints_count", ok: res.SubFingerprintCount > res.FingerprintCount, info: fmt.Sprintf("%d", res.SubFingerprintCount)},
		)
	} else {
		fingerprintEstimate, ok, err := estimatedRowCountFromStat(ctx, db, "idx_fingerprints_acoustid")
		if err != nil {
			return res, err
		}
		if ok {
			res.FingerprintCount = fingerprintEstimate
			logs = append(logs, validationCheck{
				name: "fingerprints_count_estimate",
				ok:   fingerprintEstimate > 0,
				info: fmt.Sprintf("%d", fingerprintEstimate),
			})
		}

		subFingerprintEstimate, ok, err := estimatedRowCountFromStat(ctx, db, "idx_hash")
		if err != nil {
			return res, err
		}
		if ok {
			res.SubFingerprintCount = subFingerprintEstimate
			logs = append(logs, validationCheck{
				name: "subfingerprints_count_estimate",
				ok:   subFingerprintEstimate > res.FingerprintCount,
				info: fmt.Sprintf("%d", subFingerprintEstimate),
			})
		}
	}

	acoustIDCheck, err := sampleLookupCheck(ctx, db,
		`SELECT IFNULL(MAX(id), 0) FROM fingerprints`,
		`SELECT acoustid FROM fingerprints WHERE id >= ? LIMIT 1`,
		`SELECT COUNT(*) FROM fingerprints WHERE acoustid = ?`,
		cfg.SampleCount,
		"acoustid_lookup",
	)
	if err != nil {
		return res, err
	}
	logs = append(logs, acoustIDCheck)

	hashCheck, err := sampleLookupCheck(ctx, db,
		`SELECT IFNULL(MAX(rowid), 0) FROM sub_fingerprints`,
		`SELECT hash FROM sub_fingerprints WHERE rowid >= ? LIMIT 1`,
		`SELECT COUNT(*) FROM sub_fingerprints WHERE hash = ?`,
		cfg.SampleCount,
		"hash_lookup",
	)
	if err != nil {
		return res, err
	}
	logs = append(logs, hashCheck)

	switch {
	case cfg.FullIntegrityCheck:
		var integrity string
		if err := queryIntegrityCheck(ctx, db, true, &integrity); err != nil {
			return res, err
		}
		logs = append(logs, validationCheck{name: "integrity_check", ok: integrity == "ok", info: integrity})
	case cfg.QuickCheck:
		var integrity string
		if err := queryIntegrityCheck(ctx, db, false, &integrity); err != nil {
			return res, err
		}
		logs = append(logs, validationCheck{name: "quick_check", ok: integrity == "ok", info: integrity})
	default:
		logs = append(logs, validationCheck{name: "integrity_check", ok: true, info: "skipped"})
	}

	res.OK = true
	for _, check := range logs {
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

type validationCheck struct {
	name string
	ok   bool
	info string
}

func queryIntegrityCheck(ctx context.Context, db *sql.DB, full bool, out *string) error {
	pragmaName := "quick_check"
	virtualTableName := "pragma_quick_check"
	if full {
		pragmaName = "integrity_check"
		virtualTableName = "pragma_integrity_check"
	}

	if err := db.QueryRowContext(ctx, `PRAGMA `+pragmaName).Scan(out); err == nil {
		return nil
	}

	rows, err := db.QueryContext(ctx, `SELECT * FROM `+virtualTableName)
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

func tableHasRow(ctx context.Context, db *sql.DB, query string) (bool, error) {
	var present int
	err := db.QueryRowContext(ctx, query).Scan(&present)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func indexExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var present int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1`, name).Scan(&present)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func estimatedRowCountFromStat(ctx context.Context, db *sql.DB, indexName string) (int64, bool, error) {
	statsTableExists, err := tableExists(ctx, db, "sqlite_stat1")
	if err != nil {
		return 0, false, err
	}
	if !statsTableExists {
		return 0, false, nil
	}

	var stat string
	err = db.QueryRowContext(ctx, `SELECT stat FROM sqlite_stat1 WHERE idx = ? LIMIT 1`, indexName).Scan(&stat)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	fields := strings.Fields(stat)
	if len(fields) == 0 {
		return 0, false, nil
	}
	estimate, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, false, err
	}
	return estimate, true, nil
}

func tableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var present int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1`, name).Scan(&present)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func sampleLookupCheck(ctx context.Context, db *sql.DB, maxKeyQuery, sampleQuery, lookupQuery string, sampleCount int, name string) (validationCheck, error) {
	var maxKey int64
	if err := db.QueryRowContext(ctx, maxKeyQuery).Scan(&maxKey); err != nil {
		return validationCheck{}, err
	}

	var samples []any
	for _, target := range sampleTargets(maxKey, sampleCount) {
		var value any
		err := db.QueryRowContext(ctx, sampleQuery, target).Scan(&value)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return validationCheck{}, err
		}
		samples = append(samples, value)
	}

	count := len(samples)
	found := 0
	for _, value := range samples {
		var hits int64
		if err := db.QueryRowContext(ctx, lookupQuery, value).Scan(&hits); err != nil {
			return validationCheck{}, err
		}
		if hits > 0 {
			found++
		}
	}
	return validationCheck{name: name, ok: count == found, info: fmt.Sprintf("%d/%d", found, count)}, nil
}

func sampleTargets(maxKey int64, sampleCount int) []int64 {
	if maxKey <= 0 || sampleCount <= 0 {
		return nil
	}

	targets := make([]int64, 0, sampleCount)
	for i := 1; i <= sampleCount; i++ {
		target := 1 + ((maxKey-1)*int64(i))/int64(sampleCount+1)
		targets = append(targets, target)
	}
	return targets
}

func boolInfo(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
