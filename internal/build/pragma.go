package build

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

const (
	defaultBuildCacheSizeBytes int64 = 1 << 30
	defaultBuildMmapSizeBytes  int64 = 1 << 30
	defaultIndexCacheSizeBytes int64 = 2 << 30
	defaultIndexMmapSizeBytes  int64 = 2 << 30
)

func ApplyBuildPragmas(ctx context.Context, db *sql.DB, cacheSizeBytes, mmapSizeBytes int64) error {
	pragmas := []string{
		`PRAGMA page_size = 32768;`,
		`PRAGMA synchronous = OFF;`,
		`PRAGMA locking_mode = EXCLUSIVE;`,
		`PRAGMA busy_timeout = 250;`,
		`PRAGMA cache_spill = OFF;`,
		pragmaCacheSizeBytes(resolvePragmaBytes(cacheSizeBytes, defaultBuildCacheSizeBytes)),
		`PRAGMA temp_store = MEMORY;`,
		pragmaMmapSizeBytes(resolvePragmaBytes(mmapSizeBytes, defaultBuildMmapSizeBytes)),
	}
	if mode, err := queryPragmaText(ctx, db, `PRAGMA journal_mode = OFF;`); err != nil {
		return err
	} else if !strings.EqualFold(mode, "off") {
		return fmt.Errorf("failed to disable journaling for bulk load, got %q", mode)
	}
	for _, stmt := range pragmas {
		if err := execPragma(ctx, db, stmt); err != nil {
			log.Printf("pragma warning stmt=%q err=%v", stmt, err)
		}
	}
	return nil
}

func ApplyIndexPragmas(ctx context.Context, db *sql.DB, threads int, cacheSizeBytes, mmapSizeBytes int64) error {
	pragmas := []string{
		`PRAGMA threads = ` + itoa(threads) + `;`,
		`PRAGMA temp_store = FILE;`,
		`PRAGMA cache_spill = ON;`,
		pragmaCacheSizeBytes(resolvePragmaBytes(cacheSizeBytes, defaultIndexCacheSizeBytes)),
		pragmaMmapSizeBytes(resolvePragmaBytes(mmapSizeBytes, defaultIndexMmapSizeBytes)),
	}
	for _, stmt := range pragmas {
		if err := execPragma(ctx, db, stmt); err != nil {
			log.Printf("pragma warning stmt=%q err=%v", stmt, err)
		}
	}
	return nil
}

func ApplyFinalizePragmas(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		`PRAGMA synchronous = NORMAL;`,
		`PRAGMA locking_mode = NORMAL;`,
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA wal_autocheckpoint = 10000;`,
	}
	if mode, err := queryPragmaText(ctx, db, `PRAGMA journal_mode = WAL;`); err != nil {
		return err
	} else if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("failed to enable WAL mode for finalized database, got %q", mode)
	}
	for _, stmt := range pragmas {
		if err := execPragma(ctx, db, stmt); err != nil {
			log.Printf("pragma warning stmt=%q err=%v", stmt, err)
		}
	}
	return nil
}

func CheckpointWAL(ctx context.Context, db *sql.DB) error {
	return execPragma(ctx, db, `PRAGMA wal_checkpoint(TRUNCATE);`)
}

func resolvePragmaBytes(requested, defaultValue int64) int64 {
	if requested > 0 {
		return requested
	}
	return defaultValue
}

func pragmaCacheSizeBytes(bytes int64) string {
	kib := bytes / 1024
	if bytes%1024 != 0 {
		kib++
	}
	if kib <= 0 {
		kib = 1
	}
	return fmt.Sprintf(`PRAGMA cache_size = -%d;`, kib)
}

func pragmaMmapSizeBytes(bytes int64) string {
	if bytes <= 0 {
		bytes = 1
	}
	return fmt.Sprintf(`PRAGMA mmap_size = %d;`, bytes)
}

func execPragma(ctx context.Context, db *sql.DB, stmt string) error {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return err
	}
	defer rows.Close()

	return discardRows(rows)
}

func queryPragmaText(ctx context.Context, db *sql.DB, stmt string) (string, error) {
	var value string
	if err := db.QueryRowContext(ctx, stmt).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func discardRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return rows.Err()
	}

	values := make([]any, len(cols))
	args := make([]any, len(cols))
	for i := range values {
		args[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(args...); err != nil {
			return err
		}
	}
	return rows.Err()
}
