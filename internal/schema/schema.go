package schema

const (
	CreateFingerprintsTable = `
CREATE TABLE IF NOT EXISTS fingerprints (
    id       INTEGER PRIMARY KEY,
    acoustid TEXT NOT NULL,
    mb_id    TEXT,
    title    TEXT,
    artist   TEXT,
    duration INTEGER
);`

	CreateSubFingerprintsTable = `
CREATE TABLE IF NOT EXISTS sub_fingerprints (
    hash           INTEGER NOT NULL,
    fingerprint_id INTEGER NOT NULL,
	    position       INTEGER NOT NULL
);`

	CreateAcoustIDIndex = `CREATE UNIQUE INDEX IF NOT EXISTS idx_fingerprints_acoustid ON fingerprints(acoustid);`
	CreateHashIndex     = `CREATE INDEX IF NOT EXISTS idx_hash ON sub_fingerprints(hash);`
	AnalyzeSQL          = `ANALYZE;`
)
