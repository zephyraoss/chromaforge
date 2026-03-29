package build

const metadataMergeUpdateSQL = `
UPDATE fingerprints
SET
	mb_id = CASE
		WHEN COALESCE(mb_id, '') = '' AND ? IS NOT NULL THEN ?
		ELSE mb_id
	END,
	duration = CASE
		WHEN COALESCE(duration, 0) <= 0 AND ? > 0 THEN ?
		ELSE duration
	END
WHERE acoustid = ?
`

func metadataMergeArgs(r Record) []any {
	mbid := nullIfEmpty(r.MBID)
	return []any{
		mbid, mbid,
		r.Duration, r.Duration,
		r.AcoustID,
	}
}
