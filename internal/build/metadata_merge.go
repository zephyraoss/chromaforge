package build

const metadataMergeUpdateSQL = `
UPDATE fingerprints
SET
	mb_id = CASE
		WHEN COALESCE(mb_id, '') = '' AND ? IS NOT NULL THEN ?
		ELSE mb_id
	END,
	title = CASE
		WHEN COALESCE(title, '') = '' AND ? IS NOT NULL THEN ?
		ELSE title
	END,
	artist = CASE
		WHEN COALESCE(artist, '') = '' AND ? IS NOT NULL THEN ?
		ELSE artist
	END,
	duration = CASE
		WHEN COALESCE(duration, 0) <= 0 AND ? > 0 THEN ?
		ELSE duration
	END
WHERE acoustid = ?
`

func metadataMergeArgs(r Record) []any {
	mbid := nullIfEmpty(r.MBID)
	title := nullIfEmpty(r.Title)
	artist := nullIfEmpty(r.Artist)
	return []any{
		mbid, mbid,
		title, title,
		artist, artist,
		r.Duration, r.Duration,
		r.AcoustID,
	}
}
