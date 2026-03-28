package dump

import "time"

type FingerprintUpdate struct {
	ID          int64   `json:"id"`
	Fingerprint []int64 `json:"fingerprint"`
	Length      int     `json:"length"`
	Created     string  `json:"created"`
}

type TrackUpdate struct {
	ID      int64  `json:"id"`
	GID     string `json:"gid"`
	Created string `json:"created"`
}

type TrackMetaUpdate struct {
	ID          int64  `json:"id"`
	TrackID     int64  `json:"track_id"`
	Track       string `json:"track"`
	Artist      string `json:"artist"`
	Album       string `json:"album"`
	AlbumArtist string `json:"album_artist"`
	TrackNo     int    `json:"track_no"`
	Created     string `json:"created"`
	Updated     string `json:"updated"`
}

type MetaUpdate struct {
	ID      int64  `json:"id"`
	Track   string `json:"track"`
	Artist  string `json:"artist"`
	Created string `json:"created"`
}

type TrackMBIDUpdate struct {
	ID              int64  `json:"id"`
	TrackID         int64  `json:"track_id"`
	MBID            string `json:"mbid"`
	SubmissionCount int    `json:"submission_count"`
	Disabled        bool   `json:"disabled"`
	Created         string `json:"created"`
	Updated         string `json:"updated"`
}

type TrackFingerprintUpdate struct {
	ID              int64  `json:"id"`
	TrackID         int64  `json:"track_id"`
	FingerprintID   int64  `json:"fingerprint_id"`
	SubmissionCount int    `json:"submission_count"`
	Created         string `json:"created"`
}

type ArchiveIndexEntry struct {
	Name string `json:"name"`
	Size int64  `json:"size,omitempty"`
}

func ParseDate(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, &time.ParseError{Layout: "2006-01-02", Value: value, LayoutElem: "2006", ValueElem: value}
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, value)
}
