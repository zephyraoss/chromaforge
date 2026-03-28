package dump

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FileType string

const (
	FileTypeTrack            FileType = "track"
	FileTypeTrackMeta        FileType = "track_meta"
	FileTypeMeta             FileType = "meta"
	FileTypeTrackMBID        FileType = "track_mbid"
	FileTypeTrackFingerprint FileType = "track_fingerprint"
	FileTypeFingerprint      FileType = "fingerprint"
)

var fileOrder = []FileType{
	FileTypeTrack,
	FileTypeTrackMeta,
	FileTypeMeta,
	FileTypeTrackMBID,
	FileTypeTrackFingerprint,
	FileTypeFingerprint,
}

type ArchiveFile struct {
	Name string
	Size int64
	URL  string
	Type FileType
	Day  time.Time
}

type DayFiles struct {
	Day   time.Time
	Files map[FileType]ArchiveFile
}

func (d DayFiles) OrderedFiles() []ArchiveFile {
	out := make([]ArchiveFile, 0, len(fileOrder))
	for _, ft := range fileOrder {
		if file, ok := d.Files[ft]; ok {
			out = append(out, file)
		}
	}
	return out
}

func DiscoverArchive(ctx context.Context, client *http.Client, baseURL string, startYear int, endDate string) ([]DayFiles, error) {
	baseURL = strings.TrimSuffix(baseURL, "/")
	years, err := fetchIndex(ctx, client, baseURL+"/index.json")
	if err != nil {
		return nil, err
	}
	if len(years) == 0 {
		return nil, fmt.Errorf("no archive years found")
	}

	var earliestYear int
	yearNames := make([]int, 0, len(years))
	for _, year := range years {
		y, err := strconv.Atoi(strings.TrimSuffix(year.Name, "/"))
		if err != nil {
			continue
		}
		yearNames = append(yearNames, y)
	}
	sort.Ints(yearNames)
	earliestYear = yearNames[0]
	if startYear == 0 {
		startYear = earliestYear
	}
	log.Printf("archive discovery years=%d start_year=%d", len(yearNames), startYear)

	var cutoff time.Time
	if endDate != "" {
		cutoff, err = time.Parse("2006-01-02", endDate)
		if err != nil {
			return nil, fmt.Errorf("parse --end-date: %w", err)
		}
	}

	days := map[string]DayFiles{}
	for _, year := range yearNames {
		if year < startYear {
			continue
		}
		log.Printf("archive discovery year=%d: fetching month index", year)
		months, err := fetchIndex(ctx, client, fmt.Sprintf("%s/%04d/index.json", baseURL, year))
		if err != nil {
			return nil, err
		}
		log.Printf("archive discovery year=%d: months=%d", year, len(months))
		filteredMonths := filterMonthsByCutoff(months, cutoff)
		monthResults, err := fetchMonthIndexes(ctx, client, baseURL, year, filteredMonths)
		if err != nil {
			return nil, err
		}
		for _, result := range monthResults {
			for _, entry := range result.files {
				ft, day, ok := classifyArchiveFile(entry.Name)
				if !ok {
					continue
				}
				if !cutoff.IsZero() && day.After(cutoff) {
					continue
				}
				key := day.Format("2006-01-02")
				df := days[key]
				if df.Files == nil {
					df = DayFiles{Day: day, Files: map[FileType]ArchiveFile{}}
				}
				df.Files[ft] = ArchiveFile{
					Name: entry.Name,
					Size: entry.Size,
					Type: ft,
					Day:  day,
					URL:  fmt.Sprintf("%s/%04d/%s/%s", baseURL, year, result.monthName, path.Base(entry.Name)),
				}
				days[key] = df
			}
		}
	}

	keys := make([]string, 0, len(days))
	for key := range days {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]DayFiles, 0, len(keys))
	for _, key := range keys {
		out = append(out, days[key])
	}
	return out, nil
}

type monthIndexResult struct {
	monthName string
	files     []ArchiveIndexEntry
}

func fetchMonthIndexes(ctx context.Context, client *http.Client, baseURL string, year int, months []ArchiveIndexEntry) ([]monthIndexResult, error) {
	const workers = 8

	type job struct {
		monthName string
	}
	type result struct {
		monthIndexResult
		err error
	}

	jobs := make(chan job)
	results := make(chan result, len(months))

	var wg sync.WaitGroup
	for i := 0; i < min(workers, len(months)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				log.Printf("archive discovery year=%d month=%s: fetching file index", year, job.monthName)
				files, err := fetchIndex(ctx, client, fmt.Sprintf("%s/%04d/%s/index.json", baseURL, year, job.monthName))
				if err == nil {
					log.Printf("archive discovery year=%d month=%s: files=%d", year, job.monthName, len(files))
				}
				select {
				case results <- result{monthIndexResult: monthIndexResult{monthName: job.monthName, files: files}, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, month := range months {
			monthName := strings.TrimSuffix(month.Name, "/")
			select {
			case jobs <- job{monthName: monthName}:
			case <-ctx.Done():
				return
			}
		}
	}()

	out := make([]monthIndexResult, 0, len(months))
	for range months {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-results:
			if res.err != nil {
				return nil, res.err
			}
			out = append(out, res.monthIndexResult)
		}
	}
	wg.Wait()

	sort.Slice(out, func(i, j int) bool {
		return out[i].monthName < out[j].monthName
	})
	return out, nil
}

func filterMonthsByCutoff(months []ArchiveIndexEntry, cutoff time.Time) []ArchiveIndexEntry {
	if cutoff.IsZero() {
		return months
	}

	cutoffMonth := cutoff.Format("2006-01")
	out := make([]ArchiveIndexEntry, 0, len(months))
	for _, month := range months {
		monthName := strings.TrimSuffix(month.Name, "/")
		if monthName > cutoffMonth {
			continue
		}
		out = append(out, month)
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func classifyArchiveFile(name string) (FileType, time.Time, bool) {
	parts := strings.SplitN(name, "-", 4)
	if len(parts) < 4 {
		return "", time.Time{}, false
	}
	day, err := time.Parse("2006-01-02", strings.Join(parts[:3], "-"))
	if err != nil {
		return "", time.Time{}, false
	}

	switch {
	case strings.HasSuffix(name, "-track-update.jsonl.gz"):
		return FileTypeTrack, day, true
	case strings.HasSuffix(name, "-track_meta-update.jsonl.gz"):
		return FileTypeTrackMeta, day, true
	case strings.HasSuffix(name, "-meta-update.jsonl.gz"):
		return FileTypeMeta, day, true
	case strings.HasSuffix(name, "-track_mbid-update.jsonl.gz"):
		return FileTypeTrackMBID, day, true
	case strings.HasSuffix(name, "-track_fingerprint-update.jsonl.gz"):
		return FileTypeTrackFingerprint, day, true
	case strings.HasSuffix(name, "-fingerprint-update.jsonl.gz"):
		return FileTypeFingerprint, day, true
	default:
		return "", time.Time{}, false
	}
}

func ClassifyArchiveFileForTest(name string) (FileType, time.Time, bool) {
	return classifyArchiveFile(name)
}

func fetchIndex(ctx context.Context, client *http.Client, url string) ([]ArchiveIndexEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "chromaforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s: unexpected status %s", url, resp.Status)
	}

	var entries []ArchiveIndexEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("decode %s: %w", url, err)
	}
	return entries, nil
}
