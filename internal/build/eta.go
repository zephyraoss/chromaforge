package build

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

var replayedFileTypes = []dump.FileType{
	dump.FileTypeTrack,
	dump.FileTypeTrackMBID,
	dump.FileTypeTrackFingerprint,
	dump.FileTypeFingerprint,
}

func replayableBytes(day dump.DayFiles) int64 {
	var total int64
	for _, ft := range replayedFileTypes {
		if file, ok := day.Files[ft]; ok {
			total += file.Size
		}
	}
	return total
}

func totalReplayableBytes(days []dump.DayFiles) int64 {
	var total int64
	for _, day := range days {
		total += replayableBytes(day)
	}
	return total
}

type progressTracker struct {
	start          time.Time
	totalBytes     int64
	completedBytes atomic.Int64
	currentBytes   atomic.Int64
}

func newProgressTracker(totalBytes int64, start time.Time) *progressTracker {
	return &progressTracker{start: start, totalBytes: totalBytes}
}

func (t *progressTracker) CurrentFileBytes() *atomic.Int64 {
	if t == nil {
		return nil
	}
	return &t.currentBytes
}

func (t *progressTracker) FileCompleted(size int64) {
	if t == nil {
		return
	}
	t.currentBytes.Store(0)
	t.completedBytes.Add(size)
}

func (t *progressTracker) snapshot(now time.Time) (percent float64, eta time.Duration, ok bool) {
	if t == nil || t.totalBytes <= 0 {
		return 0, 0, false
	}
	consumed := t.completedBytes.Load() + t.currentBytes.Load()
	if consumed <= 0 {
		return 0, 0, false
	}
	if consumed > t.totalBytes {
		consumed = t.totalBytes
	}
	percent = float64(consumed) / float64(t.totalBytes) * 100
	elapsed := now.Sub(t.start)
	if elapsed <= 0 {
		return 0, 0, false
	}
	bytesPerSecond := float64(consumed) / elapsed.Seconds()
	eta = time.Duration(float64(t.totalBytes-consumed) / bytesPerSecond * float64(time.Second))
	return percent, eta, true
}

func formatETA(d time.Duration) string {
	if d < time.Minute {
		return "<1m"
	}
	d = d.Round(time.Minute)
	hours := int(d / time.Hour)
	minutes := int(d % time.Hour / time.Minute)
	if hours > 0 {
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
