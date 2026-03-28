package build

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

type blockingDownloadClient struct {
	mu      sync.Mutex
	calls   map[string]int
	started chan string
	release chan struct{}
}

func (c *blockingDownloadClient) Ensure(ctx context.Context, file dump.ArchiveFile, dst string) error {
	c.mu.Lock()
	c.calls[dst]++
	c.mu.Unlock()

	select {
	case c.started <- dst:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
		return nil
	}
}

func TestPrefetchDownloaderReusesInflightDownload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := &blockingDownloadClient{
		calls:   map[string]int{},
		started: make(chan string, 1),
		release: make(chan struct{}),
	}
	prefetch := newPrefetchDownloader(ctx, base, 1)
	defer prefetch.Close()

	day := dump.DayFiles{
		Day: mustDay("2026-03-26"),
		Files: map[dump.FileType]dump.ArchiveFile{
			dump.FileTypeFingerprint: {
				Name: "2026-03-26-fingerprint-update.jsonl.gz",
				Type: dump.FileTypeFingerprint,
				Size: 123,
				URL:  "https://example.test/fingerprint.gz",
				Day:  mustDay("2026-03-26"),
			},
		},
	}
	file := day.Files[dump.FileTypeFingerprint]
	dst := filepath.Join(t.TempDir(), "2026-03", file.Name)

	prefetch.PrefetchDay(day, filepath.Dir(filepath.Dir(dst)))

	select {
	case startedDst := <-base.started:
		if startedDst != dst {
			t.Fatalf("started dst = %q, want %q", startedDst, dst)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	done := make(chan error, 1)
	go func() {
		done <- prefetch.Ensure(ctx, file, dst)
	}()

	close(base.release)

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	base.mu.Lock()
	defer base.mu.Unlock()
	if got := base.calls[dst]; got != 1 {
		t.Fatalf("download calls = %d, want 1", got)
	}
}
