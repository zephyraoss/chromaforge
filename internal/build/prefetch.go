package build

import (
	"context"
	"log"
	"path/filepath"
	"sort"
	"sync"

	"github.com/zephyraoss/chromaforge/internal/dump"
)

const (
	prefetchWindowDays      = 2
	prefetchDownloadWorkers = 4
)

type prefetchDownloader struct {
	ctx   context.Context
	base  DownloadClient
	jobs  chan *downloadTask
	tasks map[string]*downloadTask

	mu        sync.Mutex
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type downloadTask struct {
	file dump.ArchiveFile
	dst  string

	done      chan struct{}
	err       error
	queueOnce sync.Once
	doneOnce  sync.Once
}

func newPrefetchDownloader(ctx context.Context, base DownloadClient, workers int) *prefetchDownloader {
	if workers <= 0 {
		workers = 1
	}

	d := &prefetchDownloader{
		ctx:   ctx,
		base:  base,
		jobs:  make(chan *downloadTask, workers*4),
		tasks: make(map[string]*downloadTask),
	}

	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for task := range d.jobs {
				task.finish(d.base.Ensure(d.ctx, task.file, task.dst))
			}
		}()
	}

	return d
}

func (d *prefetchDownloader) Close() {
	d.closeOnce.Do(func() {
		close(d.jobs)
		d.wg.Wait()
	})
}

func (d *prefetchDownloader) Ensure(ctx context.Context, file dump.ArchiveFile, dst string) error {
	task := d.taskFor(file, dst)
	task.enqueue(d)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-task.done:
		return task.err
	}
}

func (d *prefetchDownloader) PrefetchDay(day dump.DayFiles, cacheDir string) {
	files := append([]dump.ArchiveFile(nil), day.OrderedFiles()...)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Size > files[j].Size
	})

	log.Printf("download prefetch day=%s files=%d", day.Day.Format("2006-01-02"), len(files))
	for _, file := range files {
		dst := filepath.Join(cacheDir, day.Day.Format("2006-01"), file.Name)
		d.taskFor(file, dst).enqueue(d)
	}
}

func (d *prefetchDownloader) taskFor(file dump.ArchiveFile, dst string) *downloadTask {
	d.mu.Lock()
	defer d.mu.Unlock()

	if task, ok := d.tasks[dst]; ok {
		return task
	}

	task := &downloadTask{
		file: file,
		dst:  dst,
		done: make(chan struct{}),
	}
	d.tasks[dst] = task
	return task
}

func (t *downloadTask) enqueue(d *prefetchDownloader) {
	t.queueOnce.Do(func() {
		select {
		case d.jobs <- t:
		case <-d.ctx.Done():
			t.finish(d.ctx.Err())
		}
	})
}

func (t *downloadTask) finish(err error) {
	t.doneOnce.Do(func() {
		t.err = err
		close(t.done)
	})
}
