package dump

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func writeGzipLines(t *testing.T, path string, lines int) int64 {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for i := 0; i < lines; i++ {
		fmt.Fprintf(gz, "{\"id\":%d}\n", i)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return int64(buf.Len())
}

func TestScanGzipLinesCountedReportsCompressedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lines.jsonl.gz")
	compressedSize := writeGzipLines(t, path, 500)

	var bytesRead atomic.Int64
	var lines int
	if err := ScanGzipLinesCounted(context.Background(), path, &bytesRead, func([]byte) error {
		lines++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if lines != 500 {
		t.Fatalf("lines = %d, want 500", lines)
	}
	if got := bytesRead.Load(); got != compressedSize {
		t.Fatalf("bytesRead = %d, want %d", got, compressedSize)
	}
}

func TestScanGzipLinesCountedNilCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lines.jsonl.gz")
	writeGzipLines(t, path, 3)

	var lines int
	if err := ScanGzipLinesCounted(context.Background(), path, nil, func([]byte) error {
		lines++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if lines != 3 {
		t.Fatalf("lines = %d, want 3", lines)
	}
}
