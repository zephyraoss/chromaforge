package dump

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

func ScanGzipLines(ctx context.Context, path string, fn func([]byte) error) error {
	return ScanGzipLinesCounted(ctx, path, nil, fn)
}

func ScanGzipLinesCounted(ctx context.Context, path string, compressedBytesRead *atomic.Int64, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if compressedBytesRead != nil {
		r = &countingReader{r: f, n: compressedBytesRead}
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()

	scanner := bufio.NewScanner(gz)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := append([]byte(nil), scanner.Bytes()...)
		if err := fn(line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}
	return nil
}

func DecodeJSONLine[T any](line []byte, out *T) error {
	if err := json.Unmarshal(line, out); err != nil {
		return fmt.Errorf("decode json line: %w", err)
	}
	return nil
}
