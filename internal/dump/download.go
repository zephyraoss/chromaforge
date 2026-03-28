package dump

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func DownloadFile(ctx context.Context, client *http.Client, url, dst string, wantSize int64) error {
	if info, err := os.Stat(dst); err == nil && (wantSize == 0 || info.Size() == wantSize) {
		log.Printf("cache hit for %s (%d bytes)", filepath.Base(dst), info.Size())
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	tmp := dst + ".part"
	_ = os.Remove(tmp)

	log.Printf("download started: %s", url)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "chromaforge/1")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: unexpected status %s", url, resp.Status)
	}

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	n, copyErr := io.Copy(f, resp.Body)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if wantSize > 0 && n != wantSize {
		_ = os.Remove(tmp)
		return fmt.Errorf("download %s: got %d bytes, want %d", url, n, wantSize)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	log.Printf("download completed: %s (%d bytes)", url, n)
	return nil
}
