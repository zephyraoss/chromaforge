package dump

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxDownloadAttempts = 5
	initialRetryBackoff = time.Second
	maxRetryBackoff     = 10 * time.Second
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
	if info, err := os.Stat(tmp); err == nil && wantSize > 0 && info.Size() == wantSize {
		if err := os.Rename(tmp, dst); err == nil {
			log.Printf("cache hit from partial for %s (%d bytes)", filepath.Base(dst), info.Size())
			return nil
		}
	}

	backoff := initialRetryBackoff
	for attempt := 1; attempt <= maxDownloadAttempts; attempt++ {
		retryable, err := downloadAttempt(ctx, client, url, dst, tmp, wantSize)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable || attempt == maxDownloadAttempts {
			return err
		}

		log.Printf("download retry attempt=%d/%d url=%s err=%v", attempt, maxDownloadAttempts, url, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxRetryBackoff {
			backoff *= 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
		}
	}

	return fmt.Errorf("download %s: exhausted retries", url)
}

func downloadAttempt(ctx context.Context, client *http.Client, url, dst, tmp string, wantSize int64) (bool, error) {
	resumeFrom, err := currentPartialSize(tmp, wantSize)
	if err != nil {
		return false, err
	}

	logStarted(url, resumeFrom)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "chromaforge/1")
	if resumeFrom > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeFrom))
	}

	resp, err := client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		if resumeFrom > 0 {
			resumeFrom = 0
		}
	case http.StatusPartialContent:
		if resumeFrom == 0 {
			resumeFrom = 0
		}
	case http.StatusRequestedRangeNotSatisfiable:
		if wantSize > 0 {
			if info, err := os.Stat(tmp); err == nil && info.Size() == wantSize {
				if err := os.Rename(tmp, dst); err == nil {
					log.Printf("download completed: %s (%d bytes)", url, info.Size())
					return false, nil
				}
			}
		}
		_ = os.Remove(tmp)
		return true, fmt.Errorf("download %s: range rejected, restarting", url)
	default:
		if shouldRetryStatus(resp.StatusCode) {
			return true, fmt.Errorf("download %s: unexpected status %s", url, resp.Status)
		}
		return false, fmt.Errorf("download %s: unexpected status %s", url, resp.Status)
	}

	file, err := openPartialFile(tmp, resumeFrom, resp.StatusCode == http.StatusPartialContent)
	if err != nil {
		return false, err
	}

	n, copyErr := io.Copy(file, resp.Body)
	closeErr := file.Close()
	if copyErr != nil {
		return isRetryableBodyError(copyErr), copyErr
	}
	if closeErr != nil {
		return true, closeErr
	}

	total := resumeFrom + n
	if wantSize > 0 && total != wantSize {
		return true, fmt.Errorf("download %s: got %d bytes, want %d", url, total, wantSize)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return false, err
	}

	log.Printf("download completed: %s (%d bytes)", url, total)
	return false, nil
}

func currentPartialSize(tmp string, wantSize int64) (int64, error) {
	info, err := os.Stat(tmp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if wantSize > 0 && info.Size() > wantSize {
		if err := os.Remove(tmp); err != nil {
			return 0, err
		}
		return 0, nil
	}
	return info.Size(), nil
}

func openPartialFile(tmp string, resumeFrom int64, appendMode bool) (*os.File, error) {
	if appendMode && resumeFrom > 0 {
		return os.OpenFile(tmp, os.O_WRONLY|os.O_APPEND, 0o644)
	}
	return os.Create(tmp)
}

func logStarted(url string, resumeFrom int64) {
	if resumeFrom > 0 {
		log.Printf("download resumed: %s from byte %d", url, resumeFrom)
		return
	}
	log.Printf("download started: %s", url)
}

func shouldRetryStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func isRetryableBodyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if errors.Is(urlErr.Err, context.Canceled) || errors.Is(urlErr.Err, context.DeadlineExceeded) {
			return false
		}
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "stream error") ||
		strings.Contains(msg, "internal_error") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "server closed idle connection")
}
