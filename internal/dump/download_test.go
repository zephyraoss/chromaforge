package dump

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestDownloadFileRetriesRetryableStatus(t *testing.T) {
	var requests atomic.Int32
	content := []byte("hello, retry")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	dst := filepath.Join(t.TempDir(), "archive.gz")
	if err := DownloadFile(context.Background(), srv.Client(), srv.URL, dst, int64(len(content))); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded content mismatch: got %q want %q", got, content)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}

func TestDownloadFileRetriesAndResumesPartial(t *testing.T) {
	content := bytes.Repeat([]byte("abcdef"), 4096)
	half := len(content) / 2
	var requests atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqNum := requests.Add(1)
		if reqNum == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("response writer does not support hijacking")
				return
			}
			conn, bufrw, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack failed: %v", err)
				return
			}
			defer conn.Close()

			_, _ = fmt.Fprintf(bufrw, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", len(content))
			_, _ = bufrw.Write(content[:half])
			_ = bufrw.Flush()
			return
		}

		wantRange := fmt.Sprintf("bytes=%d-", half)
		if got := r.Header.Get("Range"); got != wantRange {
			t.Errorf("range header = %q, want %q", got, wantRange)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(content)-half))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", half, len(content)-1, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[half:])
	}))
	defer srv.Close()

	dst := filepath.Join(t.TempDir(), "archive.gz")
	if err := DownloadFile(context.Background(), srv.Client(), srv.URL, dst, int64(len(content))); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded content mismatch: got %d bytes want %d", len(got), len(content))
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}
