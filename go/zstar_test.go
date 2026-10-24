package zstar

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func mustObjectSource(t *testing.T, address string) ExtentSource {
	t.Helper()
	source, err := ParseObjectSource(address)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustTestValue[T any](t testing.TB, value any) T {
	t.Helper()
	result, ok := value.(T)
	if !ok {
		t.Fatalf("expected %T, got %T", result, value)
	}
	return result
}

func mustTestWriter(t testing.TB, destination io.Writer, options WriterOptions) *Writer {
	t.Helper()
	writer, err := NewWriter(destination, options)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

type memoryRange []byte

func (m memoryRange) Size(context.Context) (int64, error) { return int64(len(m)), nil }
func (m memoryRange) ReadRange(_ context.Context, o, l int64) ([]byte, error) {
	return append([]byte(nil), m[o:o+l]...), nil
}

func TestHTTPRangeReaderRequiresExactPartialContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=2-4" {
			t.Errorf("range header: %q", r.Header.Get("Range"))
		}
		if w.Header().Get("ETag") == "" {
			w.Header().Set("ETag", `"test-snapshot"`)
		}
		w.Header().Set("Content-Range", "bytes 2-4/9")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = fmt.Fprint(w, "cde") //nolint:errcheck // A client rejecting the response may close its connection.
	}))
	defer srv.Close()
	reader := &HTTPRangeReader{URL: srv.URL}
	b, err := reader.ReadRange(context.Background(), 2, 3)
	if err != nil || string(b) != "cde" {
		t.Fatalf("ReadRange = %q, %v", b, err)
	}
}

func TestParseFooter(t *testing.T) {
	b := make([]byte, 80)
	b[0] = 0x5f
	b[1] = 0x2a
	b[2] = 0x4d
	b[3] = 0x18
	b[4] = 72
	copy(b[8:16], "ZSTAR001")
	b[16] = 1
	if _, err := ParseFooter(b); err == nil {
		t.Fatal("expected bounds error")
	}
}

func TestHTTPSizeUsesGETAndCachesSuccess(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Range") != "bytes=0-0" {
			t.Errorf("wrong probe %s %s", r.Method, r.Header.Get("Range"))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if w.Header().Get("ETag") == "" {
			w.Header().Set("ETag", `"test-snapshot"`)
		}
		w.Header().Set("Content-Range", "bytes 0-0/1000")
		w.WriteHeader(http.StatusPartialContent)
		if _, err := w.Write([]byte("x")); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	reader := &HTTPRangeReader{URL: server.URL}
	for range 2 {
		if size, err := reader.Size(context.Background()); err != nil || size != 1000 {
			t.Fatalf("size %d: %v", size, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("size probed %d times", calls.Load())
	}
}

func checkCleanup(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Error(err)
	}
}
func testClose(t testing.TB, closer io.Closer) { t.Helper(); checkCleanup(t, closer.Close()) }
