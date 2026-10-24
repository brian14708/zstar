package zstar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRefreshingHTTPSizeAndAuthentication(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Range") != "bytes=0-0" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("invalid signed probe")
		}
		if r.URL.RawQuery == "expired" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if w.Header().Get("ETag") == "" {
			w.Header().Set("ETag", `"test-snapshot"`)
		}
		w.Header().Set("Content-Range", "bytes 0-0/42")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x")) //nolint:errcheck // A client rejecting the response may close its connection.
	}))
	defer server.Close()
	reader := &RefreshingRangeReader{Provider: func(_ context.Context, _ string, _, _ int64, force bool) (string, http.Header, error) {
		call := calls.Add(1)
		if call == 1 && force {
			t.Fatal("initial request asked to force refresh")
		}
		if call == 2 && !force {
			t.Fatal("authorization retry did not force refresh")
		}
		if call == 1 {
			return server.URL + "?expired", nil, nil
		}
		return server.URL, nil, nil
	}}
	for range 2 {
		size, err := reader.Size(context.Background())
		if err != nil || size != 42 {
			t.Fatalf("size=%d, err=%v", size, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("provider calls=%d", calls.Load())
	}
}

func TestHTTPDoesNotRefreshProtocolOrServerFailures(t *testing.T) {
	for _, status := range []int{200, 302, 404, 429, 500} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			calls := 0
			reader := &RefreshingRangeReader{Provider: func(context.Context, string, int64, int64, bool) (string, http.Header, error) {
				calls++
				return server.URL, nil, nil
			}}
			if _, err := reader.Size(context.Background()); err == nil || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestHTTPRangeProtocolAndSizePinning(t *testing.T) {
	for _, kind := range []string{"encoding", "noncanonical", "oversized", "short", "size-change"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				cr, body := "bytes 0-0/8", "x"
				if calls > 1 {
					switch kind {
					case "encoding":
						w.Header().Set("Content-Encoding", "gzip")
					case "noncanonical":
						cr = "bytes 0-0/08"
					case "oversized":
						body = "xx"
					case "short":
						body = ""
					case "size-change":
						cr = "bytes 0-0/9"
					}
				}
				if w.Header().Get("ETag") == "" {
					w.Header().Set("ETag", `"test-snapshot"`)
				}
				w.Header().Set("Content-Range", cr)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, body) //nolint:errcheck // A client rejecting the response may close its connection.
			}))
			defer server.Close()
			reader := &HTTPRangeReader{URL: server.URL}
			if _, err := reader.Size(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.ReadRange(context.Background(), 0, 1); err == nil {
				t.Fatal("accepted invalid response")
			}
		})
	}
}

func TestHTTPRedactionRedirectsBudgetAndCancellation(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	reader := &HTTPRangeReader{URL: server.URL + "?secret", Headers: http.Header{"Authorization": {"secret"}}}
	if _, err := reader.Size(context.Background()); err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", reader, reader), "secret") {
		t.Fatal("debug leaks credentials")
	}
	reader.URL = ":bad?secret"
	if _, err := reader.Size(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("URL error leaks credentials")
	}
	if _, err := reader.ReadRange(context.Background(), 0, 1<<62); err == nil {
		t.Fatal("missing allocation budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.ReadRange(ctx, 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	provider := &RefreshingRangeReader{Provider: func(context.Context, string, int64, int64, bool) (string, http.Header, error) {
		return "", nil, errors.New("secret signed URL")
	}}
	if _, err := provider.Size(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("provider error leaks")
	}
}

func TestPresignedPutRefreshesReplaysExactBytesAndSupportsEmptyBody(t *testing.T) {
	var calls atomic.Int32
	var uploaded [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Header.Get("X-Signed") != "yes" {
			t.Error("signed PUT method or headers missing")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		uploaded = append(uploaded, body)
		if r.URL.Query().Get("expired") == "1" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	provider := func(_ context.Context, force bool) (string, http.Header, error) {
		call := calls.Add(1)
		if force != (call > 1) {
			t.Fatalf("call %d forceRefresh=%v", call, force)
		}
		url := server.URL
		if call == 1 {
			url += "?expired=1"
		}
		return url, http.Header{"X-Signed": {"yes"}}, nil
	}
	if err := PutPresigned(context.Background(), provider, nil, []byte("same bytes")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(uploaded) != 2 || string(uploaded[0]) != "same bytes" || string(uploaded[1]) != "same bytes" {
		t.Fatalf("calls=%d uploaded=%q", calls.Load(), uploaded)
	}
	if err := PutPresigned(context.Background(), func(_ context.Context, force bool) (string, http.Header, error) {
		if force {
			t.Fatal("empty successful upload unexpectedly refreshed")
		}
		return server.URL, http.Header{"X-Signed": {"yes"}}, nil
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(uploaded[2]) != 0 {
		t.Fatalf("empty upload body = %q", uploaded[2])
	}
}

func TestPresignedPutRedactsProviderAndTransportErrorsAndRejectsRedirects(t *testing.T) {
	if err := PutPresigned(context.Background(), func(context.Context, bool) (string, http.Header, error) {
		return "", nil, errors.New("https://secret.invalid/?token=secret")
	}, nil, []byte("x")); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("provider error not redacted: %v", err)
	}
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	err := PutPresigned(context.Background(), func(context.Context, bool) (string, http.Header, error) {
		return server.URL + "?secret=secret", nil, nil
	}, nil, []byte("x"))
	if err == nil || strings.Contains(err.Error(), "secret") || redirected.Load() != 0 {
		t.Fatalf("redirect or secret leaked: err=%v followed=%d", err, redirected.Load())
	}
}

func TestPresignedPutOnlyRetriesAuthenticationOnce(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 500} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			calls := 0
			err := PutPresigned(context.Background(), func(_ context.Context, force bool) (string, http.Header, error) {
				if force != (calls == 1) {
					t.Errorf("call %d force=%v", calls, force)
				}
				calls++
				return server.URL, nil, nil
			}, nil, nil)
			want := 1
			if status == 401 || status == 403 {
				want = 2
			}
			if err == nil || calls != want {
				t.Fatalf("calls=%d want=%d err=%v", calls, want, err)
			}
		})
	}
}

func TestPresignedPutCancellationAndBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := func(context.Context, bool) (string, http.Header, error) {
		t.Error("provider called for a rejected upload")
		return "", nil, nil
	}
	if err := PutPresigned(ctx, provider, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := PutPresigned(context.Background(), provider, nil, make([]byte, (64<<20)+1)); err == nil {
		t.Fatal("missing upload budget")
	}

	started := make(chan struct{})
	disconnected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) //nolint:errcheck // The cancellation test intentionally interrupts request-body consumption.
		close(started)
		<-r.Context().Done()
		close(disconnected)
	}))
	defer server.Close()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- PutPresigned(ctx, func(context.Context, bool) (string, http.Header, error) {
			return server.URL, nil, nil
		}, nil, []byte("body"))
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PUT did not cancel")
	}
	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("PUT request survived cancellation")
	}
}

type mutableObject struct {
	mu      sync.Mutex
	data    string
	etag    string
	modTime time.Time
}

func (o *mutableObject) replace(data, etag string, mod time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data, o.etag, o.modTime = data, etag, mod
}

func (o *mutableObject) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	data, etag, mod := o.data, o.etag, o.modTime
	o.mu.Unlock()
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	http.ServeContent(w, r, "x", mod, strings.NewReader(data))
}

func TestHTTPSameSizeReplacementDetected(t *testing.T) {
	obj := &mutableObject{data: "aaaaaaaa", etag: `"v1"`}
	server := httptest.NewServer(obj)
	defer server.Close()
	reader := &HTTPRangeReader{URL: server.URL}
	if got, err := reader.ReadRange(context.Background(), 0, 4); err != nil || string(got) != "aaaa" {
		t.Fatalf("first read: %q %v", got, err)
	}
	obj.replace("bbbbbbbb", `"v2"`, time.Time{})
	if _, err := reader.ReadRange(context.Background(), 4, 4); !errors.Is(err, ErrObjectChanged) {
		t.Fatalf("replacement not detected: %v", err)
	}
}

func TestHTTPRequiresStrongSnapshotIdentity(t *testing.T) {
	for _, tag := range []string{"", `W/"same"`, "unquoted", `"embedded"quote"`} {
		t.Run(tag, func(t *testing.T) {
			obj := &mutableObject{data: "abcdefgh", etag: tag, modTime: time.Now().Add(-time.Hour)}
			server := httptest.NewServer(obj)
			defer server.Close()
			reader := &HTTPRangeReader{URL: server.URL}
			for range 2 {
				if _, err := reader.Size(context.Background()); err == nil || !strings.Contains(err.Error(), "strong ETag") {
					t.Fatalf("accepted weak identity: %v", err)
				}
			}
		})
	}
}

func TestHTTPNoClientWideTimeout(t *testing.T) {
	if rangeClient(nil).Timeout != 0 {
		t.Fatal("whole-request timeout installed")
	}
}
