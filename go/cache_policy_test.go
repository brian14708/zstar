package zstar

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrivateContentCachePolicy(t *testing.T) {
	for _, control := range []string{"no-store", "No-Cache=\"Set-Cookie\"", "max-age=0", "max-age=oops", "max-age=60, max-age=120"} {
		if NewContentCachePolicy(control).AllowsReuse() {
			t.Fatalf("allowed %s", control)
		}
	}
	if !NewContentCachePolicy("private, max-age=60, immutable, s-maxage=0").AllowsReuse() {
		t.Fatal("private immutable policy denied")
	}
	for _, h := range []http.Header{
		{"Cache-Control": {"max-age=60"}, "Age": {"60"}},
		{"Cache-Control": {"max-age=60", "no-store"}},
		{"Cache-Control": {"max-age=60"}, "Date": {"Wed, 21 Oct 2015 07:28:00 GMT"}},
		{"Expires": {"Wed, 21 Oct 2015 07:29:00 GMT"}},
		{"Vary": {"*"}},
	} {
		if responseContentCachePolicy(h, time.Now()).AllowsReuse() {
			t.Fatalf("allowed %v", h)
		}
	}
	policy := NewContentCachePolicy("max-age=60")
	policy.expires = time.Now()
	if policy.AllowsReuse() {
		t.Fatal("expired cache policy reused")
	}
}

func TestDecodedHTTPCacheHonorsResponseHeaders(t *testing.T) {
	data := buildTestArchive(t, 16384)
	for _, test := range []struct {
		control, age string
		calls        int32
		retained     bool
	}{
		{"private, max-age=3600, immutable", "", 1, true},
		{"no-store", "", 2, false},
		{"no-cache", "", 2, false},
		{"max-age=60", "60", 2, false},
	} {
		t.Run(test.control+test.age, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Cache-Control") != "no-transform" {
					t.Error("missing transformation protection")
				}
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
					t.Error(err)
					return
				}
				w.Header().Set("ETag", `"snapshot"`)
				w.Header().Set("Cache-Control", test.control)
				if test.age != "" {
					w.Header().Set("Age", test.age)
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[start : end+1]) //nolint:errcheck // The loopback client can close a cancelled request.
			}))
			defer server.Close()
			reader, err := OpenWithOptions(context.Background(), &HTTPRangeReader{URL: server.URL}, ReaderOptions{SharedContentCache: NewMemoryDecodedCache(1 << 20), SourceIdentity: []byte("snapshot")})
			if err != nil {
				t.Fatal(err)
			}
			defer testClose(t, reader)
			before := calls.Load()
			for range 2 {
				got, err := reader.Read(context.Background(), "nested/data", 0, 4)
				if err != nil || string(got) != "abcd" {
					t.Fatalf("%q: %v", got, err)
				}
			}
			if calls.Load()-before != test.calls {
				t.Fatalf("payload reads=%d want=%d", calls.Load()-before, test.calls)
			}
			if (reader.ContentCacheBytes() > 0) != test.retained {
				t.Fatal("cache retention policy")
			}
		})
	}
}
