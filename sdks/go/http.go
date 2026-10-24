package zstar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrObjectChanged reports that the remote object no longer matches the
// strong ETag observed on the
// first successful response, or that its size changed.
var ErrObjectChanged = errors.New("zstar: HTTP object changed during session")

// httpIdleTimeout bounds the wait for response headers and then each stall in
// the response body. It replaces a whole-request timeout so that large slow
// transfers that keep making progress are not aborted.
const httpIdleTimeout = 60 * time.Second

// HTTPRangeReader requires a strong ETag and fetches exact, identity-encoded ranges. Configure its public
// fields before use and do not mutate them during concurrent reads.
type HTTPRangeReader struct {
	URL     string
	Headers http.Header
	Client  *http.Client
	state   httpRangeState
}

func (*HTTPRangeReader) String() string { return "HTTPRangeReader{URL:<redacted>, Headers:<redacted>}" }

// GoString redacts URLs and headers in Go-syntax diagnostic output.
func (r *HTTPRangeReader) GoString() string { return r.String() }

type httpRangeState struct {
	mu      sync.Mutex
	size    int64
	hasSize bool
	// Pinned object version from the first response.
	etag       string
	hasVersion bool
}

// guard cancels a request when no progress is made for its duration.
type guard struct {
	cancel context.CancelFunc
	timer  *time.Timer
	d      time.Duration
	fired  atomic.Bool
}

func newGuard(ctx context.Context, d time.Duration) (context.Context, *guard) {
	ctx, cancel := context.WithCancel(ctx)
	g := &guard{cancel: cancel, d: d}
	g.timer = time.AfterFunc(d, func() { g.fired.Store(true); cancel() })
	return ctx, g
}

func (g *guard) progress() { g.timer.Reset(g.d) }
func (g *guard) stop()     { g.timer.Stop(); g.cancel() }

type guardedBody struct {
	io.Reader
	g *guard
}

func (b guardedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if n > 0 {
		b.g.progress()
	}
	return n, err
}

var errHTTPTimeout = errors.New("zstar: HTTP request timed out")

func strongETag(v string) bool {
	if len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
		return false
	}
	for _, b := range []byte(v[1 : len(v)-1]) {
		if b != 0x21 && (b < 0x23 || b > 0x7e) && b < 0x80 {
			return false
		}
	}
	return true
}

// pin records the first observed validator and reports a mismatch with it.
func (s *httpRangeState) pin(h http.Header) error {
	etag := h.Get("ETag")
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasVersion {
		if !strongETag(etag) {
			return errors.New("zstar: HTTP archive requires a strong ETag")
		}
		s.etag, s.hasVersion = etag, true
		return nil
	}
	if etag != s.etag {
		return ErrObjectChanged
	}
	return nil
}

func (s *httpRangeState) conditions(h http.Header) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasVersion {
		return
	}
	h.Set("If-Match", s.etag)
}

// A client's own Timeout is honored; otherwise callers get an idle timeout
// (see httpIdleTimeout) plus their context, never a whole-request limit.
// The standard transport pools connections and negotiates HTTP/2. A shallow
// client copy retains that pool while rejecting redirects, even for supplied
// clients: a signed request must not be sent to a different location.
func rangeClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	cloned := *client
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &cloned
}

type httpStatusError int

func (e httpStatusError) Error() string {
	return fmt.Sprintf("zstar: HTTP range expected 206, got %d", e)
}

func cancellationError(ctx context.Context, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(cause, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}

func httpFailure(ctx context.Context, cause error) error {
	if err := cancellationError(ctx, cause); err != nil {
		return err
	}
	// Transport/body errors may include the entire signed URL or header values.
	return errors.New("zstar: HTTP range transport failed")
}

func (s *httpRangeState) knownSize() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size, s.hasSize
}

func (s *httpRangeState) probeSize(ctx context.Context, read func(context.Context, int64, int64) ([]byte, error)) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if size, ok := s.knownSize(); ok {
		return size, nil
	}
	if _, err := read(ctx, 0, 1); err != nil {
		return 0, err
	}
	size, _ := s.knownSize()
	return size, nil
}

func validateHTTPRange(ctx context.Context, offset, length int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if offset < 0 || length < 0 || offset > int64(^uint64(0)>>1)-length {
		return errors.New("zstar: invalid range")
	}
	if length > 64<<20 {
		return errors.New("zstar: HTTP range exceeds 64 MiB local budget")
	}
	return nil
}

func (s *httpRangeState) read(ctx context.Context, client *http.Client, url string, headers http.Header, offset, length int64) ([]byte, error) {
	if err := validateHTTPRange(ctx, offset, length); err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	if size, known := s.knownSize(); known && offset+length > size {
		return nil, errors.New("zstar: HTTP range outside object")
	}
	parent := ctx
	ctx, g := newGuard(ctx, httpIdleTimeout)
	defer g.stop()
	fail := func(err error) error {
		if g.fired.Load() && parent.Err() == nil {
			return errHTTPTimeout
		}
		return httpFailure(parent, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fail(err)
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	req.Header.Set("Accept-Encoding", "identity")
	s.conditions(req.Header)
	resp, err := rangeClient(client).Do(req)
	if err != nil {
		return nil, fail(err)
	}
	g.progress()
	// Response contents are checked before return; closing a read body is cleanup.
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // The bounded response has been validated; closing its body is cleanup.
	if resp.StatusCode == http.StatusPreconditionFailed || resp.StatusCode == http.StatusOK && req.Header.Get("If-Range") != "" {
		return nil, ErrObjectChanged
	}
	if resp.StatusCode != http.StatusPartialContent {
		return nil, httpStatusError(resp.StatusCode)
	}
	if err := s.pin(resp.Header); err != nil {
		return nil, err
	}
	if encoding := resp.Header.Get("Content-Encoding"); resp.Uncompressed || (encoding != "" && !strings.EqualFold(encoding, "identity")) {
		return nil, errors.New("zstar: HTTP range requires identity encoding")
	}
	prefix := fmt.Sprintf("bytes %d-%d/", offset, offset+length-1)
	cr := resp.Header.Get("Content-Range")
	total, parseErr := strconv.ParseInt(strings.TrimPrefix(cr, prefix), 10, 64)
	if parseErr != nil || total < offset+length || cr != prefix+strconv.FormatInt(total, 10) {
		return nil, errors.New("zstar: mismatched HTTP Content-Range")
	}
	if resp.ContentLength >= 0 && resp.ContentLength != length {
		return nil, errors.New("zstar: mismatched HTTP Content-Length")
	}
	s.mu.Lock()
	changed := s.hasSize && s.size != total
	s.mu.Unlock()
	if changed {
		return nil, fmt.Errorf("%w: size changed", ErrObjectChanged)
	}
	data, err := io.ReadAll(io.LimitReader(guardedBody{resp.Body, g}, length+1))
	if err != nil {
		return nil, fail(err)
	}
	if int64(len(data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasSize && s.size != total {
		return nil, fmt.Errorf("%w: size changed", ErrObjectChanged)
	}
	s.size, s.hasSize = total, true
	return data, nil
}

// Size discovers the size using GET bytes=0-0, requiring no HEAD permission.
func (r *HTTPRangeReader) Size(ctx context.Context) (int64, error) {
	return r.state.probeSize(ctx, r.ReadRange)
}

// ReadRange returns exactly length bytes starting at offset, using ctx for
// this request. Ranges are bounded to 64 MiB and the discovered source size.
func (r *HTTPRangeReader) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	return r.state.read(ctx, r.Client, r.URL, r.Headers, offset, length)
}

// PresignedURLProvider returns a signed GET request. forceRefresh is false for
// the initial request and true after a 401/403 response. It must support
// concurrent callers.
// Provider errors are redacted; use application logging for safe diagnostics.
type PresignedURLProvider func(context.Context, string, int64, int64, bool) (string, http.Header, error)

// RefreshingRangeReader fetches exact ranges through a renewable signed-URL
// provider. Configure its fields before concurrent use; never copy after use.
type RefreshingRangeReader struct {
	Provider PresignedURLProvider
	Client   *http.Client
	state    httpRangeState
}

func (*RefreshingRangeReader) String() string { return "RefreshingRangeReader{Provider:<redacted>}" }

// GoString redacts the provider in Go-syntax diagnostic output.
func (r *RefreshingRangeReader) GoString() string { return r.String() }

// Size probes the immutable archive size with a one-byte GET range.
func (r *RefreshingRangeReader) Size(ctx context.Context) (int64, error) {
	return r.state.probeSize(ctx, r.ReadRange)
}

// ReadRange fetches an exact bounded range and retries once with a refreshed
// URL after an authorization response. Cancellation signals are preserved.
func (r *RefreshingRangeReader) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	if err := validateHTTPRange(ctx, offset, length); err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	if r.Provider == nil {
		return nil, errors.New("zstar: missing URL provider")
	}
	for attempt := 0; ; attempt++ {
		url, headers, err := r.Provider(ctx, http.MethodGet, offset, length, attempt > 0)
		if err != nil {
			return nil, httpFailure(ctx, err)
		}
		data, err := r.state.read(ctx, r.Client, url, headers, offset, length)
		var status httpStatusError
		if attempt == 0 && errors.As(err, &status) && (int(status) == 401 || int(status) == 403) {
			continue
		}
		return data, err
	}
}

var _ RangeReader = (*RefreshingRangeReader)(nil)

// PresignedPutProvider returns a signed PUT request. forceRefresh is false for
// the initial upload and true after a 401/403 response. It must support calls
// from concurrent uploads.
type PresignedPutProvider func(context.Context, bool) (string, http.Header, error)

type httpPutStatusError int

func (e httpPutStatusError) Error() string {
	return fmt.Sprintf("zstar: presigned PUT failed with HTTP status %d", e)
}

// PutPresigned uploads the exact byte slice to a signed URL. A provider may
// issue one refreshed request after 401/403. The supplied client is pooled,
// redirects are rejected, and transport/provider errors do not expose URLs.
// Keep body immutable until this call returns. Bodies larger than 64 MiB are
// rejected before invoking the provider; use native multipart for larger data.
func PutPresigned(ctx context.Context, provider PresignedPutProvider, client *http.Client, body []byte) error {
	if provider == nil {
		return errors.New("zstar: missing PUT provider")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(body) > 64<<20 {
		return errors.New("zstar: signed PUT exceeds 64 MiB local budget")
	}
	for attempt := 0; ; attempt++ {
		url, headers, err := provider(ctx, attempt > 0)
		if err != nil {
			if ctxErr := cancellationError(ctx, err); ctxErr != nil {
				return ctxErr
			}
			return errors.New("zstar: signed PUT provider failed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Allow 60s plus time for the body at a floor of 128 KiB/s.
		putCtx, g := newGuard(ctx, httpIdleTimeout+time.Duration(len(body)>>17)*time.Second)
		req, err := http.NewRequestWithContext(putCtx, http.MethodPut, url, bytes.NewReader(body))
		if err != nil {
			g.stop()
			return errors.New("zstar: invalid signed PUT request")
		}
		if headers != nil {
			req.Header = headers.Clone()
		}
		resp, err := rangeClient(client).Do(req)
		if err != nil {
			defer g.stop()
			if ctxErr := cancellationError(ctx, err); ctxErr != nil {
				return ctxErr
			}
			return errors.New("zstar: signed PUT transport failed")
		}
		// Response bodies are not part of PUT success. Close without consuming
		// untrusted or stalled response bytes; an empty response remains reusable.
		_ = resp.Body.Close() //nolint:errcheck // PUT success depends on its status; response-body cleanup is advisory.
		g.stop()
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt == 0 && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return httpPutStatusError(resp.StatusCode)
		}
		return nil
	}
}
