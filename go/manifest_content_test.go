package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestSharedDecodedCacheBoundsAndOpaqueKeys(t *testing.T) {
	cache := NewMemoryDecodedCache(1024)
	key := DecodedCacheKey{Kind: 0, Identity: [32]byte{1}, Metadata: [32]byte{2}, Offset: 7, EncodedSize: 11}
	value := CachedChunk{key: key, raw: []byte("decoded"), verified: true}
	if err := cache.Put(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	got, ok, err := cache.Get(context.Background(), key)
	if err != nil || !ok || string(got.Bytes()) != "decoded" {
		t.Fatalf("cache get: %#v %v %v", got, ok, err)
	}
	output := got.Bytes()
	output[0] = 0
	if string(got.Bytes()) != "decoded" {
		t.Fatal("caller modified immutable cache content")
	}
	wrong := key
	wrong.EncodedSize++
	if _, ok, err := cache.Get(context.Background(), wrong); err != nil || ok {
		t.Fatal("accepted wrong locator")
	}
	if cache.UsedBytes() == 0 {
		t.Fatal("cache did not account retained bytes")
	}
	cache.Clear()
	if cache.UsedBytes() != 0 {
		t.Fatal("cache clear retained bytes")
	}
}

type countedManifestSource struct {
	data         []byte
	dataEnd      int64
	payloadReads int
}

func (s *countedManifestSource) Size(context.Context) (int64, error) { return int64(len(s.data)), nil }

func (s *countedManifestSource) ReadRange(_ context.Context, offset, length int64) ([]byte, error) {
	if offset < s.dataEnd {
		s.payloadReads++
	}
	return bytes.Clone(s.data[offset : offset+length]), nil
}

func openCountedManifest(t *testing.T, data, sourceID []byte, cache DecodedContentCache) (*Manifest, *countedManifestSource) {
	t.Helper()
	footer, err := ParseFooter(data[len(data)-80:])
	if err != nil {
		t.Fatal(err)
	}
	source := &countedManifestSource{data: data, dataEnd: int64(footer.ManifestOffset)}
	manifest, err := OpenManifest(context.Background(), source, ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.SetSharedContentCache(sourceID, cache); err != nil {
		t.Fatal(err)
	}
	return manifest, source
}

func TestSharedContentCacheReusesAcrossReadersAndSeparatesArchiveVersions(t *testing.T) {
	data := encodedFixture(t, "inline.zstar")
	cache := NewMemoryDecodedCache(1 << 20)
	first, firstSource := openCountedManifest(t, data, []byte("snapshot-v1"), cache)
	if got, err := first.Read(context.Background(), "a-b", 0, 5, nil, true); err != nil || string(got) != "hello" {
		t.Fatalf("first read: %q %v", got, err)
	}
	if firstSource.payloadReads != 1 {
		t.Fatalf("first read payload ranges: %d", firstSource.payloadReads)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, secondSource := openCountedManifest(t, data, []byte("snapshot-v1"), cache)
	if got, err := second.Read(context.Background(), "a-b", 0, 5, nil, true); err != nil || string(got) != "hello" {
		t.Fatalf("shared read: %q %v", got, err)
	}
	if secondSource.payloadReads != 0 {
		t.Fatalf("shared hit fetched payload %d times", secondSource.payloadReads)
	}
	if err := second.Close(); err != nil {
		t.Error(err)
	}

	replaced, replacementSource := openCountedManifest(t, data, []byte("snapshot-v2"), cache)
	if got, err := replaced.Read(context.Background(), "a-b", 0, 5, nil, true); err != nil || string(got) != "hello" {
		t.Fatalf("replacement read: %q %v", got, err)
	}
	if replacementSource.payloadReads != 1 {
		t.Fatalf("replacement reused old bytes: %d", replacementSource.payloadReads)
	}
	if err := replaced.Close(); err != nil {
		t.Error(err)
	}
}

type hostileDecodedCache struct {
	value CachedChunk
	fail  bool
}

type stalledDecodedCache struct {
	phase   string
	entered chan struct{}
	release chan struct{}
	gets    atomic.Int32
}

func (c *stalledDecodedCache) Get(context.Context, DecodedCacheKey) (CachedChunk, bool, error) {
	c.gets.Add(1)
	if c.phase == "get" {
		close(c.entered)
		<-c.release
	}
	return CachedChunk{}, false, nil
}

func (c *stalledDecodedCache) Put(context.Context, CachedChunk) error {
	if c.phase == "put" {
		close(c.entered)
		<-c.release
	}
	return nil
}

func TestSharedCacheCancellationRetainsWorkerPermit(t *testing.T) {
	for _, mode := range []string{"read", "batch"} {
		for _, phase := range []string{"get", "put"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				cache := &stalledDecodedCache{phase: phase, entered: make(chan struct{}), release: make(chan struct{})}
				defer close(cache.release)
				reader, err := OpenWithOptions(context.Background(), memoryRange(encodedFixture(t, "inline.zstar")), ReaderOptions{
					Manifest: ManifestOptions{ConcurrentChunks: 1}, SharedContentCache: cache, SourceIdentity: []byte("v1"), DisableContentCache: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer testClose(t, reader)
				read := func(ctx context.Context) error {
					if mode == "batch" {
						_, err := reader.ReadMany(ctx, []ReadRequest{{"a-b", 0, 5}, {"a-b", 1, 3}})
						return err
					}
					_, err := reader.Read(ctx, "a-b", 0, 5)
					return err
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- read(ctx) }()
				select {
				case <-cache.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("callback not entered")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancel: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("cancelled read blocked on callback")
				}
				next := make(chan error, 1)
				go func() { next <- read(context.Background()) }()
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-next:
					if err == nil {
						t.Fatal("read succeeded after close")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("close did not cancel waiting read")
				}
				if cache.gets.Load() != 1 {
					t.Fatalf("callback permit escaped: %d", cache.gets.Load())
				}
			})
		}
	}
}

func (c hostileDecodedCache) Get(_ context.Context, _ DecodedCacheKey) (CachedChunk, bool, error) {
	if c.fail {
		return CachedChunk{}, false, errors.New("cache unavailable")
	}
	return c.value, true, nil
}

func (c hostileDecodedCache) Put(context.Context, CachedChunk) error {
	return errors.New("cache unavailable")
}

func TestSharedCacheRejectsWrongTokensAndFallsBackOnBackendErrors(t *testing.T) {
	data := encodedFixture(t, "inline.zstar")
	for _, fail := range []bool{false, true} {
		cache := hostileDecodedCache{
			value: CachedChunk{key: DecodedCacheKey{Offset: 999}, raw: []byte("wrong"), verified: true},
			fail:  fail,
		}
		manifest, source := openCountedManifest(t, data, []byte("snapshot"), cache)
		got, err := manifest.Read(context.Background(), "a-b", 0, 5, nil, true)
		if err != nil || string(got) != "hello" {
			t.Fatalf("fallback read: %q %v", got, err)
		}
		if source.payloadReads != 1 {
			t.Fatalf("fallback fetched %d payload ranges", source.payloadReads)
		}
		if err := manifest.Close(); err != nil {
			t.Error(err)
		}
	}
}

type manifestGatedObject struct {
	data    []byte
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *manifestGatedObject) GetEncoded(context.Context, [32]byte, uint64, uint64) ([]byte, error) {
	s.calls.Add(1)
	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.release != nil {
		<-s.release
	}
	return bytes.Clone(s.data), nil
}

func openThinManifest(t *testing.T, options ManifestOptions) *Manifest {
	t.Helper()
	data, err := os.ReadFile("../testdata/entry-manifest/thin.zstar")
	if err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(context.Background(), memoryRange(data), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testClose(t, m) })
	return m
}

func encodedFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile("../testdata/entry-manifest/" + path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func waitManifestCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManifestContentReusePromotionAndOwnedResults(t *testing.T) {
	ctx := context.Background()
	m := openThinManifest(t, ManifestOptions{ContentCacheBytes: 1 << 20})
	resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")}
	unchecked, report, err := m.ReadWithReport(ctx, "repeat", 0, 12, resolver, false)
	if err != nil || string(unchecked) != "hello\nhello\n" || len(report.ExternalObjectsUnchecked) != 1 {
		t.Fatalf("unchecked: %q %#v %v", unchecked, report, err)
	}
	unchecked[0] = 'X'
	checked, report, err := m.ReadWithReport(ctx, "world.txt", 0, 6, resolver, true)
	if err != nil || string(checked) != "world\n" || len(report.ExternalObjectsSHA256Checked) != 1 || resolver.calls.Load() != 1 {
		t.Fatalf("promotion refetched or changed data: %q %#v %v calls=%d", checked, report, err, resolver.calls.Load())
	}
	if m.ContentCacheBytes() == 0 || m.ContentCacheBytes() > 1<<20 {
		t.Fatal("cache budget")
	}
	_, report, err = m.ReadWithReport(ctx, "hello.txt", 0, 6, resolver, false)
	if err != nil || len(report.ExternalObjectsUnchecked) != 0 || len(report.ExternalObjectsSHA256Checked) != 1 {
		t.Fatal("verified provenance lost")
	}
	testClose(t, m)
	if m.ContentCacheBytes() != 0 {
		t.Fatal("closed cache retained bytes")
	}
	if _, err = m.ReadMany(ctx, nil, resolver, true); err == nil {
		t.Fatal("closed empty batch accepted")
	}
}

func TestManifestFailedHashCannotPromoteUncheckedCache(t *testing.T) {
	m := openThinManifest(t, ManifestOptions{ContentCacheBytes: 1 << 20})
	resolver := &manifestGatedObject{data: encodedFixture(t, "invalid/object-wrong-hash.zst")}
	if _, err := m.Read(context.Background(), "hello.txt", 0, 6, resolver, false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Read(context.Background(), "hello.txt", 0, 6, resolver, true); ErrorCategory(err) != "integrity-failure" {
		t.Fatal(err)
	}
	got, report, err := m.ReadWithReport(context.Background(), "hello.txt", 0, 6, resolver, false)
	if err != nil || string(got) != "wrong\n" || len(report.ExternalObjectsUnchecked) != 1 || resolver.calls.Load() != 1 {
		t.Fatalf("failed promotion: %q %#v %v", got, report, err)
	}
}

func TestManifestExternalCacheSeparatesResolverNamespaces(t *testing.T) {
	m := openThinManifest(t, ManifestOptions{ContentCacheBytes: 1 << 20})
	wrong := &manifestGatedObject{data: encodedFixture(t, "invalid/object-wrong-hash.zst")}
	correct := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")}
	ctx := context.Background()
	for _, test := range []struct {
		resolver *manifestGatedObject
		expected string
	}{{wrong, "wrong\n"}, {correct, "hello\n"}} {
		data, report, err := m.ReadWithReport(ctx, "hello.txt", 0, 6, test.resolver, false)
		if err != nil || string(data) != test.expected || len(report.ExternalObjectsUnchecked) != 1 {
			t.Fatalf("namespace reused: %q %v %v", data, report, err)
		}
	}
	if _, err := m.Read(ctx, "hello.txt", 0, 6, wrong, true); ErrorCategory(err) != "integrity-failure" {
		t.Fatal(err)
	}
	data, report, err := m.ReadWithReport(ctx, "hello.txt", 0, 6, correct, true)
	if err != nil || string(data) != "hello\n" || len(report.ExternalObjectsSHA256Checked) != 1 || correct.calls.Load() != 1 || wrong.calls.Load() != 1 {
		t.Fatalf("independent promotion failed: %q %v %v", data, report, err)
	}
}

func TestConcurrentUncheckedResolverMissesDoNotCoalesce(t *testing.T) {
	m := openThinManifest(t, ManifestOptions{ConcurrentChunks: 2})
	a := &manifestGatedObject{data: encodedFixture(t, "invalid/object-wrong-hash.zst"), entered: make(chan struct{}, 1), release: make(chan struct{})}
	b := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst"), entered: make(chan struct{}, 1), release: make(chan struct{})}
	var onceA, onceB sync.Once
	defer onceA.Do(func() { close(a.release) })
	defer onceB.Do(func() { close(b.release) })
	errors := make(chan error, 2)
	for _, test := range []struct {
		resolver *manifestGatedObject
		expected string
	}{{a, "wrong\n"}, {b, "hello\n"}} {
		go func() {
			data, err := m.Read(context.Background(), "hello.txt", 0, 6, test.resolver, false)
			if err == nil && string(data) != test.expected {
				err = fmt.Errorf("got %q", data)
			}
			errors <- err
		}()
	}
	for _, entered := range []chan struct{}{a.entered, b.entered} {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("distinct resolver joined another namespace's flight")
		}
	}
	onceA.Do(func() { close(a.release) })
	onceB.Do(func() { close(b.release) })
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

type manifestResolverFunc func(context.Context, [32]byte, uint64, uint64) ([]byte, error)

func (f manifestResolverFunc) GetEncoded(ctx context.Context, address [32]byte, hint, maxBytes uint64) ([]byte, error) {
	return f(ctx, address, hint, maxBytes)
}

func TestReaderPinsValueResolverCacheWithoutMixingReaders(t *testing.T) {
	data := encodedFixture(t, "thin.zstar")
	object := encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")
	var calls atomic.Int32
	resolver := manifestResolverFunc(func(context.Context, [32]byte, uint64, uint64) ([]byte, error) {
		calls.Add(1)
		return bytes.Clone(object), nil
	})
	for range 2 {
		reader, err := OpenWithOptions(context.Background(), memoryRange(data), ReaderOptions{Resolver: resolver})
		if err != nil {
			t.Fatal(err)
		}
		for range 10 {
			got, err := reader.Read(context.Background(), "hello.txt", 0, 6)
			if err != nil || string(got) != "hello\n" {
				t.Fatalf("%q %v", got, err)
			}
		}
		testClose(t, reader)
	}
	if calls.Load() != 2 {
		t.Fatalf("resolver called %d times", calls.Load())
	}
}

func TestValueResolversShareWithinBatchWithoutCrossOperationReuse(t *testing.T) {
	m := openThinManifest(t, ManifestOptions{ContentCacheBytes: 1 << 20})
	object := encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")
	var calls atomic.Int32
	resolver := manifestResolverFunc(func(context.Context, [32]byte, uint64, uint64) ([]byte, error) {
		calls.Add(1)
		return bytes.Clone(object), nil
	})
	requests := []ReadRequest{{"hello.txt", 0, 6}, {"repeat", 0, 12}}
	for range 2 {
		if data, err := m.ReadMany(context.Background(), requests, resolver, false); err != nil || string(data[1]) != "hello\nhello\n" {
			t.Fatalf("%q %v", data, err)
		}
	}
	if calls.Load() != 2 || m.ContentCacheBytes() != 0 {
		t.Fatal("value resolver retained or batch repeated decoding")
	}
	// A pointer to the same function has a stable, comparable namespace.
	for range 2 {
		if _, err := m.ReadMany(context.Background(), requests, &resolver, false); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 3 || m.ContentCacheBytes() == 0 {
		t.Fatal("pointer resolver did not reuse")
	}
}

func TestManifestConcurrentMissCancellationAndBoundedDetachedWork(t *testing.T) {
	m := openThinManifest(t, ManifestOptions{ConcurrentChunks: 1})
	resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst"), entered: make(chan struct{}, 3), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := m.Read(ctx, "hello.txt", 0, 6, resolver, true); first <- err }()
	<-resolver.entered
	second := make(chan error, 1)
	go func() {
		got, err := m.Read(context.Background(), "world.txt", 0, 6, resolver, true)
		if err == nil && string(got) != "world\n" {
			err = errors.New("bad bytes")
		}
		second <- err
	}()
	waitManifestCondition(t, func() bool {
		m.content.mu.Lock()
		defer m.content.mu.Unlock()
		for _, flight := range m.content.flights {
			if flight.waiters == 2 {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(resolver.release)
	if err := <-second; err != nil || resolver.calls.Load() != 1 {
		t.Fatalf("shared miss canceled remaining waiter: %v calls=%d", err, resolver.calls.Load())
	}
	// A canceled noninterruptible callback continues holding the single slot.
	resolver.release = make(chan struct{})
	ctx, cancel = context.WithCancel(context.Background())
	go func() { _, err := m.Read(ctx, "hello.txt", 0, 6, resolver, true); first <- err }()
	<-resolver.entered
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	go func() { _, err := m.Read(context.Background(), "world.txt", 0, 6, resolver, true); second <- err }()
	waitManifestCondition(t, func() bool { m.content.mu.Lock(); defer m.content.mu.Unlock(); return len(m.content.flights) == 1 })
	if resolver.calls.Load() != 2 {
		t.Fatal("canceled callback released work slot")
	}
	testClose(t, m)
	if err := <-second; ErrorCategory(err) != "unsupported-operation" {
		t.Fatalf("close did not unblock waiter: %v", err)
	}
	close(resolver.release)
	waitManifestCondition(t, func() bool { return len(m.content.slots) == 0 })
}

func TestManifestBatchGroupingBoundsAndFileStreams(t *testing.T) {
	ctx := context.Background()
	m := openThinManifest(t, ManifestOptions{MaxReadBytes: 32})
	resolver := &manifestGatedObject{data: encodedFixture(t, "objects/4a1e67f2fe1d1cc7b31d0ca2ec441da4778203a036a77da10344c85e24ff0f92.zst")}
	requests := []ReadRequest{{"world.txt", 0, 6}, {"repeat", 0, 12}, {"hello.txt", 0, 6}}
	empty, report, err := m.ReadManyWithReport(ctx, []ReadRequest{{"repeat", 12, 0}, {"hello.txt", 0, 0}}, nil, true)
	if err != nil || len(empty) != 2 || len(empty[0]) != 0 || len(empty[1]) != 0 || !report.MetadataAuthenticated || resolver.calls.Load() != 0 {
		t.Fatalf("zero-length batch: %q %#v %v", empty, report, err)
	}
	got, report, err := m.ReadManyWithReport(ctx, requests, resolver, true)
	if err != nil || len(got) != 3 || string(got[0]) != "world\n" || string(got[1]) != "hello\nhello\n" || string(got[2]) != "hello\n" || resolver.calls.Load() != 1 {
		t.Fatalf("grouped batch: %q %v calls=%d", got, err, resolver.calls.Load())
	}
	got[1][0] = 'X'
	if string(got[2]) != "hello\n" || report.FullArchiveValidated || len(report.ExternalObjectsSHA256Checked) != 1 {
		t.Fatal("ownership or scope")
	}
	if _, err = m.ReadMany(ctx, []ReadRequest{{"hello.txt", 0, 6}, {"world.txt", 7, 0}}, resolver, true); ErrorCategory(err) != "invalid-range" || resolver.calls.Load() != 1 {
		t.Fatal("invalid batch performed I/O")
	}
	if _, err = m.ReadMany(ctx, []ReadRequest{{"repeat", 0, 12}, {"repeat", 0, 12}, {"repeat", 0, 12}}, resolver, true); ErrorCategory(err) != "resource-limit" || resolver.calls.Load() != 1 {
		t.Fatal("over-budget batch performed I/O")
	}
	file, err := m.OpenFile(ctx, "repeat", resolver, true)
	if err != nil {
		t.Fatal(err)
	}
	var streamed bytes.Buffer
	if n, err := file.WriteTo(&streamed); err != nil || n != 12 || streamed.String() != "hello\nhello\n" || resolver.calls.Load() != 2 {
		t.Fatalf("stream repeated object: %d %q %v", n, streamed.String(), err)
	}
	buffer := make([]byte, 4)
	if n, err := file.ReadAt(buffer, 10); n != 2 || !errors.Is(err, io.EOF) || string(buffer[:2]) != "o\n" {
		t.Fatalf("ReaderAt EOF: %d %q %v", n, buffer, err)
	}
	testClose(t, m)
	if _, err := file.WriteTo(io.Discard); err == nil {
		t.Fatal("closed file streamed")
	}
}

func TestManifestBatchValidatesWholeSelectedExtent(t *testing.T) {
	codec, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, codec)
	raw := []byte("hello")
	address := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	var out bytes.Buffer
	out.Write(make([]byte, 1000))
	w, err := NewManifestWriter(&out, 1000, ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.AddEntry(Entry{Path: "file", Mode: 420, Extents: []Extent{{Size: 6, Source: mustObjectSource(t, address), EncodedSize: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenManifest(context.Background(), memoryRange(out.Bytes()), ManifestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer testClose(t, m)
	resolver := &manifestGatedObject{data: codec.EncodeAll(raw, nil)}
	if _, err = m.ReadMany(context.Background(), []ReadRequest{{"file", 0, 1}}, resolver, true); ErrorCategory(err) != "invalid-range" {
		t.Fatalf("selected extent bounds ignored: %v", err)
	}
}

func TestReaderSharedCacheOptions(t *testing.T) {
	data := encodedFixture(t, "inline.zstar")
	footer, err := ParseFooter(data[len(data)-80:])
	if err != nil {
		t.Fatal(err)
	}
	cache := NewMemoryDecodedCache(1 << 20)
	for _, item := range []struct {
		identity     string
		payloadReads int
	}{{"v1", 1}, {"v1", 0}, {"v2", 1}} {
		source := &countedManifestSource{data: data, dataEnd: int64(footer.ManifestOffset)}
		reader, err := OpenWithOptions(context.Background(), source, ReaderOptions{
			SharedContentCache: cache, SourceIdentity: []byte(item.identity), DisableContentCache: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := reader.Read(context.Background(), "a-b", 0, 5)
		if err != nil || string(got) != "hello" {
			t.Fatalf("read: %q %v", got, err)
		}
		if source.payloadReads != item.payloadReads {
			t.Fatalf("payload reads: %d, want %d", source.payloadReads, item.payloadReads)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, opts := range []ReaderOptions{
		{SharedContentCache: cache},
		{SourceIdentity: []byte("v1")},
		{SharedContentCache: cache, SourceIdentity: make([]byte, 65537)},
	} {
		if reader, err := OpenWithOptions(context.Background(), nil, opts); err == nil {
			if err := reader.Close(); err != nil {
				t.Error(err)
			}
			t.Fatal("accepted invalid shared cache options")
		}
	}
}
