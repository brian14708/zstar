package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testPackStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    int
	root    string
}

func (s *testPackStore) PutPack(ctx context.Context, key string, input io.Reader, length uint64) (PackSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return PackSnapshot{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(input, int64(length)+1))
	if err != nil {
		return PackSnapshot{}, err
	}
	if uint64(len(raw)) != length {
		return PackSnapshot{}, io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string][]byte)
	}
	s.objects[key] = raw
	d := sha256.Sum256(raw)
	return PackSnapshot{Key: key, Length: length, ETag: "\"sha256:" + hex.EncodeToString(d[:]) + "\""}, nil
}

func (s *testPackStore) ReadPackedRange(ctx context.Context, snapshot PackSnapshot, offset, length uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	raw := s.objects[snapshot.Key]
	if s.root != "" {
		var err error
		raw, err = os.ReadFile(filepath.Join(s.root, snapshot.Key))
		if err != nil {
			return nil, err
		}
	}
	digest := sha256.Sum256(raw)
	if snapshot.ETag != "\"sha256:"+hex.EncodeToString(digest[:])+"\"" || uint64(len(raw)) != snapshot.Length {
		return nil, rejected("changed-pack", "snapshot")
	}
	return bytes.Clone(raw[offset : offset+length]), nil
}

func readPackedFixture(t *testing.T, root, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPackedNegativeCatalogFixtures(t *testing.T) {
	root := "../testdata/packed-objects"
	var manifest struct {
		NegativeCatalogs []struct{ File, Category string } `json:"negative_catalogs"`
	}
	if err := json.Unmarshal(readPackedFixture(t, root, "manifest.json"), &manifest); err != nil {
		t.Fatal(err)
	}
	for _, bad := range manifest.NegativeCatalogs {
		t.Run(bad.File, func(t *testing.T) {
			_, err := DecodePackedCatalog(readPackedFixture(t, root, bad.File), CatalogLimits{})
			if ErrorCategory(err) != bad.Category {
				t.Fatalf("category: %v, expected %s", err, bad.Category)
			}
		})
	}
}

func TestPackedSharedFixtures(t *testing.T) {
	for _, dataset := range []string{"", "go", "python"} {
		t.Run(dataset, func(t *testing.T) {
			root := filepath.Join("../testdata/packed-objects", dataset)
			read := func(name string) []byte { return readPackedFixture(t, root, name) }
			c, err := DecodePackedCatalog(read("catalog.cbor"), CatalogLimits{})
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := c.Encode(CatalogLimits{})
			if err != nil || !bytes.Equal(encoded, read("catalog.cbor")) {
				t.Fatal("canonical roundtrip", err)
			}
			store := &testPackStore{root: root}
			resolver, err := NewPackedObjectResolver(store, c, PackedResolverOptions{})
			if err != nil {
				t.Fatal(err)
			}
			data := read("thin.zstar")
			r, err := OpenWithOptions(context.Background(), ReaderAtSource{Reader: bytes.NewReader(data), Length: int64(len(data))}, ReaderOptions{Resolver: resolver, DisableContentCache: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := r.Close(); err != nil {
					t.Error(err)
				}
			}()
			out, err := r.Read(context.Background(), "payload", 0, uint64(len(read("payload.bin"))))
			if err != nil || !bytes.Equal(out, read("payload.bin")) {
				t.Fatal("reconstruction", err)
			}
			if store.gets != 1 {
				t.Fatalf("GETs: %d", store.gets)
			}
			g, err := DecodePackedGeneration(read("generation.cbor"))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err = g.Encode()
			if err != nil || !bytes.Equal(encoded, read("generation.cbor")) {
				t.Fatal("generation roundtrip", err)
			}
		})
	}
}

func TestPackWriterReuseBudgetsAndPublication(t *testing.T) {
	ctx := context.Background()
	store := &testPackStore{}
	w, err := NewPackWriter(store, nil, PackWriterOptions{TargetBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	extents := make([]Extent, 0, 3)
	for _, raw := range [][]byte{[]byte("one"), []byte("two"), []byte("one")} {
		e, err := w.Put(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		extents = append(extents, e)
	}
	c, err := w.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.chunks) != 2 || extents[0] != extents[2] {
		t.Fatal("dedup")
	}
	reuse, err := NewPackWriter(store, c, PackWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reuse.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err = reuse.Put(ctx, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err = reuse.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	if len(reuse.CompletedPacks()) != 0 {
		t.Fatal("base reuploaded")
	}
	r, err := NewPackedObjectResolver(store, c, PackedResolverOptions{CacheBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	d := sha256.Sum256([]byte("one"))
	frame, err := r.GetEncoded(ctx, d, 0, 17<<20)
	if err != nil {
		t.Fatal(err)
	}
	frame[0] ^= 1
	again, err := r.GetEncoded(ctx, d, 0, 17<<20)
	if err != nil || bytes.Equal(frame, again) {
		t.Fatal("mutable cache", err)
	}
	before := store.gets
	if _, err = r.GetEncoded(ctx, d, 0, 1); ErrorCategory(err) != "resource-limit" || store.gets != before {
		t.Fatal("budget", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = r.GetEncoded(cancelled, d, 0, 17<<20); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	thin, err := os.ReadFile("../testdata/packed-objects/thin.zstar")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, g, err := PublishPackedGeneration(ctx, store, "run", bytes.NewReader(thin), c, map[string]any{"algorithm": "opaque"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := store.ReadPackedRange(ctx, descriptor, 0, descriptor.Length)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePackedGeneration(raw)
	if err != nil || decoded.Archive != g.Archive {
		t.Fatal("publication", err)
	}
}

func TestPackedWarmReaderSkipsEncodedRequests(t *testing.T) {
	root := "../testdata/packed-objects"
	encoded, err := os.ReadFile(filepath.Join(root, "catalog.cbor"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodePackedCatalog(encoded, CatalogLimits{})
	if err != nil {
		t.Fatal(err)
	}
	store := &testPackStore{root: root}
	resolver, err := NewPackedObjectResolver(store, c, PackedResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	thin, err := os.ReadFile(filepath.Join(root, "thin.zstar"))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenWithOptions(context.Background(), ReaderAtSource{Reader: bytes.NewReader(thin), Length: int64(len(thin))}, ReaderOptions{Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		if _, err = reader.Read(context.Background(), "payload", 0, 21); err != nil {
			t.Fatal(err)
		}
	}
	if store.gets != 1 {
		t.Fatalf("warm GETs: %d", store.gets)
	}
}

type slowPackStore struct {
	inner        *testPackStore
	gate         chan struct{}
	started      chan struct{}
	active, peak atomic.Int64
}

func (s *slowPackStore) PutPack(ctx context.Context, key string, input io.Reader, length uint64) (PackSnapshot, error) {
	return s.inner.PutPack(ctx, key, input, length)
}

func (s *slowPackStore) ReadPackedRange(ctx context.Context, snapshot PackSnapshot, offset, length uint64) ([]byte, error) {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); active > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, active) {
			break
		}
	}
	s.started <- struct{}{}
	select {
	case <-s.gate:
		return s.inner.ReadPackedRange(ctx, snapshot, offset, length)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestPackedGlobalConcurrencyAndCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memory := &testPackStore{}
	writer, err := NewPackWriter(memory, nil, PackWriterOptions{TargetBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, raw := range []string{"one", "two", "three", "four"} {
		if _, err = writer.Put(ctx, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err := writer.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	store := &slowPackStore{inner: memory, gate: make(chan struct{}, 4), started: make(chan struct{}, 8)}
	resolver, err := NewPackedObjectResolver(store, catalog, PackedResolverOptions{Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]ObjectRequest, 0, 4)
	for _, chunk := range catalog.Chunks() {
		requests = append(requests, ObjectRequest{Digest: chunk.Digest, MaxBytes: 17 << 20})
	}
	leader, stop := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() { _, err := resolver.GetEncodedMany(leader, requests[:2]); first <- err }()
	for range 2 {
		select {
		case <-store.started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	second := make(chan error, 1)
	go func() { _, err := resolver.GetEncodedMany(ctx, requests[2:]); second <- err }()
	stop()
	if err = <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for range 2 {
		store.gate <- struct{}{}
	}
	if err = <-second; err != nil {
		t.Fatal(err)
	}
	if store.peak.Load() > 2 || store.active.Load() != 0 {
		t.Fatal("global permits leaked or exceeded")
	}
}
