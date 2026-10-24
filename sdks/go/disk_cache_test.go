package zstar

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDiskDecodedCacheRecoversVerifiedChunksAndDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	key := DecodedCacheKey{Kind: 1, ObjectDigest: [32]byte{1, 2, 3}}
	value := CachedChunk{key: key, raw: []byte("decoded object"), verified: true}
	encoded := encodeDiskChunk(value)
	cache, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: uint64(len(encoded))})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if cache.UsedBytes() != uint64(len(encoded)) {
		t.Fatalf("used bytes = %d, want %d", cache.UsedBytes(), len(encoded))
	}

	recovered, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: uint64(len(encoded))})
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := recovered.Get(context.Background(), key)
	if err != nil || !ok || string(got.raw) != string(value.raw) || !got.verified {
		t.Fatalf("recovered cache token = %#v, %v, %v", got, ok, err)
	}

	path := filepath.Join(dir, diskCacheName(diskCacheKey(key))+diskCacheSuffix)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[diskCacheHeaderBytes] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	got, ok, err = recovered.Get(context.Background(), key)
	if err != nil || ok || len(got.raw) != 0 {
		t.Fatalf("corrupt cache hit = %#v, %v, %v", got, ok, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupt cache entry still exists: %v", err)
	}
}

func TestDiskDecodedCacheEvictsLeastRecentlyUsedWithinCapacity(t *testing.T) {
	dir := t.TempDir()
	first := CachedChunk{key: DecodedCacheKey{Kind: 0, Identity: [32]byte{1}}, raw: []byte("first"), verified: true}
	second := CachedChunk{key: DecodedCacheKey{Kind: 0, Identity: [32]byte{2}}, raw: []byte("second"), verified: true}
	firstSize, secondSize := uint64(len(encodeDiskChunk(first))), uint64(len(encodeDiskChunk(second)))
	cache, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: firstSize + secondSize})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := cache.Put(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cache.Get(ctx, first.key); err != nil || !ok {
		t.Fatalf("first cache hit: %v, %v", ok, err)
	}
	third := CachedChunk{key: DecodedCacheKey{Kind: 0, Identity: [32]byte{3}}, raw: []byte("third"), verified: true}
	if err := cache.Put(ctx, third); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cache.Get(ctx, second.key); err != nil || ok {
		t.Fatalf("least-recently-used entry retained: %v, %v", ok, err)
	}
	if _, ok, err := cache.Get(ctx, first.key); err != nil || !ok {
		t.Fatalf("recently-used entry evicted: %v, %v", ok, err)
	}
	if cache.UsedBytes() > firstSize+secondSize {
		t.Fatalf("disk cache exceeded capacity: %d", cache.UsedBytes())
	}
}

func TestDiskCacheRecoveryBoundsAndStaleWrites(t *testing.T) {
	dir := t.TempDir()
	var newest CachedChunk
	var size uint64
	for i := range byte(3) {
		newest = CachedChunk{key: DecodedCacheKey{Identity: [32]byte{i}}, raw: []byte("data"), verified: true}
		data := encodeDiskChunk(newest)
		size = uint64(len(data))
		path := filepath.Join(dir, diskCacheName(diskCacheKey(newest.key))+diskCacheSuffix)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(int64(i+1), 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{".zstar-cache-interrupted", "bad.chunk"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("bad"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cache, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: size})
	if err != nil {
		t.Fatal(err)
	}
	if cache.UsedBytes() != size {
		t.Fatalf("recovery budget: got %d want %d", cache.UsedBytes(), size)
	}
	if _, ok, err := cache.Get(context.Background(), newest.key); err != nil || !ok {
		t.Fatalf("newest recovered token: %v %v", ok, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("recovery files: %v %v", entries, err)
	}
}

func TestDiskCacheMissingAndOversizedReplacementReleasesBudget(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(strconv.FormatBool(oversized), func(t *testing.T) {
			value := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{1}}, raw: []byte("data"), verified: true}
			cache, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: t.TempDir(), Capacity: 1024})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if err := cache.Put(ctx, value); err != nil {
				t.Fatal(err)
			}
			path := cache.path(diskCacheName(diskCacheKey(value.key)))
			if oversized {
				err = os.Truncate(path, 1<<30)
			} else {
				err = os.Remove(path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := cache.Get(ctx, value.key); err != nil || ok {
				t.Fatalf("invalid hit: %v %v", ok, err)
			}
			if cache.UsedBytes() != 0 {
				t.Fatalf("missing file retained budget: %d", cache.UsedBytes())
			}
		})
	}
}

func TestTieredCachePromotesDiskHitsAndPreservesTokens(t *testing.T) {
	ctx := context.Background()
	options := DiskDecodedCacheOptions{Directory: t.TempDir(), Capacity: 4096}
	cache, err := NewTieredDecodedCache(520, options)
	if err != nil {
		t.Fatal(err)
	}
	first := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{1}}, raw: []byte("first"), verified: true}
	second := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{2}}, raw: []byte("second"), verified: false}
	for _, value := range []CachedChunk{first, second} {
		if err := cache.Put(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, err := cache.memory.Get(ctx, first.key); err != nil || ok {
		t.Fatal("first token should have left memory")
	}
	got, ok, err := cache.Get(ctx, first.key)
	if err != nil || !ok || !got.verified || string(got.Bytes()) != "first" {
		t.Fatalf("disk promotion: %#v %v %v", got, ok, err)
	}
	if _, ok, err := cache.memory.Get(ctx, first.key); err != nil || !ok {
		t.Fatal("disk hit not promoted")
	}
	if err := cache.disk.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cache.Get(ctx, first.key); err != nil || !ok {
		t.Fatalf("memory hit requires disk: %v %v", ok, err)
	}
	if err := cache.Put(ctx, second); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewTieredDecodedCache(520, options)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err = restarted.Get(ctx, second.key)
	if err != nil || !ok || got.verified || string(got.Bytes()) != "second" {
		t.Fatalf("restart changed unchecked provenance: %#v %v %v", got, ok, err)
	}
	if restarted.MemoryUsedBytes() > 520 || restarted.DiskUsedBytes() > options.Capacity {
		t.Fatal("tier capacity exceeded")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := restarted.Get(cancelled, second.key); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get: %v", err)
	}
	if err := restarted.Put(cancelled, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled put: %v", err)
	}
}

func TestTieredCacheReaderRestartAndCorruptionFallback(t *testing.T) {
	options := DiskDecodedCacheOptions{Directory: t.TempDir(), Capacity: 1 << 20}
	cache, err := NewTieredDecodedCache(0, options)
	if err != nil {
		t.Fatal(err)
	}
	data := encodedFixture(t, "inline.zstar")
	read := func(cache DecodedContentCache, wantReads int) {
		t.Helper()
		manifest, source := openCountedManifest(t, data, []byte("snapshot-v1"), cache)
		defer testClose(t, manifest)
		if got, err := manifest.Read(context.Background(), "a-b", 0, 5, nil, true); err != nil || string(got) != "hello" {
			t.Fatalf("read: %q %v", got, err)
		}
		if source.payloadReads != wantReads {
			t.Fatalf("source reads = %d, want %d", source.payloadReads, wantReads)
		}
	}
	read(cache, 1)
	restarted, err := NewTieredDecodedCache(0, options)
	if err != nil {
		t.Fatal(err)
	}
	read(restarted, 0)
	entries, err := os.ReadDir(options.Directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("files: %v %v", entries, err)
	}
	if err := os.WriteFile(filepath.Join(options.Directory, entries[0].Name()), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	read(restarted, 1)
	read(restarted, 0)
}

func TestTieredCacheConcurrentAccess(t *testing.T) {
	cache, err := NewTieredDecodedCache(4096, DiskDecodedCacheOptions{Directory: t.TempDir(), Capacity: 4096})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range byte(8) {
		wg.Go(func() {
			value := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{i}}, raw: []byte("data"), verified: true}
			for range 10 {
				if err := cache.Put(context.Background(), value); err != nil {
					t.Error(err)
					return
				}
				if got, ok, err := cache.Get(context.Background(), value.key); err != nil || ok && (got.key != value.key || string(got.raw) != "data" || !got.verified) {
					t.Errorf("concurrent hit: %#v %v %v", got, ok, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if cache.MemoryUsedBytes() > 4096 || cache.DiskUsedBytes() > 4096 {
		t.Fatal("concurrent writes exceeded budget")
	}
}

func TestDiskCachePutSkipsExistingAndConcurrentUse(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	value := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{9}}, raw: []byte("payload"), verified: true}
	if err := cache.Put(ctx, value); err != nil {
		t.Fatal(err)
	}
	path := cache.path(diskCacheName(diskCacheKey(value.key)))
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Put(ctx, value); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("existing key was rewritten")
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				v := CachedChunk{key: DecodedCacheKey{Identity: [32]byte{byte(i % 40), byte(g % 2)}}, raw: bytes.Repeat([]byte{byte(i)}, 100), verified: i%2 == 0}
				_ = cache.Put(ctx, v)           //nolint:errcheck // Stress test; consistency is checked below.
				_, _, _ = cache.Get(ctx, v.key) //nolint:errcheck // Stress test.
				if i%50 == 0 {
					_ = cache.Clear() //nolint:errcheck // Stress test.
				}
			}
		})
	}
	wg.Wait()
	files, err := filepath.Glob(filepath.Join(dir, "*"+diskCacheSuffix))
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		total += uint64(info.Size())
	}
	if total != cache.UsedBytes() || len(files) != len(cache.entries) {
		t.Fatalf("index drift: disk=%d/%d index=%d/%d", total, len(files), cache.UsedBytes(), len(cache.entries))
	}
}

func TestDiskCacheRecoverySkipsUndeletableFiles(t *testing.T) {
	dir := t.TempDir()
	// A directory named like a chunk and an unparsable chunk name must not fail opening.
	if err := os.Mkdir(filepath.Join(dir, strings.Repeat("a", 64)+diskCacheSuffix), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "junk.chunk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDiskDecodedCache(DiskDecodedCacheOptions{Directory: dir, Capacity: 1 << 20}); err != nil {
		t.Fatal(err)
	}
}
