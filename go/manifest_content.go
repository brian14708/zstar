package zstar

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"reflect"
	"sync"
)

// DecodedCacheKey identifies validated decoded content independently of
// compression hints. Archive keys include the immutable source namespace,
// authenticated metadata digest, and encoded locator. Object keys use their
// verified content digest.
type DecodedCacheKey struct {
	Kind         byte // 0 archive, 1 object
	Identity     [32]byte
	Metadata     [32]byte
	Offset       uint64
	EncodedSize  uint64
	ObjectDigest [32]byte
}

// SetSharedContentCache binds a caller-owned cache to this immutable encoded
// archive version. Configure it before starting concurrent reads. Inline bytes
// use this identity plus authenticated metadata and their encoded locator.
func (m *Manifest) SetSharedContentCache(identity []byte, cache DecodedContentCache) error {
	if cache == nil {
		m.content.mu.Lock()
		defer m.content.mu.Unlock()
		m.content.shared = nil
		return nil
	}
	if len(identity) == 0 || len(identity) > 65536 {
		return errors.New("zstar: shared cache identity must contain 1..65536 bytes")
	}
	h := sha256.New()
	h.Write([]byte("zstar/decoded-cache-identity/v1\x00"))
	h.Write(identity)
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	m.content.mu.Lock()
	defer m.content.mu.Unlock()
	select {
	case <-m.content.closed:
		return categorized("unsupported-operation", fs.ErrClosed)
	default:
	}
	m.content.identity = digest
	m.content.shared = cache
	return nil
}

// CachedChunk is an immutable token created only by a successful reader load.
// Its fields stay private so a backend cannot turn arbitrary bytes into
// validated content. Bytes returns a copy for serializers and diagnostics.
type CachedChunk struct {
	key      DecodedCacheKey
	raw      []byte
	verified bool
}

// Key returns the immutable identity of the validated chunk.
func (c CachedChunk) Key() DecodedCacheKey { return c.key }

// Bytes returns an owned copy of the decoded content.
func (c CachedChunk) Bytes() []byte { return bytes.Clone(c.raw) }

// Weight returns the retained allocation and bookkeeping charge in bytes.
func (c CachedChunk) Weight() uint64 { return uint64(cap(c.raw)) + 512 }

// DecodedContentCache stores immutable validated tokens. Get and Put failures
// are advisory and fall back to the archive source. Backends must bound storage
// and must not call back into the reader. Get and Put must support concurrent
// calls, including calls from different readers sharing the same cache.
type DecodedContentCache interface {
	Get(ctx context.Context, key DecodedCacheKey) (CachedChunk, bool, error)
	Put(ctx context.Context, value CachedChunk) error
}

type sharedMemoryEntry struct {
	key    DecodedCacheKey
	chunk  CachedChunk
	charge uint64
}

// MemoryDecodedCache stores immutable tokens in a bounded memory LRU.
// Methods are safe for concurrent use. Construct it with NewMemoryDecodedCache.
type MemoryDecodedCache struct {
	mu          sync.Mutex
	limit, used uint64
	values      map[DecodedCacheKey]*list.Element
	order       list.List
}

// NewMemoryDecodedCache creates a caller-owned cache. A zero limit disables
// retention. Readers never clear or close a supplied shared cache.
func NewMemoryDecodedCache(limit uint64) *MemoryDecodedCache {
	return &MemoryDecodedCache{limit: limit, values: make(map[DecodedCacheKey]*list.Element)}
}

// Get returns a cached immutable token or a miss. Memory lookups do not block
// on I/O and do not observe context cancellation.
func (c *MemoryDecodedCache) Get(_ context.Context, key DecodedCacheKey) (CachedChunk, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.values[key]
	if !ok {
		return CachedChunk{}, false, nil
	}
	c.order.MoveToBack(node)
	value := node.Value.(*sharedMemoryEntry).chunk //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
	return value, true, nil
}

// Put retains a token when it fits, evicting least recently used entries.
// Memory insertion does not observe context cancellation.
func (c *MemoryDecodedCache) Put(_ context.Context, value CachedChunk) error {
	c.put(value)
	return nil
}

func (c *MemoryDecodedCache) put(value CachedChunk) {
	c.mu.Lock()
	defer c.mu.Unlock()
	charge := uint64(cap(value.raw)) + 512
	if charge > c.limit {
		return
	}
	if old, ok := c.values[value.key]; ok {
		c.used -= old.Value.(*sharedMemoryEntry).charge //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
		c.order.Remove(old)
		delete(c.values, value.key)
	}
	for c.used+charge > c.limit {
		old := c.order.Front()
		entry := old.Value.(*sharedMemoryEntry) //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
		delete(c.values, entry.key)
		c.used -= entry.charge
		c.order.Remove(old)
	}
	c.values[value.key] = c.order.PushBack(&sharedMemoryEntry{key: value.key, chunk: value, charge: charge})
	c.used += charge
}

// UsedBytes reports retained allocation and bookkeeping bytes.
func (c *MemoryDecodedCache) UsedBytes() uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.used }

// Clear evicts all retained tokens.
func (c *MemoryDecodedCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.values)
	c.order.Init()
	c.used = 0
}

type manifestChunkKey struct {
	source      ExtentSource
	encodedSize uint64
	// Pointer resolvers are stable, comparable identities. Keeping the pointer
	// in a bounded cache entry also prevents address reuse after collection.
	resolver  EncodedContentResolver
	transient *uint8
}

func chunkKey(extent Extent) manifestChunkKey {
	key := manifestChunkKey{source: extent.Source, encodedSize: extent.EncodedSize}
	if _, object := extent.Source.ContentAddress(); object {
		key.encodedSize = 0
	}
	return key
}

type manifestContentChunk struct {
	raw      []byte
	verified bool
}
type manifestContentFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	chunk   *manifestContentChunk
	err     error
}
type manifestContentCached struct {
	key    manifestChunkKey
	chunk  *manifestContentChunk
	charge uint64
}
type manifestContentState struct {
	decoders     frameDecoderPool
	mu           sync.Mutex
	source       RangeReader
	limit, bytes uint64
	cache        map[manifestChunkKey]*list.Element
	lru          list.List
	flights      map[manifestChunkKey]*manifestContentFlight
	slots        chan struct{}
	closed       chan struct{}
	shared       DecodedContentCache
	identity     [32]byte
	metadata     [32]byte
}

func archiveCacheKey(extent Extent, identity, metadata [32]byte) (DecodedCacheKey, bool) {
	at, ok := extent.Source.ArchiveOffset()
	if !ok {
		return DecodedCacheKey{}, false
	}
	return DecodedCacheKey{Kind: 0, Identity: identity, Metadata: metadata, Offset: at, EncodedSize: extent.EncodedSize}, true
}

func newManifestContentState(options ManifestOptions, source RangeReader) *manifestContentState {
	return &manifestContentState{
		source: source, limit: options.ContentCacheBytes,
		cache: make(map[manifestChunkKey]*list.Element), flights: make(map[manifestChunkKey]*manifestContentFlight),
		slots: make(chan struct{}, options.ConcurrentChunks), closed: make(chan struct{}),
	}
}

// Sources and resolvers can restrict private decoded-cache admission.
func (s *manifestContentState) cacheAllowed(extent Extent, resolver EncodedContentResolver) bool {
	if _, object := extent.Source.ContentAddress(); object {
		if policy, ok := resolver.(interface{ ObjectCacheAllowed([32]byte) bool }); ok {
			digest, err := ParseContentAddress(extent.Source.address)
			return err == nil && policy.ObjectCacheAllowed(digest)
		}
	} else if policy, ok := s.source.(interface{ CacheAllowed() bool }); ok {
		return policy.CacheAllowed()
	}
	return true
}

// Cache-owned decoded bytes are private and immutable. External keys exclude
// recompression-dependent hints. Checked reads promote unchecked bytes only
// after successful hashing; failed promotion leaves their provenance intact.
func (s *manifestContentState) get(ctx context.Context, extent Extent, resolver EncodedContentResolver, verify bool) (*manifestContentChunk, error) {
	key := chunkKey(extent)
	_, object := extent.Source.ContentAddress()
	if object {
		value := reflect.ValueOf(resolver)
		if value.IsValid() && value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return nil, rejected("unsupported-operation", "external resolver required")
			}
			key.resolver = resolver
		} else {
			// Function/value implementations may be noncomparable and cannot
			// safely establish a namespace across operations. Batch planning
			// still groups repeated extents before entering this loader.
			key.transient = new(uint8)
		}
	}
	wantVerified := object && verify
	var candidate *manifestContentChunk
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		allowed := s.cacheAllowed(extent, resolver)
		s.mu.Lock()
		select {
		case <-s.closed:
			s.mu.Unlock()
			return nil, categorized("unsupported-operation", fs.ErrClosed)
		default:
		}
		if node := s.cache[key]; node != nil && allowed {
			s.lru.MoveToBack(node)
			candidate = node.Value.(*manifestContentCached).chunk //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
		}
		if candidate != nil && (!wantVerified || candidate.verified) {
			s.mu.Unlock()
			return candidate, nil
		}
		flight := s.flights[key]
		if flight == nil {
			// Preserve source callback context values while making cancellation
			// depend on all active waiters rather than the first caller alone.
			work, cancel := context.WithCancel(context.WithoutCancel(ctx))
			flight = &manifestContentFlight{done: make(chan struct{}), cancel: cancel}
			s.flights[key] = flight
			go s.run(work, key, flight, candidate, extent, resolver, wantVerified)
		}
		flight.waiters++
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.leave(key, flight)
			return nil, ctx.Err()
		case <-s.closed:
			s.leave(key, flight)
			return nil, categorized("unsupported-operation", fs.ErrClosed)
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			candidate = flight.chunk
			if !wantVerified || candidate.verified {
				return candidate, nil
			}
			// Join/create a verification flight for a shared unchecked result.
		}
	}
}

func (s *manifestContentState) leave(key manifestChunkKey, flight *manifestContentFlight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	flight.waiters--
	if flight.waiters == 0 && s.flights[key] == flight {
		delete(s.flights, key)
		flight.cancel()
	}
}

func (s *manifestContentState) run(ctx context.Context, key manifestChunkKey, flight *manifestContentFlight, candidate *manifestContentChunk, extent Extent, resolver EncodedContentResolver, verify bool) {
	defer flight.cancel()
	var result *manifestContentChunk
	var err error
	select {
	case s.slots <- struct{}{}:
		result, err = s.load(ctx, candidate, extent, resolver, verify)
		<-s.slots
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flights[key] == flight {
		delete(s.flights, key)
		if err == nil && key.transient == nil && s.cacheAllowed(extent, resolver) {
			charge := uint64(cap(result.raw)) + 320
			if charge <= s.limit {
				if old := s.cache[key]; old != nil {
					s.bytes -= old.Value.(*manifestContentCached).charge //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
					s.lru.Remove(old)
					delete(s.cache, key)
				}
				for s.bytes > s.limit-charge {
					old := s.lru.Front()
					entry := old.Value.(*manifestContentCached) //nolint:errcheck // LRU nodes are inserted only with this private concrete type.
					s.bytes -= entry.charge
					delete(s.cache, entry.key)
					s.lru.Remove(old)
				}
				s.cache[key] = s.lru.PushBack(&manifestContentCached{key, result, charge})
				s.bytes += charge
			}
		}
	}
	flight.chunk, flight.err = result, err
	close(flight.done)
}

func (s *manifestContentState) load(ctx context.Context, candidate *manifestContentChunk, extent Extent, resolver EncodedContentResolver, verify bool) (*manifestContentChunk, error) {
	s.mu.Lock()
	sharedCache, identity, metadata := s.shared, s.identity, s.metadata
	s.mu.Unlock()
	var encoded []byte
	var err error
	var digest [32]byte
	_, object := extent.Source.ContentAddress()
	if object {
		digest, err = ParseContentAddress(extent.Source.address)
		if err != nil {
			return nil, err
		}
	}
	var sharedKey DecodedCacheKey
	shared := false
	if sharedCache != nil {
		if object && verify {
			sharedKey = DecodedCacheKey{Kind: 1, ObjectDigest: digest}
			shared = true
		} else if !object {
			sharedKey, shared = archiveCacheKey(extent, identity, metadata)
		}
	}
	sharedHit := false
	if candidate == nil && shared && s.cacheAllowed(extent, resolver) {
		if cached, ok, cacheErr := sharedCache.Get(ctx, sharedKey); cacheErr == nil && ok && cached.key == sharedKey && cached.verified {
			candidate = &manifestContentChunk{raw: cached.raw, verified: cached.verified}
			sharedHit = true
		}
	}
	if candidate == nil {
		prefetched, ok := ctx.Value(encodedPrefetchKey{}).(encodedPrefetch)
		if !ok {
			prefetched = nil
		}
		if frame := prefetched[chunkKey(extent)]; frame != nil {
			encoded = frame
		} else if object {
			if resolver == nil {
				return nil, rejected("unsupported-operation", "external resolver required")
			}
			encoded, err = resolver.GetEncoded(ctx, digest, extent.EncodedSize, 17<<20)
		} else {
			encoded, err = s.source.ReadRange(ctx, int64(extent.Source.offset), int64(extent.EncodedSize))
			if err == nil && uint64(len(encoded)) != extent.EncodedSize {
				err = rejected("malformed-frame", "short archive frame")
			}
		}
		if err != nil {
			return nil, err
		}
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if len(encoded) > 17<<20 {
			category := "malformed-frame"
			if object {
				category = "malformed-external-content"
			}
			return nil, rejected(category, "encoded input limit")
		}
		if !object && (len(encoded) < 5 || encoded[4]&4 == 0) {
			return nil, rejected("malformed-frame", "missing data checksum")
		}
		raw, err := s.decoders.decode(encoded, 0)
		if err != nil {
			if object {
				return nil, &FormatError{Category: "malformed-external-content", Cause: err}
			}
			return nil, categorized("malformed-frame", err)
		}
		candidate = &manifestContentChunk{raw: raw}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if object && verify && !candidate.verified {
		if err = VerifySHA256(candidate.raw, digest); err != nil {
			return nil, err
		}
		candidate = &manifestContentChunk{raw: candidate.raw, verified: true}
	}
	if !sharedHit && shared && sharedCache != nil && candidate != nil && (!object || candidate.verified) && s.cacheAllowed(extent, resolver) {
		_ = sharedCache.Put(ctx, CachedChunk{key: sharedKey, raw: bytes.Clone(candidate.raw), verified: !object || candidate.verified}) //nolint:errcheck // Shared-cache failures are advisory and must not fail validated reads.
	}
	return candidate, nil
}

func (s *manifestContentState) close() {
	s.decoders.close()
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
		return
	default:
		close(s.closed)
	}
	for key, flight := range s.flights {
		flight.cancel()
		delete(s.flights, key)
	}
	clear(s.cache)
	s.lru.Init()
	s.bytes = 0
}

// ContentCacheBytes includes retained decoded allocation capacity and bookkeeping.
// Caller-owned read results and in-progress work are outside this cache budget.
func (m *Manifest) ContentCacheBytes() uint64 {
	m.content.mu.Lock()
	defer m.content.mu.Unlock()
	return m.content.bytes
}

func (s *manifestContentState) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return categorized("unsupported-operation", fs.ErrClosed)
	default:
		return nil
	}
}
