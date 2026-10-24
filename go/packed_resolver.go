package zstar

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
)

// ObjectRequest selects a complete frame; Hint is advisory.
type ObjectRequest struct {
	Digest         [32]byte
	Hint, MaxBytes uint64
}

// BatchEncodedContentResolver communicates bounded miss plans optionally.
type BatchEncodedContentResolver interface {
	EncodedContentResolver
	GetEncodedMany(context.Context, []ObjectRequest) ([][]byte, error)
	EncodedLength([32]byte) (uint64, bool)
}

// PackedResolverOptions independently bounds ranges, overfetch, batches and retention.
type PackedResolverOptions struct {
	MaxRangeBytes, MaxOverfetchBytes, MaxBatchBytes, CacheBytes uint64
	Concurrency                                                 int
}
type packedFlight struct {
	gate chan struct{}
	refs int
	data []byte
}

// PackedObjectResolver owns one immutable catalog and a separate encoded cache.
type PackedObjectResolver struct {
	store   PackStore
	catalog *PackedCatalog
	options PackedResolverOptions
	mu      sync.Mutex
	flights map[[32]byte]*packedFlight
	cache   map[[32]byte][]byte
	used    uint64
	slots   chan struct{}
}

// NewPackedObjectResolver opens an explicit catalog without network discovery.
func NewPackedObjectResolver(store PackStore, catalog *PackedCatalog, o PackedResolverOptions) (*PackedObjectResolver, error) {
	if o.MaxRangeBytes == 0 {
		o.MaxRangeBytes = 8 << 20
	}
	if o.MaxBatchBytes == 0 {
		o.MaxBatchBytes = 64 << 20
	}
	if o.Concurrency == 0 {
		o.Concurrency = 8
	}
	if store == nil || catalog == nil || o.MaxRangeBytes > o.MaxBatchBytes || o.MaxBatchBytes > 256<<20 || o.MaxOverfetchBytes > o.MaxRangeBytes || o.Concurrency < 1 || o.Concurrency > 32 {
		return nil, errors.New("zstar: invalid packed resolver options")
	}
	return &PackedObjectResolver{store: store, catalog: catalog, options: o, flights: make(map[[32]byte]*packedFlight), cache: make(map[[32]byte][]byte), slots: make(chan struct{}, o.Concurrency)}, nil
}

// EncodedLength returns the catalog representation length, independent of hints.
func (r *PackedObjectResolver) EncodedLength(d [32]byte) (uint64, bool) {
	i, ok := r.catalog.byChunk[d]
	if !ok {
		return 0, false
	}
	return r.catalog.chunks[i].EncodedLength, true
}

// ObjectCacheAllowed follows the containing pack's response policy.
func (r *PackedObjectResolver) ObjectCacheAllowed(d [32]byte) bool {
	if policy, ok := r.store.(interface{ PackCacheAllowed(PackSnapshot) bool }); ok {
		index, found := r.catalog.byChunk[d]
		if !found {
			return false
		}
		return policy.PackCacheAllowed(r.catalog.packs[r.catalog.byPack[r.catalog.chunks[index].Pack]].Snapshot)
	}
	return true
}

// GetEncoded returns one owned complete frame under the actual-byte limit.
func (r *PackedObjectResolver) GetEncoded(ctx context.Context, d [32]byte, hint, maxBytes uint64) ([]byte, error) {
	out, err := r.GetEncodedMany(ctx, []ObjectRequest{{d, hint, maxBytes}})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

// GetEncodedMany shares flights and coalesces bounded conditional ranges.
func (r *PackedObjectResolver) GetEncodedMany(ctx context.Context, requests []ObjectRequest) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(requests) > MaxBatchRequests {
		return nil, rejected("resource-limit", "packed batch count")
	}
	unique := make(map[[32]byte]PackedChunk)
	var total uint64
	for _, q := range requests {
		i, ok := r.catalog.byChunk[q.Digest]
		if !ok {
			return nil, rejected("missing-external-content", "packed chunk")
		}
		c := r.catalog.chunks[i]
		if c.EncodedLength > q.MaxBytes {
			return nil, rejected("resource-limit", "encoded object size")
		}
		if c.EncodedLength > r.options.MaxBatchBytes-total {
			return nil, rejected("resource-limit", "packed batch bytes")
		}
		total += c.EncodedLength
		unique[q.Digest] = c
	}
	digests := make([][32]byte, 0, len(unique))
	for d := range unique {
		digests = append(digests, d)
	}
	sort.Slice(digests, func(i, j int) bool { return bytes.Compare(digests[i][:], digests[j][:]) < 0 })
	held := make(map[[32]byte]*packedFlight)
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for d, f := range held {
			f.refs--
			if f.refs == 0 {
				delete(r.flights, d)
			}
			f.gate <- struct{}{}
		}
	}()
	for _, d := range digests {
		r.mu.Lock()
		f := r.flights[d]
		if f == nil {
			f = &packedFlight{gate: make(chan struct{}, 1)}
			f.gate <- struct{}{}
			r.flights[d] = f
		}
		f.refs++
		r.mu.Unlock()
		select {
		case <-f.gate:
			held[d] = f
		case <-ctx.Done():
			r.mu.Lock()
			f.refs--
			if f.refs == 0 {
				delete(r.flights, d)
			}
			r.mu.Unlock()
			return nil, ctx.Err()
		}
		if f.data == nil && r.ObjectCacheAllowed(d) {
			r.mu.Lock()
			f.data = r.cache[d]
			r.mu.Unlock()
		}
	}
	misses := make([]PackedChunk, 0)
	for _, d := range digests {
		if held[d].data == nil {
			misses = append(misses, unique[d])
		}
	}
	sort.Slice(misses, func(i, j int) bool {
		if misses[i].Pack != misses[j].Pack {
			return bytes.Compare(misses[i].Pack[:], misses[j].Pack[:]) < 0
		}
		return misses[i].Offset < misses[j].Offset
	})
	type group struct {
		pack       [32]byte
		start, end uint64
		chunks     []PackedChunk
	}
	groups := make([]group, 0)
	var overfetch uint64
	for _, c := range misses {
		end := c.Offset + c.EncodedLength
		if len(groups) > 0 {
			g := &groups[len(groups)-1]
			gap := c.Offset - min(c.Offset, g.end)
			if g.pack == c.Pack && end-g.start <= r.options.MaxRangeBytes && gap <= r.options.MaxOverfetchBytes-overfetch && gap <= r.options.MaxBatchBytes-total-overfetch {
				g.end = end
				g.chunks = append(g.chunks, c)
				overfetch += gap
				continue
			}
		}
		groups = append(groups, group{c.Pack, c.Offset, end, []PackedChunk{c}})
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan group)
	var wg sync.WaitGroup
	var once sync.Once
	var resultError error
	for range min(r.options.Concurrency, len(groups)) {
		wg.Go(func() {
			for g := range queue {
				err := func() error {
					select {
					case r.slots <- struct{}{}:
					case <-work.Done():
						return work.Err()
					}
					defer func() { <-r.slots }()
					snapshot := r.catalog.packs[r.catalog.byPack[g.pack]].Snapshot
					data, err := r.store.ReadPackedRange(work, snapshot, g.start, g.end-g.start)
					if err != nil {
						return err
					}
					if uint64(len(data)) != g.end-g.start {
						return rejected("malformed-external-content", "short packed range")
					}
					for _, c := range g.chunks {
						frame := bytes.Clone(data[c.Offset-g.start : c.Offset-g.start+c.EncodedLength])
						held[c.Digest].data = frame
						charge := uint64(cap(frame)) + 128
						if charge <= r.options.CacheBytes && r.ObjectCacheAllowed(c.Digest) {
							r.mu.Lock()
							for r.used > r.options.CacheBytes-charge {
								for d, old := range r.cache {
									delete(r.cache, d)
									r.used -= uint64(cap(old)) + 128
									break
								}
							}
							if old := r.cache[c.Digest]; old != nil {
								r.used -= uint64(cap(old)) + 128
							}
							r.cache[c.Digest] = frame
							r.used += charge
							r.mu.Unlock()
						}
					}
					return nil
				}()
				if err != nil {
					once.Do(func() { resultError = err; cancel() })
					return
				}
			}
		})
	}
send:
	for _, g := range groups {
		select {
		case queue <- g:
		case <-work.Done():
			break send
		}
	}
	close(queue)
	wg.Wait()
	if resultError != nil {
		return nil, resultError
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([][]byte, len(requests))
	for i, q := range requests {
		out[i] = bytes.Clone(held[q.Digest].data)
	}
	return out, nil
}
