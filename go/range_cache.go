package zstar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
)

// RangeCacheOptions controls encoded-range retention and fetch concurrency.
// Start with DefaultRangeCacheOptions; zero CapacityBytes disables retention.
type RangeCacheOptions struct {
	CapacityBytes  int64
	MaxEntries     int
	ReadAheadBytes int64
	// MaxFetchBytes limits expansion, not explicitly requested range lengths.
	MaxFetchBytes int64
	MaxInFlight   int
}

// DefaultRangeCacheOptions disables speculation and retains up to 16 MiB.
func DefaultRangeCacheOptions() RangeCacheOptions {
	return RangeCacheOptions{CapacityBytes: 16 << 20, MaxEntries: 1024, MaxFetchBytes: 8 << 20, MaxInFlight: 8}
}

// RangeCacheStats is a consistent snapshot of cumulative counters and retention.
// SourceBytes counts successful exact-length responses, excluding protocol overhead.
type RangeCacheStats struct {
	ReadRequests, CacheHits, CoalescedReads, SourceReads, SourceBytes, Evictions uint64
	CachedBytes                                                                  int64
	CachedEntries                                                                int
}

type rangeCacheEntry struct {
	offset int64
	data   []byte
}

type rangeCacheFlight struct {
	offset, length int64
	done           chan struct{}
	data           []byte
	err            error
}

// CachedRangeReader decorates one immutable snapshot. It caches encoded bytes;
// decoding and validation still belong to the archive. Containing intervals can
// satisfy reads; partial overlaps are not stitched and adjacent reads are not
// merged. Retention prefers containing intervals over redundant subranges.
// Cache hits do not revalidate source identity.
//
// Returned slices are caller-owned copies. Capacity counts retained payload
// lengths, excluding in-flight buffers, returned copies, and bookkeeping. The
// caller owns the underlying source. No background goroutines are started:
// cancellation of a fetch leader lets waiting readers retry with their contexts.
type CachedRangeReader struct {
	source     RangeReader
	options    RangeCacheOptions
	slots      chan struct{}
	mu         sync.Mutex
	cache      []rangeCacheEntry // least recently used first
	flights    []*rangeCacheFlight
	stats      RangeCacheStats
	size       int64
	sized      bool
	sizeFlight chan struct{}
}

// NewCachedRangeReader validates explicit options and wraps source.
func NewCachedRangeReader(source RangeReader, options RangeCacheOptions) (*CachedRangeReader, error) {
	if source == nil || options.CapacityBytes < 0 || options.MaxEntries <= 0 || options.ReadAheadBytes < 0 || options.MaxFetchBytes <= 0 || options.MaxInFlight < 1 || options.MaxInFlight > 1024 {
		return nil, errors.New("zstar: invalid range cache options")
	}
	return &CachedRangeReader{source: source, options: options, slots: make(chan struct{}, options.MaxInFlight)}, nil
}

// Size discovers and retains the immutable source size. Errors are not cached.
// Concurrent callers share discovery; waiters can retry a cancelled discovery.
func (r *CachedRangeReader) Size(ctx context.Context) (int64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		r.mu.Lock()
		if r.sized {
			size := r.size
			r.mu.Unlock()
			return size, nil
		}
		if pending := r.sizeFlight; pending != nil {
			r.mu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		pending := make(chan struct{})
		r.sizeFlight = pending
		r.mu.Unlock()
		size, err := r.source.Size(ctx)
		if err == nil && size < 0 {
			err = rejected(CategoryInvalidRange, "negative source size")
		}
		if err == nil {
			err = ctx.Err()
		}
		r.mu.Lock()
		if err == nil {
			r.size, r.sized = size, true
		}
		r.sizeFlight = nil
		close(pending)
		r.mu.Unlock()
		return size, err
	}
}

// Stats returns counters without changing cache recency.
func (r *CachedRangeReader) Stats() RangeCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := r.stats
	stats.CachedEntries = len(r.cache)
	return stats
}

// ReadRange returns an owned copy of an exact range, checking cancellation even
// on cache hits. Underlying errors are preserved and never retained in the cache.
func (r *CachedRangeReader) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	r.mu.Lock()
	r.stats.ReadRequests++
	r.mu.Unlock()
	size, err := r.Size(ctx)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > size || length > size-offset {
		return nil, rejected(CategoryInvalidRange, "source range out of bounds")
	}
	if uint64(length) > uint64(^uint(0)>>1) {
		return nil, rejected(CategoryResourceLimit, "source range exceeds address space")
	}
	if length == 0 {
		return []byte{}, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		data, flight := r.lookup(offset, length)
		r.mu.Unlock()
		if data == nil && flight == nil {
			select {
			case r.slots <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			// Recheck under the same lock as registration: another fetch may
			// have completed or been admitted while we waited for a slot.
			r.mu.Lock()
			data, flight = r.lookup(offset, length)
			if data == nil && flight == nil {
				fetchLength := length
				if length < r.options.MaxFetchBytes {
					fetchLength += min(r.options.ReadAheadBytes, r.options.MaxFetchBytes-length, size-offset-length, int64(^uint(0)>>1)-length)
				}
				flight = &rangeCacheFlight{offset: offset, length: fetchLength, done: make(chan struct{})}
				r.flights = append(r.flights, flight)
				r.mu.Unlock()
				data, err := r.fetch(ctx, flight)
				if err != nil {
					return nil, err
				}
				return cloneRange(ctx, data[:length])
			}
			r.mu.Unlock()
			<-r.slots
		}
		if data != nil {
			return cloneRange(ctx, data)
		}
		data, err := waitRange(ctx, flight, offset, length)
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() == nil {
			continue
		}
		return data, err
	}
}

// fetch owns an admission slot and publishes the result before releasing it.
func (r *CachedRangeReader) fetch(ctx context.Context, flight *rangeCacheFlight) ([]byte, error) {
	defer func() { <-r.slots }()
	// Cancellation may have raced with admission, so check before calling source.
	err := ctx.Err()
	var data []byte
	if err == nil {
		r.mu.Lock()
		r.stats.SourceReads++
		r.mu.Unlock()
		data, err = r.source.ReadRange(ctx, flight.offset, flight.length)
		if err == nil && int64(len(data)) != flight.length {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			err = ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err == nil {
		r.stats.SourceBytes += uint64(len(data))
		r.retain(flight.offset, data)
		flight.data = data
	}
	flight.err = err
	index := slices.Index(r.flights, flight)
	r.flights = slices.Delete(r.flights, index, index+1)
	close(flight.done)
	return data, err
}

// lookup and retain require mu. Internal byte slices are immutable after publication.
func (r *CachedRangeReader) lookup(offset, length int64) ([]byte, *rangeCacheFlight) {
	for i, entry := range r.cache {
		if containsRange(entry.offset, int64(len(entry.data)), offset, length) {
			copy(r.cache[i:], r.cache[i+1:])
			r.cache[len(r.cache)-1] = entry
			r.stats.CacheHits++
			start := offset - entry.offset
			return entry.data[start : start+length], nil
		}
	}
	for _, flight := range r.flights {
		if containsRange(flight.offset, flight.length, offset, length) {
			r.stats.CoalescedReads++
			return nil, flight
		}
	}
	return nil, nil
}

func (r *CachedRangeReader) retain(offset int64, data []byte) {
	size := int64(len(data))
	if size > r.options.CapacityBytes {
		return
	}
	// A larger concurrent fetch may already have retained this entire range.
	for _, entry := range r.cache {
		if containsRange(entry.offset, int64(len(entry.data)), offset, size) {
			return
		}
	}
	// Superseded intervals are redundant, rather than capacity evictions.
	r.cache = slices.DeleteFunc(r.cache, func(entry rangeCacheEntry) bool {
		if containsRange(offset, size, entry.offset, int64(len(entry.data))) {
			r.stats.CachedBytes -= int64(len(entry.data))
			return true
		}
		return false
	})
	evicted := 0
	for r.stats.CachedBytes > r.options.CapacityBytes-size || len(r.cache)-evicted >= r.options.MaxEntries {
		r.stats.CachedBytes -= int64(len(r.cache[evicted].data))
		evicted++
	}
	r.cache = slices.Delete(r.cache, 0, evicted)
	r.stats.Evictions += uint64(evicted)
	// Retain the owned response directly unless it has spare capacity.
	if cap(data) > len(data) {
		data = bytes.Clone(data)
	}
	r.cache = append(r.cache, rangeCacheEntry{offset: offset, data: data})
	r.stats.CachedBytes += size
}

// containsRange avoids adding offsets, which could overflow near MaxInt64.
// All offsets and lengths have already been validated as nonnegative.
func containsRange(outerOffset, outerLength, offset, length int64) bool {
	start := offset - outerOffset
	return start >= 0 && start <= outerLength && length <= outerLength-start
}

func waitRange(ctx context.Context, flight *rangeCacheFlight, offset, length int64) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
	}
	if flight.err != nil {
		return nil, flight.err
	}
	start := offset - flight.offset
	return cloneRange(ctx, flight.data[start:start+length])
}

func cloneRange(ctx context.Context, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return bytes.Clone(data), nil
}
