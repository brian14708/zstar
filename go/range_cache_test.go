package zstar

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type cacheTestSource struct {
	calls     atomic.Int64
	sizeCalls atomic.Int64
	size      func(context.Context) (int64, error)
	read      func(context.Context, int64, int64) ([]byte, error)
}

func (s *cacheTestSource) Size(ctx context.Context) (int64, error) {
	s.sizeCalls.Add(1)
	if s.size != nil {
		return s.size(ctx)
	}
	return 64, ctx.Err()
}

func (s *cacheTestSource) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	s.calls.Add(1)
	if s.read != nil {
		return s.read(ctx, offset, length)
	}
	return bytes.Repeat([]byte{byte(offset)}, int(length)), nil
}

func newTestRangeCache(t *testing.T, s RangeReader, options RangeCacheOptions) *CachedRangeReader {
	t.Helper()
	r, err := NewCachedRangeReader(s, options)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRangeCacheOwnershipReadAheadAndBounds(t *testing.T) {
	ctx := t.Context()
	source := &cacheTestSource{}
	options := DefaultRangeCacheOptions()
	options.ReadAheadBytes = 8
	r := newTestRangeCache(t, source, options)
	data, err := r.ReadRange(ctx, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 99
	data, err = r.ReadRange(ctx, 4, 10)
	if err != nil || len(data) != 10 || data[0] != 4 || source.calls.Load() != 1 {
		t.Fatalf("cache ownership/read-ahead: %v %v", data, err)
	}
	for _, request := range [][2]int64{{-1, 1}, {0, -1}, {63, 2}, {65, 0}} {
		if _, err := r.ReadRange(ctx, request[0], request[1]); err == nil {
			t.Fatal("accepted invalid range")
		}
	}
	if data, err := r.ReadRange(ctx, 64, 0); err != nil || len(data) != 0 {
		t.Fatalf("empty EOF: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.ReadRange(cancelled, 4, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached cancellation: %v", err)
	}
	if source.sizeCalls.Load() != 1 {
		t.Fatal("source size was not cached")
	}
	if stats := r.Stats(); stats.CachedBytes != 12 || stats.CacheHits != 1 || stats.SourceBytes != 12 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestRangeCacheEvictionAndDisabledRetention(t *testing.T) {
	ctx := t.Context()
	options := DefaultRangeCacheOptions()
	options.CapacityBytes, options.MaxEntries = 8, 2
	r := newTestRangeCache(t, &cacheTestSource{}, options)
	for _, offset := range []int64{0, 4, 0, 8, 0, 4} {
		if _, err := r.ReadRange(ctx, offset, 4); err != nil {
			t.Fatal(err)
		}
	}
	if stats := r.Stats(); stats.SourceReads != 4 || stats.CacheHits != 2 || stats.Evictions != 2 || stats.CachedBytes != 8 {
		t.Fatalf("LRU: %+v", stats)
	}
	options.CapacityBytes = 0
	r = newTestRangeCache(t, &cacheTestSource{}, options)
	for range 2 {
		if _, err := r.ReadRange(ctx, 0, 4); err != nil {
			t.Fatal(err)
		}
	}
	if stats := r.Stats(); stats.CachedBytes != 0 || stats.SourceReads != 2 {
		t.Fatalf("disabled: %+v", stats)
	}
}

func TestRangeCacheErrorsAndExactFetchLimits(t *testing.T) {
	ctx := t.Context()
	failure := errors.New("source failure")
	source := &cacheTestSource{read: func(context.Context, int64, int64) ([]byte, error) { return nil, failure }}
	options := DefaultRangeCacheOptions()
	r := newTestRangeCache(t, source, options)
	if _, err := r.ReadRange(ctx, 0, 4); !errors.Is(err, failure) {
		t.Fatalf("error identity: %v", err)
	}
	source.read = func(context.Context, int64, int64) ([]byte, error) { return []byte{1}, nil }
	if _, err := r.ReadRange(ctx, 0, 4); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short response: %v", err)
	}
	options.MaxFetchBytes, options.ReadAheadBytes = 8, 32
	source.read = nil
	r = newTestRangeCache(t, source, options)
	if _, err := r.ReadRange(ctx, 0, 12); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadRange(ctx, 60, 2); err != nil {
		t.Fatal(err)
	}
	if stats := r.Stats(); stats.SourceBytes != 16 {
		t.Fatalf("explicit larger range / EOF expansion: %+v", stats)
	}
}

// readCacheRange is for reads expected to succeed, including reads in test goroutines.
func readCacheRange(ctx context.Context, t *testing.T, r *CachedRangeReader, offset, length int64) []byte {
	t.Helper()
	data, err := r.ReadRange(ctx, offset, length)
	if err != nil {
		t.Errorf("ReadRange(%d, %d): %v", offset, length, err)
	}
	return data
}

func TestRangeCacheSharedFetchCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		source := &cacheTestSource{}
		source.read = func(ctx context.Context, _, length int64) ([]byte, error) {
			// Only the leader blocks; its waiter retries after cancellation.
			if source.calls.Load() == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return make([]byte, int(length)), nil
		}
		options := DefaultRangeCacheOptions()
		options.CapacityBytes = 0
		r := newTestRangeCache(t, source, options)
		leaderCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var leaderErr error
		go func() { _, leaderErr = r.ReadRange(leaderCtx, 0, 8) }()
		synctest.Wait()
		go readCacheRange(ctx, t, r, 2, 2)
		synctest.Wait()
		if stats := r.Stats(); stats.CoalescedReads != 1 {
			t.Fatalf("waiter did not join leader: %+v", stats)
		}
		cancel()
		synctest.Wait()
		if !errors.Is(leaderErr, context.Canceled) {
			t.Fatalf("leader: %v", leaderErr)
		}
		if source.calls.Load() != 2 {
			t.Fatal("waiter did not retry")
		}
	})
}

func TestRangeCacheAdmissionRechecksCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		release := make(chan struct{})
		source := &cacheTestSource{read: func(ctx context.Context, _, length int64) ([]byte, error) {
			select {
			case <-release:
				return make([]byte, int(length)), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		options := DefaultRangeCacheOptions()
		options.MaxInFlight = 1
		r := newTestRangeCache(t, source, options)
		go readCacheRange(ctx, t, r, 0, 8)
		synctest.Wait()
		for range 2 {
			go readCacheRange(ctx, t, r, 16, 8)
		}
		synctest.Wait()
		close(release)
		synctest.Wait()
		if source.calls.Load() != 2 {
			t.Fatalf("duplicate queued fetch: %d", source.calls.Load())
		}
	})
}

func TestRangeCacheRejectsInvalidOptions(t *testing.T) {
	for _, options := range []RangeCacheOptions{{}, {CapacityBytes: -1, MaxEntries: 1, MaxFetchBytes: 1, MaxInFlight: 1}, {MaxEntries: 1, MaxFetchBytes: 1, MaxInFlight: 1025}} {
		if _, err := NewCachedRangeReader(&cacheTestSource{}, options); err == nil {
			t.Fatal("accepted invalid options")
		}
	}
}

func TestRangeCacheSharingAndIndependentWaiterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		release := make(chan struct{})
		source := &cacheTestSource{read: func(ctx context.Context, _, length int64) ([]byte, error) {
			select {
			case <-release:
				return bytes.Repeat([]byte{7}, int(length)), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		options := DefaultRangeCacheOptions()
		options.ReadAheadBytes, options.MaxInFlight = 8, 1
		r := newTestRangeCache(t, source, options)
		var first, second []byte
		go func() { first = readCacheRange(ctx, t, r, 0, 4) }()
		synctest.Wait()

		// Both a coalesced waiter and a queued miss cancel independently.
		for _, offset := range []int64{6, 32} {
			waiterCtx, cancel := context.WithCancel(ctx)
			var waiterErr error
			go func() { _, waiterErr = r.ReadRange(waiterCtx, offset, 2) }()
			synctest.Wait()
			cancel()
			synctest.Wait()
			if !errors.Is(waiterErr, context.Canceled) {
				t.Fatalf("waiter at %d: %v", offset, waiterErr)
			}
		}
		go func() { second = readCacheRange(ctx, t, r, 6, 2) }()
		synctest.Wait()
		if stats := r.Stats(); stats.CoalescedReads != 2 {
			t.Fatalf("waiters did not share fetch: %+v", stats)
		}
		close(release)
		synctest.Wait()
		if !bytes.Equal(first, []byte{7, 7, 7, 7}) || !bytes.Equal(second, []byte{7, 7}) {
			t.Fatalf("shared results: %v, %v", first, second)
		}
		first[0] = 99
		if second[0] != 7 {
			t.Fatal("shared outputs alias")
		}
		if stats := r.Stats(); stats.SourceReads != 1 || stats.SourceBytes != 12 {
			t.Fatalf("shared fetch: %+v", stats)
		}
		for _, flight := range r.flights[:cap(r.flights)] {
			if flight != nil {
				t.Fatal("completed flight retains response buffer")
			}
		}
	})
}

func TestRangeCacheSizeErrorsAreRetryable(t *testing.T) {
	failure := errors.New("size failure")
	source := &cacheTestSource{size: func(context.Context) (int64, error) { return 0, failure }}
	r := newTestRangeCache(t, source, DefaultRangeCacheOptions())
	if _, err := r.Size(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("size error: %v", err)
	}
	source.size = func(context.Context) (int64, error) { return -1, nil }
	if _, err := r.Size(t.Context()); err == nil {
		t.Fatal("accepted negative size")
	}
	source.size = nil
	for range 2 {
		if size, err := r.Size(t.Context()); err != nil || size != 64 {
			t.Fatalf("size: %d %v", size, err)
		}
	}
	if source.sizeCalls.Load() != 3 {
		t.Fatal("failed size was cached or successful size was not retained")
	}
}

func TestRangeCacheRetainsContainingIntervals(t *testing.T) {
	for _, test := range []struct {
		name  string
		order [2]int64
	}{
		{"smaller completes first", [2]int64{4, 0}},
		{"larger completes first", [2]int64{0, 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				releases := map[int64]chan struct{}{0: make(chan struct{}), 4: make(chan struct{})}
				source := &cacheTestSource{read: func(ctx context.Context, offset, length int64) ([]byte, error) {
					if release := releases[offset]; release != nil {
						select {
						case <-release:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return bytes.Repeat([]byte{7}, int(length)), nil
				}}
				options := DefaultRangeCacheOptions()
				options.CapacityBytes, options.MaxEntries = 12, 2
				r := newTestRangeCache(t, source, options)
				readCacheRange(ctx, t, r, 32, 4)
				// Start the subrange first so it cannot join the containing fetch.
				go readCacheRange(ctx, t, r, 4, 4)
				synctest.Wait()
				go readCacheRange(ctx, t, r, 0, 8)
				synctest.Wait()
				for _, offset := range test.order {
					close(releases[offset])
					synctest.Wait()
				}
				for _, request := range [][2]int64{{32, 4}, {0, 8}, {4, 4}} {
					data := readCacheRange(ctx, t, r, request[0], request[1])
					if !bytes.Equal(data, bytes.Repeat([]byte{7}, int(request[1]))) {
						t.Fatalf("range %v: %v", request, data)
					}
				}
				if stats := r.Stats(); stats.CachedEntries != 2 || stats.CachedBytes != 12 || stats.Evictions != 0 || stats.SourceReads != 3 || stats.CacheHits != 3 {
					t.Fatalf("redundant retention: %+v", stats)
				}
			})
		})
	}
}

func TestRangeCacheSizeDiscoveryCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		source := &cacheTestSource{}
		source.size = func(ctx context.Context) (int64, error) {
			if source.sizeCalls.Load() == 1 {
				<-ctx.Done()
				return 0, ctx.Err()
			}
			return 64, nil
		}
		r := newTestRangeCache(t, source, DefaultRangeCacheOptions())
		leaderCtx, cancelLeader := context.WithCancel(ctx)
		defer cancelLeader()
		var leaderErr error
		go func() { _, leaderErr = r.Size(leaderCtx) }()
		synctest.Wait()

		waiterCtx, cancelWaiter := context.WithCancel(ctx)
		defer cancelWaiter()
		var waiterErr error
		go func() { _, waiterErr = r.Size(waiterCtx) }()
		synctest.Wait()
		cancelWaiter()
		synctest.Wait()
		if !errors.Is(waiterErr, context.Canceled) {
			t.Fatalf("waiter cancellation: %v", waiterErr)
		}
		if source.sizeCalls.Load() != 1 {
			t.Fatal("waiter started a separate discovery")
		}

		var size int64
		var retryErr error
		go func() { size, retryErr = r.Size(ctx) }()
		synctest.Wait()
		cancelLeader()
		synctest.Wait()
		if !errors.Is(leaderErr, context.Canceled) {
			t.Fatalf("leader cancellation: %v", leaderErr)
		}
		if retryErr != nil || size != 64 {
			t.Fatalf("retry discovery: %d, %v", size, retryErr)
		}
		if source.sizeCalls.Load() != 2 {
			t.Fatal("waiter did not retry cancelled discovery")
		}
	})
}
