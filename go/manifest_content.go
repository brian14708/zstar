package zstar

import (
	"context"
	"io/fs"
	"reflect"
	"sync"
)

type manifestChunkKey struct {
	source      ExtentSource
	encodedSize uint64
	// Pointer resolvers provide stable, comparable identities for active loads.
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
type manifestContentState struct {
	decoders frameDecoderPool
	mu       sync.Mutex
	source   RangeReader
	flights  map[manifestChunkKey]*manifestContentFlight
	slots    chan struct{}
	closed   chan struct{}
}

func newManifestContentState(options manifestOptions, source RangeReader) *manifestContentState {
	return &manifestContentState{
		source:  source,
		flights: make(map[manifestChunkKey]*manifestContentFlight),
		slots:   make(chan struct{}, options.ConcurrentChunks), closed: make(chan struct{}),
	}
}

// Active loads share immutable decoded bytes. External keys exclude
// recompression-dependent hints. Explicit verification hashes complete objects.
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
		s.mu.Lock()
		select {
		case <-s.closed:
			s.mu.Unlock()
			return nil, categorized("unsupported-operation", fs.ErrClosed)
		default:
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
	}
	flight.chunk, flight.err = result, err
	close(flight.done)
}

func (s *manifestContentState) load(ctx context.Context, candidate *manifestContentChunk, extent Extent, resolver EncodedContentResolver, verify bool) (*manifestContentChunk, error) {
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
