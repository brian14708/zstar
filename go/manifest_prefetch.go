package zstar

import (
	"bytes"
	"context"
	"reflect"
	"sort"
)

type (
	encodedPrefetchKey struct{}
	encodedPrefetch    map[manifestChunkKey][]byte
)

// Admission uses the same global work slots as ordinary content I/O. Shared
// advisory caches must be consulted by the loader before network work.
func (m *Manifest) prefetch(ctx context.Context, jobs []manifestReadJob, resolver EncodedContentResolver) (context.Context, error) {
	m.content.mu.Lock()
	shared := m.content.shared != nil
	m.content.mu.Unlock()
	if shared || len(jobs) < 2 {
		return ctx, nil
	}
	frames := make([]Extent, 0)
	objects := make([]ObjectRequest, 0)
	var total uint64
	batch, canBatch := resolver.(BatchEncodedContentResolver)
	for _, job := range jobs {
		e := job.extent
		key := chunkKey(e)
		stable := true
		if _, object := e.Source.ContentAddress(); object {
			value := reflect.ValueOf(resolver)
			stable = value.IsValid() && value.Kind() == reflect.Pointer && !value.IsNil()
			if stable {
				key.resolver = resolver
			}
		}
		if stable {
			m.content.mu.Lock()
			cached := m.content.cache[key] != nil
			active := m.content.flights[key] != nil
			m.content.mu.Unlock()
			if cached && m.content.cacheAllowed(e, resolver) || active {
				continue
			}
		}
		if _, object := e.Source.ContentAddress(); object {
			if !canBatch {
				continue
			}
			digest, err := ParseContentAddress(e.Source.address)
			if err != nil {
				return nil, err
			}
			length, ok := batch.EncodedLength(digest)
			if !ok {
				length = 17 << 20
			}
			if length > 64<<20-total {
				return ctx, nil
			}
			total += length
			objects = append(objects, ObjectRequest{digest, e.EncodedSize, 17 << 20})
		} else {
			if e.EncodedSize > 64<<20-total {
				return ctx, nil
			}
			total += e.EncodedSize
			frames = append(frames, e)
		}
	}
	data := make(encodedPrefetch)
	if len(objects) > 1 {
		select {
		case m.content.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		out, err := batch.GetEncodedMany(ctx, objects)
		<-m.content.slots
		if err != nil {
			return nil, err
		}
		if len(out) != len(objects) {
			return nil, rejected("malformed-external-content", "batch count")
		}
		for i, q := range objects {
			if uint64(len(out[i])) > q.MaxBytes {
				return nil, rejected("resource-limit", "encoded batch object")
			}
			data[chunkKey(Extent{Source: ObjectSource(q.Digest)})] = out[i]
		}
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].Source.offset < frames[j].Source.offset })
	for i := 0; i < len(frames); {
		first := i
		start := frames[i].Source.offset
		end := start + frames[i].EncodedSize
		i++
		for i < len(frames) && frames[i].Source.offset == end && end+frames[i].EncodedSize-start <= 8<<20 {
			end += frames[i].EncodedSize
			i++
		}
		if i-first < 2 {
			continue
		}
		select {
		case m.content.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		raw, err := m.content.source.ReadRange(ctx, int64(start), int64(end-start))
		<-m.content.slots
		if err != nil {
			return nil, err
		}
		if uint64(len(raw)) != end-start {
			return nil, rejected("malformed-frame", "short coalesced range")
		}
		for _, e := range frames[first:i] {
			offset := e.Source.offset - start
			data[chunkKey(e)] = bytes.Clone(raw[offset : offset+e.EncodedSize])
		}
	}
	return context.WithValue(ctx, encodedPrefetchKey{}, data), nil
}
