package zstar

import (
	"context"
	"errors"
	"io"
	"math"
	"sort"
)

// File is a logical member view. It retains bounded entry metadata and
// borrows its manifest, which must remain open. ReadAt supports concurrent calls;
// WriteTo streams complete selected extents without a whole-file allocation.
// One decoded source is retained within a private 2 MiB budget. Close releases
// the cache without closing the archive or source; dropping the handle also
// allows its storage to be reclaimed.
type File struct {
	manifest       *manifest
	entry          *Entry
	resolver       EncodedContentResolver
	verifyExternal bool
	size           uint64
	starts         []uint64
	cache          memberCache
}

// OpenFile returns a concurrent random-access view of a regular member.
// The manifest must remain open. ctx applies only to opening; use
// ReadAtContext and WriteToContext to bound later I/O.
func (m *manifest) OpenFile(ctx context.Context, path string, resolver EncodedContentResolver, verifyExternal bool) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entryValue, err := m.entrySnapshotContext(ctx, path)
	entry := &entryValue
	if err != nil {
		return nil, pathError("open", path, err)
	}
	if entry == nil || entry.Type != TypeFile {
		return nil, rejected(CategoryUnsupportedOperation, "entry is not a file")
	}
	return m.openEntryFile(entry, resolver, verifyExternal)
}

// entry is already an owned regular-file snapshot; keep it private to the view.
func (m *manifest) openEntryFile(entry *Entry, resolver EncodedContentResolver, verifyExternal bool) (*File, error) {
	size, err := entry.Size()
	if err != nil {
		return nil, err
	}
	starts := make([]uint64, len(entry.Extents)+1)
	for i, extent := range entry.Extents {
		starts[i+1] = starts[i] + extent.Size
	}
	entry.extentEnds = starts[1:]
	return &File{manifest: m, entry: entry, resolver: resolver, verifyExternal: verifyExternal, size: size, starts: starts}, nil
}

// Close releases the decoded cache and prevents new reads. It is idempotent
// and does not close the archive or source. Concurrent reads may already hold
// decoded buffers until they finish; Close does not wait for them.
func (f *File) Close() error {
	f.cache.close()
	return nil
}

// Size returns the logical member length in bytes.
func (f *File) Size() uint64 { return f.size }

// ReadAt implements io.ReaderAt using a background context. Calls may run
// concurrently. Use ReadAtContext for remote reads with deadlines.
func (f *File) ReadAt(output []byte, offset int64) (int, error) {
	return f.ReadAtContext(context.Background(), output, offset)
}

// ReadAtContext reads a member range using ctx for this operation only.
// A short read at the end of the member returns io.EOF.
func (f *File) ReadAtContext(ctx context.Context, output []byte, offset int64) (n int, err error) {
	defer func() {
		if err != nil && !errors.Is(err, io.EOF) {
			err = pathError("read", f.entry.Path, err)
		}
	}()
	if err := f.cache.checkOpen(ctx, f.manifest.content); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, rejected(CategoryInvalidRange, "negative file offset")
	}
	if len(output) == 0 {
		return 0, nil
	}
	if uint64(offset) >= f.size {
		return 0, io.EOF
	}
	size := min(uint64(len(output)), f.size-uint64(offset))
	first := sort.Search(len(f.entry.Extents), func(i int) bool { return f.starts[i+1] > uint64(offset) })
	// A single extent needs no batch plan or temporary result buffer. Copy
	// only after validating the complete source and declared selection.
	if size <= f.starts[first+1]-uint64(offset) || size > f.manifest.maxRead {
		return f.readAtSerial(ctx, output, first, uint64(offset), size)
	}
	_, _, err = f.manifest.readRanges(ctx,
		[]readSelection{{ReadRequest: ReadRequest{f.entry.Path, uint64(offset), size}, entry: f.entry, file: f, output: output[:int(size)]}}, f.resolver, f.verifyExternal, false)
	if err != nil {
		return 0, err
	}
	n = int(size)
	if n < len(output) {
		return n, io.EOF
	}
	return n, nil
}

// WriteTo implements io.WriterTo and streams the complete member using a
// background context. Use WriteToContext for operation deadlines.
func (f *File) WriteTo(output io.Writer) (int64, error) {
	return f.WriteToContext(context.Background(), output)
}

// WriteToContext streams the complete member using ctx for this operation only.
// The destination remains caller-owned; failures return the bytes written.
func (f *File) WriteToContext(ctx context.Context, output io.Writer) (written int64, err error) {
	defer func() {
		if err != nil {
			err = pathError("read", f.entry.Path, err)
		}
	}()
	if err := f.cache.checkOpen(ctx, f.manifest.content); err != nil {
		return 0, err
	}
	if f.size > math.MaxInt64 {
		return 0, rejected(CategoryResourceLimit, "stream count exceeds int64")
	}
	for _, e := range f.entry.Extents {
		if e.Size > f.manifest.maxRead {
			return f.writeToSerial(ctx, output)
		}
	}
	for i := 0; i < len(f.entry.Extents); {
		start := f.starts[i]
		end := f.starts[i+1]
		i++
		for i < len(f.entry.Extents) && f.starts[i+1]-start <= min(uint64(8<<20), f.manifest.maxRead) {
			end = f.starts[i+1]
			i++
		}
		results, _, err := f.manifest.readRanges(ctx,
			[]readSelection{{ReadRequest: ReadRequest{f.entry.Path, start, end - start}, entry: f.entry, file: f}},
			f.resolver, f.verifyExternal, false)
		if err != nil {
			return written, err
		}
		data := results[0]
		n, err := writeBuffer(output, data)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (f *File) readAtSerial(ctx context.Context, output []byte, first int, offset, size uint64) (int, error) {
	var n int
	var previous manifestChunkKey
	var chunk *manifestContentChunk
	for i := first; uint64(n) < size; i++ {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		extent := f.entry.Extents[i]
		key := chunkKey(extent)
		if chunk == nil || key != previous {
			var err error
			chunk, err = f.cache.get(ctx, f, extent)
			if err != nil {
				return n, err
			}
			previous = key
		}
		selection, err := chunk.selection(extent)
		if err != nil {
			return n, err
		}
		within := offset + uint64(n) - f.starts[i]
		length := min(extent.Size-within, size-uint64(n))
		n += copy(output[n:], selection[within:within+length])
	}
	if n < len(output) {
		return n, io.EOF
	}
	return n, nil
}

func (f *File) writeToSerial(ctx context.Context, output io.Writer) (int64, error) {
	var written int64
	var previous manifestChunkKey
	var chunk *manifestContentChunk
	for _, extent := range f.entry.Extents {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		key := chunkKey(extent)
		if chunk == nil || key != previous {
			var err error
			chunk, err = f.cache.get(ctx, f, extent)
			if err != nil {
				return written, err
			}
			previous = key
		}
		selection, err := chunk.selection(extent)
		if err != nil {
			return written, err
		}
		n, err := writeBuffer(output, selection)
		written += int64(n)
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
