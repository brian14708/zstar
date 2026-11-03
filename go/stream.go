package zstar

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"sort"
	"sync"
	"unsafe"
)

// Stream is a seekable member reader with one bounded cached decoded source.
// Operations serialize; closing releases the cache without closing the archive.
// Keep its Reader open. Cache hits still check cancellation and reader lifetime.
type Stream struct {
	mu         sync.Mutex
	file       *File
	position   int64
	cacheBytes uint64
	key        manifestChunkKey
	chunk      *manifestContentChunk
}

// OpenStream opens a regular member with a private decoded-source cache.
// Zero cacheBytes selects 2 MiB, including fixed stream/cache bookkeeping.
// Larger sources are decoded under the archive's frame budgets but not retained. ctx applies only to opening.
func (r *Reader) OpenStream(ctx context.Context, path string, cacheBytes uint64) (*Stream, error) {
	f, err := r.OpenFile(ctx, path)
	if err != nil {
		return nil, err
	}
	if f.size > math.MaxInt64 {
		return nil, rejected(CategoryResourceLimit, "stream size exceeds int64")
	}
	if cacheBytes == 0 {
		cacheBytes = 2 << 20
	}
	return &Stream{file: f, cacheBytes: cacheBytes}, nil
}

// Read implements io.Reader using a background context.
func (s *Stream) Read(output []byte) (int, error) {
	return s.ReadContext(context.Background(), output)
}

// ReadContext reads from the cursor. A read returns at most one extent's bytes.
// Complete source and selection validation precede copying to output.
func (s *Stream) ReadContext(ctx context.Context, output []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return 0, fs.ErrClosed
	}
	f := s.file
	defer func() {
		if err != nil && !errors.Is(err, io.EOF) {
			err = pathError("read", f.entry.Path, err)
		}
	}()
	if err := f.manifest.content.checkOpen(ctx); err != nil {
		return 0, err
	}
	if len(output) == 0 {
		return 0, nil
	}
	if uint64(s.position) >= f.size {
		return 0, io.EOF
	}
	i := sort.Search(len(f.entry.Extents), func(i int) bool { return f.starts[i+1] > uint64(s.position) })
	extent := f.entry.Extents[i]
	key := chunkKey(extent)
	chunk := s.chunk
	if chunk == nil || key != s.key {
		// Release the previous cache before admitting another source load.
		s.chunk = nil
		var err error
		chunk, err = f.manifest.content.get(ctx, extent, f.resolver, f.verifyExternal)
		if err != nil {
			return 0, err
		}
	}
	selection, err := chunk.selection(extent)
	if err != nil {
		return 0, err
	}
	cacheCost := uint64(cap(chunk.raw)) + uint64(unsafe.Sizeof(Stream{})) + uint64(unsafe.Sizeof(manifestContentChunk{}))
	if cacheCost <= s.cacheBytes {
		s.key, s.chunk = key, chunk
	}
	within := uint64(s.position) - f.starts[i]
	count := min(uint64(len(output)), extent.Size-within)
	n = copy(output, selection[within:within+count])
	s.position += int64(n)
	return n, nil
}

// Seek implements io.Seeker. Seeking beyond EOF is allowed.
func (s *Stream) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return 0, fs.ErrClosed
	}
	if err := s.file.manifest.content.checkOpen(context.Background()); err != nil {
		return 0, err
	}
	position, err := seekPosition(s.position, int64(s.file.size), offset, whence)
	if err != nil {
		return 0, pathError("seek", s.file.entry.Path, err)
	}
	s.position = position
	return s.position, nil
}

// Close releases retained metadata and decoded bytes. It is idempotent.
func (s *Stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.file, s.chunk = nil, nil
	s.key = manifestChunkKey{}
	return nil
}
