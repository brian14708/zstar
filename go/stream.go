package zstar

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"sort"
	"sync"
)

// Stream is a seekable member reader with one bounded cached decoded source.
// Operations serialize; closing releases the cache without closing the archive.
// Keep its Reader open. Cache hits still check cancellation and reader lifetime.
type Stream struct {
	mu       sync.Mutex
	file     *File
	position int64
}

// OpenStream opens the recommended sequential member reader.
// ctx applies only to opening; use ReadContext for per-read deadlines.
// Larger decoded sources are validated but not retained in the cache.
func (r *Reader) OpenStream(ctx context.Context, path string) (*Stream, error) {
	f, err := r.OpenFile(ctx, path)
	if err != nil {
		return nil, err
	}
	if f.size > math.MaxInt64 {
		return nil, rejected(CategoryResourceLimit, "stream size exceeds int64")
	}
	return &Stream{file: f}, nil
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
	chunk, err := f.cache.get(ctx, f, extent)
	if err != nil {
		return 0, err
	}
	selection, err := chunk.selection(extent)
	if err != nil {
		return 0, err
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
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			return err
		}
		s.file = nil
	}
	return nil
}
