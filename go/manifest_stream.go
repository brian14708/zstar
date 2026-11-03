package zstar

import (
	"context"
	"io/fs"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync"
)

// A sequential bounded range buffer amortizes remote metadata requests. take
// returns owned bytes because subsequent reads can replace the read-ahead.
type manifestRangeStream struct {
	category  string
	ctx       context.Context
	source    RangeReader
	at, end   uint64
	buffer    []byte
	readAhead uint64
}

func (s *manifestRangeStream) shortRange() error {
	category := s.category
	if category == "" {
		category = CategoryMalformedTOC
	}
	return rejected(category, "short sequential range")
}

func (s *manifestRangeStream) take(size uint64) ([]byte, error) {
	if size > s.end-s.at {
		return nil, s.shortRange()
	}
	return s.appendTo(make([]byte, 0, int(size)), size)
}

// Append directly from read-ahead to a frame buffer, avoiding a temporary
// payload allocation and copy for every Zstandard block.
func (s *manifestRangeStream) appendTo(out []byte, size uint64) ([]byte, error) {
	if size > s.end-s.at {
		return nil, s.shortRange()
	}
	for size > 0 {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
		if len(s.buffer) == 0 {
			target := s.readAhead
			if target == 0 {
				target = 1 << 20
			}
			n := min(target, s.end-s.at)
			b, err := s.source.ReadRange(s.ctx, int64(s.at), int64(n))
			if err != nil {
				return nil, err
			}
			if uint64(len(b)) != n {
				return nil, s.shortRange()
			}
			s.buffer = b
		}
		n := min(size, uint64(len(s.buffer)))
		out = append(out, s.buffer[:n]...)
		s.buffer = s.buffer[n:]
		s.at += n
		size -= n
	}
	return out, nil
}

// ManifestCursor returns owned entries from the validated document.
type ManifestCursor struct {
	mu       sync.Mutex
	manifest *Manifest
	entry    int
	end      int
	prefix   string
	started  bool
	err      error
}

// Cursor creates an independent bounded cursor over sorted owned entries.
func (m *Manifest) Cursor() *ManifestCursor { return &ManifestCursor{manifest: m} }

// CursorPrefix visits paths beginning with the literal UTF-8 byte prefix.
// Use "dir/" for descendants of a directory, or "" for all entries.
// It seeks by binary search and does not scan unrelated entries.
func (m *Manifest) CursorPrefix(prefix string) *ManifestCursor {
	return &ManifestCursor{manifest: m, prefix: prefix}
}

// Entries yields owned metadata in path order, filtered by a literal prefix.
// Empty prefix selects all entries. Iteration holds no lock across yield,
// retains at most one metadata batch, and stops after the first error.
func (m *Manifest) Entries(prefix string) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		cursor := m.CursorPrefix(prefix)
		defer cursor.Close()
		for {
			entries, err := cursor.Next(128)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			if len(entries) == 0 {
				return
			}
			for _, entry := range entries {
				if !yield(entry, nil) {
					return
				}
			}
		}
	}
}

// Next returns up to limit owned entries; limit must be in [1, 4096].
// An empty result with no error signals exhaustion. Calls are serialized.
func (c *ManifestCursor) Next(limit int) ([]Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if limit < 1 || limit > 4096 {
		return nil, rejected(CategoryResourceLimit, "cursor batch must be in [1, 4096]")
	}
	m := c.manifest
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, categorized(CategoryUnsupportedOperation, fs.ErrClosed)
	}
	if c.err != nil {
		return nil, c.err
	}
	if !c.started {
		c.entry, _ = slices.BinarySearchFunc(m.entries, c.prefix, compareEntryPath)
		c.end = c.entry + sort.Search(len(m.entries)-c.entry, func(i int) bool {
			return !strings.HasPrefix(m.entries[c.entry+i].Path, c.prefix)
		})
		c.started = true
	}
	end := min(c.entry+limit, c.end)
	out := make([]Entry, 0, end-c.entry)
	for _, e := range m.entries[c.entry:end] {
		out = append(out, cloneEntry(e))
	}
	c.entry = end
	return out, nil
}

// Close invalidates this cursor without closing the manifest.
func (c *ManifestCursor) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = rejected(CategoryUnsupportedOperation, "closed manifest cursor")
}
