package zstar

import (
	"context"
	"io/fs"
	"strings"

	"golang.org/x/exp/collection/set"
)

// checkOpen checks operation cancellation and manifest lifetime.
// Metadata authentication and validation complete during openManifest.
func (m *manifest) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return categorized(CategoryUnsupportedOperation, fs.ErrClosed)
	}
	return nil
}

func (m *manifest) validateEntries(ctx context.Context) error {
	directories := set.Of[string]()
	external := false
	for _, entry := range m.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		for at := strings.IndexByte(entry.Path, '/'); at >= 0; {
			if !directories.Contains(entry.Path[:at]) {
				return rejected(CategoryMalformedTOC, "missing ancestor directory")
			}
			next := strings.IndexByte(entry.Path[at+1:], '/')
			if next < 0 {
				break
			}
			at += next + 1
		}
		if entry.Type == TypeDirectory {
			directories.Insert(entry.Path)
		}
		external = external || entry.External()
	}
	if external != (len(m.requires) != 0) {
		return rejected(CategoryMalformedTOC, "manifest required feature")
	}
	return nil
}

// EntryCount returns the authenticated number of members, excluding the root.
func (m *manifest) EntryCount() uint64 { return m.count }
