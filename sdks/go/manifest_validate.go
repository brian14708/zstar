package zstar

import (
	"context"
	"io/fs"
	"strings"
)

// ValidateMetadata confirms the authenticated manifest remains open
// and checks cancellation. It performs no content I/O.
func (m *Manifest) ValidateMetadata(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return categorized("unsupported-operation", fs.ErrClosed)
	}
	return nil
}

func (m *Manifest) validateEntries(ctx context.Context) error {
	directories := make(map[string]bool)
	external := false
	for _, entry := range m.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		for at := strings.IndexByte(entry.Path, '/'); at >= 0; {
			if !directories[entry.Path[:at]] {
				return rejected("malformed-toc", "missing ancestor directory")
			}
			next := strings.IndexByte(entry.Path[at+1:], '/')
			if next < 0 {
				break
			}
			at += next + 1
		}
		if entry.Type == TypeDirectory {
			directories[entry.Path] = true
		}
		external = external || entry.External()
	}
	if external != (len(m.requiredFeatures) != 0) {
		return rejected("malformed-toc", "manifest required feature")
	}
	return nil
}

// EntryCount returns the authenticated number of members, excluding the root.
func (m *Manifest) EntryCount() uint64 { return m.count }
