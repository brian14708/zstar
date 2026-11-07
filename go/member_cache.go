package zstar

import (
	"context"
	"io/fs"
	"sync"
	"unsafe"
)

// Private per-handle decoded-source retention, including fixed bookkeeping.
const decodedSourceCacheBytes = 2 << 20

// memberCache retains one immutable, validated decoded source. Its owning File
// pins the archive, resolver, and verification policy, so these need not be part
// of the key. Active loads remain shared by manifestContentState.
type memberCache struct {
	mu     sync.Mutex
	key    manifestChunkKey
	chunk  *manifestContentChunk
	closed bool
}

func (c *memberCache) checkOpen(ctx context.Context, content *manifestContentState) error {
	if err := content.checkOpen(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fs.ErrClosed
	}
	return nil
}

func (c *memberCache) get(ctx context.Context, f *File, extent Extent) (*manifestContentChunk, error) {
	if err := c.checkOpen(ctx, f.manifest.content); err != nil {
		return nil, err
	}
	key := chunkKey(extent)
	c.mu.Lock()
	chunk := c.chunk
	if chunk == nil || c.key != key {
		// Drop the previous retained source before loading a replacement.
		c.chunk = nil
		c.key = manifestChunkKey{}
		chunk = nil
	}
	c.mu.Unlock()
	if chunk == nil {
		var err error
		chunk, err = f.manifest.content.get(ctx, extent, f.resolver, f.verifyExternal)
		if err != nil {
			return nil, err
		}
	}
	// Always validate the complete declared selection, including on cache hits.
	if _, err := chunk.selection(extent); err != nil {
		return nil, err
	}
	if err := f.manifest.content.checkOpen(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fs.ErrClosed
	}
	cost := uint64(cap(chunk.raw)) + uint64(unsafe.Sizeof(memberCache{})) + uint64(unsafe.Sizeof(manifestContentChunk{}))
	if cost <= decodedSourceCacheBytes {
		c.key, c.chunk = key, chunk
	}
	return chunk, nil
}

func (c *memberCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.key, c.chunk = manifestChunkKey{}, nil
}
