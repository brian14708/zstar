package zstar

import "context"

// TieredDecodedCache keeps hot immutable tokens in memory and a larger working
// set on disk. Disk hits are promoted to memory. Capacity and trust rules are
// those of MemoryDecodedCache and DiskDecodedCache; source loading and miss
// coalescing remain the reader's responsibility. Methods are concurrency-safe.
type TieredDecodedCache struct {
	memory *MemoryDecodedCache
	disk   *DiskDecodedCache
}

// NewTieredDecodedCache recovers the disk tier and creates an empty memory tier.
// A zero memoryBytes budget disables memory retention.
func NewTieredDecodedCache(memoryBytes uint64, disk DiskDecodedCacheOptions) (*TieredDecodedCache, error) {
	storage, err := NewDiskDecodedCache(disk)
	if err != nil {
		return nil, err
	}
	return &TieredDecodedCache{memory: NewMemoryDecodedCache(memoryBytes), disk: storage}, nil
}

// Get retrieves a validated token, promoting disk hits into memory.
func (c *TieredDecodedCache) Get(ctx context.Context, key DecodedCacheKey) (CachedChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return CachedChunk{}, false, err
	}
	if value, ok, err := c.memory.Get(ctx, key); err != nil || ok {
		return value, ok, err
	}
	value, ok, err := c.disk.Get(ctx, key)
	if err == nil && ok {
		c.memory.put(value)
	}
	return value, ok, err
}

// Put retains a token in the bounded disk and memory tiers.
func (c *TieredDecodedCache) Put(ctx context.Context, value CachedChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep the memory hit even when disk storage is unavailable. Readers treat
	// backend errors as advisory and can always reload from the source.
	c.memory.put(value)
	return c.disk.Put(ctx, value)
}

// MemoryUsedBytes reports the retained memory-tier charge.
func (c *TieredDecodedCache) MemoryUsedBytes() uint64 { return c.memory.UsedBytes() }

// DiskUsedBytes reports the retained disk-tier charge.
func (c *TieredDecodedCache) DiskUsedBytes() uint64 { return c.disk.UsedBytes() }

// Clear drops both tiers. A disk removal failure is returned after the memory
// tier has already been cleared.
func (c *TieredDecodedCache) Clear() error {
	c.memory.Clear()
	return c.disk.Clear()
}
