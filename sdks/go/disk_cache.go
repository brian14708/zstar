package zstar

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	diskCacheMagic       = "ZSTARDC1"
	diskCacheHeaderBytes = len(diskCacheMagic) + 1 + 32 + 32 + 8 + 8 + 32 + 8 + 1
	diskCacheDigestBytes = 32
	diskCacheSuffix      = ".chunk"
)

type diskCacheEntry struct {
	id       [32]byte
	name     string
	size     uint64
	verified bool
}

// DiskDecodedCacheOptions controls a disk cache. Capacity includes the small
// per-entry header and digest and must be non-zero. The directory is created
// if needed and must be private to this cache instance, including across processes.
type DiskDecodedCacheOptions struct {
	Directory string
	Capacity  uint64
}

// DiskDecodedCache stores verified decoded bytes on disk. It implements
// DecodedContentCache and is safe for concurrent readers and writers. The
// directory is trusted local storage: the integrity digest detects accidental
// corruption, not malicious replacement. Each in-flight Put uses at most one
// temporary chunk beyond Capacity. It does not fsync the directory for
// power-loss durability. The internal lock guards only the index and LRU list;
// file reads, writes and fsyncs run outside it.
type DiskDecodedCache struct {
	mu       sync.Mutex
	dir      string
	capacity uint64
	used     uint64
	entries  map[[32]byte]*list.Element // value: *diskCacheEntry
	lru      *list.List                 // front is most recently used
	// pending holds ids whose file is being written or removed; Puts for them
	// are skipped so a slow removal cannot delete a fresh write.
	pending map[[32]byte]struct{}
}

// NewDiskDecodedCache opens or creates a disk cache and indexes entries left by
// an earlier process using only directory metadata (name, size, mtime). Entry
// contents are validated lazily by Get, which discards corrupt files.
// Interrupted temporary writes and unusable files are removed on a best-effort
// basis; files that cannot be removed are skipped rather than failing.
func NewDiskDecodedCache(options DiskDecodedCacheOptions) (*DiskDecodedCache, error) {
	if options.Directory == "" || options.Capacity == 0 {
		return nil, errors.New("zstar: disk cache requires directory and capacity")
	}
	if err := os.MkdirAll(options.Directory, 0o700); err != nil {
		return nil, fmt.Errorf("zstar: create disk cache: %w", err)
	}
	c := &DiskDecodedCache{
		dir:      options.Directory,
		capacity: options.Capacity,
		entries:  make(map[[32]byte]*list.Element),
		lru:      list.New(),
		pending:  make(map[[32]byte]struct{}),
	}
	if err := c.recover(); err != nil {
		return nil, err
	}
	return c, nil
}

func diskCacheKey(key DecodedCacheKey) [32]byte {
	var encoded [1 + 32 + 32 + 8 + 8 + 32]byte
	at := 0
	encoded[at] = key.Kind
	at++
	copy(encoded[at:], key.Identity[:])
	at += 32
	copy(encoded[at:], key.Metadata[:])
	at += 32
	binary.LittleEndian.PutUint64(encoded[at:], key.Offset)
	at += 8
	binary.LittleEndian.PutUint64(encoded[at:], key.EncodedSize)
	at += 8
	copy(encoded[at:], key.ObjectDigest[:])
	return sha256.Sum256(append([]byte("zstar/disk-cache/v1\x00"), encoded[:]...))
}

func (c *DiskDecodedCache) path(name string) string {
	return filepath.Join(c.dir, name+diskCacheSuffix)
}

func diskCacheName(id [32]byte) string {
	return hex.EncodeToString(id[:])
}

func encodeDiskChunk(value CachedChunk) []byte {
	raw := value.raw
	buf := make([]byte, diskCacheHeaderBytes+len(raw)+diskCacheDigestBytes)
	at := copy(buf, diskCacheMagic)
	buf[at] = value.key.Kind
	at++
	copy(buf[at:], value.key.Identity[:])
	at += 32
	copy(buf[at:], value.key.Metadata[:])
	at += 32
	binary.LittleEndian.PutUint64(buf[at:], value.key.Offset)
	at += 8
	binary.LittleEndian.PutUint64(buf[at:], value.key.EncodedSize)
	at += 8
	copy(buf[at:], value.key.ObjectDigest[:])
	at += 32
	binary.LittleEndian.PutUint64(buf[at:], uint64(len(raw)))
	at += 8
	if value.verified {
		buf[at] = 1
	}
	copy(buf[at+1:], raw)
	digest := sha256.Sum256(buf[:len(buf)-diskCacheDigestBytes])
	copy(buf[len(buf)-diskCacheDigestBytes:], digest[:])
	return buf
}

func decodeDiskChunk(data []byte) (CachedChunk, error) {
	if len(data) < diskCacheHeaderBytes+diskCacheDigestBytes || string(data[:len(diskCacheMagic)]) != diskCacheMagic {
		return CachedChunk{}, errors.New("invalid disk cache header")
	}
	digest := sha256.Sum256(data[:len(data)-diskCacheDigestBytes])
	if !bytes.Equal(digest[:], data[len(data)-diskCacheDigestBytes:]) {
		return CachedChunk{}, errors.New("invalid disk cache digest")
	}
	at := len(diskCacheMagic)
	value := CachedChunk{}
	value.key.Kind = data[at]
	at++
	if value.key.Kind > 1 {
		return CachedChunk{}, errors.New("invalid disk cache key kind")
	}
	copy(value.key.Identity[:], data[at:at+32])
	at += 32
	copy(value.key.Metadata[:], data[at:at+32])
	at += 32
	value.key.Offset = binary.LittleEndian.Uint64(data[at:])
	at += 8
	value.key.EncodedSize = binary.LittleEndian.Uint64(data[at:])
	at += 8
	copy(value.key.ObjectDigest[:], data[at:at+32])
	at += 32
	rawSize := binary.LittleEndian.Uint64(data[at:])
	at += 8
	payloadEnd := len(data) - diskCacheDigestBytes
	if data[at] > 1 || rawSize > uint64(payloadEnd-at-1) || int(rawSize) != payloadEnd-at-1 {
		return CachedChunk{}, errors.New("invalid disk cache payload")
	}
	value.verified = data[at] == 1
	at++
	value.raw = data[at:payloadEnd:payloadEnd]
	return value, nil
}

// Bound reads by the format's chunk limit as well as cache capacity. Stat is
// advisory; read exactly that bounded size and probe for growth afterward.
func readDiskChunk(path string, capacity uint64) ([]byte, error) {
	initial, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !initial.Mode().IsRegular() {
		return nil, errors.New("invalid disk cache file type")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // Read-only descriptor cleanup cannot change validated content.
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	limit := min(capacity, uint64(maxFrameSize+diskCacheHeaderBytes+diskCacheDigestBytes))
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) > limit {
		return nil, errors.New("invalid disk cache file size or type")
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		return nil, errors.New("disk cache file changed size")
	}
	return data, nil
}

func (c *DiskDecodedCache) recover() error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("zstar: scan disk cache: %w", err)
	}
	type found struct {
		diskCacheEntry
		mtime time.Time
	}
	var kept []found
	limit := min(c.capacity, uint64(maxFrameSize+diskCacheHeaderBytes+diskCacheDigestBytes))
	for _, item := range entries {
		name := item.Name()
		path := filepath.Join(c.dir, name)
		if strings.HasPrefix(name, ".zstar-cache-") {
			_ = os.Remove(path) //nolint:errcheck // Best-effort cleanup; an undeletable leftover must not block opening.
			continue
		}
		if !strings.HasSuffix(name, diskCacheSuffix) {
			continue
		}
		base := strings.TrimSuffix(name, diskCacheSuffix)
		id, nameErr := parseDiskCacheName(base)
		info, infoErr := item.Info()
		if nameErr != nil || infoErr != nil || !info.Mode().IsRegular() || uint64(info.Size()) < uint64(diskCacheHeaderBytes+diskCacheDigestBytes) || uint64(info.Size()) > limit {
			if infoErr == nil || errors.Is(infoErr, fs.ErrNotExist) {
				_ = os.Remove(path) //nolint:errcheck // Best-effort cleanup of an invalid entry.
			}
			continue
		}
		kept = append(kept, found{diskCacheEntry{id: id, name: base, size: uint64(info.Size())}, info.ModTime()})
	}
	sort.Slice(kept, func(i, j int) bool {
		if !kept[i].mtime.Equal(kept[j].mtime) {
			return kept[i].mtime.Before(kept[j].mtime)
		}
		return kept[i].name < kept[j].name
	})
	for _, f := range kept {
		entry := f.diskCacheEntry
		c.entries[entry.id] = c.lru.PushFront(&entry)
		c.used += entry.size
	}
	for _, v := range c.evictLocked(0) {
		_ = os.Remove(c.path(v.name)) //nolint:errcheck // Best-effort; Get treats a surviving file as an unindexed leftover.
		delete(c.pending, v.id)
	}
	return nil
}

func parseDiskCacheName(name string) ([32]byte, error) {
	var id [32]byte
	if len(name) != 64 {
		return id, errors.New("invalid disk cache name")
	}
	for i := range id {
		value := func(b byte) (byte, bool) {
			switch {
			case b >= '0' && b <= '9':
				return b - '0', true
			case b >= 'a' && b <= 'f':
				return b - 'a' + 10, true
			default:
				return 0, false
			}
		}
		hi, ok := value(name[i*2])
		if !ok {
			return id, errors.New("invalid disk cache name")
		}
		lo, ok := value(name[i*2+1])
		if !ok {
			return id, errors.New("invalid disk cache name")
		}
		id[i] = hi<<4 | lo
	}
	return id, nil
}

// evictLocked drops least-recently-used entries from the index until required
// more bytes fit and returns them; the caller removes their files outside the
// lock and then clears their pending marks. Eviction is O(1) per entry.
func (c *DiskDecodedCache) evictLocked(required uint64) []diskCacheEntry {
	var victims []diskCacheEntry
	for (c.used > c.capacity || required > c.capacity-c.used) && c.lru.Len() != 0 {
		back := c.lru.Back()
		entry := back.Value.(*diskCacheEntry) //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
		c.lru.Remove(back)
		delete(c.entries, entry.id)
		c.used -= entry.size
		c.pending[entry.id] = struct{}{}
		victims = append(victims, *entry)
	}
	return victims
}

func (c *DiskDecodedCache) removeVictims(victims []diskCacheEntry) error {
	var result error
	for _, v := range victims {
		if err := os.Remove(c.path(v.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("zstar: evict disk cache entry: %w", err))
		}
	}
	c.mu.Lock()
	for _, v := range victims {
		delete(c.pending, v.id)
	}
	c.mu.Unlock()
	return result
}

// Get returns a validated token when the key exists, or a cache miss.
// It checks ctx before disk I/O; corrupt entries are discarded. The cache lock
// is not held while reading the file.
func (c *DiskDecodedCache) Get(ctx context.Context, key DecodedCacheKey) (CachedChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return CachedChunk{}, false, err
	}
	id := diskCacheKey(key)
	c.mu.Lock()
	element, ok := c.entries[id]
	if !ok {
		c.mu.Unlock()
		return CachedChunk{}, false, nil
	}
	entry := *element.Value.(*diskCacheEntry) //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
	c.lru.MoveToFront(element)
	c.mu.Unlock()
	data, readErr := readDiskChunk(c.path(entry.name), entry.size)
	value, err := decodeDiskChunk(data)
	if readErr != nil || err != nil || diskCacheKey(value.key) != id || value.key != key {
		// Drop the entry only if it is still the one we read; it may have been
		// evicted and rewritten meanwhile.
		c.mu.Lock()
		if current, ok := c.entries[id]; ok && current == element {
			c.lru.Remove(element)
			delete(c.entries, id)
			c.used -= entry.size
			c.pending[id] = struct{}{}
			c.mu.Unlock()
			removeErr := os.Remove(c.path(entry.name))
			c.mu.Lock()
			delete(c.pending, id)
			c.mu.Unlock()
			if removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
				return CachedChunk{}, false, removeErr
			}
			return CachedChunk{}, false, nil
		}
		c.mu.Unlock()
		return CachedChunk{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return CachedChunk{}, false, err
	}
	c.mu.Lock()
	if current, ok := c.entries[id]; ok && current == element {
		element.Value.(*diskCacheEntry).verified = value.verified //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
	}
	c.mu.Unlock()
	now := time.Now()
	_ = os.Chtimes(c.path(entry.name), now, now) //nolint:errcheck // Persisting LRU timestamps is advisory; the in-memory LRU was updated.
	return value, true, nil
}

// Put stores a reader-created token within the configured disk budget.
// Writes are atomic on filesystems supporting replacement rename. A key that
// is already present is not rewritten, except to upgrade an unverified token
// to a verified one.
func (c *DiskDecodedCache) Put(ctx context.Context, value CachedChunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	size := uint64(len(value.raw)) + uint64(diskCacheHeaderBytes+diskCacheDigestBytes)
	if size > c.capacity || len(value.raw) > maxFrameSize {
		return nil
	}
	id := diskCacheKey(value.key)
	name := diskCacheName(id)
	c.mu.Lock()
	if element, ok := c.entries[id]; ok {
		entry := element.Value.(*diskCacheEntry) //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
		if entry.verified || !value.verified {
			c.lru.MoveToFront(element)
			c.mu.Unlock()
			return nil
		}
	}
	if _, busy := c.pending[id]; busy {
		c.mu.Unlock()
		return nil
	}
	c.pending[id] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	data := encodeDiskChunk(value)
	temp, err := os.CreateTemp(c.dir, ".zstar-cache-")
	if err != nil {
		return fmt.Errorf("zstar: create disk cache entry: %w", err)
	}
	tempName := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempName) //nolint:errcheck // Failed temporary-file cleanup does not replace the write error; recovery removes leftovers.
		}
	}()
	if err = temp.Chmod(0o600); err == nil {
		_, err = temp.Write(data)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("zstar: write disk cache entry: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Reserve space (dropping any entry being replaced) under the lock, then
	// do file work outside it.
	c.mu.Lock()
	var old *list.Element
	var oldEntry diskCacheEntry
	if element, ok := c.entries[id]; ok {
		old, oldEntry = element, *element.Value.(*diskCacheEntry) //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
		c.lru.Remove(element)
		delete(c.entries, id)
		c.used -= oldEntry.size
	}
	victims := c.evictLocked(size)
	c.used += size
	c.mu.Unlock()
	evictErr := c.removeVictims(victims)

	renameErr := os.Rename(tempName, c.path(name))
	c.mu.Lock()
	if renameErr != nil {
		c.used -= size
		if old != nil {
			c.entries[id] = c.lru.PushBack(&oldEntry)
			c.used += oldEntry.size
		}
		c.mu.Unlock()
		return errors.Join(evictErr, fmt.Errorf("zstar: commit disk cache entry: %w", renameErr))
	}
	removeTemp = false
	c.entries[id] = c.lru.PushFront(&diskCacheEntry{id: id, name: name, size: size, verified: value.verified})
	c.mu.Unlock()
	return evictErr
}

// UsedBytes returns retained on-disk bytes, including headers and digests.
func (c *DiskDecodedCache) UsedBytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

// Clear removes all indexed entries. Removal failures are returned so
// callers can distinguish an unavailable cache from an empty one.
func (c *DiskDecodedCache) Clear() error {
	c.mu.Lock()
	type item struct {
		element *list.Element
		entry   diskCacheEntry
	}
	snapshot := make([]item, 0, len(c.entries))
	for _, element := range c.entries {
		snapshot = append(snapshot, item{element, *element.Value.(*diskCacheEntry)}) //nolint:errcheck,forcetypeassert // The list only stores *diskCacheEntry.
	}
	c.mu.Unlock()
	var result error
	for _, it := range snapshot {
		element, entry := it.element, it.entry
		if err := os.Remove(c.path(entry.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			result = errors.Join(result, err)
			continue
		}
		c.mu.Lock()
		if current, ok := c.entries[entry.id]; ok && current == element {
			c.lru.Remove(element)
			delete(c.entries, entry.id)
			c.used -= entry.size
		}
		c.mu.Unlock()
	}
	return result
}
