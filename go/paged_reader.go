package zstar

import (
	"context"
	"encoding/binary"
	"io/fs"
	"math"
	"slices"
	"strings"
	"sync"
	"unsafe"
)

// pagedState validates the directory on open and caches complete entry pages on demand.
// Calls validate proper ancestors before exposing selected entries or bytes.
// Use ValidateMetadata or Verify for checks of every page and the complete namespace.
type pagedState struct {
	owner             *manifest
	full              bool
	source            RangeReader
	options           ReaderOptions
	config            manifestOptions
	start             uint64
	root              map[any]any
	pages             []pageReference
	mu                sync.Mutex
	cached            map[int][]Entry
	retained, count   uint64
	directoryRetained uint64
	content           *manifestContentState
	metadataCtx       context.Context
	cancelMetadata    context.CancelFunc
	closed            bool
}

// openPaged opens the footer and directory without fetching entry or extension pages.
func openPaged(ctx context.Context, source RangeReader, options ReaderOptions) (*pagedState, error) {
	if source == nil {
		return nil, rejected(CategoryMalformedTOC, "nil source")
	}
	config := options.Manifest.defaults()
	if config.ConcurrentChunks < 1 || config.ConcurrentChunks > 256 {
		return nil, rejected(CategoryResourceLimit, "invalid concurrency")
	}
	size, err := source.Size(ctx)
	if err != nil {
		return nil, err
	}
	if size < FooterSize {
		return nil, rejected(CategoryNotZstar, "source size")
	}
	raw, err := source.ReadRange(ctx, size-FooterSize, FooterSize)
	if err != nil {
		return nil, err
	}
	if len(raw) != FooterSize {
		return nil, rejected(CategoryMalformedTOC, "short footer")
	}
	footer, err := ParseFooter(raw)
	if err != nil {
		return nil, err
	}
	start, _, err := footer.ManifestRange(uint64(size))
	if err != nil {
		return nil, err
	}
	if footer.ManifestEncodedSize > config.MaxEncodedBytes || footer.ManifestDecodedSize > config.MaxDecodedBytes || footer.DirectoryDecodedSize > manifestHeaderMax {
		return nil, rejected(CategoryResourceLimit, "metadata size")
	}
	raw, err = readMetadataPage(ctx, source, uint64(size-FooterSize)-footer.DirectoryEncodedSize, footer.DirectoryEncodedSize, footer.DirectoryDecodedSize, config)
	if err != nil {
		return nil, err
	}
	value, err := decodePage(&manifestSpool{memory: raw, size: uint64(len(raw))}, 0, uint64(len(raw)))
	if err != nil {
		return nil, err
	}
	root, pages, err := validateDirectoryValue(value, nil, nil, footer)
	if err != nil {
		return nil, err
	}
	version, err := manifestUint(root["version"])
	if err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, rejected(CategoryUnsupportedVersion, "directory")
	}
	requires, ok := root["requires"].([]any)
	if !ok {
		return nil, rejected(CategoryMalformedTOC, "requires")
	}
	previous := ""
	for _, v := range requires {
		feature, ok := v.(string)
		if !ok || !validFeature(feature) || previous != "" && feature <= previous {
			return nil, rejected(CategoryMalformedTOC, "requires")
		}
		if feature != "external-content" {
			return nil, rejected(CategoryUnsupportedFeature, "required feature")
		}
		previous = feature
	}
	retained := uint64(len(raw)) * 8
	if retained > config.MaxRetainedEntryBytes {
		return nil, rejected(CategoryResourceLimit, "retained directory")
	}
	metadataCtx, cancelMetadata := context.WithCancel(context.Background())
	return &pagedState{
		source:            source,
		options:           options,
		config:            config,
		start:             start,
		root:              root,
		pages:             pages,
		cached:            map[int][]Entry{},
		retained:          retained,
		directoryRetained: retained,
		content:           newManifestContentState(config, source),
		metadataCtx:       metadataCtx,
		cancelMetadata:    cancelMetadata,
	}, nil
}

// metadataContext preserves caller values and cancellation while also allowing
// Close to interrupt transport calls without acquiring the metadata mutex.
func (r *pagedState) metadataContext(ctx context.Context) (context.Context, context.CancelFunc) {
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.metadataCtx, cancel)
	if r.metadataCtx.Err() != nil {
		cancel()
	}
	return work, func() {
		stop()
		cancel()
	}
}

func readMetadataPage(ctx context.Context, source RangeReader, start, encoded, decoded uint64, config manifestOptions) ([]byte, error) {
	if encoded <= 8 || encoded > math.MaxInt64 || start > math.MaxInt64-encoded || decoded > uint64(^uint(0)>>1) {
		return nil, rejected(CategoryResourceLimit, "page address space")
	}
	if err := checkLimit(encoded-8, config.Limits.MaxFrameEncodedBytes, "encoded metadata page"); err != nil {
		return nil, err
	}
	if err := checkLimit(decoded, config.Limits.MaxFrameDecodedBytes, "decoded metadata page"); err != nil {
		return nil, err
	}
	raw, err := source.ReadRange(ctx, int64(start), int64(encoded))
	if err != nil {
		return nil, err
	}
	if uint64(len(raw)) != encoded || len(raw) < 13 || binary.LittleEndian.Uint32(raw) != 0x184d2a5e || uint64(binary.LittleEndian.Uint32(raw[4:8]))+8 != encoded || raw[12]&4 == 0 {
		return nil, rejected(CategoryMalformedTOC, "metadata wrapper")
	}
	decoder := frameDecoder{limits: config.Limits}
	defer decoder.close()
	out, err := decoder.decode(raw[8:], 0, int(decoded), addressLimit(config.Limits.MaxFrameEncodedBytes))
	if err != nil {
		if ErrorCategory(err) == CategoryResourceLimit && strings.Contains(err.Error(), "decoded output limit") {
			return nil, rejected(CategoryMalformedTOC, "page decoded length")
		}
		return nil, categorized(CategoryMalformedTOC, err)
	}
	if uint64(len(out)) != decoded {
		return nil, rejected(CategoryMalformedTOC, "page decoded length")
	}
	return out, nil
}

// page runs with mu held, so concurrent requests share one complete load.
func (r *pagedState) page(ctx context.Context, index int) ([]Entry, error) {
	if entries, ok := r.cached[index]; ok {
		return entries, nil
	}
	ref := r.pages[index]
	raw, err := readMetadataPage(ctx, r.source, r.start+ref.offset, ref.encoded, ref.decoded, r.config)
	if err != nil {
		return nil, err
	}
	refs := append([]pageReference{}, r.pages...)
	refs[index].rawOffset = 0
	sequence := pageEntryReader{spool: &manifestSpool{memory: raw, size: uint64(len(raw))}, pages: refs, page: index}
	var entries []Entry
	requires, ok := r.root["requires"].([]any)
	if !ok {
		return nil, rejected(CategoryMalformedTOC, "requires")
	}
	external := len(requires) != 0
	var cost uint64
	for len(entries) == 0 || len(sequence.entries) > 0 {
		value, e := sequence.next()
		if e != nil {
			return nil, e
		}
		entry, e := parseEntry(value, r.start)
		if e != nil {
			return nil, e
		}
		if e = r.config.Limits.entry(entry, sequence.lastSize); e != nil {
			return nil, e
		}
		if entry.External() && !external {
			return nil, rejected(CategoryMalformedTOC, "undeclared external content")
		}
		if len(entries) > 0 && entries[len(entries)-1].Path >= entry.Path {
			return nil, rejected(CategoryMalformedTOC, "page order")
		}
		cost += entryRetainedCost(entry, sequence.lastSize)
		if cost > r.config.MaxRetainedEntryBytes-r.retained {
			return nil, rejected(CategoryResourceLimit, "retained metadata pages")
		}
		if len(entry.Extents) != 0 {
			entry.extentEnds = make([]uint64, len(entry.Extents))
			var end uint64
			for i, extent := range entry.Extents {
				end += extent.Size
				entry.extentEnds[i] = end
			}
		}
		entries = append(entries, entry)
		if r.config.MaxEntries != 0 && uint64(len(entries))+r.count > r.config.MaxEntries {
			return nil, rejected(CategoryResourceLimit, "entry count")
		}
	}
	spare := uint64(cap(entries)-len(entries)) * uint64(unsafe.Sizeof(Entry{}))
	if spare > r.config.MaxRetainedEntryBytes-r.retained-cost {
		return nil, rejected(CategoryResourceLimit, "retained metadata pages")
	}
	cost += spare
	r.retained += cost
	r.count += uint64(len(entries))
	r.cached[index] = entries
	return entries, nil
}

func (r *pagedState) localEntry(ctx context.Context, path string) (*Entry, error) {
	index, _ := slices.BinarySearchFunc(r.pages, path, func(p pageReference, path string) int { return strings.Compare(p.lastPath, path) })
	if index == len(r.pages) {
		return nil, nil
	}
	entries, err := r.page(ctx, index)
	if err != nil {
		return nil, err
	}
	i, found := slices.BinarySearchFunc(entries, path, compareEntryPath)
	if !found {
		return nil, nil
	}
	return &entries[i], nil
}

// Entry returns an immutable internal snapshot after validating its page and ancestors.
func (r *pagedState) Entry(ctx context.Context, path string) (*Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fs.ErrClosed
	}
	if r.full {
		entry, err := r.owner.indexedEntrySnapshot(path)
		if err != nil {
			return nil, err
		}
		owned := entry
		return &owned, nil
	}
	if !validPath(path) {
		return nil, fs.ErrInvalid
	}
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	entry, err := r.localEntry(ctx, path)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fs.ErrNotExist
	}
	for _, parent := range pathAncestors(path) {
		ancestor, e := r.localEntry(ctx, parent)
		if e != nil {
			return nil, e
		}
		if ancestor == nil || ancestor.Type != TypeDirectory {
			return nil, rejected(CategoryMalformedTOC, "missing ancestor directory")
		}
	}
	owned := *entry
	return &owned, nil
}

func (r *pagedState) ensureFull(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fs.ErrClosed
	}
	if r.full {
		return nil
	}
	// Full scans replace cached pages with the complete index.
	r.cached = map[int][]Entry{}
	r.retained, r.count = r.directoryRetained, 0
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	m, err := openManifest(ctx, r.source, r.options.Manifest)
	if err != nil {
		return err
	}
	r.owner.mu.Lock()
	r.owner.entries = m.entries
	r.owner.count = m.count
	r.owner.requires = m.requires
	r.owner.rootExtensions = m.rootExtensions
	r.owner.mu.Unlock()
	m.entries = nil
	m.content.close()
	if err = m.spool.close(); err != nil {
		return err
	}
	r.cached = nil
	r.full = true
	return nil
}

// Close releases cached pages and content without closing the caller-owned source.
func (r *pagedState) Close() {
	r.cancelMetadata()
	r.content.close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cached = nil
}
