package zstar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"

	"github.com/klauspost/compress/zstd"
)

// PackStore owns runtime storage configuration. Put publishes a new immutable
// key; ReadPackedRange must match the supplied snapshot and exact byte range.
type PackStore interface {
	PutPack(ctx context.Context, key string, input io.Reader, length uint64) (PackSnapshot, error)
	ReadPackedRange(ctx context.Context, snapshot PackSnapshot, offset, length uint64) ([]byte, error)
}

// PackWriterOptions bounds physical packs, metadata and temporary spools.
type PackWriterOptions struct {
	Prefix           string
	TargetBytes      uint64
	CompressionLevel int
	Catalog          CatalogLimits
	Spool            ManifestOptions
}

// PackWriter serializes one compression job, spool and upload. Its catalog
// index has a separate budget. Methods are not concurrent; always defer Close.
type PackWriter struct {
	store          PackStore
	options        PackWriterOptions
	catalog        *PackedCatalog
	pending        map[[32]byte]PackedChunk
	spool          manifestSpool
	hash           hash.Hash
	codec          *zstd.Encoder
	encoded        []byte
	failed, closed bool
	completed      []PackSnapshot
}

// NewPackWriter creates a session with optional explicit base-catalog reuse.
func NewPackWriter(store PackStore, base *PackedCatalog, options PackWriterOptions) (*PackWriter, error) {
	if store == nil {
		return nil, errors.New("zstar: nil pack store")
	}
	if options.Prefix == "" {
		options.Prefix = "objects"
	}
	if options.TargetBytes == 0 {
		options.TargetBytes = 64 << 20
	}
	if options.CompressionLevel == 0 {
		options.CompressionLevel = 3
	}
	options.Catalog = options.Catalog.normalized()
	options.Spool = options.Spool.defaults()
	if !validPath(options.Prefix) || options.TargetBytes > options.Spool.MaxEncodedBytes || options.CompressionLevel < -131072 || options.CompressionLevel > 22 {
		return nil, errors.New("zstar: invalid pack writer settings")
	}
	if base == nil {
		var err error
		base, err = NewPackedCatalog(nil, nil, options.Catalog)
		if err != nil {
			return nil, err
		}
	}
	if _, err := base.Encode(options.Catalog); err != nil {
		return nil, err
	}
	codec, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(maxFrameSize), zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(manifestCompressionLevel(options.CompressionLevel)))
	if err != nil {
		return nil, err
	}
	return &PackWriter{store: store, options: options, catalog: base, pending: make(map[[32]byte]PackedChunk), spool: manifestSpool{options: options.Spool}, hash: sha256.New(), codec: codec}, nil
}

// CompletedPacks returns uploaded immutable keys for orphan accounting.
func (w *PackWriter) CompletedPacks() []PackSnapshot {
	return append([]PackSnapshot(nil), w.completed...)
}

func (w *PackWriter) check() error {
	if w.failed || w.closed {
		return errors.New("zstar: pack writer failed or closed")
	}
	return nil
}

// Put independently hashes and compresses one complete raw chunk.
func (w *PackWriter) Put(ctx context.Context, raw []byte) (Extent, error) {
	if err := w.check(); err != nil {
		return Extent{}, err
	}
	w.failed = true
	if err := ctx.Err(); err != nil {
		return Extent{}, err
	}
	if len(raw) == 0 || len(raw) > maxFrameSize {
		return Extent{}, rejected("resource-limit", "external object size")
	}
	digest := sha256.Sum256(raw)
	existing, exists := w.pending[digest]
	if index, ok := w.catalog.byChunk[digest]; ok {
		existing = w.catalog.chunks[index]
		exists = true
	}
	if exists {
		if existing.DecodedLength != uint64(len(raw)) {
			return Extent{}, rejected("malformed-catalog", "inconsistent decoded size")
		}
		w.failed = false
		return Extent{Size: uint64(len(raw)), Source: ObjectSource(digest), EncodedSize: existing.EncodedLength}, nil
	}
	count := len(w.catalog.chunks) + len(w.pending) + 1
	if count > w.options.Catalog.MaxChunks || uint64(count) > w.options.Catalog.MaxMemoryBytes/512 {
		return Extent{}, rejected("resource-limit", "pack hash index")
	}
	w.encoded = w.codec.EncodeAll(raw, w.encoded[:0])
	encoded := w.encoded
	if len(encoded) > 17<<20 {
		return Extent{}, rejected("resource-limit", "encoded object size")
	}
	if w.spool.size != 0 && uint64(len(encoded)) > w.options.TargetBytes-min(w.options.TargetBytes, w.spool.size) {
		if err := w.flush(ctx); err != nil {
			return Extent{}, err
		}
	}
	offset := w.spool.size
	if err := w.spool.append(encoded); err != nil {
		return Extent{}, err
	}
	_, _ = w.hash.Write(encoded)
	w.pending[digest] = PackedChunk{Digest: digest, Offset: offset, EncodedLength: uint64(len(encoded)), DecodedLength: uint64(len(raw))}
	w.failed = false
	return Extent{Size: uint64(len(raw)), Source: ObjectSource(digest), EncodedSize: uint64(len(encoded))}, nil
}

type packSpoolInput struct {
	spool    *manifestSpool
	position uint64
}

func (r *packSpoolInput) Read(out []byte) (int, error) {
	if len(out) == 0 {
		return 0, nil
	}
	if r.position == r.spool.size {
		return 0, io.EOF
	}
	count := min(uint64(len(out)), r.spool.size-r.position, uint64(64<<10))
	data, err := r.spool.read(r.position, count)
	if err != nil {
		return 0, err
	}
	copy(out, data)
	r.position += count
	return int(count), nil
}

func (w *PackWriter) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	if len(w.catalog.packs) == w.options.Catalog.MaxPacks {
		return rejected("resource-limit", "pack count")
	}
	var digest [32]byte
	copy(digest[:], w.hash.Sum(nil))
	key := w.options.Prefix + "/packs/" + hex.EncodeToString(digest[:]) + ".zstp"
	snapshot, err := w.store.PutPack(ctx, key, &packSpoolInput{spool: &w.spool}, w.spool.size)
	if err != nil {
		return err
	}
	if err = snapshot.Validate(); err != nil {
		return err
	}
	if snapshot.Key != key || snapshot.Length != w.spool.size {
		return rejected("changed-pack", "published identity")
	}
	w.completed = append(w.completed, snapshot)
	packs := w.catalog.Packs()
	packs = append(packs, PackRecord{ID: digest, Snapshot: snapshot})
	chunks := w.catalog.Chunks()
	for _, chunk := range w.pending {
		chunk.Pack = digest
		chunks = append(chunks, chunk)
	}
	catalog, err := NewPackedCatalog(packs, chunks, w.options.Catalog)
	if err != nil {
		return err
	}
	if err = w.spool.close(); err != nil {
		return err
	}
	w.catalog = catalog
	w.spool = manifestSpool{options: w.options.Spool}
	clear(w.pending)
	w.hash.Reset()
	return nil
}

// Finish uploads pending frames and closes the session, returning its catalog.
func (w *PackWriter) Finish(ctx context.Context) (*PackedCatalog, error) {
	if err := w.check(); err != nil {
		return nil, err
	}
	w.failed = true
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.flush(ctx); err != nil {
		return nil, err
	}
	if _, err := w.catalog.Encode(w.options.Catalog); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return w.catalog, nil
}

// Close releases temporary storage; it never deletes completed or shared packs.
func (w *PackWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.encoded = nil
	return errors.Join(w.codec.Close(), w.spool.close())
}
