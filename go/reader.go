package zstar

import (
	"context"
	"errors"
	"reflect"
)

// ReaderOptions controls bounded metadata/content storage and external hashes.
// Content caching defaults to 16 MiB; DisableContentCache explicitly disables it.
// Metadata is always authenticated and archive data checksums are mandatory.
type ReaderOptions struct {
	Manifest            ManifestOptions
	Resolver            EncodedContentResolver
	SkipExternalHash    bool
	DisableContentCache bool
	// SharedContentCache requires an immutable encoded archive SourceIdentity.
	SharedContentCache DecodedContentCache
	SourceIdentity     []byte
}

// Reader owns authenticated sorted entries and bounded content caches.
// Metadata spools are released after open; retained entries use O(N) memory.
// Close releases entries/caches and cancels loads, without closing source.
// Sources and resolvers must support concurrent calls and stay immutable.
type Reader struct {
	manifest       *Manifest
	resolver       EncodedContentResolver
	verifyExternal bool
}

// A reader pins one resolver namespace for its lifetime, including value and
// function implementations that have no stable comparable pointer identity.
type readerResolver struct{ inner EncodedContentResolver }

func (r *readerResolver) GetEncoded(ctx context.Context, address [32]byte, hint, maxBytes uint64) ([]byte, error) {
	return r.inner.GetEncoded(ctx, address, hint, maxBytes)
}

func (r *readerResolver) EncodedLength(d [32]byte) (uint64, bool) {
	if b, ok := r.inner.(BatchEncodedContentResolver); ok {
		return b.EncodedLength(d)
	}
	return 0, false
}

func (r *readerResolver) GetEncodedMany(ctx context.Context, requests []ObjectRequest) ([][]byte, error) {
	if b, ok := r.inner.(BatchEncodedContentResolver); ok {
		return b.GetEncodedMany(ctx, requests)
	}
	out := make([][]byte, len(requests))
	for i, q := range requests {
		data, err := r.GetEncoded(ctx, q.Digest, q.Hint, q.MaxBytes)
		if err != nil {
			return nil, err
		}
		out[i] = data
	}
	return out, nil
}

// Open authenticates an archive using default resource budgets. The caller
// owns the immutable, concurrently readable source. Close the returned reader
// to release SDK resources. ctx applies only to the opening operation.
func Open(ctx context.Context, source RangeReader) (*Reader, error) {
	return OpenWithOptions(ctx, source, ReaderOptions{})
}

// OpenWithOptions authenticates an archive using options. The caller owns
// the source, resolver and any shared cache. Sources and resolvers must support
// concurrent calls. ctx applies only to opening, not subsequent reads.
func OpenWithOptions(ctx context.Context, source RangeReader, options ReaderOptions) (*Reader, error) {
	if (options.SharedContentCache == nil) != (len(options.SourceIdentity) == 0) {
		return nil, rejected("unsupported-operation", "shared cache and source identity must be supplied together")
	}
	if len(options.SourceIdentity) > 65536 {
		return nil, rejected("resource-limit", "shared cache identity exceeds 65536 bytes")
	}
	options.SourceIdentity = append([]byte(nil), options.SourceIdentity...)
	if options.DisableContentCache {
		options.Manifest.ContentCacheBytes = 0
	} else if options.Manifest.ContentCacheBytes == 0 {
		options.Manifest.ContentCacheBytes = 16 << 20
	}
	manifest, err := OpenManifest(ctx, source, options.Manifest)
	if err != nil {
		return nil, err
	}
	if options.SharedContentCache != nil {
		if err := manifest.SetSharedContentCache(options.SourceIdentity, options.SharedContentCache); err != nil {
			return nil, errors.Join(err, manifest.Close())
		}
	}
	resolver := options.Resolver
	if resolver != nil && reflect.ValueOf(resolver).Kind() != reflect.Pointer {
		resolver = &readerResolver{inner: resolver}
	}
	return &Reader{manifest, resolver, !options.SkipExternalHash}, nil
}

// Close cancels active loads and releases metadata and private caches.
// It leaves the source, resolver and caller-owned shared cache open.
func (r *Reader) Close() error { return r.manifest.Close() }

// Entry returns owned member metadata without following links. Missing paths
// match fs.ErrNotExist; invalid paths match fs.ErrInvalid. Errors are *fs.PathError.
func (r *Reader) Entry(path string) (*Entry, error) { return r.manifest.Entry(path) }

// EntryCount returns the member count without listing entries.
func (r *Reader) EntryCount() uint64 { return r.manifest.EntryCount() }

// RootExtensions returns an owned copy of authenticated root metadata.
func (r *Reader) RootExtensions() Extensions { return r.manifest.RootExtensions() }

// Cursor creates an independent cursor over sorted owned member metadata.
func (r *Reader) Cursor() *ManifestCursor { return r.manifest.Cursor() }

// VisitEntries visits sorted owned entries without holding the metadata lock.
// Callbacks may read the archive. The first callback error stops visitation.
func (r *Reader) VisitEntries(visit func(Entry) error) error { return r.manifest.VisitEntries(visit) }

// ValidateMetadata confirms authenticated metadata remains open and observes
// cancellation. It does not validate physical archive content.
func (r *Reader) ValidateMetadata(ctx context.Context) error {
	return r.manifest.ValidateMetadata(ctx)
}

// Read returns an owned buffer containing exactly size bytes at the logical
// member offset. The range must fit within the member and MaxReadBytes.
// Symlinks are never followed. ctx bounds this operation only.
func (r *Reader) Read(ctx context.Context, path string, offset, size uint64) ([]byte, error) {
	return r.manifest.Read(ctx, path, offset, size, r.resolver, r.verifyExternal)
}

// ReadMany reads bounded ranges in request order, sharing chunk loads.
// Ranges and the aggregate budget are checked before I/O; failure returns no
// results. Each successful result is owned by the caller.
func (r *Reader) ReadMany(ctx context.Context, requests []ReadRequest) ([][]byte, error) {
	return r.manifest.ReadMany(ctx, requests, r.resolver, r.verifyExternal)
}

// ReadWithReport reads a bounded member range and reports checks of selected
// chunks. It does not establish full physical archive validity.
func (r *Reader) ReadWithReport(ctx context.Context, path string, offset, size uint64) ([]byte, ReadReport, error) {
	return r.manifest.ReadWithReport(ctx, path, offset, size, r.resolver, r.verifyExternal)
}

// ReadManyWithReport reads a bounded batch and reports checks of selected
// chunks. Results are in caller order and own independent buffers.
func (r *Reader) ReadManyWithReport(ctx context.Context, requests []ReadRequest) ([][]byte, ReadReport, error) {
	return r.manifest.ReadManyWithReport(ctx, requests, r.resolver, r.verifyExternal)
}

// OpenFile opens a regular member for concurrent random-access reads and
// streaming. Keep the reader open while using the view. ctx applies only to
// opening; ReadAtContext and WriteToContext accept per-operation deadlines.
func (r *Reader) OpenFile(ctx context.Context, path string) (*File, error) {
	return r.manifest.OpenFile(ctx, path, r.resolver, r.verifyExternal)
}

// ReadLink returns the exact stored symlink target without following it.
// Missing paths return an error matching fs.ErrNotExist.
func (r *Reader) ReadLink(path string) (string, error) {
	entry, err := r.Entry(path)
	if err != nil {
		return "", pathError("readlink", path, err)
	}
	if entry.Type != TypeSymlink || entry.Linkname == nil {
		return "", pathError("readlink", path, rejected("unsupported-operation", "entry is not a symlink"))
	}
	return *entry.Linkname, nil
}

// Verify validates the complete physical archive. If externalClosure is
// true, it also resolves every external extent. SkipExternalHash controls
// object hashing; codec checksums and range checks always apply.
func (r *Reader) Verify(ctx context.Context, externalClosure bool) (VerificationReport, error) {
	return r.manifest.Verify(ctx, r.resolver, r.verifyExternal, externalClosure)
}

// VisitPayloads validates physical content while invoking borrowed payload
// callbacks. Bytes are valid only until callback return and output remains
// provisional until success. External payloads are pointer text; directories
// and symlinks yield no payload. Callbacks must not close the reader.
func (r *Reader) VisitPayloads(ctx context.Context, visit func(Entry, []byte) error) (ArchiveReport, error) {
	return r.manifest.VisitPayloads(ctx, visit)
}

// ContentCacheBytes reports private decoded-cache allocation and bookkeeping.
func (r *Reader) ContentCacheBytes() uint64 { return r.manifest.ContentCacheBytes() }
