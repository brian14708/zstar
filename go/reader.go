package zstar

import (
	"context"
	"iter"
	"reflect"
)

// ReaderOptions controls bounded metadata/content storage and external hashes.
// Metadata is always authenticated and archive data checksums are mandatory.
type ReaderOptions struct {
	Manifest ManifestReaderOptions
	Resolver EncodedContentResolver
	// VerifyExternalHash opts into SHA-256 verification of resolved objects.
	// The default checks object availability, encoded bounds and decoded extent lengths.
	VerifyExternalHash bool
}

// Reader owns authenticated sorted entries and bounded read state.
// Metadata spools are released after open; retained entries use O(N) memory.
// Close releases entries and cancels loads, without closing source.
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

// Open authenticates an archive using default resource budgets. The caller
// owns the immutable, concurrently readable source. Close the returned reader
// to release SDK resources. ctx applies only to the opening operation.
func Open(ctx context.Context, source RangeReader) (*Reader, error) {
	return OpenWithOptions(ctx, source, ReaderOptions{})
}

// OpenWithOptions authenticates an archive using options. The caller owns
// the source and resolver. Sources and resolvers must support
// concurrent calls. ctx applies only to opening, not subsequent reads.
func OpenWithOptions(ctx context.Context, source RangeReader, options ReaderOptions) (*Reader, error) {
	manifest, err := OpenManifest(ctx, source, options.Manifest)
	if err != nil {
		return nil, err
	}
	resolver := options.Resolver
	if resolver != nil && reflect.ValueOf(resolver).Kind() != reflect.Pointer {
		resolver = &readerResolver{inner: resolver}
	}
	return &Reader{manifest, resolver, options.VerifyExternalHash}, nil
}

// Close cancels active loads and releases metadata state.
// It leaves the source and resolver open.
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

// CursorPrefix visits paths beginning with prefix. Use "dir/" for descendants
// and an empty prefix for all entries. Returned metadata is caller-owned.
func (r *Reader) CursorPrefix(prefix string) *ManifestCursor {
	return r.manifest.CursorPrefix(prefix)
}

// Entries yields owned entries whose paths begin with prefix, in path order.
// Use "dir/" for descendants or "" for all entries. Stop on a nonnil error.
func (r *Reader) Entries(prefix string) iter.Seq2[Entry, error] {
	return r.manifest.Entries(prefix)
}

// VisitEntries visits sorted owned entries without holding the metadata lock.
// Callbacks may read the archive. The first callback error stops visitation.
func (r *Reader) VisitEntries(visit func(Entry) error) error { return r.manifest.VisitEntries(visit) }

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
		return "", pathError("readlink", path, rejected(CategoryUnsupportedOperation, "entry is not a symlink"))
	}
	return *entry.Linkname, nil
}

// VerificationScope selects the checks performed by Reader.VerifyScope.
type VerificationScope uint8

const (
	// VerifyArchive checks metadata and every physical frame and tar member.
	VerifyArchive VerificationScope = iota
	// VerifyExternalClosure additionally resolves all external selections.
	// Hash checking still follows ReaderOptions.VerifyExternalHash.
	VerifyExternalClosure
)

// VerifyScope validates the archive under an explicit verification scope.
func (r *Reader) VerifyScope(ctx context.Context, scope VerificationScope) (VerificationReport, error) {
	if scope != VerifyArchive && scope != VerifyExternalClosure {
		return VerificationReport{}, rejected(CategoryUnsupportedOperation, "unknown verification scope")
	}
	return r.manifest.Verify(ctx, r.resolver, r.verifyExternal, scope == VerifyExternalClosure)
}

// Verify is the compatibility shorthand for VerifyScope. It validates the
// complete physical archive and, when externalClosure is true, every external extent. External object SHA-256 is
// checked only when VerifyExternalHash is set; codec checksums and range checks always apply.
func (r *Reader) Verify(ctx context.Context, externalClosure bool) (VerificationReport, error) {
	scope := VerifyArchive
	if externalClosure {
		scope = VerifyExternalClosure
	}
	return r.VerifyScope(ctx, scope)
}

// VisitPayloads validates physical content while invoking borrowed payload
// callbacks. Bytes are valid only until callback return and output remains
// provisional until success. External payloads are pointer JSON; directories
// and symlinks yield no payload. Callbacks must not close the reader.
func (r *Reader) VisitPayloads(ctx context.Context, visit func(Entry, []byte) error) (ArchiveReport, error) {
	return r.manifest.VisitPayloads(ctx, visit)
}
