package zstar

import (
	"context"
	"iter"
	"reflect"
)

// ReaderOptions controls bounded metadata/content storage and external hashes.
// Selected metadata is validated and archive data checksums are mandatory.
type ReaderOptions struct {
	// Manifest controls metadata storage and content resource budgets.
	Manifest ManifestReaderOptions
	Resolver EncodedContentResolver
	// VerifyExternalHash opts into SHA-256 verification of resolved objects.
	// The default checks object availability, encoded bounds and decoded extent lengths.
	VerifyExternalHash bool
}

// Reader owns a validated directory, cached entry pages, and bounded read state.
// Counting and listing scan the complete metadata.
// Close releases entries and cancels loads, without closing source.
// Sources and resolvers must support concurrent calls and stay immutable.
type Reader struct {
	manifest       *manifest
	resolver       EncodedContentResolver
	verifyExternal bool
}

// A reader pins one resolver namespace for its lifetime, including value and
// function implementations that have no stable comparable pointer identity.
type readerResolver struct{ inner EncodedContentResolver }

func (r *readerResolver) GetEncoded(ctx context.Context, address [32]byte, hint, maxBytes uint64) ([]byte, error) {
	return r.inner.GetEncoded(ctx, address, hint, maxBytes)
}

// Open validates an archive using default resource budgets. The caller
// owns the immutable, concurrently readable source. Close the returned reader
// to release SDK resources. ctx applies only to the opening operation.
func Open(ctx context.Context, source RangeReader) (*Reader, error) {
	return OpenWithOptions(ctx, source, ReaderOptions{})
}

// OpenWithOptions validates an archive using options. The caller owns
// the source and resolver. Sources and resolvers must support
// concurrent calls. ctx applies only to opening, not subsequent reads.
func OpenWithOptions(ctx context.Context, source RangeReader, options ReaderOptions) (*Reader, error) {
	paged, err := openPaged(ctx, source, options)
	if err != nil {
		return nil, err
	}
	extensions := Extensions{}
	for key, value := range paged.root {
		if key, ok := key.(string); ok && len(key) >= 2 && key[:2] == "x-" {
			extensions[key] = value
		}
	}
	manifest := &manifest{source: paged.source, start: paged.start, maxRead: paged.config.MaxReadBytes, rootExtensions: extensions, spool: manifestSpool{options: decodedSpoolOptions(paged.config)}, content: paged.content, paged: paged}
	paged.owner = manifest
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

// EntryCount validates every metadata page and returns the derived member count.
func (r *Reader) EntryCount() (uint64, error) {
	if r.manifest.paged != nil {
		if err := r.manifest.paged.ensureFull(context.Background()); err != nil {
			return 0, err
		}
	}
	return r.manifest.EntryCount(), nil
}

// RootExtensions returns an owned copy of validated root metadata.
func (r *Reader) RootExtensions() Extensions { return r.manifest.RootExtensions() }

// Entries yields owned entries whose paths begin with prefix, in path order.
// Use "dir/" for descendants or "" for all entries. Stop on a nonnil error.
func (r *Reader) Entries(prefix string) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if r.manifest.paged != nil {
			if err := r.manifest.paged.ensureFull(context.Background()); err != nil {
				yield(Entry{}, err)
				return
			}
		}
		r.manifest.Entries(prefix)(yield)
	}
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
// The view retains one bounded decoded source; Close releases it independently
// of the reader and other handles.
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
