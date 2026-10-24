package zstar

import (
	"context"
	"io"
	"math"
	"sort"
)

// File is a logical member view. It retains bounded entry metadata and
// borrows its manifest, which must remain open. ReadAt supports concurrent calls;
// WriteTo streams complete selected extents without a whole-file allocation.
type File struct {
	manifest       *Manifest
	entry          *Entry
	resolver       EncodedContentResolver
	verifyExternal bool
	size           uint64
	starts         []uint64
}

// OpenFile returns a concurrent random-access view of a regular member.
// The manifest must remain open. ctx applies only to opening; use
// ReadAtContext and WriteToContext to bound later I/O.
func (m *Manifest) OpenFile(ctx context.Context, path string, resolver EncodedContentResolver, verifyExternal bool) (*File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := m.Entry(path)
	if err != nil {
		return nil, pathError("open", path, err)
	}
	if entry == nil || entry.Type != TypeFile {
		return nil, rejected("unsupported-operation", "entry is not a file")
	}
	return m.openEntryFile(entry, resolver, verifyExternal)
}

// entry is already an owned regular-file snapshot; keep it private to the view.
func (m *Manifest) openEntryFile(entry *Entry, resolver EncodedContentResolver, verifyExternal bool) (*File, error) {
	size, err := entry.Size()
	if err != nil {
		return nil, err
	}
	starts := make([]uint64, len(entry.Extents)+1)
	for i, extent := range entry.Extents {
		starts[i+1] = starts[i] + extent.Size
	}
	return &File{manifest: m, entry: entry, resolver: resolver, verifyExternal: verifyExternal, size: size, starts: starts}, nil
}

// Size returns the logical member length in bytes.
func (f *File) Size() uint64 { return f.size }

// ReadAt implements io.ReaderAt using a background context. Calls may run
// concurrently. Use ReadAtContext for remote reads with deadlines.
func (f *File) ReadAt(output []byte, offset int64) (int, error) {
	return f.ReadAtContext(context.Background(), output, offset)
}

// ReadAtContext reads a member range using ctx for this operation only.
// A short read at the end of the member returns io.EOF.
func (f *File) ReadAtContext(ctx context.Context, output []byte, offset int64) (int, error) {
	if err := f.manifest.content.checkOpen(ctx); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, rejected("invalid-range", "negative file offset")
	}
	if len(output) == 0 {
		return 0, nil
	}
	if uint64(offset) >= f.size {
		return 0, io.EOF
	}
	size := min(uint64(len(output)), f.size-uint64(offset))
	if size > f.manifest.maxRead {
		return f.readAtSerial(ctx, output, uint64(offset), size)
	}
	data, err := f.manifest.Read(ctx, f.entry.Path, uint64(offset), size, f.resolver, f.verifyExternal)
	if err != nil {
		return 0, err
	}
	n := copy(output, data)
	if n < len(output) {
		return n, io.EOF
	}
	return n, nil
}

// WriteTo implements io.WriterTo and streams the complete member using a
// background context. Use WriteToContext for operation deadlines.
func (f *File) WriteTo(output io.Writer) (int64, error) {
	return f.WriteToContext(context.Background(), output)
}

// WriteToContext streams the complete member using ctx for this operation only.
// The destination remains caller-owned; failures return the bytes written.
func (f *File) WriteToContext(ctx context.Context, output io.Writer) (int64, error) {
	if err := f.manifest.content.checkOpen(ctx); err != nil {
		return 0, err
	}
	if f.size > math.MaxInt64 {
		return 0, rejected("resource-limit", "stream count exceeds int64")
	}
	for _, e := range f.entry.Extents {
		if e.Size > f.manifest.maxRead {
			return f.writeToSerial(ctx, output)
		}
	}
	var written int64
	for i := 0; i < len(f.entry.Extents); {
		start := f.starts[i]
		end := f.starts[i+1]
		i++
		for i < len(f.entry.Extents) && f.starts[i+1]-start <= min(uint64(8<<20), f.manifest.maxRead) {
			end = f.starts[i+1]
			i++
		}
		data, err := f.manifest.Read(ctx, f.entry.Path, start, end-start, f.resolver, f.verifyExternal)
		if err != nil {
			return written, err
		}
		n, err := output.Write(data)
		if n < 0 || n > len(data) {
			return written, io.ErrShortWrite
		}
		written += int64(n)
		if err != nil {
			return written, err
		}
		if n != len(data) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// VerificationReport describes physical archive and optional external closure
// validation, including whether external object hashes were checked.
type VerificationReport struct {
	ArchiveReport
	ExternalClosureValidated     bool `json:"external_closure_validated"`
	ExternalObjectsSHA256Checked bool `json:"external_objects_sha256_checked"`
	ExternalObjectsUnchecked     bool `json:"external_objects_unchecked"`
}

// Verify checks full physical validity and optionally every external selected
// range. verifyExternal controls object hashing; checksums and range/resource
// checks always apply. No reconstructed whole-file hash is computed.
func (m *Manifest) Verify(ctx context.Context, resolver EncodedContentResolver, verifyExternal, externalClosure bool) (VerificationReport, error) {
	physical, err := m.ValidateArchive(ctx)
	if err != nil {
		return VerificationReport{}, err
	}
	report := VerificationReport{ArchiveReport: physical}
	if externalClosure {
		err = m.VisitEntries(func(entry Entry) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !entry.External() {
				return nil
			}
			var previous manifestChunkKey
			var chunk *manifestContentChunk
			for _, extent := range entry.Extents {
				key := chunkKey(extent)
				if chunk == nil || key != previous {
					var err error
					chunk, err = m.content.get(ctx, extent, resolver, verifyExternal)
					if err != nil {
						return err
					}
					previous = key
				}
				if extent.Offset > uint64(len(chunk.raw)) || extent.Size > uint64(len(chunk.raw))-extent.Offset {
					return rejected("invalid-range", "decoded extent bounds")
				}
				if chunk.verified {
					report.ExternalObjectsSHA256Checked = true
				} else {
					report.ExternalObjectsUnchecked = true
				}
			}
			return nil
		})
		if err != nil {
			return VerificationReport{}, err
		}
		report.ExternalClosureValidated = true
	}
	return report, nil
}

func (f *File) readAtSerial(ctx context.Context, output []byte, offset, size uint64) (int, error) {
	first := sort.Search(len(f.entry.Extents), func(i int) bool { return f.starts[i+1] > offset })
	var n int
	var previous manifestChunkKey
	var chunk *manifestContentChunk
	for i := first; uint64(n) < size; i++ {
		extent := f.entry.Extents[i]
		key := chunkKey(extent)
		if chunk == nil || key != previous {
			var err error
			chunk, err = f.manifest.content.get(ctx, extent, f.resolver, f.verifyExternal)
			if err != nil {
				return n, err
			}
			previous = key
		}
		if extent.Offset > uint64(len(chunk.raw)) || extent.Size > uint64(len(chunk.raw))-extent.Offset {
			return n, rejected("invalid-range", "decoded extent bounds")
		}
		within := offset + uint64(n) - f.starts[i]
		length := min(extent.Size-within, size-uint64(n))
		start := extent.Offset + within
		n += copy(output[n:], chunk.raw[start:start+length])
	}
	if n < len(output) {
		return n, io.EOF
	}
	return n, nil
}

func (f *File) writeToSerial(ctx context.Context, output io.Writer) (int64, error) {
	var written int64
	var previous manifestChunkKey
	var chunk *manifestContentChunk
	for _, extent := range f.entry.Extents {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		key := chunkKey(extent)
		if chunk == nil || key != previous {
			var err error
			chunk, err = f.manifest.content.get(ctx, extent, f.resolver, f.verifyExternal)
			if err != nil {
				return written, err
			}
			previous = key
		}
		if extent.Offset > uint64(len(chunk.raw)) || extent.Size > uint64(len(chunk.raw))-extent.Offset {
			return written, rejected("invalid-range", "decoded extent bounds")
		}
		n, err := output.Write(chunk.raw[extent.Offset : extent.Offset+extent.Size])
		if n < 0 || uint64(n) > extent.Size {
			return written, io.ErrShortWrite
		}
		written += int64(n)
		if err != nil {
			return written, err
		}
		if uint64(n) != extent.Size {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
