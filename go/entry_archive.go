package zstar

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// EntryArchiveWriter emits checksummed data frames and a deterministic manifest
// sequence in bounded compressed segments.
// Input paths may arrive in arbitrary order; tar follows insertion order. Metadata and transient frame
// records spill to disk; no complete entry or frame table is kept in memory.
// Close finalizes the archive and releases spools, but does not close output.
type EntryArchiveWriter struct {
	output                io.Writer
	codec                 *zstd.Encoder
	options               ManifestOptions
	target                int
	pending               []byte
	raw, emitted, encoded uint64
	frames, records       manifestSpool
	err                   error
	closed                bool
}

// NewEntryArchiveWriter creates an archive encoder with bounded frame and
// metadata storage. Zero frameSize selects 1 MiB. The caller owns output.
func NewEntryArchiveWriter(output io.Writer, frameSize int, options ManifestOptions) (*EntryArchiveWriter, error) {
	if output == nil {
		return nil, errors.New("zstar: nil destination")
	}
	if frameSize == 0 {
		frameSize = 1 << 20
	}
	if frameSize < 512 || frameSize > maxFrameSize || frameSize%512 != 0 {
		return nil, errors.New("zstar: invalid frame target")
	}
	options = options.defaults()
	if err := validateExtensions(options.RootExtensions); err != nil {
		return nil, err
	}
	options.RootExtensions = cloneExtensions(options.RootExtensions)
	codec, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true), zstd.WithWindowSize(frameWindow(frameSize)), zstd.WithEncoderLevel(manifestCompressionLevel(options.CompressionLevel)))
	if err != nil {
		return nil, err
	}
	return &EntryArchiveWriter{output: output, codec: codec, options: options, target: frameSize, pending: make([]byte, 0, frameSize), frames: manifestSpool{options: options}, records: manifestSpool{options: options}}, nil
}

func (w *EntryArchiveWriter) flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	encoded := w.codec.EncodeAll(w.pending, nil)
	if len(encoded) > 17<<20 {
		return rejected("malformed-frame", "encoded frame limit")
	}
	if uint64(len(encoded)) > math.MaxUint64-w.encoded {
		return rejected("resource-limit", "archive size overflow")
	}
	if err := writeExact(w.output, encoded); err != nil {
		return err
	}
	record := make([]byte, 32)
	for i, n := range []uint64{w.emitted, uint64(len(w.pending)), w.encoded, uint64(len(encoded))} {
		binary.LittleEndian.PutUint64(record[i*8:], n)
	}
	if err := w.frames.append(record); err != nil {
		return err
	}
	w.emitted += uint64(len(w.pending))
	w.encoded += uint64(len(encoded))
	w.pending = w.pending[:0]
	return nil
}

func (w *EntryArchiveWriter) append(data []byte) error {
	if uint64(len(data)) > math.MaxUint64-w.raw {
		return rejected("resource-limit", "decoded archive overflow")
	}
	for len(data) != 0 {
		n := min(len(data), w.target-len(w.pending))
		w.pending = append(w.pending, data[:n]...)
		data = data[n:]
		w.raw += uint64(n)
		if len(w.pending) == w.target {
			if err := w.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Add consumes exactly physicalSize bytes for an inline regular file. For an
// external entry, physicalSize must be zero and the canonical pointer text is
// generated from its extents. Non-files also require physicalSize zero.
// Invalid arguments are rejected without side effects and do not poison the
// writer; I/O, body-read and budget failures after output begins are sticky.
func (w *EntryArchiveWriter) Add(ctx context.Context, entry Entry, physicalSize uint64, body io.Reader) (err error) {
	if w.closed {
		return errors.New("zstar: writer closed")
	}
	if w.err != nil {
		return w.err
	}
	// Caller-input errors found before any state is touched leave the writer
	// usable; only failures after output/spool mutation poison it.
	mutated := false
	defer func() {
		// Cancellation stays sticky so a canceled build cannot be mistaken for complete.
		if err != nil && (mutated || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			w.err = err
		}
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = entry.validateFields(math.MaxUint64); err != nil {
		return err
	}
	entryRaw, err := encodeEntry(entry)
	if err != nil {
		return err
	}
	if entry.Type != TypeFile && (physicalSize != 0 || body != nil) {
		return errors.New("zstar: non-file body")
	}
	if entry.External() {
		if physicalSize != 0 || body != nil {
			return errors.New("zstar: external entry body must be omitted")
		}
		pointer, e := EncodeContentPointer(entry.Extents)
		if e != nil {
			return e
		}
		physicalSize = uint64(len(pointer))
		body = bytes.NewReader(pointer)
	} else if len(entry.Extents) != 0 {
		return errors.New("zstar: inline extents are assigned by encoder")
	}
	if physicalSize != 0 && body == nil {
		return errors.New("zstar: missing file body")
	}
	kind := "file"
	if entry.Type == TypeSymlink {
		kind = "symlink"
	}
	if entry.Type == TypeDirectory {
		kind = "directory"
	}
	target := ""
	if entry.Linkname != nil {
		target = *entry.Linkname
	}
	header, err := memberHeader(Header{Path: entry.Path, Kind: Kind(kind), Mode: entry.Mode, UID: entry.UID, GID: entry.GID, MtimeNS: entry.MtimeNS, Size: physicalSize, Target: target})
	if err != nil {
		return err
	}
	if physicalSize > math.MaxUint64-511 {
		return rejected("resource-limit", "payload padding overflow")
	}
	padded := (physicalSize + 511) / 512 * 512
	if padded > math.MaxUint64-uint64(len(header)) || padded+uint64(len(header)) > math.MaxUint64-w.raw {
		return rejected("resource-limit", "member size overflow")
	}
	mutated = true
	if len(w.pending) != 0 && padded+uint64(len(header)) > uint64(w.target-len(w.pending)) {
		if err = w.flush(); err != nil {
			return err
		}
	}
	if err = w.append(header); err != nil {
		return err
	}
	start := w.raw
	remaining := physicalSize
	for remaining != 0 {
		if err = ctx.Err(); err != nil {
			return err
		}
		at := len(w.pending)
		n := min(remaining, uint64(w.target-at))
		w.pending = w.pending[:at+int(n)]
		read, e := io.ReadFull(body, w.pending[at:])
		if e != nil {
			return e
		}
		remaining -= uint64(read)
		w.raw += uint64(read)
		if len(w.pending) == w.target {
			if err = w.flush(); err != nil {
				return err
			}
		}
	}
	if body != nil {
		var extra [1]byte
		n, e := io.ReadFull(body, extra[:])
		if n != 0 {
			return errors.New("zstar: body longer than declared size")
		}
		if e != nil && !errors.Is(e, io.EOF) {
			return e
		}
	}
	if err = w.append(make([]byte, padded-physicalSize)); err != nil {
		return err
	}
	if err = w.records.appendRecord([]any{cborRaw(entryRaw), start, physicalSize}); err != nil {
		return err
	}
	return nil
}

func stringsHasPathPrefix(path, directory string) bool { return strings.HasPrefix(path, directory+"/") }

func pathAncestors(path string) []string {
	var out []string
	for at := strings.IndexByte(path, '/'); at >= 0; {
		out = append(out, path[:at])
		next := strings.IndexByte(path[at+1:], '/')
		if next < 0 {
			break
		}
		at += next + 1
	}
	return out
}

// Close finalizes the archive and releases temporary storage. It never closes
// the destination. Earlier Add failures remain sticky.
func (w *EntryArchiveWriter) Close() (err error) {
	if w.closed {
		return w.err
	}
	w.closed = true
	defer func() { w.err = errors.Join(w.err, w.codec.Close(), w.frames.close(), w.records.close()); err = w.err }()
	if w.err != nil {
		return w.err
	}
	w.err = w.finish()
	return w.err
}

func (w *EntryArchiveWriter) finish() (err error) {
	if err := w.append(make([]byte, 1024)); err != nil {
		return err
	}
	if err := w.flush(); err != nil {
		return err
	}
	metadata, err := NewManifestWriter(w.output, w.encoded, w.options)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, metadata.encoder.Close()) }()
	var at, frameAt uint64
	var current [4]uint64
	loaded := false
	for at < w.records.size {
		record, err := w.records.readRecord(&at)
		if err != nil {
			return err
		}
		values, ok := record.([]any)
		if !ok || len(values) != 3 {
			return errors.New("zstar: invalid entry spool")
		}
		entry, err := parseEntry(values[0], w.encoded)
		if err != nil {
			return err
		}
		start, err := manifestUint(values[1])
		if err != nil {
			return err
		}
		size, err := manifestUint(values[2])
		if err != nil {
			return err
		}
		if entry.Type == TypeFile && !entry.External() && size != 0 {
			end := start + size
			for {
				if !loaded {
					if frameAt >= w.frames.size {
						return errors.New("zstar: missing payload frame")
					}
					raw, err := w.frames.read(frameAt, 32)
					if err != nil {
						return err
					}
					for i := range current {
						current[i] = binary.LittleEndian.Uint64(raw[i*8:])
					}
					frameAt += 32
					loaded = true
				}
				raw, decoded, encoded, encodedSize := current[0], current[1], current[2], current[3]
				if raw+decoded <= start {
					loaded = false
					continue
				}
				a, b := max(start, raw), min(end, raw+decoded)
				if b > a {
					entry.Extents = append(entry.Extents, Extent{Size: b - a, Source: ArchiveSource(encoded), EncodedSize: encodedSize, Offset: a - raw})
				}
				if len(entry.Extents) > manifestExtentsMax {
					return rejected("resource-limit", "entry extent maximum")
				}
				if raw+decoded >= end {
					break
				}
				loaded = false
			}
		}
		if err := metadata.AddEntry(entry); err != nil {
			return err
		}
	}
	return metadata.Close()
}

// Frames are independent and never exceed their target. A larger history
// window cannot improve their compression and only reserves codec memory.
func frameWindow(target int) int {
	return max(1024, 1<<bits.Len(uint(target-1)))
}
