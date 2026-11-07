package zstar

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/exp/collection/set"
)

// manifestWriter writes a deterministic manifest sequence in bounded compressed segments,
// then the existing terminal footer. It does not close the archive destination.
type manifestWriter struct {
	output                          io.Writer
	encoder                         *zstd.Encoder
	encodedBuffer                   []byte
	start, position, decoded, count uint64
	options                         manifestOptions
	entries                         manifestSpool
	external                        bool
	err                             error
	closed                          bool
}

// newManifestWriter creates a bounded manifest encoder for data ending at
// dataEnd. The caller owns the destination and must close the encoder.
func newManifestWriter(output io.Writer, dataEnd uint64, config ManifestWriterOptions) (*manifestWriter, error) {
	if output == nil || dataEnd == 0 {
		return nil, errors.New("zstar: invalid metadata destination")
	}
	options := config.defaults()
	if err := validateExtensions(options.RootExtensions); err != nil {
		return nil, err
	}
	for _, value := range options.RootExtensions {
		// Root extension values begin one level below the header map.
		if err := validateExtensionValue(value, 1); err != nil {
			return nil, err
		}
	}
	options.RootExtensions = cloneExtensions(options.RootExtensions)
	if options.BlockTarget < 512 {
		return nil, errors.New("zstar: block target must be at least 512 bytes")
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(defaultEncoderWindow), zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(manifestCompressionLevel(options.CompressionLevel)))
	if err != nil {
		return nil, err
	}
	return &manifestWriter{output: output, encoder: encoder, start: dataEnd, position: dataEnd, options: options, entries: manifestSpool{options: decodedSpoolOptions(options)}}, nil
}

// AddEntry snapshots and spools one entry. Paths may arrive in any order.
// Close sorts entries and rejects duplicates and missing ancestors.
func (w *manifestWriter) AddEntry(entry Entry) error {
	if w.closed {
		return errors.New("zstar: manifest writer closed")
	}
	if w.err != nil {
		return w.err
	}
	// Invalid caller input is rejected before any state changes and does not
	// poison the writer.
	if err := entry.validateFields(w.start); err != nil {
		return err
	}
	raw, err := encodeEntry(entry)
	if err != nil {
		return err
	}
	if err := w.options.Limits.entry(entry, len(raw)); err != nil {
		return err
	}
	if w.count == math.MaxUint64 {
		w.err = rejected(CategoryMalformedTOC, "entry count overflow")
		return w.err
	}
	w.count++
	w.external = w.external || entry.External()
	if err = w.entries.appendRecord(cborRaw(raw)); err != nil {
		w.err = err
		return err
	}
	return nil
}

func cborHead(major byte, n uint64) []byte {
	if n < 24 {
		return []byte{major<<5 | byte(n)}
	}
	if n <= 0xff {
		return []byte{major<<5 | 24, byte(n)}
	}
	if n <= 0xffff {
		return []byte{major<<5 | 25, byte(n >> 8), byte(n)}
	}
	if n <= 0xffffffff {
		return []byte{major<<5 | 26, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
	}
	out := make([]byte, 9)
	out[0] = major<<5 | 27
	binary.BigEndian.PutUint64(out[1:], n)
	return out
}

func (w *manifestWriter) writeSegment(raw []byte) error {
	if len(raw) == 0 {
		return rejected(CategoryMalformedTOC, "metadata segment maximum")
	}
	if uint64(len(raw)) > w.options.MaxDecodedBytes-w.decoded {
		return rejected(CategoryResourceLimit, "decoded manifest size")
	}
	if err := checkLimit(uint64(len(raw)), w.options.Limits.MaxFrameDecodedBytes, "decoded metadata frame size"); err != nil {
		return err
	}
	w.encodedBuffer = w.encoder.EncodeAll(raw, w.encodedBuffer[:0])
	encoded := w.encodedBuffer
	if err := w.options.Limits.frame(encoded); err != nil {
		return err
	}
	if uint64(len(encoded)) > math.MaxUint32 {
		return rejected(CategoryMalformedTOC, "metadata wrapper length")
	}
	size := uint64(len(encoded)) + 8
	if size > w.options.MaxEncodedBytes-(w.position-w.start) || size > math.MaxUint64-w.position {
		return rejected(CategoryResourceLimit, "encoded manifest size")
	}
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header, 0x184D2A5E)
	binary.LittleEndian.PutUint32(header[4:], uint32(len(encoded)))
	if err := writeExact(w.output, header); err != nil {
		return err
	}
	if err := writeExact(w.output, encoded); err != nil {
		return err
	}
	w.position += size
	w.decoded += uint64(len(raw))
	return nil
}

func (w *manifestWriter) writeDocument() (err error) {
	type field struct {
		keyBytes []byte
		value    []byte
	}
	var fields []field
	add := func(key string, value any) error {
		b, err := manifestEncode(value)
		if err == nil {
			fields = append(fields, field{keyBytes: mustManifestKey(key), value: b})
		}
		return err
	}
	if err := add("version", uint64(1)); err != nil {
		return err
	}
	features := []string{}
	if w.external {
		features = append(features, "external-content")
	}
	if err := add("requires", features); err != nil {
		return err
	}
	for key, value := range w.options.RootExtensions {
		if !strings.HasPrefix(key, "x-") {
			return rejected(CategoryMalformedTOC, "root extension key")
		}
		if err := add(key, value); err != nil {
			return err
		}
	}
	slices.SortFunc(fields, func(a, b field) int {
		if order := cmp.Compare(len(a.keyBytes), len(b.keyBytes)); order != 0 {
			return order
		}
		return bytes.Compare(a.keyBytes, b.keyBytes)
	})
	var pending []byte
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		err := w.writeSegment(pending)
		pending = pending[:0]
		return err
	}
	write := func(data []byte) error {
		for len(data) > 0 {
			room := w.options.BlockTarget - len(pending)
			n := min(room, len(data))
			pending = append(pending, data[:n]...)
			data = data[n:]
			if len(pending) == w.options.BlockTarget {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	sorted, err := sortManifestRecords(&w.entries)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, sorted.close()) }()
	previous := ""
	directories := set.Of[string]()
	for at := uint64(0); at < sorted.size; {
		value, e := sorted.readRecord(&at)
		if e != nil {
			return e
		}
		entry, e := parseEntry(value, w.start)
		if e != nil {
			return e
		}
		if previous != "" && previous >= entry.Path {
			return rejected(CategoryMalformedTOC, "duplicate entry path")
		}
		for d := range directories {
			if !stringsHasPathPrefix(entry.Path, d) && entry.Path > d+"/" {
				directories.Delete(d)
			}
		}
		for _, parent := range pathAncestors(entry.Path) {
			if !directories.Contains(parent) {
				return rejected(CategoryMalformedTOC, "missing ancestor directory")
			}
		}
		if entry.Type == TypeDirectory {
			directories.Insert(entry.Path)
		}
		previous = entry.Path
	}
	if err = write(cborHead(5, uint64(len(fields)))); err != nil {
		return err
	}
	for _, field := range fields {
		if err = write(field.keyBytes); err != nil {
			return err
		}
		if err = write(field.value); err != nil {
			return err
		}
	}
	for at := uint64(0); at < sorted.size; {
		value, e := sorted.readRecord(&at)
		if e != nil {
			return e
		}
		raw, e := manifestEncode(value)
		if e != nil {
			return e
		}
		if e = write(raw); e != nil {
			return e
		}
	}
	return flush()
}

// Close sorts and validates entries, writes metadata and the footer, and
// releases spools. It leaves the destination open; failures are sticky.
func (w *manifestWriter) Close() (err error) {
	if w.closed {
		return w.err
	}
	w.closed = true
	defer func() {
		w.encodedBuffer = nil
		err = errors.Join(err, w.encoder.Close(), w.entries.close())
		w.err = err
	}()
	if w.err != nil {
		return w.err
	}
	if err = w.writeDocument(); err != nil {
		return err
	}
	footer := make([]byte, FooterSize)
	binary.LittleEndian.PutUint32(footer, 0x184D2A5F)
	binary.LittleEndian.PutUint32(footer[4:], 20)
	binary.LittleEndian.PutUint64(footer[8:], w.position-w.start)
	binary.LittleEndian.PutUint64(footer[16:], w.decoded)
	copy(footer[24:], "Z*01")
	return writeExact(w.output, footer)
}

func manifestCompressionLevel(level int) zstd.EncoderLevel {
	if level == 0 {
		return zstd.SpeedDefault
	}
	return zstd.EncoderLevelFromZstd(level)
}

func mustManifestKey(key string) []byte {
	b, err := manifestEncode(key)
	if err != nil {
		panic(err)
	}
	return b
}
