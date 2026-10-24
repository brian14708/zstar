package zstar

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// ManifestWriter writes a deterministic manifest sequence in bounded compressed segments,
// then the existing terminal footer. It does not close the archive destination.
type ManifestWriter struct {
	output                          io.Writer
	encoder                         *zstd.Encoder
	start, position, decoded, count uint64
	hash                            hash.Hash
	options                         ManifestOptions
	entries                         manifestSpool
	external                        bool
	err                             error
	closed                          bool
}

// NewManifestWriter creates a bounded manifest encoder for data ending at
// dataEnd. The caller owns the destination and must close the encoder.
func NewManifestWriter(output io.Writer, dataEnd uint64, options ManifestOptions) (*ManifestWriter, error) {
	if output == nil || dataEnd == 0 {
		return nil, errors.New("zstar: invalid metadata destination")
	}
	options = options.defaults()
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
	if options.BlockTarget < 512 || options.BlockTarget > 1<<20 {
		return nil, errors.New("zstar: block target must be in [512, 1 MiB]")
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(maxFrameSize), zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(manifestCompressionLevel(options.CompressionLevel)))
	if err != nil {
		return nil, err
	}
	return &ManifestWriter{output: output, encoder: encoder, start: dataEnd, position: dataEnd, hash: sha256.New(), options: options, entries: manifestSpool{options: decodedSpoolOptions(options)}}, nil
}

// AddEntry snapshots and spools one entry. Paths may arrive in any order.
// Close sorts entries and rejects duplicates and missing ancestors.
func (w *ManifestWriter) AddEntry(entry Entry) error {
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
	if len(raw) > manifestEntryMax {
		return rejected("malformed-toc", "entry maximum")
	}
	if w.count == math.MaxUint64 {
		w.err = rejected("malformed-toc", "entry count overflow")
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

func (w *ManifestWriter) writeSegment(raw []byte) error {
	if len(raw) == 0 || len(raw) > manifestBlockMax {
		return rejected("malformed-toc", "metadata segment maximum")
	}
	if uint64(len(raw)) > w.options.MaxDecodedBytes-w.decoded {
		return rejected("resource-limit", "decoded manifest size")
	}
	encoded := w.encoder.EncodeAll(raw, nil)
	if len(encoded) > 17<<20 {
		return rejected("malformed-toc", "encoded metadata segment maximum")
	}
	size := uint64(len(encoded)) + 8
	if size > w.options.MaxEncodedBytes-(w.position-w.start) || size > math.MaxUint64-w.position {
		return rejected("resource-limit", "encoded manifest size")
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
	_, _ = w.hash.Write(raw)
	w.position += size
	w.decoded += uint64(len(raw))
	return nil
}

func (w *ManifestWriter) writeDocument() (err error) {
	var fields []struct {
		key      string
		keyBytes []byte
		value    []byte
	}
	add := func(key string, value any) error {
		b, err := manifestEncode(value)
		if err == nil {
			fields = append(fields, struct {
				key      string
				keyBytes []byte
				value    []byte
			}{key: key, keyBytes: mustManifestKey(key), value: b})
		}
		return err
	}
	if err := add("version", uint64(1)); err != nil {
		return err
	}
	features := []any{}
	if w.external {
		features = append(features, "external-content")
	}
	if err := add("required_features", features); err != nil {
		return err
	}
	for key, value := range w.options.RootExtensions {
		if !strings.HasPrefix(key, "x-") {
			return rejected("malformed-toc", "root extension key")
		}
		if err := add(key, value); err != nil {
			return err
		}
	}
	sort.Slice(fields, func(i, j int) bool {
		a, b := fields[i].keyBytes, fields[j].keyBytes
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return bytes.Compare(a, b) < 0
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
	directories := map[string]bool{}
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
			return rejected("malformed-toc", "duplicate entry path")
		}
		for d := range directories {
			if !stringsHasPathPrefix(entry.Path, d) && entry.Path > d+"/" {
				delete(directories, d)
			}
		}
		for _, parent := range pathAncestors(entry.Path) {
			if !directories[parent] {
				return rejected("malformed-toc", "missing ancestor directory")
			}
		}
		if entry.Type == TypeDirectory {
			directories[entry.Path] = true
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
func (w *ManifestWriter) Close() (err error) {
	if w.closed {
		return w.err
	}
	w.closed = true
	defer func() { err = errors.Join(err, w.encoder.Close(), w.entries.close()); w.err = err }()
	if w.err != nil {
		return w.err
	}
	if err = w.writeDocument(); err != nil {
		return err
	}
	footer := make([]byte, 80)
	binary.LittleEndian.PutUint32(footer, 0x184D2A5F)
	binary.LittleEndian.PutUint32(footer[4:], 72)
	copy(footer[8:], "ZSTAR001")
	binary.LittleEndian.PutUint32(footer[16:], 1)
	binary.LittleEndian.PutUint64(footer[24:], w.start)
	binary.LittleEndian.PutUint64(footer[32:], w.position-w.start)
	binary.LittleEndian.PutUint64(footer[40:], w.decoded)
	copy(footer[48:], w.hash.Sum(nil))
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
