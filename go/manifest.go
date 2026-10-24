package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
)

const (
	manifestBlockMax   = 16 << 20
	manifestEntryMax   = 8 << 20
	manifestExtentsMax = 65536
	// manifestHeaderMax bounds the root header map. The spec sets no header
	// size, only the 8 MiB entry bound, so this is a deliberate local policy
	// several metadata segments, so large root extensions still open.
	manifestHeaderMax = 64 << 20
	// Default budget for retained []Entry metadata.
	manifestRetainedDefault = 1 << 30
)

// Extent selects decoded bytes from an archive frame or an external object.
// EncodedSize is only a hint for objects.
type Extent struct {
	Size        uint64
	Source      ExtentSource
	EncodedSize uint64
	Offset      uint64
}

func (e Extent) value() []any {
	v := []any{e.Size, e.Source.value(), e.EncodedSize}
	if e.Offset != 0 {
		v = append(v, e.Offset)
	}
	return v
}

func (e Extent) validate(dataEnd uint64) error {
	if e.Size == 0 || e.Size > maxFrameSize || e.EncodedSize == 0 || e.Offset > maxFrameSize || e.Size > maxFrameSize-e.Offset {
		return rejected("malformed-toc", "extent bounds")
	}
	if s, archive := e.Source.ArchiveOffset(); archive {
		if e.EncodedSize > 17<<20 || s > dataEnd || e.EncodedSize > dataEnd-s {
			return rejected("malformed-toc", "archive extent bounds")
		}
	} else {
		if _, err := ParseContentAddress(e.Source.address); err != nil {
			return err
		}
	}
	return nil
}

// ParseContentAddress accepts a canonical lowercase SHA-256 content address.
func ParseContentAddress(s string) ([32]byte, error) {
	var out [32]byte
	if len(s) != 71 || !strings.HasPrefix(s, "sha256:") {
		return out, rejected("malformed-toc", "object address")
	}
	for _, c := range s[7:] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return out, rejected("malformed-toc", "object address")
		}
	}
	b, err := hex.DecodeString(s[7:])
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func parseExtent(value any, dataEnd uint64) (Extent, error) {
	var e Extent
	v, ok := value.([]any)
	if !ok || len(v) < 3 || len(v) > 4 {
		return e, rejected("malformed-toc", "extent tuple")
	}
	var err error
	e.Size, err = manifestUint(v[0])
	if err != nil {
		return e, err
	}
	switch source := v[1].(type) {
	case uint64:
		e.Source = ArchiveSource(source)
	case string:
		e.Source, err = ParseObjectSource(source)
		if err != nil {
			return e, err
		}
	default:
		return e, rejected("malformed-toc", "extent source")
	}
	e.EncodedSize, err = manifestUint(v[2])
	if err != nil {
		return e, err
	}
	if len(v) == 4 {
		e.Offset, err = manifestUint(v[3])
		if err != nil || e.Offset == 0 {
			return e, rejected("malformed-toc", "explicit default offset")
		}
	}
	return e, e.validate(dataEnd)
}

// Entry contains effective metadata, never raw PAX records or a file hash.
type Entry struct {
	Path           string
	Type           EntryType
	Mode, UID, GID uint64
	MtimeNS        int64
	Extents        []Extent
	Linkname       *string
	Attributes     Extensions
	// Cumulative logical ends for immutable reader-owned entries only.
	extentEnds []uint64
}

func (e Entry) indexedSize() (uint64, error) {
	if len(e.extentEnds) != 0 {
		return e.extentEnds[len(e.extentEnds)-1], nil
	}
	return e.Size()
}

// Size returns the logical length of the extents, rejecting uint64 overflow.
func (e Entry) Size() (uint64, error) {
	var n uint64
	for _, x := range e.Extents {
		if x.Size > math.MaxUint64-n {
			return 0, rejected("malformed-toc", "file size overflow")
		}
		n += x.Size
	}
	return n, nil
}

// External reports whether the member uses external object extents.
func (e Entry) External() bool {
	if len(e.Extents) == 0 {
		return false
	}
	_, ok := e.Extents[0].Source.ContentAddress()
	return ok
}

func (e Entry) value() []any {
	extents := make([]any, len(e.Extents))
	for i, x := range e.Extents {
		extents[i] = x.value()
	}
	v := []any{e.Path, uint64(e.Type), e.Mode, e.UID, e.GID, e.MtimeNS, extents}
	attrs := make(Extensions, len(e.Attributes)+1)
	maps.Copy(attrs, e.Attributes)
	if e.Linkname != nil {
		attrs["linkname"] = *e.Linkname
	}
	if len(attrs) != 0 {
		v = append(v, attrs)
	}
	return v
}

func (e Entry) validateFields(dataEnd uint64) error {
	if !validPath(e.Path) || e.Mode > 4095 || len(e.Extents) > manifestExtentsMax {
		return rejected("malformed-toc", "entry metadata")
	}
	if e.Type != TypeFile && e.Type != TypeSymlink && e.Type != TypeDirectory {
		return rejected("malformed-toc", "entry type")
	}
	if (e.Type == TypeSymlink) != (e.Linkname != nil) || e.Type != TypeFile && len(e.Extents) != 0 {
		return rejected("malformed-toc", "entry fields")
	}
	if e.Linkname != nil && (len(*e.Linkname) == 0 || len(*e.Linkname) > 4096 || strings.ContainsRune(*e.Linkname, 0) || !utf8.ValidString(*e.Linkname)) {
		return rejected("malformed-toc", "linkname")
	}
	if err := validateExtensions(e.Attributes); err != nil {
		return categorized("malformed-toc", err)
	}
	for _, x := range e.Extents {
		if err := x.validate(dataEnd); err != nil {
			return err
		}
		_, external := x.Source.ContentAddress()
		if external != e.External() {
			return rejected("malformed-toc", "mixed extent sources")
		}
	}
	if _, err := e.Size(); err != nil {
		return err
	}
	return nil
}

func (e Entry) validate(dataEnd uint64) error {
	if err := e.validateFields(dataEnd); err != nil {
		return err
	}
	raw, err := encodeEntry(e)
	if err != nil {
		return err
	}
	if len(raw) > manifestEntryMax {
		return rejected("malformed-toc", "entry maximum")
	}
	return nil
}

func parseEntry(value any, dataEnd uint64) (Entry, error) {
	var e Entry
	v, ok := value.([]any)
	if !ok || len(v) < 7 || len(v) > 8 {
		return e, rejected("malformed-toc", "entry tuple")
	}
	e.Path, ok = v[0].(string)
	if !ok {
		return e, rejected("malformed-toc", "entry path")
	}
	var entryType uint64
	ns := []*uint64{&entryType, &e.Mode, &e.UID, &e.GID}
	for i, n := range ns {
		var err error
		*n, err = manifestUint(v[i+1])
		if err != nil {
			return e, err
		}
	}
	e.Type = EntryType(entryType)
	switch n := v[5].(type) {
	case uint64:
		if n > math.MaxInt64 {
			return e, rejected("malformed-toc", "mtime range")
		}
		e.MtimeNS = int64(n)
	case int64:
		e.MtimeNS = n
	default:
		return e, rejected("malformed-toc", "mtime")
	}
	values, ok := v[6].([]any)
	if !ok || len(values) > manifestExtentsMax {
		return e, rejected("malformed-toc", "extent list")
	}
	e.Extents = make([]Extent, 0, len(values))
	for _, v := range values {
		x, err := parseExtent(v, dataEnd)
		if err != nil {
			return e, err
		}
		e.Extents = append(e.Extents, x)
	}
	e.Attributes = Extensions{}
	if len(v) == 8 {
		m, ok := v[7].(map[any]any)
		if !ok || len(m) == 0 {
			return e, rejected("malformed-toc", "attributes")
		}
		for key, value := range m {
			s, ok := key.(string)
			if !ok {
				return e, rejected("malformed-toc", "attribute key")
			}
			switch {
			case s == "linkname":
				s, ok := value.(string)
				if !ok {
					return e, rejected("malformed-toc", "linkname")
				}
				e.Linkname = &s
			case strings.HasPrefix(s, "x-"):
				e.Attributes[s] = value
			default:
				return e, rejected("malformed-toc", "unknown attribute")
			}
		}
	}
	return e, e.validate(dataEnd)
}

// entryRetainedCost conservatively estimates heap retained by a parsed entry.
func entryRetainedCost(e Entry, encoded int) uint64 {
	cost := uint64(unsafe.Sizeof(e)) + uint64(len(e.Path)) + uint64(cap(e.Extents))*uint64(unsafe.Sizeof(Extent{}))
	cost += uint64(len(e.Extents)) * 8
	for _, x := range e.Extents {
		cost += uint64(len(x.Source.address))
	}
	if e.Linkname != nil {
		cost += uint64(len(*e.Linkname)) + 16
	}
	if len(e.Attributes) != 0 {
		// Decoded maps/interfaces cost several times their wire size.
		cost += 8 * uint64(encoded)
	}
	return cost
}

func manifestUint(v any) (uint64, error) {
	n, ok := v.(uint64)
	if !ok {
		return 0, rejected("malformed-toc", "uint64")
	}
	return n, nil
}

// Codec modes are immutable and safe to reuse across readers and workers.
var (
	manifestEncoder, manifestEncoderError = cbor.CoreDetEncOptions().EncMode()
	manifestDecoder, manifestDecoderError = (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden, MaxNestedLevels: 64, MaxArrayElements: 2147483647, MaxMapPairs: 1_000_000}).DecMode()
)

func manifestEncode(value any) ([]byte, error) {
	if manifestEncoderError != nil {
		return nil, manifestEncoderError
	}
	return manifestEncoder.Marshal(value)
}

// ManifestOptions controls resource budgets. Zero values select documented
// defaults, except ContentCacheBytes, which disables the lower-level manifest
// cache at zero.
//
// Reader fields (OpenManifest): MemoryBytes, MaxEncodedBytes, MaxDecodedBytes,
// MaxReadBytes, MaxRetainedEntryBytes, MaxEntries, ContentCacheBytes,
// ConcurrentChunks, ReadAheadBytes, DisableDisk, SpoolBudget, CreateSpillStore.
// Writer fields: CompressionLevel, RootExtensions, BlockTarget, plus the
// spool fields (MemoryBytes, DisableDisk, SpoolBudget, CreateSpillStore).
// Fields outside the applicable side are ignored.
type ManifestOptions struct {
	MemoryBytes, MaxEncodedBytes, MaxDecodedBytes uint64
	MaxReadBytes                                  uint64
	// MaxRetainedEntryBytes bounds the estimated heap retained by parsed
	// entry metadata, including spare entry-slice capacity (default 1 GiB). Reader only. Exceeding it, or MaxEntries,
	// fails OpenManifest with a "resource-limit" error.
	MaxRetainedEntryBytes uint64
	// MaxEntries bounds the number of entries; zero means no separate count cap.
	// Reader only.
	MaxEntries uint64
	// ContentCacheBytes is a decoded-content budget, disabled by default.
	ContentCacheBytes uint64
	// ConcurrentChunks bounds fetch/decode/hash work, including canceled calls.
	ConcurrentChunks int
	// ReadAheadBytes bounds sequential transport buffers (default 1 MiB).
	ReadAheadBytes uint64
	// Writer settings; zero compression level selects the codec default.
	CompressionLevel int
	RootExtensions   Extensions
	// BlockTarget is a writer target in [512, 1 MiB], default 64 KiB.
	BlockTarget int
	DisableDisk bool
	// SpoolBudget is shared by all derived spools. Nil creates a 4 GiB quota
	// for this reader/writer; an explicit pointer can share it across owners.
	SpoolBudget *SpoolBudget
	// CreateSpillStore supplies an empty append destination with independent
	// ReadAt support. Ownership transfers to the SDK until Close.
	CreateSpillStore func() (ManifestSpillStore, error)
}

// ManifestSpillStore is an empty append/read-at store owned by the SDK.
// Appended bytes must remain immutable; Close releases the store.
type ManifestSpillStore interface {
	io.Writer
	io.ReaderAt
	io.Closer
}

func (o ManifestOptions) defaults() ManifestOptions {
	if o.SpoolBudget == nil {
		o.SpoolBudget = NewSpoolBudget(4 << 30)
	}
	if o.ReadAheadBytes == 0 {
		o.ReadAheadBytes = 1 << 20
	}
	if o.ConcurrentChunks == 0 {
		o.ConcurrentChunks = 8
	}
	if o.MemoryBytes == 0 {
		o.MemoryBytes = 8 << 20
	}
	if o.MaxEncodedBytes == 0 {
		o.MaxEncodedBytes = 4 << 30
	}
	if o.MaxDecodedBytes == 0 {
		o.MaxDecodedBytes = 4 << 30
	}
	if o.MaxRetainedEntryBytes == 0 {
		o.MaxRetainedEntryBytes = manifestRetainedDefault
	}
	if o.MaxReadBytes == 0 {
		o.MaxReadBytes = 64 << 20
	}
	if o.BlockTarget == 0 {
		o.BlockTarget = 64 << 10
	}
	return o
}

type manifestSpool struct {
	memory   []byte
	file     *os.File
	custom   ManifestSpillStore
	size     uint64
	charged  uint64
	options  ManifestOptions
	closed   bool
	writeErr error
}

func (s *manifestSpool) append(b []byte) error {
	return s.appendParts(b, nil)
}

func (s *manifestSpool) appendParts(first, second []byte) (err error) {
	if s.closed {
		return errors.New("zstar: closed manifest spool")
	}
	if s.writeErr != nil {
		return s.writeErr
	}
	added := uint64(len(first)) + uint64(len(second))
	if s.size > s.options.MaxEncodedBytes || added > s.options.MaxEncodedBytes-s.size {
		return rejected("resource-limit", "manifest spool size")
	}
	end := s.size + added
	charge := end - min(end, s.charged)
	if err := s.options.SpoolBudget.reserve(charge); err != nil {
		return err
	}
	committed := false
	defer func() {
		s.options.SpoolBudget.release(charge)
		if committed && err != nil {
			s.writeErr = err
		}
	}()
	if s.file == nil && s.custom == nil && end > s.options.MemoryBytes {
		if s.options.CreateSpillStore != nil {
			store, err := s.options.CreateSpillStore()
			if err != nil {
				return err
			}
			if store == nil {
				return errors.New("zstar: nil spill store")
			}
			var probe [1]byte
			if n, probeErr := store.ReadAt(probe[:], 0); n != 0 || !errors.Is(probeErr, io.EOF) {
				return errors.Join(rejected("unsupported-operation", "spill store must be empty"), probeErr, store.Close())
			}
			if err = writeExact(store, s.memory); err != nil {
				return errors.Join(err, store.Close())
			}
			s.custom, s.memory = store, nil
		} else {
			if s.options.DisableDisk {
				return rejected("resource-limit", "manifest memory; supply disk storage")
			}
			f, err := os.CreateTemp("", "zstar-manifest-*")
			if err != nil {
				return err
			}
			if err = writeExact(f, s.memory); err != nil {
				return errors.Join(err, f.Close(), os.Remove(f.Name()))
			}
			s.file = f
			s.memory = nil
		}
	}
	// Retain the full high-water charge if a store partially writes then fails.
	// Close releases it, even when store cleanup itself reports a failure.
	s.charged += charge
	charge = 0
	committed = true
	switch {
	case s.custom != nil:
		if err := writeExact(s.custom, first); err != nil {
			return err
		}
		if len(second) != 0 {
			if err := writeExact(s.custom, second); err != nil {
				return err
			}
		}
	case s.file == nil:
		s.memory = append(s.memory, first...)
		s.memory = append(s.memory, second...)
	default:
		if _, err := s.file.Seek(int64(s.size), io.SeekStart); err != nil {
			return err
		}
		if err := writeExact(s.file, first); err != nil {
			return err
		}
		if len(second) != 0 {
			if err := writeExact(s.file, second); err != nil {
				return err
			}
		}
	}
	s.size = end
	return nil
}

func (s *manifestSpool) read(at, n uint64) ([]byte, error) {
	if s.closed {
		return nil, errors.New("zstar: closed manifest spool")
	}
	if at > s.size || n > s.size-at {
		return nil, rejected("malformed-toc", "spool range")
	}
	out := make([]byte, int(n))
	if s.custom != nil {
		n, err := s.custom.ReadAt(out, int64(at))
		if err == nil && n != len(out) {
			err = io.ErrUnexpectedEOF
		}
		return out, err
	}
	if s.file == nil {
		copy(out, s.memory[at:at+n])
		return out, nil
	}
	_, err := s.file.ReadAt(out, int64(at))
	return out, err
}

func (s *manifestSpool) appendRecord(value any) error {
	raw, err := manifestEncode(value)
	if err != nil {
		return err
	}
	var h [4]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(raw)))
	return s.appendParts(h[:], raw)
}

func (s *manifestSpool) readRecord(at *uint64) (any, error) {
	if *at == s.size {
		return nil, io.EOF
	}
	h, e := s.read(*at, 4)
	if e != nil {
		return nil, e
	}
	*at += 4
	n := uint64(binary.LittleEndian.Uint32(h))
	raw, e := s.read(*at, n)
	if e != nil {
		return nil, e
	}
	*at += n
	var v any
	if e = manifestDecoder.Unmarshal(raw, &v); e != nil {
		return nil, e
	}
	return v, nil
}

type cborRaw []byte

func (v cborRaw) MarshalCBOR() ([]byte, error) { return []byte(v), nil }
func (s *manifestSpool) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	defer func() {
		s.options.SpoolBudget.release(s.charged)
		s.charged = 0
	}()
	s.memory = nil
	if s.custom != nil {
		store := s.custom
		s.custom = nil
		return store.Close()
	}
	if s.file == nil {
		return nil
	}
	name := s.file.Name()
	err := s.file.Close()
	removeErr := os.Remove(name)
	s.file = nil
	return errors.Join(err, removeErr)
}

// Manifest authenticates and validates the complete sequence before exposing entries.
type Manifest struct {
	mu               sync.Mutex
	closed           bool
	spool            manifestSpool
	start, count     uint64
	entries          []Entry
	requiredFeatures []string
	rootExtensions   Extensions
	source           RangeReader
	maxRead          uint64
	readAhead        uint64
	content          *manifestContentState
}

// OpenManifest authenticates and validates metadata using the supplied budgets.
// The caller owns the immutable source, which must support concurrent reads.
// The opening context is used only during open; subsequent operations supply
// their own contexts. Close releases SDK storage without closing the source.
func OpenManifest(ctx context.Context, source RangeReader, options ManifestOptions) (result *Manifest, err error) {
	if source == nil {
		return nil, errors.New("zstar: nil source")
	}
	options = options.defaults()
	if options.ReadAheadBytes > 17<<20 {
		return nil, rejected("resource-limit", "read-ahead exceeds 17 MiB")
	}
	if options.ConcurrentChunks < 1 || options.ConcurrentChunks > 256 {
		return nil, errors.New("zstar: concurrent chunks must be in [1, 256]")
	}
	if options.MaxReadBytes > uint64(^uint(0)>>1) || options.MaxEncodedBytes > math.MaxInt64 {
		return nil, errors.New("zstar: manifest budget exceeds address space")
	}
	n, err := source.Size(ctx)
	if err != nil {
		return nil, err
	}
	if n < 80 {
		return nil, rejected("not-zstar", "source size")
	}
	var footer Footer
	var prefetched []byte
	if hinted, ok := source.(ManifestRangeSource); ok {
		offset, size, present, hintErr := hinted.ManifestRange(ctx)
		if hintErr != nil {
			return nil, hintErr
		}
		if present && offset > 0 && offset <= uint64(n-80) && size > 8 &&
			size == uint64(n-80)-offset && size <= options.MaxEncodedBytes &&
			uint64(n)-offset <= options.ReadAheadBytes {
			raw, readErr := source.ReadRange(ctx, int64(offset), n-int64(offset))
			if readErr != nil {
				return nil, readErr
			}
			if uint64(len(raw)) != uint64(n)-offset {
				return nil, rejected("malformed-toc", "short hinted metadata range")
			}
			candidate, parseErr := ParseFooter(raw[len(raw)-80:])
			if parseErr == nil && candidate.ManifestOffset == offset && candidate.ManifestEncodedSize == size {
				footer, prefetched = candidate, raw[:len(raw)-80]
			}
		}
	}
	if prefetched == nil {
		fb, readErr := source.ReadRange(ctx, n-80, 80)
		if readErr != nil {
			return nil, readErr
		}
		footer, err = ParseFooter(fb)
		if err != nil {
			return nil, err
		}
	}
	if footer.ManifestOffset > uint64(n-80) || footer.ManifestEncodedSize != uint64(n-80)-footer.ManifestOffset {
		return nil, rejected("malformed-footer", "metadata coverage")
	}
	if footer.ManifestEncodedSize > options.MaxEncodedBytes || footer.ManifestDecodedSize > options.MaxDecodedBytes {
		return nil, rejected("resource-limit", "manifest size")
	}
	m := &Manifest{spool: manifestSpool{options: decodedSpoolOptions(options)}, start: footer.ManifestOffset, source: source, maxRead: options.MaxReadBytes, readAhead: options.ReadAheadBytes}
	m.content = newManifestContentState(options, source)
	m.content.metadata = footer.ManifestSHA256
	defer func() {
		if err != nil {
			m.content.close()
			err = errors.Join(err, m.spool.close())
		}
	}()
	decoder := frameDecoder{}
	defer decoder.close()
	hash := sha256.New()
	var decoded uint64
	if footer.ManifestDecodedSize > uint64(^uint(0)>>1) {
		return nil, rejected("resource-limit", "manifest exceeds address space")
	}
	end := footer.ManifestOffset + footer.ManifestEncodedSize
	input := manifestRangeStream{ctx: ctx, source: source, at: footer.ManifestOffset, end: end, readAhead: options.ReadAheadBytes, buffer: prefetched}
	for at := footer.ManifestOffset; at < end; {
		if end-at < 8 {
			return nil, rejected("malformed-toc", "metadata header")
		}
		header, e := input.take(8)
		if e != nil {
			return nil, e
		}
		if len(header) != 8 || binary.LittleEndian.Uint32(header[:4]) != 0x184D2A5E {
			return nil, rejected("malformed-toc", "metadata wrapper")
		}
		size := uint64(binary.LittleEndian.Uint32(header[4:]))
		if size == 0 || size > 17<<20 || size > end-at-8 {
			return nil, rejected("malformed-toc", "metadata frame size")
		}
		encoded, e := input.take(size)
		if e != nil {
			return nil, e
		}
		if uint64(len(encoded)) != size {
			return nil, rejected("malformed-toc", "short metadata")
		}
		raw, e := decoder.decode(encoded, 0, manifestBlockMax, 17<<20)
		if e != nil {
			return nil, categorized("malformed-toc", e)
		}
		if len(raw) == 0 || uint64(len(raw)) > footer.ManifestDecodedSize-decoded {
			return nil, rejected("malformed-toc", "decoded metadata size")
		}
		decoded += uint64(len(raw))
		hash.Write(raw)
		if e = m.spool.append(raw); e != nil {
			return nil, e
		}
		at += 8 + size
	}
	if decoded != footer.ManifestDecodedSize {
		return nil, rejected("malformed-toc", "decoded manifest length")
	}
	if !bytes.Equal(hash.Sum(nil), footer.ManifestSHA256[:]) {
		return nil, rejected("integrity-failure", "manifest hash")
	}
	sequence := sequenceReader{spool: &m.spool, limit: manifestHeaderMax}
	value, e := sequence.next()
	if e != nil {
		return nil, categorized("malformed-toc", e)
	}
	root, e := schemaMap(value, []string{"version", "required_features"}, nil)
	if e != nil {
		return nil, categorized("malformed-toc", e)
	}
	m.rootExtensions = Extensions{}
	for key, value := range root {
		if key, ok := key.(string); ok && strings.HasPrefix(key, "x-") {
			m.rootExtensions[key] = value
		}
	}
	if e = validateExtensions(m.rootExtensions); e != nil {
		return nil, categorized("malformed-toc", e)
	}
	version, e := manifestUint(root["version"])
	if e != nil {
		return nil, e
	}
	if version != 1 {
		return nil, rejected("unsupported-version", "manifest root")
	}
	requiredFeatures, ok := root["required_features"].([]any)
	if !ok {
		return nil, rejected("malformed-toc", "required_features")
	}
	for _, v := range requiredFeatures {
		s, ok := v.(string)
		if !ok || !validFeature(s) || len(m.requiredFeatures) > 0 && m.requiredFeatures[len(m.requiredFeatures)-1] >= s {
			return nil, rejected("malformed-toc", "required_features")
		}
		if s != "external-content" {
			return nil, rejected("unsupported-feature", "required feature")
		}
		m.requiredFeatures = append(m.requiredFeatures, s)
	}
	sequence.limit = manifestEntryMax
	var retained uint64
	for {
		value, e := sequence.next()
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return nil, e
		}
		if options.MaxEntries != 0 && uint64(len(m.entries)) >= options.MaxEntries {
			return nil, rejected("resource-limit", "manifest entry count")
		}
		entry, e := parseEntry(value, m.start)
		if e != nil {
			return nil, e
		}
		if len(m.entries) > 0 && m.entries[len(m.entries)-1].Path >= entry.Path {
			return nil, rejected("malformed-toc", "entry order")
		}
		slotBytes := uint64(unsafe.Sizeof(Entry{}))
		cost := entryRetainedCost(entry, sequence.lastSize) - slotBytes
		if cost > options.MaxRetainedEntryBytes-min(retained, options.MaxRetainedEntryBytes) {
			return nil, rejected("resource-limit", "manifest retained entry memory")
		}
		retained += cost
		slots := (options.MaxRetainedEntryBytes - retained) / slotBytes
		if slots <= uint64(len(m.entries)) {
			return nil, rejected("resource-limit", "manifest retained entry memory")
		}
		if uint64(cap(m.entries)) > slots {
			compact := make([]Entry, len(m.entries))
			copy(compact, m.entries)
			m.entries = compact
		}
		if len(m.entries) == cap(m.entries) {
			// Reserve half the spare-slot budget for subsequent owned metadata
			// so tight limits do not force compaction on every few entries.
			minimum := uint64(len(m.entries)) + 1
			capacityLimit := minimum + (slots-minimum)/2
			target := min(max(uint64(cap(m.entries))*2, 16), capacityLimit)
			if options.MaxEntries != 0 {
				target = min(target, options.MaxEntries)
			}
			target = min(target, uint64(^uint(0)>>1)/slotBytes)
			if target <= uint64(len(m.entries)) {
				return nil, rejected("resource-limit", "manifest index address space")
			}
			grown := make([]Entry, len(m.entries), int(target))
			copy(grown, m.entries)
			m.entries = grown
		}
		if len(entry.Extents) != 0 {
			entry.extentEnds = make([]uint64, len(entry.Extents))
			var end uint64
			for i, extent := range entry.Extents {
				end += extent.Size
				entry.extentEnds[i] = end
			}
		}
		m.entries = append(m.entries, entry)
	}
	m.count = uint64(len(m.entries))
	if e = m.validateEntries(ctx); e != nil {
		return nil, e
	}
	if e = m.spool.close(); e != nil {
		return nil, e
	}
	return m, nil
}

// Close cancels content loads and releases retained metadata and SDK storage.
// It leaves the caller-owned source, resolver and shared cache open.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.content.close()
	m.entries = nil
	return m.spool.close()
}

// Entry returns owned metadata for path. Missing paths return an error
// matching fs.ErrNotExist; invalid paths match fs.ErrInvalid. Errors include
// the path in an *fs.PathError. Symlinks are never followed.
func (m *Manifest) Entry(path string) (*Entry, error) {
	entry, err := m.entrySnapshot(path)
	if err != nil {
		return nil, err
	}
	owned := cloneEntry(entry)
	return &owned, nil
}

// Metadata is immutable after open. Internal reads can copy the entry header
// without cloning extents and attributes that never escape to the caller.
// Copying the header avoids retaining the complete entries slice after Close.
func (m *Manifest) entrySnapshot(path string) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Entry{}, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrClosed}
	}
	if !validPath(path) {
		return Entry{}, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrInvalid}
	}
	i := sort.Search(len(m.entries), func(i int) bool { return m.entries[i].Path >= path })
	if i == len(m.entries) || m.entries[i].Path != path {
		return Entry{}, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrNotExist}
	}
	return m.entries[i], nil
}

func cloneEntry(entry Entry) Entry {
	entry.extentEnds = nil
	entry.Extents = append([]Extent(nil), entry.Extents...)
	if len(entry.Attributes) != 0 {
		entry.Attributes = cloneExtensions(entry.Attributes)
	} else {
		entry.Attributes = nil
	}
	if entry.Linkname != nil {
		text := *entry.Linkname
		entry.Linkname = &text
	}
	return entry
}

func (m *Manifest) entryRank(path string) (*Entry, uint64, error) {
	if m.closed {
		return nil, 0, fs.ErrClosed
	}
	if !validPath(path) {
		return nil, 0, nil
	}
	i := sort.Search(len(m.entries), func(i int) bool { return m.entries[i].Path >= path })
	if i == len(m.entries) || m.entries[i].Path != path {
		return nil, 0, nil
	}
	owned := cloneEntry(m.entries[i])
	return &owned, uint64(i), nil
}

// VisitEntries invokes callbacks without holding the metadata lock. It retains
// at most one bounded batch and permits callbacks to read from this manifest.
func (m *Manifest) VisitEntries(visit func(Entry) error) error {
	if visit == nil {
		return errors.New("zstar: nil entry visitor")
	}
	cursor := m.Cursor()
	for {
		entries, err := cursor.Next(128)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		for _, entry := range entries {
			if err := visit(entry); err != nil {
				return err
			}
		}
	}
}

// EncodedContentResolver returns a complete encoded object, with actual-input
// limits enforced independently of the encoded-size hint. Implementations must
// support concurrent calls and return caller-owned bytes. Readers never close
// the resolver. A pointer implementation provides a stable cache namespace.
type EncodedContentResolver interface {
	GetEncoded(ctx context.Context, digest [32]byte, encodedSizeHint, maxBytes uint64) ([]byte, error)
}

// Read reads an exact logical member range using the supplied resolver/hash
// policy. The returned buffer is caller-owned and bounded by MaxReadBytes.
// Missing paths match fs.ErrNotExist; symlinks are never followed.
func (m *Manifest) Read(ctx context.Context, path string, offset, size uint64, resolver EncodedContentResolver, verifyExternal bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := m.entrySnapshot(path)
	if err != nil {
		return nil, pathError("read", path, err)
	}
	if entry.Type != TypeFile {
		return nil, rejected("unsupported-operation", "entry is not a file")
	}
	out, err := m.ReadMany(ctx, []ReadRequest{{path, offset, size}}, resolver, verifyExternal)
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

func (e Entry) String() string { return fmt.Sprintf("%s (%d)", e.Path, e.Type) }

// RootExtensions returns owned application metadata from the authenticated root.
func (m *Manifest) RootExtensions() Extensions {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneExtensions(m.rootExtensions)
}

func decodedSpoolOptions(o ManifestOptions) ManifestOptions {
	o.MaxEncodedBytes = o.MaxDecodedBytes
	return o
}
