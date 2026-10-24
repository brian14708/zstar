package zstar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxFrameSize = 16 << 20

// Header supplies effective tar metadata and an inline physical size. External
// files supply Extents with Size zero and no body. Kind defaults to file.
type Header struct {
	Extensions     Extensions
	Path           string
	Kind           Kind
	Mode, UID, GID uint64
	MtimeNS        int64
	Size           uint64
	Target         string
	Extents        []Extent
}

// WriterOptions controls frame size and manifest encoding/storage budgets.
// A zero FrameSize selects 1 MiB; nonzero values must be multiples of 512
// between 512 bytes and 16 MiB.
type WriterOptions struct {
	Manifest  ManifestOptions
	FrameSize int
}

// Writer streams checksummed tar frames and bounded, spooled entry metadata.
// Paths may arrive in any order with explicit ancestor directories. Close never
// closes the destination. Methods are not concurrent; failures are sticky.
type Writer struct{ engine *EntryArchiveWriter }

// NewWriter creates an archive encoder. The caller owns output and must call
// Close to finalize metadata and release temporary storage.
func NewWriter(output io.Writer, options WriterOptions) (*Writer, error) {
	engine, err := NewEntryArchiveWriter(output, options.FrameSize, options.Manifest)
	if err != nil {
		return nil, err
	}
	return &Writer{engine: engine}, nil
}

// Add snapshots metadata and consumes exactly Header.Size bytes for an inline
// regular file, rejecting extra or truncated input. External files use Extents
// with no body and Size zero; non-files also have no body. ctx applies to this
// operation. Invalid arguments are rejected without side effects; I/O and
// body failures after output begins are sticky and output may be partial.
func (w *Writer) Add(ctx context.Context, header Header, body io.Reader) error {
	if w.engine.closed {
		return errors.New("zstar: writer closed")
	}
	if w.engine.err != nil {
		return w.engine.err
	}
	kind := header.Kind
	if kind == "" {
		kind = "file"
	}
	entry := Entry{Path: header.Path, Mode: header.Mode, UID: header.UID, GID: header.GID, MtimeNS: header.MtimeNS, Extents: header.Extents, Attributes: header.Extensions}
	switch kind {
	case "file":
	case "directory":
		entry.Type = TypeDirectory
	case "symlink":
		entry.Type = TypeSymlink
		target := header.Target
		entry.Linkname = &target
	default:
		return errors.New("zstar: unsupported member kind")
	}
	if kind != "symlink" && header.Target != "" {
		return errors.New("zstar: target on nonsymlink")
	}
	return w.engine.Add(ctx, entry, header.Size, body)
}

// Close finalizes the archive and releases temporary storage. It leaves the
// destination open and returns earlier sticky failures. Calls are not concurrent.
func (w *Writer) Close() error { return w.engine.Close() }

func validPath(path string) bool {
	if !utf8.ValidString(path) || len(path) == 0 || len(path) > 4096 || strings.ContainsRune(path, 0) {
		return false
	}
	for component := range strings.SplitSeq(path, "/") {
		if component == "" || component == "." || component == ".." || len(component) > 255 {
			return false
		}
	}
	return true
}

func writeExact(destination io.Writer, data []byte) error {
	n, err := destination.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

func memberHeader(h Header) ([]byte, error) {
	path := h.Path
	kind := byte('0')
	switch h.Kind {
	case "directory":
		path += "/"
		kind = '5'
	case "symlink":
		kind = '2'
	case KindFile, "":
	}
	pax := map[string]string{}
	tarPath, target := path, h.Target
	if len(path) > 100 {
		pax["path"] = path
		tarPath = ".zstar/member"
	}
	if len(target) > 100 {
		pax["linkpath"] = target
		target = ""
	}
	uid, gid, size := h.UID, h.GID, h.Size
	if uid > 0o7777777 {
		pax["uid"] = strconv.FormatUint(uid, 10)
		uid = 0
	}
	if gid > 0o7777777 {
		pax["gid"] = strconv.FormatUint(gid, 10)
		gid = 0
	}
	if size > 0o77777777777 {
		pax["size"] = strconv.FormatUint(size, 10)
		size = 0
	}
	seconds := uint64(0)
	if h.MtimeNS < 0 || h.MtimeNS%1_000_000_000 != 0 {
		negative := h.MtimeNS < 0
		var ns uint64
		if negative {
			ns = uint64(-(h.MtimeNS + 1)) + 1
		} else {
			ns = uint64(h.MtimeNS)
		}
		value := strconv.FormatUint(ns/1_000_000_000, 10)
		if ns%1_000_000_000 != 0 {
			value += "." + strings.TrimRight(fmt.Sprintf("%09d", ns%1_000_000_000), "0")
		}
		if negative {
			value = "-" + value
		}
		pax["mtime"] = value
	} else {
		seconds = uint64(h.MtimeNS / 1_000_000_000)
	}
	header := ustarHeader(tarPath, target, kind, h.Mode, uid, gid, size, seconds)
	if len(pax) == 0 {
		return header, nil
	}
	keys := make([]string, 0, len(pax))
	for key := range pax {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var payload strings.Builder
	for _, key := range keys {
		body := key + "=" + pax[key] + "\n"
		length := len(body) + 2
		for {
			next := len(body) + len(strconv.Itoa(length)) + 1
			if next == length {
				break
			}
			length = next
		}
		fmt.Fprintf(&payload, "%d %s", length, body)
	}
	p := []byte(payload.String())
	out := ustarHeader("PaxHeaders/zstar", "", 'x', 0o644, 0, 0, uint64(len(p)), 0)
	out = append(out, p...)
	out = append(out, make([]byte, (512-len(p)%512)%512)...)
	out = append(out, header...)
	if len(out) > 1<<20 {
		return nil, errors.New("zstar: PAX header size limit")
	}
	return out, nil
}

func ustarHeader(path, target string, kind byte, mode, uid, gid, size, mtime uint64) []byte {
	h := make([]byte, 512)
	copy(h[:100], path)
	copy(h[157:257], target)
	h[156] = kind
	octal := func(dst []byte, value uint64) { text := fmt.Sprintf("%0*o", len(dst)-1, value); copy(dst, text) }
	octal(h[100:108], mode)
	octal(h[108:116], uid)
	octal(h[116:124], gid)
	octal(h[124:136], size)
	octal(h[136:148], mtime)
	copy(h[257:263], "ustar\x00")
	copy(h[263:265], "00")
	for i := 148; i < 156; i++ {
		h[i] = ' '
	}
	sum := uint64(0)
	for _, b := range h {
		sum += uint64(b)
	}
	octal(h[148:156], sum)
	return h
}
