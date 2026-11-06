package zstar

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
)

type manifestTarCursor struct {
	limits                 Limits
	decoder                frameDecoder
	input                  manifestRangeStream
	raw                    []byte
	at                     int
	frameOffset, frameSize uint64
}

// Scan a single frame using its block lengths, without a permanent frame table.
// Range read-ahead and each allocation remain bounded even for hostile lengths.
func (c *manifestTarCursor) nextFrame() error {
	c.frameOffset = c.input.at
	encoded, err := c.input.take(5)
	if err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(encoded) != 0xFD2FB528 || encoded[4]&4 == 0 {
		return rejected(CategoryMalformedFrame, "data frame magic or missing checksum")
	}
	fd := encoded[4]
	single := fd&32 != 0
	fcs := []uint64{0, 2, 4, 8}[fd>>6]
	if single && fcs == 0 {
		fcs = 1
	}
	rest := []uint64{0, 1, 2, 4}[fd&3] + fcs
	if !single {
		rest++
	}
	h, err := c.input.take(rest)
	if err != nil {
		return err
	}
	encoded = append(encoded, h...)
	for {
		h, err = c.input.take(3)
		if err != nil {
			return err
		}
		block := uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16
		kind, size := (block>>1)&3, uint64(block>>3)
		if kind == 3 {
			return rejected(CategoryMalformedFrame, "reserved block type")
		}
		if size > 128<<10 {
			return rejected(CategoryMalformedFrame, "block size limit")
		}
		if kind == 1 {
			size = 1
		}
		if err = checkLimit(uint64(len(encoded))+3+size+4, c.limits.MaxFrameEncodedBytes, "encoded frame size"); err != nil {
			return err
		}
		encoded = append(encoded, h...)
		encoded, err = c.input.appendTo(encoded, size)
		if err != nil {
			return err
		}
		if block&1 != 0 {
			break
		}
	}
	checksum, err := c.input.take(4)
	if err != nil {
		return err
	}
	encoded = append(encoded, checksum...)
	raw, err := c.decoder.decode(encoded, 0, addressLimit(c.limits.MaxFrameDecodedBytes), addressLimit(c.limits.MaxFrameEncodedBytes))
	if err != nil {
		return categorized(CategoryMalformedFrame, err)
	}
	if len(raw) == 0 {
		return rejected(CategoryMalformedFrame, "empty data frame")
	}
	c.raw, c.at = raw, 0
	c.frameSize = uint64(len(encoded))
	return nil
}

func (c *manifestTarCursor) consume(size uint64, visit func([]byte, uint64, uint64, uint64) error) error {
	for size > 0 {
		if err := c.input.ctx.Err(); err != nil {
			return err
		}
		if c.at == len(c.raw) {
			if c.input.at == c.input.end {
				return badTar("truncated decoded stream")
			}
			if err := c.nextFrame(); err != nil {
				return err
			}
		}
		n := min(size, uint64(len(c.raw)-c.at))
		if err := visit(c.raw[c.at:c.at+int(n)], c.frameOffset, c.frameSize, uint64(c.at)); err != nil {
			return err
		}
		c.at += int(n)
		size -= n
	}
	return nil
}

func (c *manifestTarCursor) take(size uint64) ([]byte, error) {
	if size > uint64(^uint(0)>>1) {
		return nil, rejected(CategoryResourceLimit, "tar payload exceeds address space")
	}
	// A tar/PAX length is untrusted; grow only as physical payload arrives.
	out := make([]byte, 0, int(min(size, 64<<10)))
	err := c.consume(size, func(b []byte, _, _, _ uint64) error { out = append(out, b...); return nil })
	return out, err
}

func (c *manifestTarCursor) zero(size uint64) error {
	return c.consume(size, func(b []byte, _, _, _ uint64) error {
		if !allZero(b) {
			return badTar("nonzero padding or end markers")
		}
		return nil
	})
}

type effectiveManifestHeader struct {
	entry Entry
	size  uint64
}

func (c *manifestTarCursor) header(first []byte) (effectiveManifestHeader, error) {
	var result effectiveManifestHeader
	h, err := parseTarHeader(first)
	if err != nil {
		return result, err
	}
	fields := map[string]string{}
	if h.kind == 'x' {
		if h.link != "" || h.size == 0 || h.size > (1<<20)-1024 {
			return result, badTar("local PAX header bounds")
		}
		payload, err := c.take(h.size)
		if err != nil {
			return result, err
		}
		fields, err = parsePAX(payload)
		if err != nil {
			return result, err
		}
		if err = c.zero((512 - h.size%512) % 512); err != nil {
			return result, err
		}
		member, err := c.take(512)
		if err != nil {
			return result, err
		}
		h, err = parseTarHeader(member)
		if err != nil {
			return result, err
		}
	}
	switch h.kind {
	case 0, '0':
		result.entry.Type = TypeFile
	case '2':
		result.entry.Type = TypeSymlink
	case '5':
		result.entry.Type = TypeDirectory
	default:
		return result, badTar("unsupported member type")
	}
	result.entry.Path = h.name
	if path, ok := fields["path"]; ok {
		result.entry.Path = path
	}
	if result.entry.Type == TypeDirectory {
		if !strings.HasSuffix(result.entry.Path, "/") {
			return result, badTar("directory trailing slash")
		}
		result.entry.Path = strings.TrimSuffix(result.entry.Path, "/")
	}
	if !validPath(result.entry.Path) {
		return result, badTar("effective path")
	}
	result.entry.Mode, result.entry.UID, result.entry.GID = h.mode, h.uid, h.gid
	result.size = h.size
	for _, field := range []struct {
		key string
		dst *uint64
	}{
		{"uid", &result.entry.UID}, {"gid", &result.entry.GID}, {"size", &result.size},
	} {
		if value, ok := fields[field.key]; ok {
			*field.dst, err = tarDecimal(value)
			if err != nil {
				return result, err
			}
		}
	}
	if value, ok := fields["mtime"]; ok {
		result.entry.MtimeNS, err = paxMtime(value)
		if err != nil {
			return result, err
		}
	} else {
		if h.mtime > math.MaxInt64/1_000_000_000 {
			return result, badTar("mtime range")
		}
		result.entry.MtimeNS = int64(h.mtime) * 1_000_000_000
	}
	link := h.link
	if value, ok := fields["linkpath"]; ok {
		if result.entry.Type != TypeSymlink {
			return result, badTar("linkpath on nonsymlink")
		}
		link = value
	}
	if result.entry.Mode > 4095 {
		return result, badTar("effective mode range")
	}
	if result.entry.Type == TypeSymlink {
		if len(link) == 0 || len(link) > 4096 || strings.ContainsRune(link, 0) {
			return result, badTar("effective link target")
		}
		result.entry.Linkname = &link
	} else if h.link != "" {
		return result, badTar("link field on nonsymlink")
	}
	if result.entry.Type != TypeFile && result.size != 0 {
		return result, badTar("nonfile payload")
	}
	return result, nil
}

func sameEffectiveMetadata(a, b Entry) bool {
	return a.Path == b.Path && a.Type == b.Type && a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID && a.MtimeNS == b.MtimeNS &&
		(a.Linkname == nil && b.Linkname == nil || a.Linkname != nil && b.Linkname != nil && *a.Linkname == *b.Linkname)
}

// ValidateArchive checks metadata and all physical frames, effective headers,
// inline mappings, pointers, padding and end markers. It fetches no external
// objects and makes no external-closure claim.
func (m *manifest) ValidateArchive(ctx context.Context) (err error) {
	if err = m.checkOpen(ctx); err != nil {
		return err
	}
	c := manifestTarCursor{limits: m.spool.options.Limits, decoder: frameDecoder{limits: m.spool.options.Limits}, input: manifestRangeStream{ctx: ctx, source: m.source, end: m.start, category: CategoryMalformedFrame, readAhead: m.readAhead}}
	defer c.decoder.close()
	cursor := m.Cursor()
	ordered := true
	var count uint64
	observed := manifestSpool{options: m.spool.options}
	defer func() { err = errors.Join(err, observed.close()) }()
	for {
		first, e := c.take(512)
		if e != nil {
			return e
		}
		if allZero(first) {
			if e = c.zero(512); e != nil {
				return e
			}
			if c.at != len(c.raw) || c.input.at != c.input.end {
				return badTar("trailing decoded data or frames")
			}
			break
		}
		header, e := c.header(first)
		if e != nil {
			return e
		}
		var entry *Entry
		if ordered {
			batch, e := cursor.Next(1)
			if e != nil {
				return e
			}
			if len(batch) == 1 && batch[0].Path == header.entry.Path {
				entry = &batch[0]
			}
		}
		if entry == nil {
			if ordered {
				// The previously matched prefix has ranks [0,count). Create the
				// temporary proof only when physical order actually differs.
				buffer := make([]byte, 0, 64<<10)
				for rank := range count {
					buffer = binary.LittleEndian.AppendUint64(buffer, rank)
					if len(buffer) == cap(buffer) {
						if e = observed.append(buffer); e != nil {
							return e
						}
						buffer = buffer[:0]
					}
				}
				if e = observed.append(buffer); e != nil {
					return e
				}
				ordered = false
			}
			m.mu.Lock()
			var rank uint64
			entry, rank, e = m.entryRank(header.entry.Path)
			m.mu.Unlock()
			if e != nil {
				return e
			}
			if entry == nil {
				return tarMismatch("unindexed member")
			}
			if e = observed.append(binary.LittleEndian.AppendUint64(nil, rank)); e != nil {
				return e
			}
		}
		if !sameEffectiveMetadata(*entry, header.entry) {
			return tarMismatch("member metadata")
		}
		if count >= m.count {
			return tarMismatch("extra physical member")
		}
		count++
		if entry.External() {
			if err = checkLimit(header.size, c.limits.MaxPointerBytes, "pointer size"); err != nil {
				return err
			}
			pointer, e := c.take(header.size)
			if e != nil {
				return e
			}
			extents, e := decodeContentPointer(pointer, len(entry.Extents))
			if e != nil {
				return e
			}
			if len(extents) != len(entry.Extents) {
				return tarMismatch("pointer recipe")
			}
			for i := range extents {
				if extents[i] != entry.Extents[i] {
					return tarMismatch("pointer recipe")
				}
			}
		} else {
			size, e := entry.Size()
			if e != nil {
				return e
			}
			if size != header.size {
				return tarMismatch("inline payload size")
			}
			extentIndex := 0
			var extentAt uint64
			e = c.consume(size, func(b []byte, frameOffset, frameSize, offset uint64) error {
				remaining := uint64(len(b))
				for remaining > 0 {
					if extentIndex == len(entry.Extents) {
						return tarMismatch("inline extent coverage")
					}
					x := entry.Extents[extentIndex]
					if x.Source != ArchiveSource(frameOffset) || x.EncodedSize != frameSize || x.Offset+extentAt != offset {
						return tarMismatch("inline payload selection")
					}
					n := min(remaining, x.Size-extentAt)
					extentAt += n
					offset += n
					remaining -= n
					if extentAt == x.Size {
						extentIndex++
						extentAt = 0
					}
				}
				return nil
			})
			if e != nil {
				return e
			}
			if extentIndex != len(entry.Extents) || extentAt != 0 {
				return tarMismatch("inline extent coverage")
			}
		}
		if e = c.zero((512 - header.size%512) % 512); e != nil {
			return e
		}
	}
	if count != m.count {
		return tarMismatch("missing physical member")
	}
	if !ordered {
		if err = validateObservedRanks(ctx, &observed, count); err != nil {
			return err
		}
	}
	return nil
}
