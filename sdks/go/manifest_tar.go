package zstar

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
)

// ArchiveReport describes full physical validation, independently of
// external closure. Counts keep reports bounded for archives with many entries.
type ArchiveReport struct {
	MetadataAuthenticated bool   `json:"metadata_authenticated"`
	MetadataValidated     bool   `json:"metadata_validated"`
	FullArchiveValidated  bool   `json:"full_archive_validated"`
	DataFramesChecked     uint64 `json:"data_frames_checked"`
	TarMembersChecked     uint64 `json:"tar_members_checked"`
	PointerMembersChecked uint64 `json:"pointer_members_checked"`
}

type manifestTarCursor struct {
	decoder                        frameDecoder
	input                          manifestRangeStream
	raw                            []byte
	at                             int
	frameOffset, frameSize, frames uint64
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
		return rejected("malformed-frame", "data frame magic or missing checksum")
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
			return rejected("malformed-frame", "reserved block type")
		}
		if size > 128<<10 {
			return rejected("malformed-frame", "block size limit")
		}
		if kind == 1 {
			size = 1
		}
		if uint64(len(encoded))+3+size+4 > 17<<20 {
			return rejected("malformed-frame", "encoded frame limit")
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
	raw, err := c.decoder.decode(encoded, 0, maxFrameSize, 17<<20)
	if err != nil {
		return categorized("malformed-frame", err)
	}
	if len(raw) == 0 {
		return rejected("malformed-frame", "empty data frame")
	}
	c.raw, c.at = raw, 0
	c.frameSize = uint64(len(encoded))
	c.frames++
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
	out := make([]byte, 0, int(size))
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
		if len(link) > 4096 || strings.ContainsRune(link, 0) {
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
func (m *Manifest) ValidateArchive(ctx context.Context) (ArchiveReport, error) {
	return m.VisitPayloads(ctx, nil)
}

// VisitPayloads validates while streaming physical file payloads. External
// payloads are pointer text. Empty regular files produce one empty callback;
// directories and symlinks produce none. Bytes are borrowed until callback
// return. Output is provisional until this method succeeds. Callbacks may read
// metadata/content through m, but must not close it during validation. Entry
// metadata is isolated from validation state and reused for each member chunk.
func (m *Manifest) VisitPayloads(ctx context.Context, visit func(Entry, []byte) error) (report ArchiveReport, err error) {
	if err = m.ValidateMetadata(ctx); err != nil {
		return report, err
	}
	defer func() {
		if err != nil {
			report = ArchiveReport{}
		}
	}()
	c := manifestTarCursor{input: manifestRangeStream{ctx: ctx, source: m.source, end: m.start, category: "malformed-frame", readAhead: m.readAhead}}
	defer c.decoder.close()
	cursor := m.Cursor()
	ordered := true
	var count uint64
	observed := manifestSpool{options: m.spool.options}
	defer func() { err = errors.Join(err, observed.close()) }()
	for {
		first, e := c.take(512)
		if e != nil {
			return report, e
		}
		if allZero(first) {
			if e = c.zero(512); e != nil {
				return report, e
			}
			if c.at != len(c.raw) || c.input.at != c.input.end {
				return report, badTar("trailing decoded data or frames")
			}
			break
		}
		header, e := c.header(first)
		if e != nil {
			return report, e
		}
		var entry *Entry
		if ordered {
			batch, e := cursor.Next(1)
			if e != nil {
				return report, e
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
							return report, e
						}
						buffer = buffer[:0]
					}
				}
				if e = observed.append(buffer); e != nil {
					return report, e
				}
				ordered = false
			}
			m.mu.Lock()
			var rank uint64
			entry, rank, e = m.entryRank(header.entry.Path)
			m.mu.Unlock()
			if e != nil {
				return report, e
			}
			if entry == nil {
				return report, tarMismatch("unindexed member")
			}
			if e = observed.append(binary.LittleEndian.AppendUint64(nil, rank)); e != nil {
				return report, e
			}
		}
		if !sameEffectiveMetadata(*entry, header.entry) {
			return report, tarMismatch("member metadata")
		}
		if count >= m.count {
			return report, tarMismatch("extra physical member")
		}
		count++
		var visitorEntry Entry
		if visit != nil {
			visitorEntry = cloneEntry(*entry)
		}
		if entry.External() {
			if header.size > 16<<20 {
				return report, rejected("malformed-pointer", "pointer size limit")
			}
			pointer, e := c.take(header.size)
			if e != nil {
				return report, e
			}
			extents, e := DecodeContentPointer(pointer)
			if e != nil {
				return report, e
			}
			if len(extents) != len(entry.Extents) {
				return report, tarMismatch("pointer recipe")
			}
			for i := range extents {
				if extents[i] != entry.Extents[i] {
					return report, tarMismatch("pointer recipe")
				}
			}
			if visit != nil {
				if e = visit(visitorEntry, pointer); e != nil {
					return report, e
				}
			}
			report.PointerMembersChecked++
		} else {
			size, e := entry.Size()
			if e != nil {
				return report, e
			}
			if size != header.size {
				return report, tarMismatch("inline payload size")
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
				if visit != nil {
					return visit(visitorEntry, b)
				}
				return nil
			})
			if e != nil {
				return report, e
			}
			if extentIndex != len(entry.Extents) || extentAt != 0 {
				return report, tarMismatch("inline extent coverage")
			}
			if size == 0 && entry.Type == TypeFile && visit != nil {
				if e = visit(visitorEntry, []byte{}); e != nil {
					return report, e
				}
			}
		}
		if e = c.zero((512 - header.size%512) % 512); e != nil {
			return report, e
		}
	}
	if count != m.count {
		return report, tarMismatch("missing physical member")
	}
	if !ordered {
		if err = validateObservedRanks(ctx, &observed, count); err != nil {
			return report, err
		}
	}
	report.MetadataAuthenticated, report.MetadataValidated, report.FullArchiveValidated = true, true, true
	report.DataFramesChecked, report.TarMembersChecked = c.frames, count
	return report, nil
}
