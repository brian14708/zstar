package zstar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
)

// sequenceReader frames one bounded CBOR item at a time across storage segments.
type sequenceReader struct {
	spool  *manifestSpool
	at     uint64
	buffer []byte
	// limit caps the encoded size of the item being framed. It is enforced
	// before any claimed length is read or allocated.
	limit uint64
	// lastSize is the encoded size of the most recent item returned by next.
	lastSize int
}

// Append each header and payload directly into the current item buffer.
func (r *sequenceReader) appendTo(out []byte, n uint64) ([]byte, error) {
	if r.limit != 0 && (uint64(len(out)) > r.limit || n > r.limit-uint64(len(out))) {
		return nil, rejected(CategoryResourceLimit, "CBOR item size")
	}
	if n > r.spool.size-r.at {
		return nil, rejected(CategoryMalformedTOC, "truncated CBOR item")
	}
	for n > 0 {
		if len(r.buffer) == 0 {
			var err error
			r.buffer, err = r.spool.read(r.at, min(64<<10, r.spool.size-r.at))
			if err != nil {
				return nil, err
			}
		}
		count := min(n, uint64(len(r.buffer)))
		out = append(out, r.buffer[:count]...)
		r.buffer = r.buffer[count:]
		r.at += count
		n -= count
	}
	return out, nil
}

func (r *sequenceReader) next() (any, error) {
	if manifestDecoderError != nil {
		return nil, manifestDecoderError
	}
	if r.at == r.spool.size {
		return nil, io.EOF
	}
	raw := make([]byte, 0)
	items := 0
	var item func(int) error
	item = func(depth int) error {
		items++
		if depth > 64 || items > 16000000 {
			return rejected(CategoryResourceLimit, "CBOR nesting or items")
		}
		var err error
		raw, err = r.appendTo(raw, 1)
		if err != nil {
			return err
		}
		h := raw[len(raw)-1]
		major, ai := h>>5, h&31
		n := uint64(ai)
		if ai >= 24 && ai <= 27 {
			width := uint64(1) << uint(ai-24)
			start := len(raw)
			raw, err = r.appendTo(raw, width)
			if err != nil {
				return err
			}
			n = 0
			for _, v := range raw[start:] {
				n = n<<8 | uint64(v)
			}
		} else if ai > 27 {
			return rejected(CategoryMalformedTOC, "indefinite CBOR")
		}
		switch major {
		case 2, 3:
			raw, err = r.appendTo(raw, n)
			if err != nil {
				return err
			}
		case 4, 5:
			if r.limit != 0 && n > r.limit {
				return rejected(CategoryResourceLimit, "CBOR item size")
			}
			if major == 5 {
				if n > r.spool.size-r.at {
					return rejected(CategoryMalformedTOC, "CBOR map size")
				}
				n *= 2
			}
			if n > r.spool.size-r.at {
				return rejected(CategoryMalformedTOC, "CBOR container size")
			}
			for ; n > 0; n-- {
				if err := item(depth + 1); err != nil {
					return err
				}
			}
		case 0, 1, 7:
		default:
			return rejected(CategoryMalformedTOC, "unsupported CBOR type")
		}
		return nil
	}
	if err := item(0); err != nil {
		return nil, err
	}
	var value any
	if err := manifestDecoder.Unmarshal(raw, &value); err != nil {
		return nil, categorized(CategoryMalformedTOC, err)
	}
	if err := validValue(value); err != nil {
		return nil, categorized(CategoryMalformedTOC, err)
	}
	canonical, err := manifestEncode(value)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, rejected(CategoryMalformedTOC, "noncanonical CBOR")
	}
	r.lastSize = len(raw)
	return value, nil
}

type manifestSortRecord struct {
	path  string
	value any // Preserve the decoder's boxed CBOR value for re-encoding.
}

func (s *manifestSpool) readSortRecord(at *uint64) (manifestSortRecord, error) {
	value, err := s.readRecord(at)
	if err != nil {
		return manifestSortRecord{}, err
	}
	fields, ok := value.([]any)
	if !ok || len(fields) == 0 {
		return manifestSortRecord{}, rejected(CategoryMalformedTOC, "invalid entry sort record")
	}
	path, ok := fields[0].(string)
	if !ok {
		return manifestSortRecord{}, rejected(CategoryMalformedTOC, "invalid entry sort path")
	}
	return manifestSortRecord{path: path, value: value}, nil
}

// External merge sorting keeps both records and the run directory spillable.
func sortManifestRecords(input *manifestSpool) (result *manifestSpool, err error) {
	options := input.options
	runs := &manifestSpool{options: options}
	directory := &manifestSpool{options: options}
	defer func() {
		err = errors.Join(err, directory.close())
		if err != nil {
			err = errors.Join(err, runs.close())
		}
	}()
	addRun := func(dir *manifestSpool, start, end uint64) error {
		var b [16]byte
		binary.LittleEndian.PutUint64(b[:], start)
		binary.LittleEndian.PutUint64(b[8:], end)
		return dir.append(b[:])
	}
	count := uint64(0)
	values := []manifestSortRecord{}
	for at := uint64(0); at < input.size; {
		begin := at
		for at < input.size && (at-begin < 64<<10 || len(values) == 0) {
			v, e := input.readSortRecord(&at)
			if e != nil {
				return nil, e
			}
			values = append(values, v)
		}
		slices.SortFunc(values, func(a, b manifestSortRecord) int { return strings.Compare(a.path, b.path) })
		start := runs.size
		for _, v := range values {
			if e := runs.appendRecord(v.value); e != nil {
				return nil, e
			}
		}
		if e := addRun(directory, start, runs.size); e != nil {
			return nil, e
		}
		clear(values)
		values = values[:0]
		count++
	}
	if e := input.close(); e != nil {
		return nil, e
	}
	for count > 1 {
		next := &manifestSpool{options: options}
		nextDir := &manifestSpool{options: options}
		failed := func() error {
			for pos := uint64(0); pos < count; pos += 8 {
				n := int(min(8, count-pos))
				var starts, ends [8]uint64
				var values [8]manifestSortRecord
				for i := range n {
					b, e := directory.read((pos+uint64(i))*16, 16)
					if e != nil {
						return e
					}
					starts[i] = binary.LittleEndian.Uint64(b)
					ends[i] = binary.LittleEndian.Uint64(b[8:])
					values[i], e = runs.readSortRecord(&starts[i])
					if e != nil {
						return e
					}
				}
				start := next.size
				for {
					best := -1
					for i, v := range values[:n] {
						if v.value != nil && (best < 0 || v.path < values[best].path) {
							best = i
						}
					}
					if best < 0 {
						break
					}
					if e := next.appendRecord(values[best].value); e != nil {
						return e
					}
					values[best] = manifestSortRecord{}
					if starts[best] < ends[best] {
						var e error
						values[best], e = runs.readSortRecord(&starts[best])
						if e != nil {
							return e
						}
					}
				}
				if e := addRun(nextDir, start, next.size); e != nil {
					return e
				}
			}
			return nil
		}()
		if failed != nil {
			return nil, errors.Join(failed, next.close(), nextDir.close())
		}
		if e := errors.Join(runs.close(), directory.close()); e != nil {
			return nil, errors.Join(e, next.close(), nextDir.close())
		}
		runs = next
		directory = nextDir
		count = (count + 7) / 8
	}
	return runs, nil
}
