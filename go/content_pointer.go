package zstar

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

func validateObjectExtents(extents []Extent) error {
	if len(extents) == 0 {
		return rejected(CategoryMalformedPointer, "extent count")
	}
	var total uint64
	for _, e := range extents {
		if _, ok := e.Source.ContentAddress(); !ok {
			return rejected(CategoryMalformedPointer, "object source required")
		}
		if err := e.validate(0); err != nil {
			return &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		if e.Size > math.MaxUint64-total {
			return rejected(CategoryMalformedPointer, "file size overflow")
		}
		total += e.Size

	}
	return nil
}

// EncodeContentReference uses the same extent tuples as archive entries.
func EncodeContentReference(extents []Extent) ([]byte, error) {
	if err := validateObjectExtents(extents); err != nil {
		return nil, err
	}
	values := make([][]any, len(extents))
	for i, e := range extents {
		values[i] = e.value()
	}
	raw, err := manifestEncode(map[string]any{"version": uint64(1), "extents": values})
	return raw, err
}

// DecodeContentReference decodes and validates canonical external extent tuples.
func DecodeContentReference(raw []byte) ([]Extent, error) {
	if len(raw) == 0 {
		return nil, rejected(CategoryMalformedPointer, "reference size limit")
	}
	mode, err := (cbor.DecOptions{DupMapKey: cbor.DupMapKeyEnforcedAPF, IndefLength: cbor.IndefLengthForbidden, TagsMd: cbor.TagsForbidden, MaxArrayElements: 2147483647, MaxMapPairs: 1_000_000, MaxNestedLevels: 64}).DecMode()
	if err != nil {
		return nil, err
	}
	var value any
	if err = mode.Unmarshal(raw, &value); err != nil {
		return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
	}
	canonical, err := manifestEncode(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, rejected(CategoryMalformedPointer, "noncanonical CBOR")
	}
	m, ok := value.(map[any]any)
	if !ok || len(m) != 2 || m["version"] != uint64(1) {
		return nil, rejected(CategoryMalformedPointer, "reference fields or version")
	}
	values, ok := m["extents"].([]any)
	if !ok {
		return nil, rejected(CategoryMalformedPointer, "extents")
	}
	out := make([]Extent, 0, len(values))
	for _, v := range values {
		e, err := parseExtent(v, 0)
		if err != nil {
			return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		out = append(out, e)
	}
	return out, validateObjectExtents(out)
}

// EncodeContentPointer omits whole-file OID, file size and zero offsets.
func EncodeContentPointer(extents []Extent) ([]byte, error) {
	if err := validateObjectExtents(extents); err != nil {
		return nil, err
	}
	var out strings.Builder
	out.WriteString("version zstar-pointer-v1\n")
	for _, e := range extents {
		out.WriteString("chunk ")
		out.WriteString(e.Source.address)
		out.WriteByte(' ')
		out.WriteString(strconv.FormatUint(e.Size, 10))
		out.WriteByte(' ')
		out.WriteString(strconv.FormatUint(e.EncodedSize, 10))
		if e.Offset != 0 {
			out.WriteByte(' ')
			out.WriteString(strconv.FormatUint(e.Offset, 10))
		}
		out.WriteByte('\n')
	}
	return []byte(out.String()), nil
}

// DecodeContentPointer decodes and validates canonical external pointer text.
func DecodeContentPointer(raw []byte) ([]Extent, error) {
	if !bytes.HasPrefix(raw, []byte("version zstar-pointer-v1\n")) || len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return nil, rejected(CategoryMalformedPointer, "pointer header or length")
	}
	lines := strings.Split(string(raw[len("version zstar-pointer-v1\n"):len(raw)-1]), "\n")
	var out []Extent
	for _, line := range lines {
		fields := strings.Split(line, " ")
		if len(fields) < 4 || len(fields) > 5 || fields[0] != "chunk" {
			return nil, rejected(CategoryMalformedPointer, "chunk grammar")
		}
		source, err := ParseObjectSource(fields[1])
		if err != nil {
			return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		e := Extent{Source: source}
		values := []*uint64{&e.Size, &e.EncodedSize, &e.Offset}
		for i, text := range fields[2:] {
			n, err := strconv.ParseUint(text, 10, 64)
			if err != nil || strconv.FormatUint(n, 10) != text {
				return nil, rejected(CategoryMalformedPointer, "canonical integer")
			}
			*values[i] = n
		}
		if len(fields) == 5 && e.Offset == 0 {
			return nil, rejected(CategoryMalformedPointer, "explicit default offset")
		}
		out = append(out, e)
	}
	return out, validateObjectExtents(out)
}
