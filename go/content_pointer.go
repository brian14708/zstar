package zstar

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"

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

// EncodeContentPointer serializes JSON chunks, omitting zero offsets.
func EncodeContentPointer(extents []Extent) ([]byte, error) {
	if err := validateObjectExtents(extents); err != nil {
		return nil, err
	}
	type chunk struct {
		Source      string `json:"source"`
		Size        uint64 `json:"size"`
		EncodedSize uint64 `json:"encoded_size"`
		Offset      uint64 `json:"offset,omitempty"`
	}
	pointer := struct {
		Version string  `json:"version"`
		Chunks  []chunk `json:"chunks"`
	}{Version: "zstar-pointer-v1", Chunks: make([]chunk, len(extents))}
	for i, e := range extents {
		pointer.Chunks[i] = chunk{e.Source.address, e.Size, e.EncodedSize, e.Offset}
	}
	return json.Marshal(pointer)
}

// pointerFields reads a JSON object without accepting duplicate or unknown keys.
func pointerFields(raw []byte, allowed ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, rejected(CategoryMalformedPointer, "pointer object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, rejected(CategoryMalformedPointer, "pointer key")
		}
		name, ok := key.(string)
		if !ok {
			return nil, rejected(CategoryMalformedPointer, "pointer key")
		}
		if _, duplicate := fields[name]; !slices.Contains(allowed, name) || duplicate {
			return nil, rejected(CategoryMalformedPointer, "unknown or duplicate pointer field")
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, rejected(CategoryMalformedPointer, "pointer value")
		}
		fields[name] = value
	}
	if _, err = decoder.Token(); err != nil {
		return nil, rejected(CategoryMalformedPointer, "pointer object end")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, rejected(CategoryMalformedPointer, "pointer trailing bytes")
	}
	return fields, nil
}

// DecodeContentPointer parses strict JSON chunks with exact uint64 values.
func DecodeContentPointer(raw []byte) ([]Extent, error) {
	return decodeContentPointer(raw, math.MaxInt)
}

func decodeContentPointer(raw []byte, maximumChunks int) ([]Extent, error) {
	if !utf8.Valid(raw) {
		return nil, rejected(CategoryMalformedPointer, "pointer UTF-8")
	}
	fields, err := pointerFields(raw, "version", "chunks")
	if err != nil {
		return nil, err
	}
	var version string
	if err = json.Unmarshal(fields["version"], &version); err != nil || version != "zstar-pointer-v1" {
		return nil, rejected(CategoryMalformedPointer, "pointer version")
	}
	decoder := json.NewDecoder(bytes.NewReader(fields["chunks"]))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, rejected(CategoryMalformedPointer, "pointer chunks")
	}
	var out []Extent
	for decoder.More() {
		if len(out) == maximumChunks {
			return nil, rejected(CategoryMalformedPointer, "pointer chunk count exceeds manifest")
		}
		var chunk json.RawMessage
		if err = decoder.Decode(&chunk); err != nil {
			return nil, rejected(CategoryMalformedPointer, "pointer chunk")
		}
		fields, err := pointerFields(chunk, "source", "size", "encoded_size", "offset")
		if err != nil {
			return nil, err
		}
		var address string
		if err = json.Unmarshal(fields["source"], &address); err != nil {
			return nil, rejected(CategoryMalformedPointer, "pointer source")
		}
		source, err := ParseObjectSource(address)
		if err != nil {
			return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		e := Extent{Source: source}
		for _, field := range [...]struct {
			name   string
			target *uint64
		}{{"size", &e.Size}, {"encoded_size", &e.EncodedSize}, {"offset", &e.Offset}} {
			value, exists := fields[field.name]
			if field.name == "offset" && !exists {
				continue
			}
			n, err := strconv.ParseUint(string(value), 10, 64)
			if err != nil || n == 0 || strconv.FormatUint(n, 10) != string(value) {
				return nil, rejected(CategoryMalformedPointer, "pointer positive uint64")
			}
			*field.target = n
		}
		out = append(out, e)
	}
	if _, err = decoder.Token(); err != nil {
		return nil, rejected(CategoryMalformedPointer, "pointer chunks end")
	}
	return out, validateObjectExtents(out)
}
