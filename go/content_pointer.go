package zstar

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"math"

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

// encodeContentPointer serializes JSON chunks, omitting zero offsets.
func encodeContentPointer(extents []Extent) ([]byte, error) {
	if err := validateObjectExtents(extents); err != nil {
		return nil, err
	}
	type chunk struct {
		Source      string `json:"source"`
		Size        uint64 `json:"size"`
		EncodedSize uint64 `json:"encoded_size"`
		Offset      uint64 `json:"offset,omitzero"`
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

// decodeContentPointer parses strict JSON chunks with exact uint64 values.
func decodeContentPointer(raw []byte, maximumChunks int) ([]Extent, error) {
	var pointer struct {
		Version string         `json:"version"`
		Chunks  jsontext.Value `json:"chunks"`
	}
	if err := json.Unmarshal(raw, &pointer, json.RejectUnknownMembers(true)); err != nil {
		return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
	}
	if pointer.Version != "zstar-pointer-v1" {
		return nil, rejected(CategoryMalformedPointer, "pointer version")
	}
	decoder := jsontext.NewDecoder(bytes.NewReader(pointer.Chunks))
	token, err := decoder.ReadToken()
	if err != nil || token.Kind() != '[' {
		return nil, rejected(CategoryMalformedPointer, "pointer chunks")
	}
	var out []Extent
	for decoder.PeekKind() != ']' {
		if len(out) == maximumChunks {
			return nil, rejected(CategoryMalformedPointer, "pointer chunk count exceeds manifest")
		}
		var chunk struct {
			Source      string         `json:"source"`
			Size        uint64         `json:"size"`
			EncodedSize uint64         `json:"encoded_size"`
			Offset      jsontext.Value `json:"offset"`
		}
		if err := json.UnmarshalDecode(decoder, &chunk, json.RejectUnknownMembers(true)); err != nil {
			return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		source, err := ParseObjectSource(chunk.Source)
		if err != nil {
			return nil, &FormatError{Category: CategoryMalformedPointer, Cause: err}
		}
		e := Extent{Source: source, Size: chunk.Size, EncodedSize: chunk.EncodedSize}
		if len(chunk.Offset) != 0 {
			if err := json.Unmarshal(chunk.Offset, &e.Offset); err != nil || e.Offset == 0 {
				return nil, rejected(CategoryMalformedPointer, "pointer positive offset")
			}
		}
		out = append(out, e)
	}
	if _, err = decoder.ReadToken(); err != nil {
		return nil, rejected(CategoryMalformedPointer, "pointer chunks end")
	}
	return out, validateObjectExtents(out)
}
