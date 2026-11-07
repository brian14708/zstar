package zstar

import (
	"bytes"
	"io"
	"math"
	"slices"
)

type pageReference struct {
	offset, encoded, decoded uint64
	lastPath                 string
	rawOffset                uint64
}

// decodePage bounds the CBOR parser to one complete independently decoded value.
func decodePage(spool *manifestSpool, offset, length uint64) (any, error) {
	bounded := *spool
	bounded.size = offset + length
	r := sequenceReader{spool: &bounded, at: offset, limit: length}
	value, err := r.next()
	if err != nil {
		return nil, err
	}
	if r.at != bounded.size {
		return nil, rejected(CategoryMalformedTOC, "trailing page bytes")
	}
	return value, nil
}

func parsePageReference(value any, entry bool) (pageReference, error) {
	var out pageReference
	v, ok := value.([]any)
	base := 0
	if entry {
		base = 1
	}
	if !ok || (len(v) != base+3 && len(v) != base+4) {
		return out, rejected(CategoryMalformedTOC, "page reference tuple")
	}
	if entry {
		out.lastPath, ok = v[0].(string)
		if !ok || !validPath(out.lastPath) {
			return out, rejected(CategoryMalformedTOC, "page separator path")
		}
	}
	var err error
	if out.offset, err = manifestUint(v[base]); err != nil {
		return out, err
	}
	if out.encoded, err = manifestUint(v[base+1]); err != nil {
		return out, err
	}
	if out.decoded, err = manifestUint(v[base+2]); err != nil {
		return out, err
	}
	if out.encoded <= 8 || out.decoded == 0 {
		return out, rejected(CategoryMalformedTOC, "page reference lengths")
	}
	if len(v) == base+4 {
		attrs, err := schemaMap(v[base+3], nil, nil)
		if err != nil || len(attrs) == 0 {
			return out, rejected(CategoryMalformedTOC, "reference attributes")
		}
	}
	return out, nil
}

func validatePageDirectory(spool *manifestSpool, physical []pageReference, footer Footer) (map[any]any, []pageReference, error) {
	fail := func() (map[any]any, []pageReference, error) {
		return nil, nil, rejected(CategoryMalformedTOC, "directory coverage or page reference")
	}
	if len(physical) == 0 {
		return fail()
	}
	directory := physical[len(physical)-1]
	if directory.encoded != footer.DirectoryEncodedSize || directory.decoded != footer.DirectoryDecodedSize {
		return fail()
	}
	value, err := decodePage(spool, footer.ManifestDecodedSize-directory.decoded, directory.decoded)
	if err != nil {
		return nil, nil, err
	}
	return validateDirectoryValue(value, spool, physical, footer)
}

func validateDirectoryValue(value any, spool *manifestSpool, physical []pageReference, footer Footer) (map[any]any, []pageReference, error) {
	fail := func() (map[any]any, []pageReference, error) {
		return nil, nil, rejected(CategoryMalformedTOC, "directory coverage or page reference")
	}
	root, err := schemaMap(value, []string{"version", "requires", "pages"}, []string{"extension-pages"})
	if err != nil {
		return nil, nil, categorized(CategoryMalformedTOC, err)
	}
	descriptors, ok := root["pages"].([]any)
	if !ok {
		return fail()
	}
	references := make([]pageReference, 0, len(descriptors))
	for _, v := range descriptors {
		ref, e := parsePageReference(v, true)
		if e != nil {
			return nil, nil, e
		}
		if len(references) > 0 && ref.lastPath <= references[len(references)-1].lastPath {
			return fail()
		}
		references = append(references, ref)
	}
	entryPages := len(references)
	if value, exists := root["extension-pages"]; exists {
		groups, ok := value.(map[any]any)
		if !ok || len(groups) == 0 {
			return fail()
		}
		keys := make([]string, 0, len(groups))
		for k := range groups {
			name, ok := k.(string)
			if !ok || len(name) < 2 || name[:2] != "x-" {
				return fail()
			}
			keys = append(keys, name)
		}
		slices.SortFunc(keys, func(a, b string) int { return bytes.Compare(mustManifestKey(a), mustManifestKey(b)) })
		for _, key := range keys {
			refs, ok := groups[key].([]any)
			if !ok || len(refs) == 0 {
				return fail()
			}
			for _, v := range refs {
				ref, e := parsePageReference(v, false)
				if e != nil {
					return nil, nil, e
				}
				references = append(references, ref)
			}
		}
	}
	if physical != nil && len(references)+1 != len(physical) {
		return fail()
	}
	var end, rawOffset uint64
	decoded := footer.DirectoryDecodedSize
	for i := range references {
		ref := &references[i]
		if physical != nil {
			actual := physical[i]
			if ref.offset != actual.offset || ref.encoded != actual.encoded || ref.decoded != actual.decoded {
				return fail()
			}
		}
		if ref.offset != end || ref.encoded > math.MaxUint64-end || ref.decoded > math.MaxUint64-decoded {
			return fail()
		}
		ref.rawOffset = rawOffset
		rawOffset += ref.decoded
		end += ref.encoded
		decoded += ref.decoded
		if spool != nil && i >= entryPages {
			if _, e := decodePage(spool, ref.rawOffset, ref.decoded); e != nil {
				return nil, nil, e
			}
		}
	}
	if end != footer.ManifestEncodedSize-footer.DirectoryEncodedSize || decoded != footer.ManifestDecodedSize {
		return fail()
	}
	return root, references[:entryPages], nil
}

type pageEntryReader struct {
	spool    *manifestSpool
	pages    []pageReference
	page     int
	entries  []any
	lastSize int
}

func (r *pageEntryReader) next() (any, error) {
	if len(r.entries) == 0 {
		if r.page == len(r.pages) {
			return nil, io.EOF
		}
		ref := r.pages[r.page]
		value, err := decodePage(r.spool, ref.rawOffset, ref.decoded)
		if err != nil {
			return nil, err
		}
		fields, err := schemaMap(value, []string{"entries"}, nil)
		if err != nil {
			return nil, categorized(CategoryMalformedTOC, err)
		}
		entries, ok := fields["entries"].([]any)
		if !ok || len(entries) == 0 {
			return nil, rejected(CategoryMalformedTOC, "empty entry page")
		}
		first, firstOK := entries[0].([]any)
		last, lastOK := entries[len(entries)-1].([]any)
		if !firstOK || !lastOK || len(first) == 0 || len(last) == 0 || last[0] != ref.lastPath {
			return nil, rejected(CategoryMalformedTOC, "page path range")
		}
		firstPath, ok := first[0].(string)
		if !ok || r.page > 0 && firstPath <= r.pages[r.page-1].lastPath {
			return nil, rejected(CategoryMalformedTOC, "page path range")
		}
		r.entries = entries
		r.page++
	}
	value := r.entries[0]
	r.entries[0] = nil
	r.entries = r.entries[1:]
	raw, err := manifestEncode(value)
	r.lastSize = len(raw)
	return value, err
}
