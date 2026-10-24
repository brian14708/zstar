package zstar

import (
	"bytes"
	"errors"
	"sort"
	"strings"
)

// PackSnapshot records a relative key and version and/or strong ETag.
type PackSnapshot struct {
	Key           string
	Length        uint64
	Version, ETag string
}

// Validate checks relative paths and immutable snapshot identity.
func (s PackSnapshot) Validate() error {
	if !validPath(s.Key) || len(s.Key) > 4096 || s.Length == 0 || len(s.Version) > 4096 || len(s.ETag) > 4096 || (s.Version == "" || s.Version == "null") && !packedStrongETag(s.ETag) || s.ETag != "" && !packedStrongETag(s.ETag) {
		return rejected("malformed-catalog", "invalid snapshot")
	}
	return nil
}

func packedStrongETag(s string) bool {
	if len(s) < 2 || !strings.HasPrefix(s, "\"") || !strings.HasSuffix(s, "\"") {
		return false
	}
	for _, b := range []byte(s[1 : len(s)-1]) {
		if b != 0x21 && (b < 0x23 || b == 0x7f) {
			return false
		}
	}
	return true
}
func (s PackSnapshot) value() []any { return []any{s.Key, s.Length, s.Version, s.ETag} }

// PackRecord identifies one complete encoded pack by SHA-256.
type PackRecord struct {
	ID       [32]byte
	Snapshot PackSnapshot
}

// PackedChunk locates one complete frame; Digest hashes its decoded bytes.
type PackedChunk struct {
	Digest, Pack                         [32]byte
	Offset, EncodedLength, DecodedLength uint64
}

// CatalogLimits bounds linear catalog loading, including parsing scratch.
type CatalogLimits struct {
	MaxEncodedBytes, MaxMemoryBytes uint64
	MaxPacks, MaxChunks             int
}

func (l CatalogLimits) normalized() CatalogLimits {
	if l.MaxEncodedBytes == 0 {
		l.MaxEncodedBytes = 16 << 20
	}
	if l.MaxMemoryBytes == 0 {
		l.MaxMemoryBytes = 512 << 20
	}
	if l.MaxPacks == 0 {
		l.MaxPacks = 65536
	}
	if l.MaxChunks == 0 {
		l.MaxChunks = 262144
	}
	return l
}

// PackedCatalog owns immutable sorted records and bounded lookup indexes.
type PackedCatalog struct {
	packs   []PackRecord
	chunks  []PackedChunk
	byPack  map[[32]byte]int
	byChunk map[[32]byte]int
}

// NewPackedCatalog copies, sorts and bounds immutable catalog records.
func NewPackedCatalog(packs []PackRecord, chunks []PackedChunk, limits CatalogLimits) (*PackedCatalog, error) {
	limits = limits.normalized()
	if limits.MaxPacks < 0 || limits.MaxChunks < 0 || len(packs) > limits.MaxPacks || len(chunks) > limits.MaxChunks {
		return nil, rejected("resource-limit", "catalog records")
	}
	cost := uint64(len(chunks)) * 256
	for _, pack := range packs {
		if err := pack.Snapshot.Validate(); err != nil {
			return nil, err
		}
		cost += uint64(256 + len(pack.Snapshot.Key)*2 + len(pack.Snapshot.Version) + len(pack.Snapshot.ETag))
	}
	if cost > limits.MaxMemoryBytes/2 {
		return nil, rejected("resource-limit", "catalog memory")
	}
	c := &PackedCatalog{packs: append([]PackRecord(nil), packs...), chunks: append([]PackedChunk(nil), chunks...), byPack: make(map[[32]byte]int, len(packs)), byChunk: make(map[[32]byte]int, len(chunks))}
	sort.Slice(c.packs, func(i, j int) bool { return bytes.Compare(c.packs[i].ID[:], c.packs[j].ID[:]) < 0 })
	sort.Slice(c.chunks, func(i, j int) bool { return bytes.Compare(c.chunks[i].Digest[:], c.chunks[j].Digest[:]) < 0 })
	keys := make(map[string]bool, len(packs))
	for i, pack := range c.packs {
		if _, ok := c.byPack[pack.ID]; ok || keys[pack.Snapshot.Key] {
			return nil, rejected("malformed-catalog", "duplicate pack")
		}
		c.byPack[pack.ID] = i
		keys[pack.Snapshot.Key] = true
	}
	ranges := make(map[[32]byte][]PackedChunk)
	for i, chunk := range c.chunks {
		index, ok := c.byPack[chunk.Pack]
		if !ok {
			return nil, rejected("malformed-catalog", "unknown pack")
		}
		length := c.packs[index].Snapshot.Length
		if chunk.EncodedLength == 0 || chunk.EncodedLength > 17<<20 || chunk.DecodedLength == 0 || chunk.DecodedLength > 16<<20 || chunk.Offset > length || chunk.EncodedLength > length-chunk.Offset {
			return nil, rejected("malformed-catalog", "chunk bounds")
		}
		if _, ok := c.byChunk[chunk.Digest]; ok {
			return nil, rejected("malformed-catalog", "duplicate chunk")
		}
		c.byChunk[chunk.Digest] = i
		ranges[chunk.Pack] = append(ranges[chunk.Pack], chunk)
	}
	for _, group := range ranges {
		sort.Slice(group, func(i, j int) bool { return group[i].Offset < group[j].Offset })
		for i := 1; i < len(group); i++ {
			if group[i-1].Offset+group[i-1].EncodedLength > group[i].Offset {
				return nil, rejected("malformed-catalog", "overlapping frames")
			}
		}
	}
	return c, nil
}

// Packs returns an owned copy of the sorted pack records.
func (c *PackedCatalog) Packs() []PackRecord { return append([]PackRecord(nil), c.packs...) }

// Chunks returns an owned copy of the sorted chunk records.
func (c *PackedCatalog) Chunks() []PackedChunk { return append([]PackedChunk(nil), c.chunks...) }

// Encode emits deterministic CBOR within the supplied resource limits.
func (c *PackedCatalog) Encode(limits CatalogLimits) ([]byte, error) {
	if c == nil {
		return nil, errors.New("zstar: nil packed catalog")
	}
	limits = limits.normalized()
	if _, err := NewPackedCatalog(c.packs, c.chunks, limits); err != nil {
		return nil, err
	}
	packs := make([]any, len(c.packs))
	chunks := make([]any, len(c.chunks))
	for i, pack := range c.packs {
		packs[i] = []any{pack.ID[:], pack.Snapshot.value()}
	}
	for i, chunk := range c.chunks {
		chunks[i] = []any{chunk.Digest[:], uint64(c.byPack[chunk.Pack]), chunk.Offset, chunk.EncodedLength, chunk.DecodedLength}
	}
	raw, err := manifestEncode([]any{uint64(1), packs, chunks})
	if err != nil {
		return nil, err
	}
	if uint64(len(raw)) > limits.MaxEncodedBytes || uint64(len(raw)) > limits.MaxMemoryBytes/128 {
		return nil, rejected("resource-limit", "catalog encoded bytes or parsing memory")
	}
	return raw, nil
}

func packedTuple(value any, n int) ([]any, error) {
	fields, ok := value.([]any)
	if !ok || len(fields) != n {
		return nil, rejected("malformed-catalog", "tuple length")
	}
	return fields, nil
}

func packedUint(value any) (uint64, error) {
	n, ok := value.(uint64)
	if !ok {
		return 0, rejected("malformed-catalog", "uint64")
	}
	return n, nil
}

func packedDigest(value any) ([32]byte, error) {
	var out [32]byte
	b, ok := value.([]byte)
	if !ok || len(b) != 32 {
		return out, rejected("malformed-catalog", "digest bytes")
	}
	copy(out[:], b)
	return out, nil
}

func packedSnapshot(value any) (PackSnapshot, error) {
	var out PackSnapshot
	v, err := packedTuple(value, 4)
	if err != nil {
		return out, err
	}
	var ok bool
	out.Key, ok = v[0].(string)
	if !ok {
		return out, rejected("malformed-catalog", "snapshot key")
	}
	out.Length, err = packedUint(v[1])
	if err != nil {
		return out, err
	}
	out.Version, ok = v[2].(string)
	if !ok {
		return out, rejected("malformed-catalog", "snapshot version")
	}
	out.ETag, ok = v[3].(string)
	if !ok {
		return out, rejected("malformed-catalog", "snapshot ETag")
	}
	return out, out.Validate()
}

// DecodePackedCatalog validates canonical CBOR, ordering, identity and ranges.
func DecodePackedCatalog(raw []byte, limits CatalogLimits) (*PackedCatalog, error) {
	limits = limits.normalized()
	if uint64(len(raw)) > limits.MaxEncodedBytes || uint64(len(raw)) > limits.MaxMemoryBytes/128 {
		return nil, rejected("resource-limit", "catalog encoded bytes or parsing memory")
	}
	if manifestDecoderError != nil {
		return nil, manifestDecoderError
	}
	var value any
	if err := manifestDecoder.Unmarshal(raw, &value); err != nil {
		return nil, rejected("malformed-catalog", "invalid deterministic CBOR")
	}
	canonical, err := manifestEncode(value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, rejected("malformed-catalog", "noncanonical CBOR")
	}
	fields, err := packedTuple(value, 3)
	if err != nil {
		return nil, err
	}
	version, err := packedUint(fields[0])
	if err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, rejected("unsupported-version", "packed catalog")
	}
	ps, ok := fields[1].([]any)
	if !ok {
		return nil, rejected("malformed-catalog", "pack array")
	}
	cs, ok := fields[2].([]any)
	if !ok {
		return nil, rejected("malformed-catalog", "chunk array")
	}
	if len(ps) > limits.MaxPacks || len(cs) > limits.MaxChunks {
		return nil, rejected("resource-limit", "catalog records")
	}
	packs := make([]PackRecord, len(ps))
	chunks := make([]PackedChunk, len(cs))
	for i, value := range ps {
		v, err := packedTuple(value, 2)
		if err != nil {
			return nil, err
		}
		packs[i].ID, err = packedDigest(v[0])
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(packs[i-1].ID[:], packs[i].ID[:]) >= 0 {
			return nil, rejected("malformed-catalog", "pack ordering")
		}
		packs[i].Snapshot, err = packedSnapshot(v[1])
		if err != nil {
			return nil, err
		}
	}
	for i, value := range cs {
		v, err := packedTuple(value, 5)
		if err != nil {
			return nil, err
		}
		chunks[i].Digest, err = packedDigest(v[0])
		if err != nil {
			return nil, err
		}
		if i > 0 && bytes.Compare(chunks[i-1].Digest[:], chunks[i].Digest[:]) >= 0 {
			return nil, rejected("malformed-catalog", "chunk ordering")
		}
		index, err := packedUint(v[1])
		if err != nil || index >= uint64(len(packs)) {
			return nil, rejected("malformed-catalog", "pack index")
		}
		chunks[i].Pack = packs[index].ID
		chunks[i].Offset, err = packedUint(v[2])
		if err != nil {
			return nil, err
		}
		chunks[i].EncodedLength, err = packedUint(v[3])
		if err != nil {
			return nil, err
		}
		chunks[i].DecodedLength, err = packedUint(v[4])
		if err != nil {
			return nil, err
		}
	}
	return NewPackedCatalog(packs, chunks, limits)
}
