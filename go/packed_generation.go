package zstar

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
)

// PackedGeneration is the final immutable publication point. Provenance is opaque.
type PackedGeneration struct {
	Archive, Catalog PackSnapshot
	Provenance       map[string]any
}

// Encode emits a bounded deterministic generation descriptor.
func (g PackedGeneration) Encode() ([]byte, error) {
	if err := g.Archive.Validate(); err != nil {
		return nil, err
	}
	if err := g.Catalog.Validate(); err != nil {
		return nil, err
	}
	p := g.Provenance
	if p == nil {
		p = map[string]any{}
	}
	raw, err := manifestEncode([]any{uint64(1), g.Archive.value(), g.Catalog.value(), p})
	if err != nil {
		return nil, err
	}
	if len(raw) > 64<<10 {
		return nil, rejected("resource-limit", "generation descriptor")
	}
	return raw, nil
}

// DecodePackedGeneration validates descriptor encoding and snapshot identities.
func DecodePackedGeneration(raw []byte) (PackedGeneration, error) {
	var g PackedGeneration
	if len(raw) > 64<<10 {
		return g, rejected("resource-limit", "generation descriptor")
	}
	var value any
	if err := manifestDecoder.Unmarshal(raw, &value); err != nil {
		return g, rejected("malformed-catalog", "generation CBOR")
	}
	canonical, err := manifestEncode(value)
	if err != nil || !bytes.Equal(raw, canonical) {
		return g, rejected("malformed-catalog", "generation canonical CBOR")
	}
	v, err := packedTuple(value, 4)
	if err != nil {
		return g, err
	}
	version, err := packedUint(v[0])
	if err != nil {
		return g, err
	}
	if version != 1 {
		return g, rejected("unsupported-version", "packed generation")
	}
	g.Archive, err = packedSnapshot(v[1])
	if err != nil {
		return g, err
	}
	g.Catalog, err = packedSnapshot(v[2])
	if err != nil {
		return g, err
	}
	m, ok := v[3].(map[any]any)
	if !ok {
		return g, rejected("malformed-catalog", "provenance map")
	}
	g.Provenance = make(map[string]any, len(m))
	for k, value := range m {
		key, ok := k.(string)
		if !ok {
			return g, rejected("malformed-catalog", "provenance key")
		}
		g.Provenance[key] = value
	}
	return g, nil
}

// PublicationError reports completed immutable artifacts; shared objects are never deleted.
type PublicationError struct {
	Cause     error
	Completed []PackSnapshot
}

func (e *PublicationError) Error() string {
	return "zstar: generation publication failed: " + e.Cause.Error()
}
func (e *PublicationError) Unwrap() error { return e.Cause }

// PublishPackedGeneration validates a finalized archive and publishes catalog,
// archive, then descriptor. Seekable input remains caller-owned.
func PublishPackedGeneration(ctx context.Context, store PackStore, prefix string, archive io.ReadSeeker, catalog *PackedCatalog, provenance map[string]any) (descriptor PackSnapshot, generation PackedGeneration, err error) {
	completed := make([]PackSnapshot, 0, 3)
	defer func() {
		if err != nil {
			err = &PublicationError{err, completed}
		}
	}()
	if store == nil || archive == nil || !validPath(prefix) {
		return descriptor, generation, errors.New("zstar: invalid publication options")
	}
	length, err := archive.Seek(0, io.SeekEnd)
	if err != nil || length < 0 {
		return descriptor, generation, errors.New("zstar: invalid archive size")
	}
	manifest, err := OpenManifest(ctx, &publicationSource{reader: archive, length: length}, ManifestOptions{})
	if err != nil {
		return descriptor, generation, err
	}
	_, err = manifest.ValidateArchive(ctx)
	closeErr := manifest.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return descriptor, generation, err
	}
	encoded, err := catalog.Encode(CatalogLimits{})
	if err != nil {
		return descriptor, generation, err
	}
	generation.Catalog, err = putPackedArtifact(ctx, store, prefix, "catalogs", "cbor", bytes.NewReader(encoded), uint64(len(encoded)))
	if err != nil {
		return descriptor, generation, err
	}
	completed = append(completed, generation.Catalog)
	if _, err = archive.Seek(0, io.SeekStart); err != nil {
		return descriptor, generation, err
	}
	generation.Archive, err = putPackedArtifact(ctx, store, prefix, "archives", "zstar", archive, uint64(length))
	if err != nil {
		return descriptor, generation, err
	}
	completed = append(completed, generation.Archive)
	generation.Provenance = provenance
	encoded, err = generation.Encode()
	if err != nil {
		return descriptor, generation, err
	}
	descriptor, err = putPackedArtifact(ctx, store, prefix, "generations", "cbor", bytes.NewReader(encoded), uint64(len(encoded)))
	return descriptor, generation, err
}

type publicationSource struct {
	mu     sync.Mutex
	reader io.ReadSeeker
	length int64
}

func (s *publicationSource) Size(context.Context) (int64, error) { return s.length, nil }
func (s *publicationSource) ReadRange(ctx context.Context, offset, length int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 || offset > s.length || length > s.length-offset || length > 256<<20 {
		return nil, rejected("invalid-range", "publication archive range")
	}
	if _, err := s.reader.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, int(length))
	_, err := io.ReadFull(s.reader, data)
	return data, err
}

func putPackedArtifact(ctx context.Context, store PackStore, prefix, directory, suffix string, input io.ReadSeeker, length uint64) (PackSnapshot, error) {
	var out PackSnapshot
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, input, make([]byte, 64<<10)); err != nil {
		return out, err
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return out, err
	}
	key := prefix + "/" + directory + "/" + hex.EncodeToString(hash.Sum(nil)) + "." + suffix
	snapshot, err := store.PutPack(ctx, key, input, length)
	if err != nil {
		return out, err
	}
	if err = snapshot.Validate(); err != nil {
		return out, err
	}
	if snapshot.Key != key || snapshot.Length != length {
		return out, rejected("changed-pack", "artifact identity")
	}
	return snapshot, nil
}
