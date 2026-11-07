# Zstar external-content draft

**Status:** working draft; see SDK READMEs for support.

This optional extension to [Zstar core draft](core.md) defines the
`external-content` required feature, external extent sources, thin archive
pointer payloads, standalone content references, and reconstruction. Core
framing, deterministic CBOR, integer bounds, namespace, file-read rules, and
implementation profiles remain normative unless explicitly extended here. MUST,
MUST NOT, SHOULD, and MAY have their BCP 14 meanings.

An external object's address is SHA-256 of its complete raw content. Its Zstandard
storage or transport representation is separate from identity: recompression
preserves the address. Identical object bytes have the same address regardless of
reference kind; object type MUST NOT introduce a separate identity namespace.
The informative [CAS storage appendix](#appendix-cas-storage) explains this model.

## Declaration and extents

`external-content` MUST occur in `requires` exactly when any entry
uses object sources. A file's extents MUST be entirely archive-backed or
entirely external. Directories, symlinks, and empty files retain empty extents;
empty files remain inline. Core-only implementations MAY reject this feature.
Supporting readers MUST understand object addresses and decoded selections.

An object extent uses the core tuple `[size, source, encoded_size, ?offset]`,
with `source` the ASCII string `sha256:` followed by exactly 64 lowercase
hexadecimal digits, identifying the complete decoded object. `size` is positive
uint64; `offset` defaults to zero, which MUST be omitted. Checked `offset +
size` MUST fit uint64 and the actual complete decoded object length. An object
may hold one file, part of a file, or ranges from several files. Selections MAY
repeat or overlap. The logical file is their concatenation in extent order.

`encoded_size` is a positive uint64 advisory compressed-size hint. It MUST NOT
control allocation, truncate fetching, replace actual-byte limits, or reject an
equivalent recompressed object of a different length.

External files are regular tar members whose physical size equals the
serialized pointer JSON length, rather than the logical extent sum. Their
metadata still matches core's effective tar metadata. Full validation compares
pointer payloads with the manifest.

## External references and pointer JSON

### Standalone content references

A standalone external reference is the deterministic CBOR map `{"version": 1,
"extents": [object_extent, ...]}` using the same object extent tuples as
entries. It has no separate whole-file object ID or logical size. A reference
MUST contain one or more object extents; their checked size sum MUST fit uint64.
Readers MUST reject versions other than 1 and fields other than `version` and
`extents`. Empty content has no standalone reference and remains inline in
archives. Reference byte and extent-count limits are caller policy. Core's
deterministic CBOR and recursive value rules apply.

### Physical pointer payloads

The physical payload is a JSON document:

```json
{
  "version": "zstar-pointer-v1",
  "chunks": [
    {
      "source": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "size": 4096,
      "encoded_size": 1024
    },
    {
      "source": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "size": 2048,
      "encoded_size": 768,
      "offset": 4096
    }
  ]
}
```

Hashes are illustrative. The document has exactly two fields: `version`, the
exact string `"zstar-pointer-v1"`, and `chunks`, a nonempty array of objects.
Array order is reconstruction order. Repeated and overlapping selections are
allowed. Empty content stays inline and has no pointer.

Each chunk has the following required fields and optional `offset`:

| Field | Meaning |
| --- | --- |
| `source` | `sha256:` plus exactly 64 lowercase hexadecimal digits identifying the complete decoded object. |
| `size` | Positive uint64 count of decoded bytes to select. |
| `encoded_size` | Positive uint64 advisory compressed-size hint. |
| `offset` | Optional positive uint64 decoded-object offset; omitted means zero. Explicit zero is rejected. |

Numeric values MUST use JSON integer tokens with decimal digits only: no signs,
fractional parts, or exponents. Parsers MUST preserve exact uint64 values rather
than round through floating point. Checked `offset + size` and the sum of chunk
sizes MUST fit uint64. Every selection must fit its complete decoded object.
`encoded_size` remains advisory, including after recompression.

A pointer MUST be one UTF-8 JSON object without a BOM or trailing non-whitespace
bytes. Reject duplicate keys at every level, unknown fields, wrong types,
unsupported versions, invalid addresses, empty chunks, and invalid numbers.
Field names are case-sensitive after JSON escape decoding. JSON strings MUST
decode to Unicode scalar values; unpaired surrogate escapes are invalid.
Object member order and insignificant JSON whitespace have no meaning; no final
newline or canonical JSON serialization is required. Apply pointer-byte,
chunk-count, and parser budgets before unbounded allocation.

Only files with external extents are resolved as pointers. Inline files remain
literal even if their bytes are valid pointer JSON. Physical tar member size is
the serialized JSON byte length, not the reconstructed file size.
A zero-byte physical pointer payload is invalid even when its tar size and
padding are otherwise valid. Metadata-only and selective reads do not parse
physical pointer payloads; full archive validation MUST parse every one.

Full archive validation compares each parsed chunk with the corresponding
manifest object extent: `size`, `source`, `encoded_size`, and defaulted `offset`
must agree exactly, including chunk order and count. Validation does not require
canonical JSON bytes. A fetched object's current encoded length must not replace
its stored advisory hint during comparison.

Pointer decoders and full archive validators MUST reject non-JSON payloads.

## External object resolution

A resolver returns one complete Zstandard representation of an object, reports
it missing, or fails with a transport error. Encoded and decoded lengths MUST
be positive uint64 values and satisfy the caller's source budgets. The reader
MUST decode the complete representation, enforce input, output, and window
limits, and verify the complete object SHA-256 under the selected policy before
selecting bytes. A response MUST NOT mix object bytes or encoded
representations. Equivalent representations MAY differ between complete
lookups, including recompression with different encoded lengths, unless an
adapter promises a stronger snapshot. The SHA-256 address remains the object's
identity.

Storage providers MAY pack multiple external objects into shared storage. A
resolver MUST still return exactly one complete independent frame for each
object lookup and preserve the object identity, verification, and
resource-budget rules above. Packing does not change archive extents: source
addresses remain decoded-object hashes, selection offsets remain decoded-object
offsets, and encoded sizes remain advisory hints. Pack layouts, lookup indexes,
and pack-specific publication and retention policies are outside this
specification; archive conformance does not require packing support.

Missing objects, malformed encodings, hash failures, and local resource
rejections are distinct outcomes. Sources and caches follow the [core reader
profile](core.md#reader-implementation-profile).

Each representation MUST satisfy the [core frame
requirements](core.md#zstandard-frame-requirements) for independent Zstandard
sources, including framing, dictionary, reserved-bit, content-size, and
resource-budget rules. Its checksum is optional, but MUST be valid when
present. Unlike archive data and metadata frames, external representations do not require
checksums.

## External identity and verification

External identity is SHA-256 of the complete object bytes obtained by decoding
the resolver representation. Storage encoding is not part of identity; verified
object bytes MAY be reused after recompression. Cache identity MUST NOT use
encoded-size hints. Caches of encoded representations MUST also bind their
bytes, lengths, and ranges to the selected representation rather than assume an
object ID fixes an encoding. Resolvers and stores MUST isolate unverified
provenance; address equality alone MUST NOT promote unchecked bytes or
interpretation-specific validation results. Cached objects MUST retain
provenance supporting the declared verification policy.

External SHA-256 verification is opt-in and defaults to disabled. APIs MUST
offer an explicit opt-in that hashes each complete decoded external object and
checks it against its address. All other required checks remain enabled
regardless of this policy. Unhashed bytes MUST NOT be treated as hash-verified
without hashing the complete object. Failed promotion
MUST NOT expose unchecked bytes to verified callers.

Resolver attestation of object bytes does not establish local SHA-256
verification. Complete object bytes already locally hash-verified against an address MAY
be reused. Source framing, selection bounds, and any required interpretation
checks still apply. Each complete external lookup binds to one
immutable representation, even when hashing is disabled; the archive's snapshot
binding does not require identical external encodings across separate lookups.
Cache ownership, cancellation, and resource accounting follow the [core reader
profile](core.md#reader-implementation-profile).

## Validation and conformance

A conformant thin archive satisfies core and this extension even when
referenced objects are unavailable. A **complete external closure** supplies
valid, complete raw objects for every external extent. Implementations declare
supported classes:

| Class | Additional behavior |
| --- | --- |
| Thin writer | Emit a conformant archive and declare `external-content` exactly when object extents occur. |
| Metadata reader | Check declaration agreement, object extent schema, and checked logical sizes; no object fetch is required. |
| Selective reader | Validate each complete fetched object and its complete declared selections before returning requested bytes, under the selected hash policy. |
| Full archive validator | Parse every external pointer and compare all tuple values with entry extents; no external fetch is required. |
| External resolver/reconstructor | Supply or consume complete bounded encoded objects, validate selected raw objects, and reconstruct selections in order under the selected hash policy. |
| External-closure validator | Perform full archive validation and resolve every external selection. |

A zero-length valid file read fetches no objects but still checksum-checks and
validates the complete manifest. External-closure validation with SHA-256
verification disabled still checks framing and bounds, but MUST NOT claim a
verified closure. Full archive validation does not establish external
availability, verified closure, or content authenticity. Implementations MUST
distinguish missing objects, malformed encodings, integrity failures, transport
failures, and local resource rejections, and MUST NOT claim checks they did not
complete. Validation APIs MAY return only success or an error; the selected
scope and hash policy determine which checks success establishes.

## Security and retention

Resolving an object reveals all its decoded bytes, so producers SHOULD
partition objects by disclosure policy. Addresses contain no credentials or
transport URLs. Skipping external hashes loses content-address binding;
unchecked bytes MUST NOT be reported as verified. Digests establish
consistency, not publisher authenticity.

Retention MUST account for every retained archive or standalone reference using
an object. Keeping archive bytes alone does not retain their external objects.
Automatic external-object deletion MUST NOT proceed when those references cannot
be enumerated safely. External resolution requires no lease or publication API.

## Formal definitions

### Manifest and standalone reference schema

For archives declaring this extension, replace core's `file-entry` with the
production below; all other core productions and invariants remain unchanged.
Object-only extents are nonempty, and mixing archive and object sources in one
file is forbidden.

```cddl
file-entry = [
  path: archive-path, type: 0, entry-fields,
  extents: [* archive-extent] / [+ object-extent],
  ? attributes: extension-attributes
]
object-extent = [
  size: 1..18446744073709551615, source: object-address,
  encoded-size: 1..18446744073709551615, ? offset: 1..18446744073709551615
]
object-address = tstr .regexp "sha256:[0-9a-f]{64}"

content-reference = { "version": 1, "extents": [+ object-extent] }
```

### Physical pointer JSON schema

This CDDL describes JSON structure and value bounds. Its `object-address` rule
is defined above. The lexical integer, UTF-8, duplicate-key, optional-offset,
and checked-sum requirements in [Physical pointer payloads](#physical-pointer-payloads)
also apply. A conforming reader MUST NOT accept CBOR or positional arrays in
place of these JSON objects.

```cddl
content-pointer = {
  "version": "zstar-pointer-v1",
  "chunks": [+ pointer-chunk]
}
pointer-chunk = {
  "source": object-address,
  "size": 1..18446744073709551615,
  "encoded_size": 1..18446744073709551615,
  ? "offset": 1..18446744073709551615
}
```

## Normative references

- [Zstar core draft](core.md), including its CBOR, CDDL, ABNF, and Zstandard references
- [RFC 8259: JSON](https://www.rfc-editor.org/rfc/rfc8259)
- [FIPS 180-4: SHA-256](https://doi.org/10.6028/NIST.FIPS.180-4)

## Appendix: CAS storage

**Informative:** this appendix adds no conformance requirements.

Content-addressed storage (CAS) retrieves content by a digest of its bytes.
Zstar requires no particular backend layout, transport, or retention API.

### Object identity and storage representation

```text
object_id = "sha256:" + lowercase_hex(SHA-256(object_bytes))
```

For external content, object bytes are the complete decoded content; recompressing
its Zstandard representation preserves the address. Extent offsets select decoded
bytes, and encoded-size hints are advisory.

For an archive stored as an object, object bytes are the complete `.tar.zst`
file. Changing internal compression changes its address. The archive footer
stores metadata lengths and no cryptographic digest.

### Lookup and verification

The [resolver contract](#external-object-resolution) returns one complete immutable
Zstandard representation per lookup. Equivalent representations may differ between
lookups. [Verification and cache rules](#external-identity-and-verification) bind
encoded caches to a representation and distinguish unchecked from hash-verified bytes.

### Retention

Retain objects reachable from every retained archive or standalone reference.
See [security and retention](#security-and-retention) for deletion rules.
