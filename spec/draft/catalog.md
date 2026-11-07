# Zstar catalog proposal

**Status:** design proposal; not implemented by the SDKs.

A catalog is a named JSON document that organizes Zstar archives stored in
content-addressed storage (CAS). Each collection combines one or more archives
as layers. Slash-separated entry names group collections into folders, and
references to other catalogs let those folders contain further collections.

This profile uses [core](core.md) for layers and the
[CAS identity model](external-content.md#appendix-cas-storage) for references.
MUST, MUST NOT, SHOULD, and MAY have their BCP 14 meanings.

## File format

```json
{
  "version": "zstar-catalog-v1",
  "entries": [
    {
      "path": "analytics/events",
      "layers": [
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      ]
    },
    {
      "path": "shared",
      "catalog": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  ]
}
```

A document has exactly two fields:
`version`, the string `"zstar-catalog-v1"`, and `entries`, an array of entry objects.
An empty array represents an empty catalog. The order of entries does not
matter; the order within each `layers` array controls merging.

Each entry has exactly one of these forms:

| Entry | Fields |
| --- | --- |
| Collection | `path`, `layers`: a nonempty array of archive addresses; optional `base-path`. |
| Delegation | `path`, `catalog`: the address of another JSON catalog. |

Layers are applied from oldest to newest using the overlay rules below. If an
address appears more than once, its layer is applied each time. An empty
collection uses one empty layer.

Every entry MUST have a string `path` that satisfies core archive-path syntax
and byte limits. Catalog folders need no separate entries: `analytics/events`
implicitly creates the folder `analytics`.
Entry paths MUST be unique, and no entry path may be an ancestor of another:
`analytics` and `analytics/events` cannot coexist. Siblings such as
`analytics/events` and `analytics/users` are allowed. Prefix checks compare whole
path components: `a` and `ab` do not overlap. Reject duplicates and overlaps
regardless of entry order or kind.

`base-path` defaults to `.`, which exposes the whole merged tree. Any other
value must be a core archive path naming a directory in that tree. Selection
does not follow symlinks. Return paths relative to the selected directory;
reject a missing path or a path that is not a directory. The view has no root
directory metadata when `base-path` is `.`; otherwise its root metadata comes
from the selected directory.

A catalog MUST be one UTF-8 JSON object without a BOM or trailing non-whitespace
bytes. Reject duplicate keys at every level, unknown fields, other versions,
wrong field types, mixed entry forms, invalid names, and invalid addresses.
Field names are case-sensitive after JSON escape decoding. JSON strings MUST
decode to Unicode scalar values; unpaired surrogate escapes are invalid.
JSON object order, insignificant whitespace, and equivalent string escapes do
not affect interpretation. Canonical serialization is not required.

## Nesting and identity

The `shared` entry above mounts the referenced catalog under `shared`. If that
catalog contains an `events` collection, its full name is `shared/events`.
Delegations cannot have local children, layers, or `base-path`. A referenced
catalog may itself contain delegations. Nested references MUST be addresses,
never mutable names.

Every address is `sha256:` followed by exactly 64 lowercase hexadecimal digits:

```text
address = "sha256:" + lowercase_hex(SHA-256(object_bytes))
```

For `catalog`, object bytes are the complete UTF-8 JSON document. For `layers`,
they are the complete `.tar.zst` archive, including frames, manifest, and footer.
Reformatting JSON or recompressing an archive changes its address. The `catalog`
or `layers` field determines how the referenced bytes are interpreted; both use
the same address space. Any outer storage encoding is separate from object bytes.

A resolver returns an immutable, bounded source of object bytes, or
reports that the object is missing or the transport failed. Before exposing a
result derived from an object, catalog operations MUST verify its complete hash
or reuse bytes with proof of prior complete verification. The first archive
lookup therefore requires the whole object; later indexed reads may use a
verified cache. Checking the footer, directory, or even complete metadata does
not verify the archive address. An adapter using the external-content resolver
must decode the outer Zstandard representation to obtain object bytes.

Readers MUST detect a cycle when an address repeats along the active delegation
chain. Reusing an address in separate branches is valid. Implementations must
set local budgets for depth, total references, composed path length, JSON bytes,
metadata, and decompression. Enforce JSON byte and parser budgets while parsing,
before an unbounded object can be built in memory. Report budget exhaustion
separately from malformed input.

Layers MUST satisfy core and MAY use `external-content` if supported. No other
required extension is defined here. External payloads retain their existing
identity and verification policy: hashing a layer archive does not verify those
payloads.

## Overlay semantics

Each layer is an ordinary conformant Zstar archive using the following
whiteout convention. Core forbids device entries, so version 1 uses **empty
inline regular files**, not native OverlayFS character-device whiteouts or
xattrs. All path components beginning `.wh.` are reserved in layers. They cannot
be literal user data, directories, symlinks, or ancestors of other entries.

Two marker kinds are defined:

| Layer member | Effect on lower layers |
| --- | --- |
| `d/.wh.name` | Remove `d/name` and its complete subtree. |
| `d/.wh..wh..opq` | Remove all descendants of `d`, preserving `d` itself. |

At the archive root, use `.wh.name` and `.wh..wh..opq`. A normal whiteout target
MUST be a valid single core path component and MUST NOT begin `.wh.`. Thus `.wh.`,
`.wh..`, `.wh...`, and other reserved `.wh..wh.*` spellings except the exact
opaque marker are invalid. Marker entries MUST have zero size and no extents.
Marker mode, ownership, and time have no effect on the merged view. Whiteouts
are never returned as data entries.

Marker paths, including the `.wh.` prefix, obey core's 255-byte component and
4096-byte total path limits. Normal whiteouts therefore require target names of
at most 251 bytes and target paths of at most 4092 bytes. To delete a longer
name or path, use an opaque marker at an ancestor where it fits (the root always
fits), then recreate all descendants to retain, including their contents and
metadata, in the same layer.

To apply a layer to the current tree:

1. Interpret **all markers against the incoming lower tree**. Normal whiteouts
   remove their targets recursively; opaque markers remove descendants of their
   parent. Missing targets are harmless. A marker MUST NOT traverse a lower
   symlink or other non-directory. If its parent or an ancestor is absent or
   non-directory in the lower tree, its deletion has no effect; ordinary entries
   in this layer will establish the required directories.
2. Apply ordinary entries in ancestor-before-descendant order. A regular file or
   symlink replaces any existing entry and its descendants. A directory replaces
   an existing non-directory; over an existing directory it merges children.
   Explicit directory metadata always comes from the newer layer.
3. Return the resulting tree without marker entries. Continue with the next
   layer, then apply `base-path` once at the end.

The result MUST NOT depend on physical tar order or where markers sort by name.
Core requires explicit ancestor directories and forbids files or symlinks as
ancestors within a layer. Each layer therefore includes every directory its
entries need, and even a directory included only to hold a child replaces the
lower directory's metadata.

Deletion affects lower entries only: `d/.wh.x` and `d/x` in the same layer are
allowed and replace a lower subtree with the new `d/x`. An opaque directory can
also contain new children in that layer. Redundant markers are allowed. A later
layer can recreate a deleted path. An empty layer is a no-op, and a single empty
layer represents an empty collection at `base-path .`.

Symlink entries are preserved exactly and never followed during merge, lookup,
or base-path selection. A later layer can replace a lower symlink with a directory
and children. Export to a host filesystem must additionally apply core's
restoration and destination-confinement rules. This profile does not claim binary
compatibility with kernel OverlayFS upper directories or arbitrary OCI layers.

### Worked merge

```text
bottom                           top
data/                            data/
data/old.parquet                  data/.wh.old.parquet
data/keep.parquet                 data/new.parquet
data/tmp/                        data/tmp/
data/tmp/stale                    data/tmp/.wh..wh..opq
                                 data/tmp/fresh
```

The result is `data/keep.parquet`, `data/new.parquet`, and `data/tmp/fresh`,
with their directories. `base-path data` exposes these as `keep.parquet`,
`new.parquet`, and `tmp/fresh`.

## Publication and retention

The catalog name is a mutable location containing the JSON document directly.
A reader MUST bind to one immutable revision of its bytes for the whole
operation. All references within it are pinned by hash.

Publish external payloads first, then layers and nested catalogs, and finally
atomically replace the named JSON document. Publication SHOULD check closure
availability first. Updates MUST use a conditional write against the observed
revision or equivalent compare-and-swap; initial creation requires the name to
be absent. Conflicts fail instead of silently losing an update. The storage
revision is an opaque concurrency token, not necessarily a content hash. A
backend without conditional atomic replacement cannot claim this publication
contract; last-writer-wins requires explicit application policy.

Historical snapshots MAY retain the exact published JSON bytes in CAS. The
format requires no history or parent field. Delete or rename a collection by
publishing a new document with its entry removed or renamed.

Retention roots include published names, retained history, and active readers.
Traverse every delegation, every listed layer, and all external-content
references, including hidden data. Whiteouts do not authorize physical deletion.
Garbage collection must coordinate with uploads, publication, and active readers;
if references cannot be enumerated safely, automatic deletion MUST NOT proceed.

Compaction MAY replace layers with one equivalent layer preserving visible
paths, bytes, links, and metadata under the same base projection. This publishes
a new snapshot; old layers remain retained while reachable from any root.

## Validation and conformance

| Class | Required behavior |
| --- | --- |
| Catalog reader/writer | Validate or emit the complete local JSON schema and nonoverlapping path namespace. |
| View resolver | Resolve pinned delegations and layers, verify archive identities, merge entries, and select the base path. |
| Publisher | Publish dependencies before atomically updating a name with a revision precondition. |

Conformance to one class does not imply conformance to another.

Distinguish invalid JSON/schema, invalid archive, unsupported feature, missing
reference, transport error, integrity failure, invalid base path, delegation
cycle, publication conflict, and budget rejection. An invalid or unavailable
upper layer MUST fail resolution, never expose a lower fallback view. Hashes
provide consistency, not publisher authenticity or access control.

Catalog structure validation parses the document and validates all local entries
without fetching references. View resolution additionally verifies the required
object hashes, completes core metadata validation for participating layers,
validates their whiteout constraints, and merges the requested collection.
Directory-only or selective page validation is insufficient to establish a
layer's complete namespace and whiteout effects. Readers MUST report only checks
they have completed. Reading one collection does not validate unrelated delegations or
payloads.

### Implementation plan

Before declaring support, add shared positive and negative fixtures under
`testdata/` and Rust, Go, and Python coverage for:

- Empty catalogs, implicit folders, and duplicate or ancestor entry-path rejection.
- Sibling paths and non-component string prefixes such as `a` and `ab`.
- Duplicate JSON keys, unknown fields, wrong types, bad versions and addresses.
- Layer precedence, repeated layers, all directory/file/symlink replacements.
- Whiteouts, opacity, same-layer recreation, and tar-order independence.
- Invalid marker types, contents, targets, and ancestors.
- Marker component and total path limits, including opaque deletion and
  recreation of retained entries when a normal whiteout cannot fit.
- Post-merge base selection, including deleted and symlink base paths.
- Delegation reuse, nesting, cycle detection, and budget exhaustion.
- Missing/corrupt layers and nested catalogs, without fallback.
- Valid archive hashes with missing or corrupt external payloads.
- Pinned readers during replacement and conflicting conditional updates.

## Formal definitions

This CDDL describes the JSON structure. `archive-path` and `object-address`
use core archive-path syntax and the external-content address syntax. JSON
scalar-value, duplicate-key, namespace-overlap, and base-path selection rules
above apply in addition to this schema. JSON objects cannot be replaced by
positional arrays.

```cddl
catalog = {
  "version": "zstar-catalog-v1",
  "entries": [* (collection / delegation)]
}
collection = {
  "path": archive-path,
  "layers": [+ object-address],
  ? "base-path": "." / archive-path
}
delegation = {
  "path": archive-path,
  "catalog": object-address
}
```

## Normative references

- [Zstar core draft](core.md)
- [External content and CAS identity](external-content.md)
- [RFC 8259: JSON](https://www.rfc-editor.org/rfc/rfc8259)
- [RFC 8610: CDDL](https://www.rfc-editor.org/rfc/rfc8610)
- [FIPS 180-4: SHA-256](https://doi.org/10.6028/NIST.FIPS.180-4)
