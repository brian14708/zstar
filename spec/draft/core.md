# Zstar core draft

**Status:** working draft.

Zstar is an indexed tar stream in independent Zstandard frames. This
specification defines the core wire format, validation, and implementation
profiles. Core archives keep file content in archive data frames. The
[external-content extension](external-content.md) defines thin archives,
JSON content-pointer payloads, standalone CBOR content references, and
reconstruction. It retains footer profile
and manifest version 1. The informative [CAS storage appendix](external-content.md#appendix-cas-storage)
explains object identity and storage representations.

The [catalog proposal](catalog.md) describes a separate JSON document naming
collections of archived layers. Catalog JSON is not an archive manifest or a
required core extension. Ordinary archive readers do not interpret catalogs or
apply whiteouts; `.wh.*` members remain ordinary files outside catalog operations.

[Wire format](#wire-format) · [Validation](#validation-and-failures) ·
[Profiles](#implementation-profiles) · [Security](#security-considerations) ·
[Schema and grammar](#formal-definitions)

## Terminology and conformance

A **conformant archive** satisfies all layout, frame, tar, manifest, and
cross-field rules in this document and every declared required extension. A
valid archive may contain entries that a host filesystem cannot represent.

Implementations declare which conformance classes they support:

| Class | Minimum behavior |
| --- | --- |
| Portable writer | Emit a conformant archive with only archive-backed file extents. |
| Metadata reader / full metadata validator | Authenticate all decoded metadata, validate the complete deterministic CBOR sequence, schema, namespace and required features. |
| Selective reader | Perform metadata-reader checks and validate each selected complete source and extent before returning its requested bytes. |
| Full archive validator | Perform full metadata validation and validate every physical frame, tar member, payload mapping and end record, plus declared extension checks. |
| Filesystem capturer/restorer | Follow the normative [filesystem profile](#filesystem-capture-and-restoration-profile), in addition to the applicable writer/reader classes. |

Core-only implementations MAY reject extension features. Implementations that
accept a required feature MUST apply its specification in addition to core.
Unknown required features MUST be rejected before interpreting affected
entries.

An operation establishes only the checks it completes. Metadata validation and
selective reads do not establish full archive validity. Full archive validation
does not establish content authenticity or availability of extension
dependencies. Implementations MUST NOT claim checks they did not complete.
Rejection requirements apply to the structures and dependencies being
validated; selective readers need not inspect unrelated tar regions.

## Conventions

MUST, MUST NOT, SHOULD, and MAY have the BCP 14 meanings defined by RFC 2119
and RFC 8174. Wire definitions below are normative.

Offsets are zero-based; ranges are half-open. Sizes are measured in bytes. MiB
means 1048576 bytes. An archive or encoded source length MUST fit uint64.
Integers are unsigned 64-bit unless otherwise specified; `mtime_ns` is signed
64-bit nanoseconds since the Unix epoch. Booleans are not integers. All
arithmetic, rounding, and cumulative sums MUST be checked for overflow before
allocation, addressing, or slicing.

Byte positions use three coordinates:

| Coordinate | Origin and use |
| --- | --- |
| Encoded archive position | Byte zero of the archive; footer manifest offsets and integer extent sources use this coordinate. |
| Decoded tar position | Byte zero of the concatenated data-frame output; member headers, payloads and physical coverage use this coordinate. |
| Decoded source selection | Byte zero of one complete decoded frame or object; an extent's optional `offset` uses this coordinate. |

A file-read offset addresses the logical bytes formed by concatenating that
file's extent selections.

`roundUp(n, 512)` rounds `n` up to a multiple of 512; `roundUp(0, 512) = 0`.
The result MUST fit the address space and format limits.

## Wire format

### Layout

```text
[independent Zstandard data frames encoding one tar stream]
[metadata skippable frames containing segments of a compressed CBOR sequence]
[80-byte terminal skippable-frame footer]
```

Data frames occupy every byte before the metadata region. The metadata region
immediately precedes the footer, which ends the archive. No other skippable
frames or gaps are permitted.

#### Zstandard frame requirements

Each data frame or compressed metadata segment contains exactly one
independently decodable Zstandard frame. Skippable frames, concatenated frames,
trailing bytes, and external dictionaries are forbidden within a source range.
Dictionary IDs MUST be absent or zero.

The format sets no fixed encoded-frame, decoded-frame, or window byte limit.
Encoded and decoded lengths MUST fit uint64. Readers and writers MAY impose
caller-configurable byte, window, extent-count, and metadata-item budgets.
Exceeding a configured budget is a local resource rejection, not malformed
input. Readers MUST enforce their selected budgets before exceeding input,
output, or codec allocation limits, including for single-segment frames. Codec
and host address-space limitations are implementation limitations, not wire
maxima. Metadata payload lengths must additionally fit their uint32 wrapper
field.

Data frames require checksums. Metadata checksums and all content-size fields
are optional; when present they MUST be valid. A declared content size MUST
equal the actual decoded length. Every frame MUST use standard magic
`0xFD2FB528` and RFC 8878 reserved-bit, block, and frame encodings. The
Frame_Header_Descriptor reserved bit (bit 3) MUST be zero. Readers MUST ignore
its unused bit (bit 4), as RFC 8878 specifies.

#### Decoded tar stream

Tar members, PAX sequences, records, payloads, and padding MAY cross frame
boundaries. Several members MAY share a frame. Every data frame decodes to at
least one byte. The concatenated output MUST end with exactly two zero 512-byte
records and no trailing bytes. An empty archive has at least one data frame
encoding those 1024 zero bytes.

#### Footer

The footer is exactly 80 bytes. Integer fields are unsigned little-endian;
offsets are absolute encoded archive positions.

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 4 | Skippable magic `0x184D2A5F` |
| 4 | 4 | Payload length: 72 |
| 8 | 8 | ASCII `ZSTAR001` |
| 16 | 4 | Profile version: 1 |
| 20 | 4 | Reserved: zero |
| 24 | 8 | Start of the manifest region |
| 32 | 8 | Total encoded manifest region length, including every wrapper |
| 40 | 8 | Sum of decoded CBOR segment lengths |
| 48 | 32 | SHA-256 of concatenated decoded CBOR segment bytes, in physical order |

For archive length `N`, manifest offset `T`, and region length `L`, require `N
>= 80`, `T > 0`, `L > 8`, and `T + L = N - 80`, using checked arithmetic.
Reject incorrect magic, reserved values, versions, or digests, and any
inconsistent fields or bounds.

#### Metadata region

Metadata wrappers exactly cover the manifest region. Each wrapper has an 8-byte
header followed immediately by one Zstandard payload. Its header fields are
little-endian uint32: magic `0x184D2A5E` and the encoded payload length.

Apply local aggregate and per-frame budgets before fetching. Each segment must
fit its uint32 wrapper length. Decode and hash all segments in physical order,
checking the aggregate decoded length and SHA-256 against the footer before
using the manifest. The decoded sequence follows the [manifest
rules](#entry-oriented-manifest).

#### Recognition and media type

Readers MUST validate the terminal footer and manifest before recognizing a
file as Zstar. A filename extension or media type is insufficient. V1 uses
`application/zstd`; generic `application/octet-stream` is acceptable. No Zstar
media type is registered. Ordinary Zstandard/tar tools can decode the tar
stream but do not interpret the index. Thin-content resolution follows the
[external-content extension](external-content.md).

### Tar profile

Use POSIX ustar with at most one nonempty local PAX header (`x`) immediately
before a member. Supported member types are regular file (`0` or NUL),
directory (`5`), and symlink (`2`). V1 excludes global PAX headers, hard links,
sparse files, devices, FIFOs, xattrs, and all other types. Writers MUST reject
unsupported input or explicitly convert it at the caller's request; readers
MUST reject unsupported encodings.

Numeric ustar fields contain octal digits with optional leading and trailing
ASCII spaces. A field may end with a NUL terminator or fill its width exactly;
only NUL or spaces may follow the digits. An empty numeric field or one
containing only spaces represents zero. Signs and GNU base-256 are forbidden.
Values MUST fit the applicable numeric range.

Header checksums sum unsigned bytes, treating the checksum field as spaces.
Owner and group display names are empty. Device major/minor fields MUST be zero
for every supported kind. Non-symlink member link fields and local PAX header
link fields MUST be empty. The `x` header has a valid checksum, its size is the
PAX payload length, and its name, mode, uid, gid and mtime are ignored. Header
and payload padding is zero.

Ustar path, prefix and link fields are UTF-8 with zero padding after any NUL
terminator. The path is `prefix + "/" + name` when the prefix is nonempty, and
`name` otherwise. PAX `path` replaces both prefix and name; PAX `linkpath`
replaces the ustar symlink target. Underlying fields require valid UTF-8 and
padding even when overridden. PAX supplies values that do not fit ustar. A
complete local PAX/member-header sequence is at most 1 MiB.

PAX permits only `path`, `linkpath`, `size`, `uid`, `gid`, and `mtime`. Reject
unknown or duplicate keys. Records use POSIX `<length> <key>=<value>` followed
by LF; the decimal length includes every byte of the record. Records MUST
consume the exact PAX payload length, excluding zero padding.

The [lexical grammar](#lexical-grammar) defines `pax-uint` and `pax-mtime`.
`size`, `uid` and `gid` accept one or more decimal digits, including leading
zeros, and MUST fit uint64. Signs, decimal points, and whitespace are
forbidden.

`mtime` permits an optional leading minus sign, one or more integer digits, and
an optional decimal point followed by one or more fractional digits. Leading
zeros and negative zero are accepted. A plus sign, `.5`, `1.`, exponents and
whitespace are rejected. Interpret `mtime` as exact decimal seconds since the
Unix epoch: its nanosecond value MUST be integral without rounding and fit
int64. Fractional digits after the ninth are accepted only when all are zero.
Ustar times are integral seconds. Overridden ustar fields still require valid
syntax, but need not equal their PAX values.

Apply PAX overrides before locating payloads or comparing with the manifest.
The effective path, kind, mode, uid, gid, mtime, and symlink target MUST match
the manifest entry. Core file size equals the extent sum. Extensions define any
alternative payload representation and its agreement with the manifest.

Effective tar directory names have exactly one trailing `/`; remove it before
comparing with the manifest path. Directory and symlink effective sizes MUST be
zero; neither kind carries a payload. `linkpath` applies only to symlinks.

#### Paths

Archive paths are relative UTF-8, compared bytewise without Unicode
normalization. They use `/` separators and contain no NUL bytes, empty
components, `.` or `..` components, leading `/`, or trailing `/`. Manifest
paths are limited to 4096 bytes, with at most 255 bytes per component. A tar
directory name may be one byte longer for its trailing `/`. No root-directory
entry is stored.

Paths MUST be unique. Every ancestor directory MUST have an explicit entry; a
regular file or symlink cannot be an ancestor. Physical member order is
independent of manifest path order.

#### Symlinks

Symlink targets are nonempty UTF-8 text without NUL, at most 4096 bytes.
Writers and readers MUST preserve target bytes without normalization or
rewriting. Absolute targets, dangling links, cycles, and targets containing `.`
or `..` components, repeated slashes, or trailing slashes are permitted.
Archive-path component restrictions do not apply to symlink targets.

Lookup returns the entry at the exact archive path without following symlinks.
A read-link operation returns that entry's stored symlink target (`linkname`);
it MUST reject entries of other kinds. File reads MUST reject directory and
symlink entries. Automatic symlink resolution is outside v1.

### Entry-oriented manifest

The [manifest schema](#manifest-schema) defines sequence, entry, and extent
structures. This section defines encoding and cross-field rules.

The metadata region encodes a deterministic CBOR sequence (RFC 8742). Every
item uses RFC 8949 core deterministic encoding. Emit exactly one header map,
followed by zero or more entry tuples:

```text
{"version": 1, "requires": [...], "x-...": ...}
entry
entry
...
```

An empty manifest contains only the header. Entries MUST be strictly sorted by
UTF-8 path bytes across the entire sequence; physical tar order may differ. The
sequence has no enclosing array, entry count, end marker, pages, page
references, or global frame or object tables. Readers derive the entry count
and may build a lookup index while parsing, but MUST NOT use it until the
complete sequence is authenticated and validated.

Each [metadata wrapper](#metadata-region) contains a frame that decodes to a
nonempty sequence segment. Segment boundaries MAY split any item, including its
CBOR header. Readers MUST parse incrementally across segments and reject
malformed or truncated items, a missing or repeated header, enclosing arrays,
scalars in place of entries, and trailing items or bytes that do not form valid
entry tuples.

The footer's decoded length and SHA-256 cover all decoded segments concatenated
in physical order; there are no per-segment hashes. Readers MUST authenticate
and validate the entire sequence before indexed reads. Aggregate metadata and
parser limits are local policy. The wrapper length does not bound the decoded
sequence or individual items.

The CDDL `manifest` group describes consecutive sequence items. Schema tools
that require one value may validate a temporary `[manifest]` array; this array
is never encoded on the wire.

Use preferred integer and length widths, definite lengths, and text-keyed maps
whose keys are sorted by deterministic encoded key bytes. Reject duplicate
keys, trailing item bytes, tags, floats, null, indefinite lengths, and
unsupported simple values. Negative integers fit int64; nonnegative integers
fit uint64. Omit optional fields whose values equal their defaults. These
restrictions apply recursively to extension values.

Implementations MUST use bounded or iterative traversal to prevent stack
exhaustion. A local nesting budget of 64 is a suitable default. Each sequence
item starts at depth zero; each array element, map key, or map value increases
depth by one. Exceeding a configured nesting or item budget is a local resource
rejection. The schema and metadata wrapper length requirements still apply.

#### Header fields and namespace

The header requires `version` and `requires`. Version is 1. Only `x-`
extension fields may be added. Entry count is derived from the subsequent
tuples; `entries`, `entry_count`, `index`, and root wrappers are forbidden. See
[Extensions and versions](#extensions-and-versions) for extension rules.

The [lexical grammar](#lexical-grammar) defines `required-feature`. A name is
1..128 ASCII bytes. It starts with a lowercase letter, followed by lowercase
letters, digits, or `.` or `-` paired with a following letter or digit.
Leading, trailing, and adjacent separators are forbidden. The array is bytewise
sorted and unique. Unknown required features are rejected. Each supported
extension defines agreement between its feature declaration and entry
semantics.

#### Entries

Each entry uses this tuple, with no encoded `size` field:

```text
[path, type, mode, uid, gid, mtime_ns, extents, ?attributes]
```

`path` obeys the namespace rules. There is no format-level entry-byte or
extent-count limit; local parser and index budgets may bound entries and extent
lists. `type` is 0 for a regular file, 2 for a symlink, or 5 for a directory.
`mode` is 0..4095; `uid` and `gid` are uint64; `mtime_ns` is signed int64
Unix-epoch nanoseconds. Entries record effective metadata after PAX overrides.

The optional `attributes` map holds extension fields and symlink targets.
`linkname` is required for symlinks and forbidden otherwise; it preserves the
exact effective symlink target. Other attribute names MUST start with `x-`. An
empty attributes map MUST be omitted. Directories, symlinks, and empty files
have empty extents. Empty files remain inline. The logical size of a file is
the checked sum of its extent sizes.

The manifest does not retain header offsets, header-sequence lengths, physical
payload sizes, owner/group display names, prefix splits, or file digests.
Reconstruction generates equivalent tar/PAX metadata with new headers,
checksums, and padding; it need not reproduce the original header bytes. Inline
physical payload size equals logical size. Extensions define the physical
payload sizes of their alternative representations.

#### Extents

Every extent has this tuple:

```text
[size, source, encoded_size, ?offset]
```

`size` is a positive uint64 number of decoded bytes selected. `offset` is the
selection's starting position in the decoded source. It defaults to zero;
explicit zero MUST be omitted. Check `offset + size` for overflow and require
it to fit uint64. After decoding the source, require the sum to be at most its
actual decoded length. Configured selection and source budgets apply
independently of these structural checks.

`source` is an absolute uint64 encoded archive offset referring to a data frame
in this archive only. `encoded_size` is the exact complete encoded frame
length, a positive uint64. The encoded range, computed with checked arithmetic,
MUST end at or before the metadata region.

Archive extents select the file's own physical payload in logical order, as
established by full validation. Multiple members may share a frame, and a
member may span frames. Source validation follows the [frame
requirements](#zstandard-frame-requirements).

The [external-content extension](external-content.md) adds object-address
sources, advisory encoded sizes, and pointer payloads. These representations
are valid only with the required extension declaration. See
[Security](#security-considerations) for integrity guarantees.

#### Derived values and coverage

These values are derived, not additional manifest fields:

| Value | Definition |
| --- | --- |
| `logical_size(entry)` | Checked sum of decoded extent `size` values; zero for directories/symlinks. |
| `physical_size(member)` | Effective tar size after PAX overrides: logical file bytes in core, extension-defined bytes for alternative representations, zero for other kinds. |
| `effective_header_size(member)` | 512 bytes, or 512-byte local PAX header + its padded payload + the 512-byte member header. |
| `payload_offset(member)` | Decoded member start + effective header size. |
| `member_end(member)` | Payload offset + `roundUp(physical_size, 512)`. |
| `raw_source_size` | Actual complete decoded data-frame length, determined by decoding. |

For encoded data-frame sizes `c_i` and decoded sizes `d_i`, the frame starts
satisfy `E_0 = D_0 = 0`, `E_(i+1) = E_i + c_i` and `D_(i+1) = D_i + d_i`. The
final encoded end equals manifest start `T`. The decoded member sequence starts
at zero; every next member starts at the previous `member_end`. Exactly 1024
zero bytes follow the final member (or start at decoded zero for an empty
archive), and their end equals the sum of all `d_i`. There are no undecoded
gaps or trailing physical bytes.

For an inline extent selecting frame `i`, its decoded interval is `[D_i +
offset, D_i + offset + size)`. In logical order, those intervals MUST cover
exactly that member's physical payload, without reordering, overlap, gaps, or
another member's bytes. Full archive validation establishes this mapping.
Selective reads check metadata and fetched-source bounds without establishing
physical membership.

Aggregate logical content is the sum of each regular file's logical size,
including any content reused through extensions. It is not a stored field.
Implementations using bounded integers MUST detect and report counter overflow.

### Extensions and versions

Footer profile and manifest versions are both exactly 1. Reject other versions.
Extensions define their own version markers. The numeric CBOR manifest version
does not change when an application document uses a string version marker:
physical JSON pointers use `"zstar-pointer-v1"`, and the catalog proposal uses
`"zstar-catalog-v1"`.

Core maps accept extension fields only in the manifest header and entry
attributes, where the CDDL permits them. Required features may extend other
structures as explicitly defined by their specifications; the core schema below
covers archives without those extensions. Extension field names are valid CBOR
text beginning with `x-`, with no suffix restriction. Values obey the CBOR
rules above. Maps within extension values use text keys under their published
schema; those keys need not start with `x-`. Readers MAY ignore optional
extensions; semantics required for correct interpretation MUST be declared in
`requires`. Unknown unprefixed fields and unknown required feature
names are rejected. Writer extension-preservation requirements are in the
[writer API contract](#writer-api-contract-normative-profile). Extension and
required-feature names SHOULD use collision-resistant namespaces. Incompatible
core semantics require a new version/profile.

## Validation and failures

### Validation and selective reads

Metadata readers MUST validate footer structure and bounds, the complete
manifest decoded length and hash, deterministic CBOR, sequence schema, the
complete namespace, global ordering, and required-feature agreement before
using indexed ranges. Selective readers MUST also validate selected source
frames and extent bounds. Implementations MUST reject unsupported versions,
unknown required semantics, malformed data, overflows, and digest mismatches. A
failed Zstar check MUST NOT silently fall back to unverified tar extraction.
Source snapshots, cache provenance, and ownership follow the [reader
profile](#reader-implementation-profile).

A failed required check stops the operation.

| Check | Metadata | Selective read | Full archive |
| --- | --- | --- | --- |
| Footer and complete manifest | All | All | All |
| Physical data frames | None | Intersecting sources | All |
| Tar headers, payload mapping, padding and end records | None | Not established | All |

Extensions add their own checks at these scopes. External source validation,
pointer agreement, and closure validation are defined by the [external-content
extension](external-content.md#validation-and-conformance).

#### File reads and verification scopes

For a file request `(offset, length)`, compute logical size by checked extent
summation. Both request values must fit uint64, with `offset <= logical_size`
and `length <= logical_size - offset`. Reject out-of-range requests rather than
truncating them. A valid zero-length request fetches no content, including at
EOF, but still requires footer and complete-manifest hash validation.

Use cumulative extent sizes to locate only sources intersecting the request.
Decode each complete fetched source under its source-kind rules. Check each
complete declared selection against the actual decoded length, then return the
requested bytes in logical order. A complete-file read does not establish a
whole-file hash.

#### Full archive validation

Full archive validation MUST scan every physical data frame and tar member,
validate checksums, effective metadata, payload mapping, declared extension
payload rules, zero padding, physical coverage, and exactly two final zero
records. Every manifest entry MUST correspond to exactly one physical tar
member, and every physical tar member MUST correspond to exactly one manifest
entry. Reject missing, unindexed, or duplicate members, including directories,
symlinks, and empty files. A local PAX header belongs to its following member
and is not a separate manifest entry.

#### Verification claims

Footer and manifest digest checks and codec checks cannot be disabled.
Extensions define verification of their external dependencies. Digests establish
internal consistency, not authenticity. The core archive stores no digest of its
complete encoded bytes.

### Failure classes

Results distinguish absent or invalid Zstar format, malformed structure,
unsupported core versions, unsupported required semantics, integrity failure,
missing required content, transport failure or unavailable content, invalid
logical requests, and local budget rejection. SDKs choose their exception
names. Violating a wire limit makes input malformed; exceeding a lower local
budget may reject valid input. A hash or checksum mismatch is an integrity
failure when distinguishable from a codec structure error.

Recognition, metadata authentication, and required-feature checks precede
indexed use. Complete source validation precedes returning selected bytes. For
independent defects, an operation may report any applicable failure unless a
dependency requires another check first.

## Implementation profiles

Profiles apply to the declared conformance classes.

### Writer API contract (normative profile)

Writers MUST preserve rewritten entry/header extensions or report their loss,
and reject unknown required semantics. They MUST preserve supplied metadata and
write tar members in insertion order. Writers MAY require input entries in
strict UTF-8 path order; such APIs MUST document and enforce this requirement.
Arbitrary-order APIs MUST sort manifest entries before emission. All writers
MUST emit the sorted manifest and reject duplicates, invalid ancestors, and
required-feature disagreement. For known-size inline input, writers SHOULD
reject entries that cannot fit configured entry-byte or extent-count budgets
before consuming the payload, using the configured frame size. Writers SHOULD
stream payloads through bounded frame buffers and spill finalized records when
retention is needed. A sorted-input API may validate the namespace
incrementally without sorting.

Explicit storage choices MUST be honored or reported as conflicts; portable
members cannot be silently externalized. Optimization hints may be declined
with diagnostics but cannot silently change capture scope. Filesystem
operations follow the [filesystem
profile](#filesystem-capture-and-restoration-profile).

### Reader implementation profile

Reader APIs, caches, eviction, and transports may vary subject to these rules.

#### Source snapshots

A reader session MUST bind source length/identity, footer, manifest, frame
reads, and cached bytes to one immutable snapshot. Replacement or instability
MUST invalidate affected state or fail; snapshots MUST NOT be mixed. Borrowed
buffers must remain immutable. Remote adapters may pin versions or enforce
strong ETags with conditional reads.

#### Cache ownership

Cache capacity includes retained payloads and bookkeeping. APIs SHOULD document
separate memory held by streams, entries, and returned buffers. Returned bytes
MUST survive eviction; callbacks MUST NOT alter verified bytes or proofs.

#### Temporary storage and cancellation

Temporary stores and worker queues MUST have explicit budgets. Operations MUST
release owned resources on success, failure, cancellation, or close. Preserve
the primary error and report cleanup failures separately. Noninterruptible work
MUST retain capacity until completion, even after cancellation. Coalesced
misses MUST preserve cancellation, limits, and verification provenance.

#### Manifest parsing and index budgets

Nesting, item, aggregate metadata, and index budgets are local policy; readers
MAY impose lower limits but MUST distinguish budget rejection from malformed
input. Parsing MUST retain at most one item and bounded read-ahead beyond the
parsed index, applying aggregate decoded-byte budgets before parsing. Spooling
does not bound index, parsing-scratch, or codec memory.

### Filesystem capture and restoration profile

Filesystem operations MUST follow this profile and applicable reader/writer
rules.

#### Capture

Capture MUST preserve symlinks and their exact target text without following
them by default; following requires explicit caller selection. Symlinks have no
payload or extents.

Capture MUST sort paths by UTF-8 bytes and default to these metadata values:
regular files 0644 (0755 if any executable bit is set), directories 0755,
symlinks 0777, UID/GID/mtime zero, and special bits cleared. It MUST offer
preservation of each entry's mode, ownership, and nanosecond mtime. Low-level
writers retain supplied metadata. Preservation does not authorize restoration.

#### Restoration

##### Symlink preservation

Restoration MUST preserve exact symlink targets, including absolute, dangling,
cyclic, and out-of-root targets, without requiring target existence or
replacing links with content. Unrepresentable links MUST fail rather than be
converted or omitted. Explicit stricter policies MAY reject valid links without
declaring the archive malformed.

##### Destination confinement

All writes and metadata changes MUST remain within the destination root and
MUST NOT follow preexisting or restored symlinks, including during concurrent
path changes. Out-of-root link targets MUST NOT be accessed. Restorers SHOULD
create directories and files before links, regardless of member order.

##### Host paths and collisions

Map archive `/` to host separators. Every other component MUST remain one exact
host component, without separator reinterpretation, Unicode normalization,
reserved-name substitution, or lossy conversion. Unrepresentable entries MUST
fail as host limitations. Collision checks MUST include case-insensitive and
normalizing host identities; collisions and silent path normalization MUST be
rejected. Ownership and set-id bits MUST NOT be applied without authorization.

#### Metadata and completion

Unauthorized or unsupported mode, ownership, timestamp, or special-bit changes
MUST fail the entry or report a partial result naming every omitted field;
restorers MUST NOT claim complete restoration after silent loss. Link metadata
MUST affect only the link, never its target. Directory metadata SHOULD be
finalized after descendants to preserve timestamps and avoid restrictive modes
blocking creation.

Stricter path/link policies MUST identify policy or host rejection, not
malformed archive data.

## Security considerations

Bound decompression, traversal, allocation, storage, and parallel work before
consuming untrusted input. A one-byte selection may require a whole source.
Selective validation leaves unread tar data unchecked; only full validation
establishes original frame boundaries and physical membership.

Metadata hashes and data checksums establish consistency, not publisher
authenticity: replacing both can produce another valid archive. Inline bytes have
codec checksums, not a cryptographic manifest binding. The external-content
extension defines content-address verification for external objects. Core archive
digests do not cover the complete encoded archive.

Archive replacement MUST follow the snapshot contract. Follow the [filesystem
profile](#filesystem-capture-and-restoration-profile) against traversal,
out-of-root writes, and host collisions.

## Formal definitions

### Manifest schema

This CDDL defines the CBOR structures. The preceding sections define
deterministic encoding, namespace rules, and cross-field invariants.

```cddl
; Normative entry-oriented v1 structure. See core.md for deterministic encoding,
; bounded nesting, sorted namespace and required-feature agreement.
; A CBOR sequence group: emit these items consecutively, without an array.
manifest = (manifest-header, * entry)
manifest-header = {
  "version": 1,
  "requires": [* required-feature],
  * extension-key => extension-value
}

entry = file-entry / directory-entry / symlink-entry
entry-fields = (
  mode: 0..4095,
  uid: uint,
  gid: uint,
  mtime-ns: -9223372036854775808..9223372036854775807
)
file-entry = [
  path: archive-path, type: 0, entry-fields,
  extents: [* archive-extent],
  ? attributes: extension-attributes
]
directory-entry = [
  path: archive-path, type: 5, entry-fields,
  extents: [], ? attributes: extension-attributes
]
symlink-entry = [
  path: archive-path, type: 2, entry-fields,
  extents: [], attributes: symlink-attributes
]
archive-path = tstr .size (1..4096)
archive-extent = [
  size: 1..18446744073709551615, source: uint,
  encoded-size: 1..18446744073709551615, ? offset: 1..18446744073709551615
]
symlink-attributes = {
  "linkname": tstr .size (1..4096),
  * extension-key => extension-value
}
extension-attributes = { + extension-key => extension-value }
extension-key = tstr .regexp "x-[\\s\\S]*"
extension-value = uint / (-9223372036854775808..-1) / bool / tstr / bstr
                / [* extension-value]
                / { * tstr => extension-value }

required-feature = (tstr .size (1..128)) .regexp "[a-z]([a-z0-9]|[.-][a-z0-9])*"
```

### Lexical grammar

This ABNF defines required-feature names and PAX decimal fields. The preceding
sections define numeric and size bounds.

```abnf
; Normative RFC 5234 / RFC 7405 grammar. Input is consumed in its entirety.
; Numeric value bounds and size rules are defined in core.md.
; SP, LF and DIGIT are RFC 5234 core rules.
; A complete required-feature is additionally limited to 1..128 ASCII bytes.
required-feature = lower *(alnum / separator alnum)
lower = %x61-7A
alnum = lower / DIGIT
separator = %x2D / %x2E

; PAX decimal fields permit leading zeros.
pax-uint = 1*DIGIT
pax-mtime = [%s"-"] 1*DIGIT [%s"." 1*DIGIT]
```

## Normative references

- [POSIX.1-2024 (Issue 8) tar/PAX](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/pax.html)
- [RFC 8878: Zstandard](https://www.rfc-editor.org/rfc/rfc8878)
- [RFC 8949: CBOR](https://www.rfc-editor.org/rfc/rfc8949)
- [RFC 8742: CBOR sequences](https://www.rfc-editor.org/rfc/rfc8742)
- [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174): requirement language
- [RFC 3629: UTF-8](https://www.rfc-editor.org/rfc/rfc3629)
- [RFC 5234: ABNF](https://www.rfc-editor.org/rfc/rfc5234)
- [RFC 7405: case-sensitive ABNF literals](https://www.rfc-editor.org/rfc/rfc7405)
- [RFC 8610: CDDL](https://www.rfc-editor.org/rfc/rfc8610)
- [FIPS 180-4: SHA-256](https://doi.org/10.6028/NIST.FIPS.180-4)
