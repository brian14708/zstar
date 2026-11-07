# Zstar core draft

**Status:** working draft, wire version 1.

This specification defines Zstar's self-contained indexed archive format: a tar
stream in independent Zstandard frames, with deterministic CBOR metadata.
It covers the core wire format, validation, and implementation profiles.
Core archives contain all file content. Optional extensions build on this
format, such as [external content](external-content.md) for external-object
references and reconstruction.

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
| Directory reader | Validate the footer and complete manifest directory, including required features and all declared page ranges. |
| Metadata reader / full metadata validator | Perform directory-reader checks and validate every metadata page, the complete namespace, global ordering, and required-feature agreement. |
| Selective reader | Perform directory-reader checks, validate selected entry pages and ancestor entries, and validate each selected complete source and extent before returning requested bytes. |
| Filesystem capturer/restorer | Follow the normative [filesystem profile](#filesystem-capture-and-restoration-profile), in addition to the applicable writer/reader classes. |

Core-only implementations MAY reject extension features. Implementations that
accept a required feature MUST apply its specification in addition to core.
Unknown required features MUST be rejected while validating the directory,
before any indexed entry use.

An operation establishes only the checks it completes. Metadata validation and
selective reads do not establish full archive validity, content authenticity,
or availability of extension dependencies. Implementations MUST NOT claim checks
they did not complete.
Rejection requirements apply to the structures and dependencies being
validated; selective readers need not inspect unrelated entry pages, optional
extension pages, or tar regions.

## Conventions

MUST, MUST NOT, SHOULD, and MAY have the BCP 14 meanings defined by RFC 2119
and RFC 8174. Wire definitions below are normative.

Offsets are zero-based; ranges are half-open. Sizes are measured in bytes. MiB
means 1048576 bytes. An archive or encoded source length MUST fit uint64.
Integers are unsigned 64-bit unless otherwise specified; `mtime_ns` is signed
64-bit nanoseconds since the Unix epoch. Booleans are not integers. All
arithmetic, rounding, and cumulative sums MUST be checked for overflow before
allocation, addressing, or slicing.

Byte positions use four coordinates:

| Coordinate | Origin and use |
| --- | --- |
| Encoded archive position | Byte zero of the archive; derived manifest offsets and integer extent sources use this coordinate. |
| Metadata-relative position | Metadata start `T`; entry-page and extension-page offsets use this coordinate. |
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
[entry-page skippable wrappers in increasing path order]
[optional extension-page skippable wrappers]
[manifest-directory skippable wrapper]
[44-byte terminal skippable-frame footer]
```

Data frames occupy every byte before the metadata region. The metadata region
immediately precedes the footer, which ends the archive. No other skippable
frames or gaps are permitted.

#### Zstandard frame requirements

Each data frame or metadata wrapper payload contains exactly one
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

Data and metadata frames MUST include valid Zstandard content checksums. There
is no separate metadata, page, or directory checksum field. All content-size
fields are optional; when present they MUST be valid. A declared content size
MUST equal the actual decoded length. Every frame MUST use standard magic
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

The footer is exactly 44 bytes and ends at EOF. Integer fields are unsigned
little-endian. Its terminal signature identifies this core wire layout.

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 4 | Skippable magic `0x184D2A5F` |
| 4 | 4 | Payload length: 36 |
| 8 | 8 | Total encoded metadata region length, including every wrapper |
| 16 | 8 | Sum of actual decoded payload lengths of all metadata wrappers |
| 24 | 8 | Encoded directory length, including its wrapper |
| 32 | 8 | Decoded directory length |
| 40 | 4 | ASCII `Z*01` (literal bytes `5A 2A 30 31`) |

For archive length `N`, total encoded metadata length `L`, and encoded directory
length `R`, derive metadata start `T = N - 44 - L` and directory start
`Q = N - 44 - R` using checked subtraction. Require `N >= 44`, `T > 0`,
`L >= R > 8`, and nonzero declared directory and total decoded lengths. The
directory immediately precedes the footer. Reject incorrect magic, payload
length, signature, inconsistent lengths, or bounds. Trailing bytes and
concatenated archives are invalid. Readers MUST reject unsupported terminal
signatures.

#### Metadata region

Metadata wrappers exactly cover `[T, N - 44)`. Each wrapper has an 8-byte header
followed immediately by one Zstandard payload. Header fields are little-endian
uint32: magic `0x184D2A5E` and encoded payload length. Zero-length payloads are
invalid. Encoded page and directory lengths include these 8 bytes and MUST equal
`8 + payload_length` when the wrapper is read.

Every wrapper decodes to exactly one complete deterministic CBOR value; no
value or entry crosses a wrapper boundary. Entry pages occupy the region first,
in increasing path order, followed by extension pages, then the directory.
The [paged manifest](#paged-manifest) specifies each payload and its references.
The directory lists every page exactly once. No unlisted wrappers, gaps,
overlap, or references into the content region, directory, or footer are allowed.

Apply local encoded, decoded, item, and codec budgets before fetching or
allocating. Every fetched page used by an operation MUST be completely decoded
and validated, including its Zstandard frame checksum. A selective reader MAY
leave unselected pages unread. Declared decoded lengths permit budget and aggregate
consistency checks; only decoding establishes each page's actual length.

#### Recognition and media type

Readers MUST validate the terminal footer and complete directory before
recognizing a file as this Zstar profile. Recognition does not establish
complete metadata or physical archive validity. A filename extension or media
type is insufficient. Use `application/zstd`; generic
`application/octet-stream` is acceptable. No Zstar media type is registered.
Ordinary Zstandard/tar tools decode the tar stream and skip metadata wrappers.
Thin-content resolution follows the [external-content extension](external-content.md).

### Tar profile

Use POSIX ustar with at most one nonempty local PAX header (`x`) immediately
before a member. Supported member types are regular file (`0` or NUL),
directory (`5`), and symlink (`2`). This profile excludes global PAX headers,
hard links, sparse files, devices, FIFOs, xattrs, and all other types. Writers MUST reject
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
symlink entries. Automatic symlink resolution is outside this profile.

### Paged manifest

The manifest consists of entry pages, optional extension pages, and one flat
directory. The [manifest schema](#manifest-schema) defines their CBOR structures.
Pages are independently usable decoding/cache units. There is no recursive
index, enclosing CBOR sequence, or global content-frame or object table.

#### Directory and page references

The directory is a deterministic CBOR map with required fields `version`,
`requires`, and `pages`. Version is exactly 1. `pages` is an array of entry-page
descriptors in increasing path and physical-offset order:

```text
[last_path, offset, encoded_length, decoded_length, ?attributes]
```

`offset` is relative to metadata start `T`. `encoded_length` includes the
wrapper and is greater than 8; `decoded_length` is positive. `last_path` is a
valid archive path and the exact largest path in that page. Separators MUST be
strictly increasing in UTF-8 byte order. The optional nonempty `attributes` map
contains only `x-` extension fields; omit it when empty. A descriptor has exactly
four elements, or five when attributes are present; unknown trailing elements
are forbidden.

The directory MAY also contain `extension-pages`, a nonempty map from extension
names beginning with `x-` to nonempty arrays of page references. An extension
page reference has no path separator:

```text
[offset, encoded_length, decoded_length, ?attributes]
```

Its length, offset, and optional attribute rules are the same as for entry-page
descriptors. Extension pages follow all entry pages. Their physical order MUST
match the deterministic encoded-key order of extension groups, then array order
within each group. Omit an empty `extension-pages` map. Other directory fields
MUST begin with `x-`. Archive-wide metadata belongs in these directory fields;
page-local metadata belongs in the page or reference attributes.

Starting at relative offset zero, walk entry-page descriptors, then extension
page references in the order above. Each offset MUST equal the preceding
encoded end, and the final end MUST equal `Q - T`. This rule validates declared
coverage without visiting pages and rejects gaps, aliases, and overlap. The
checked sum of every declared page decoded length plus the footer's decoded
directory length MUST equal the footer's total decoded length. The actual
decoded directory length MUST equal its footer field. Empty archives use an
empty `pages` array; optional extension pages MAY still be present. Without
extension pages, an empty manifest contains only the directory.

The `requires` array is bytewise sorted and unique. The [lexical
grammar](#lexical-grammar) defines its feature names: 1..128 ASCII bytes,
starting with a lowercase letter, followed by lowercase letters, digits, or
`.` or `-` paired with a following letter or digit. Leading, trailing, and
adjacent separators are forbidden. Unknown required features MUST be rejected
before indexed entry use. Each extension specifies declaration agreement and
validation at the scopes below.

#### Entry pages and lookup

Each entry page is a deterministic CBOR map:

```text
{"entries": [entry, ...], "x-...": ...}
```

`entries` is required and nonempty. Other fields MUST begin with `x-`. This map
provides an extension point without CBOR tags, page-type codes, or positional
fields appended to the page. Its role is determined by its directory reference.
Unknown optional page fields MAY be ignored after validating their CBOR values.

Entries MUST be strictly sorted by UTF-8 path bytes within and across pages;
physical tar order may differ. When page `i` is decoded, its final path MUST
equal descriptor `i`'s `last_path`. Every path in it MUST exceed descriptor
`i-1`'s `last_path`, if any. The decoded length and wrapper length MUST match the
reference. Readers MUST validate the complete page, including every entry's
local schema and extension declaration agreement, before using any of its
entries. Cross-page ancestor checks are required for entries actually selected;
complete namespace validation is a separate scope.

For an exact path, binary-search the directory for the first `last_path` at
least equal to it. If there is none, the path is absent from that indexed view;
otherwise decode and search that page. A selected entry's proper ancestors
MUST be looked up and verified as explicit directories before the entry or
its bytes are returned. Page fetches SHOULD be batched and deduplicated.
Lookup does not follow symlinks. Iteration advances through descriptors in
order, and MAY yield validated entries before a later page fails.

Entry count is derived by scanning entry pages; it is not a stored directory
or page-reference field. Page boundaries have no namespace semantics. In
particular, `a!` may sort between directory `a` and descendant `a/b`; checking
only the preceding entry is insufficient to validate ancestors.

Writers SHOULD target 64 KiB decoded entry pages, cutting only between complete
entries and including map/container overhead. This is a recommended target, not
a wire maximum. An oversized entry MAY occupy a page alone, subject to local
resource limits and the wrapper payload ceiling. The full selected entry,
including its extent list, MUST be validated before using any extent.

#### Extension pages

An extension page decodes to one `extension-value` under the CBOR rules below.
Its group's published specification defines its payload schema and the relation
between its `x-` group name and any required-feature declaration. A standard
logical extension-page identifier is `[group_name, page_index]`, where the index
is zero-based within that group's directory array. Extensions SHOULD use these
identifiers instead of embedding physical offsets in opaque fields; their
consumers MUST check group existence and index bounds before following them.

Optional extension pages MUST be safely ignorable without changing core entry
lookup, namespace, logical bytes, or required validation. Any extension page,
page field, or reference attribute needed to interpret an entry correctly MUST
be governed by a declared required feature. Unknown optional groups remain
listed in directory coverage and declared-length checks; selective readers
need not fetch them. Complete metadata validation MUST decode every extension
page, check its Zstandard frame checksum, and validate its base CBOR value, even for an
unknown optional group, but does not establish unknown extension semantics.

An extension that introduces references or dependency chains MUST specify
validation, cycle handling, and budgets. Only pages actually needed by a selected
operation need be fetched in selective mode. Extensions MUST NOT redefine the
core reference coordinates, framing, checksums, or entry-page routing fields.
Such changes require a new core version. Extension pages are metadata and MUST
NOT be used as core archive-extent content sources.

#### Deterministic CBOR

Each directory and page uses RFC 8949 core deterministic encoding. Use preferred
integer and length widths, definite lengths, and text-keyed maps whose keys are
sorted by deterministic encoded key bytes. Reject duplicate keys, trailing
bytes after the one payload value, tags, floats, null, indefinite lengths, and
unsupported simple values. Negative integers fit int64; nonnegative integers
fit uint64. Omit optional fields whose values equal their defaults. These
restrictions apply recursively to extension values, including unknown optional
extensions. No self-describing or custom CBOR tag is required or permitted.

Implementations MUST use bounded or iterative traversal to prevent stack
exhaustion. A local nesting budget of 64 is a suitable default. Each payload
value starts at depth zero; each array element, map key, or map value increases
depth by one. Exceeding a configured nesting or item budget is a local resource
rejection. Schema and wrapper-length requirements still apply.

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
another member's bytes. Selective reads check metadata and fetched-source bounds
without establishing physical membership.

Aggregate logical content is the sum of each regular file's logical size,
including any content reused through extensions. It is not a stored field.
Implementations using bounded integers MUST detect and report counter overflow.

### Extensions and versions

The footer signature is `Z*01` and the directory version is exactly 1. Reject
other core versions. Extensions define their own version markers; independent
formats such as standalone external references need not share the core number.

Core extension points are directory fields, entry-page fields, page-reference
attributes, entry attributes, and named extension-page groups. Extension field
and group names are valid CBOR text beginning with `x-`, with no suffix
restriction. Values obey the CBOR rules above. Maps within extension values use
text keys under their published schema; those keys need not start with `x-`.
Unknown unprefixed fields, unknown trailing tuple elements, and unknown required
features MUST be rejected.

Readers MAY ignore optional extensions after the base checks applicable to the
operation. Optional extensions MUST NOT change core interpretation, lookup,
content, or safety checks. Required features may extend entry or extent schemas
as explicitly defined by their specifications; the base CDDL covers archives
without those schema extensions. Required extensions MUST keep core directory
coverage and entry routing unambiguous, and MUST define agreement checks at
selective and complete scopes. Merely storing data in an `x-` field or extension
page does not declare required semantics.

For example, an optional search index may use an `x-search` page group as an
accelerator while ordinary entry lookup remains correct without it. A future
paged-extent representation that changes how file bytes are located must declare
its required feature and define how entry attributes reference the extent pages.
Neither requires changing the footer or appending positional descriptor fields.

Extension and feature names SHOULD use collision-resistant namespaces. Writer
preservation requirements are in the [writer API contract](#writer-api-contract-normative-profile).
Incompatible core framing or directory semantics require a new core version.

## Validation and failures

### Validation and selective reads

Directory readers MUST validate footer bounds, the directory
frame's Zstandard checksum,
actual directory decoded length, deterministic CBOR and schema, all required
features, sorted page separators, and declared coverage and aggregate lengths
before indexed use. This does not establish the contents of unread pages.

Selective readers MUST additionally validate every selected entry page, each
selected entry's explicit directory ancestors, and any required extension-page
dependencies. Before returning requested content, they MUST validate selected
complete sources and extent bounds. Local page checks include schema and
feature agreement for all entries in a fetched page; complete cross-page
namespace checks apply only at the complete metadata scope.

Complete metadata validation MUST scan all referenced pages in physical order,
check every Zstandard frame checksum and actual length, validate every entry's
schema, global
ordering and namespace, and complete required-feature agreement. It MUST also
apply supported optional-extension checks and all declared required-extension
checks. Unknown optional extension pages receive base CBOR validation only.
The directory's exact contiguous coverage makes this a linear page scan;
no recursive node traversal or visited-page set is required.

Reject unsupported versions, unknown required semantics, malformed data,
overflows, and digest mismatches within the operation's scope. A failed required
check stops the operation. A failed Zstar check MUST NOT silently fall back to
unverified tar extraction. Snapshot, provenance, and ownership rules follow the
[reader profile](#reader-implementation-profile).

| Check | Directory/open | Selective read | Complete metadata |
| --- | --- | --- | --- |
| Footer and complete directory | All | All | All |
| Entry-page Zstandard checksum, length, local schema and intervals | None | Selected pages | All |
| Namespace and feature agreement | Directory declarations | Selected entries/ancestors and required dependencies | Global |
| Extension pages | Declared references | Required dependencies | All, with semantics as above |
| Physical data frames | None | Intersecting sources | None |
| Tar headers, payload mapping, padding and end records | None | Not established | Not established |

A negative lookup describes the validated routing view of this snapshot. It is
not proof that unread pages contain no misplaced entry. A bad separator can
hide malformed metadata until the affected page or all metadata is validated.
Callers requiring complete conformance before any result MUST complete metadata
validation first; a selective reader MUST NOT claim that guarantee at open.

External source validation and pointer agreement are defined
by the [external-content extension](external-content.md#validation-and-conformance).

#### File reads

For a file request `(offset, length)`, compute logical size by checked extent
summation. Both request values must fit uint64, with `offset <= logical_size`
and `length <= logical_size - offset`. Reject out-of-range requests rather than
truncating them. A valid zero-length request fetches no content, including at
EOF, but still requires directory validation, the selected entry page, ancestor
checks, and any extension metadata needed to establish the request's validity.
It does not require validating unrelated metadata pages.

Use cumulative extent sizes to locate only sources intersecting the request.
Decode each complete fetched source under its source-kind rules. Check each
complete declared selection against the actual decoded length, then return the
requested bytes in logical order. A complete-file read does not establish a
whole-file hash.

#### Required read checks

Directory validation, required selected-page validation, and Zstandard frame
checksum checks cannot be disabled. Complete metadata validation is an explicit
scope and MUST NOT be inferred from directory validation or selected reads. Extensions
define verification of their external dependencies. Checksums detect corruption,
not authenticity. Core stores no cryptographic digest of its metadata or
complete encoded bytes.

### Failure classes

Results distinguish absent or invalid Zstar format, malformed structure,
unsupported core versions, unsupported required semantics, integrity failure,
missing required content, transport failure or unavailable content, invalid
logical requests, and local budget rejection. SDKs choose their exception
names. Violating a wire limit makes input malformed; exceeding a lower local
budget may reject valid input. A hash or checksum mismatch is an integrity
failure when distinguishable from a codec structure error.

Recognition, directory validation, and required-feature checks precede
indexed use. Selected-page and ancestor checks precede returning selected
entries. Complete source validation precedes returning selected bytes. For
independent defects, an operation may report any applicable failure unless a
dependency requires another check first.

## Implementation profiles

Profiles apply to the declared conformance classes.

### Writer API contract (normative profile)

Writers MUST preserve rewritten directory, page, reference, and entry extensions
or report their loss, and reject unknown required semantics. Preserving extension
pages includes their payloads, group/index identities, and published references;
physical page offsets MUST be recomputed after rewriting. A writer unable to
preserve an optional extension's semantics MUST report its loss rather than
silently claim preservation. Writers MUST preserve supplied metadata and
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

Writers SHOULD spool entry-page descriptors while emitting sorted pages, then
stream the directory from those descriptors. They MUST know definite CBOR
container lengths before encoding them and MAY use bounded temporary storage
to determine compressed wrapper lengths. Extension pages MUST precede the
directory in their prescribed order. Writers need not seek or rewrite emitted
content; implementations MUST budget descriptor/page spools as well as data.

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

#### Metadata parsing, caching, and budgets

Directory, descriptor-count, page, entry, extent, nesting, item, codec-window,
retained-memory, and in-flight budgets are local policy. Readers MAY impose
lower limits but MUST distinguish budget rejection from malformed input.
Readers MUST enforce encoded and decoded limits before exceeding allocation,
fetch, or decode budgets; a declared decoded size is not permission to allocate
it without limits. Retained directory/index memory, typed pages, parser scratch,
codec workspace, and speculative encoded buffers require explicit accounting.
Spooling does not bound other owners' memory.

A reader MAY retain the complete directory and a bounded page cache, without
retaining every entry or a global extent index. Cache provenance MUST bind the
source snapshot, exact page reference, and completed validation checks. A page
validated under one separator interval MUST NOT be reused under an inconsistent
reference without checking that interval. Eviction MUST NOT invalidate returned
entries or file handles. Iteration MUST surface later-page failures and MUST
NOT silently report complete success after a truncated scan.

#### Remote range planning

Page boundaries are decoding/cache units, not required network-request
boundaries. Remote readers SHOULD use a bounded suffix prefetch for the footer
and directory, reuse covered byte ranges, batch page and ancestor lookups,
coalesce nearby ranges, and use bounded contiguous read-ahead for scans.
All entry-page locations are known after directory validation; no recursive
index requests are needed. For small metadata regions, a reader MAY fetch the
whole encoded region within its budgets without decoding every page eagerly.

A 128 KiB initial suffix and 256 KiB–1 MiB sequential windows are reasonable
starting policies to measure, not wire requirements or coverage guarantees.
Sparse requests SHOULD avoid excessive speculative transfer. S3 GetObject
supports one byte range per request; distant ranges require separate requests,
which MAY run with bounded concurrency. All requests MUST follow the snapshot
contract. Speculative bytes MUST NOT be treated as validated pages before the
complete required checks, and their storage and cancellation MUST be budgeted.

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
Selective validation leaves unread metadata pages and tar data unchecked.
Directory validation establishes declared routing and coverage, not actual page
contents or global namespace validity. Only complete metadata validation
establishes the complete namespace. Selective reads do not establish original
data-frame boundaries or physical membership.

Zstandard frame checksums detect corruption, not publisher authenticity. They
do not cryptographically bind the directory to referenced pages or prevent
replacement of intact frames. Length and schema checks do not replace an
aggregate integrity check. Inline bytes have codec
checksums, not a cryptographic manifest binding. The external-content
extension defines content-address verification for external objects. Core
archives contain no cryptographic digest of the complete encoded archive.

Archive replacement MUST follow the snapshot contract. Follow the [filesystem
profile](#filesystem-capture-and-restoration-profile) against traversal,
out-of-root writes, and host collisions.

## Formal definitions

### Manifest schema

This CDDL defines the CBOR structures. The preceding sections define
deterministic encoding, namespace rules, and cross-field invariants.

```cddl
; Each directory/page is one independent CBOR value, not a CBOR sequence.
manifest-directory = {
  "version": 1,
  "requires": [* required-feature],
  "pages": [* entry-page-descriptor],
  ? "extension-pages": { + extension-key => [+ extension-page-reference] },
  * extension-key => extension-value
}
entry-page-descriptor = [
  last-path: archive-path,
  offset: uint, encoded-length: 9..18446744073709551615,
  decoded-length: 1..18446744073709551615,
  ? attributes: extension-attributes
]
extension-page-reference = [
  offset: uint, encoded-length: 9..18446744073709551615,
  decoded-length: 1..18446744073709551615,
  ? attributes: extension-attributes
]
entry-page = {
  "entries": [+ entry],
  * extension-key => extension-value
}
extension-page = extension-value
extension-page-id = [group-name: extension-key, page-index: uint]

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
; Input is consumed in its entirety.
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
- [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174): requirement language
- [RFC 3629: UTF-8](https://www.rfc-editor.org/rfc/rfc3629)
- [RFC 5234: ABNF](https://www.rfc-editor.org/rfc/rfc5234)
- [RFC 7405: case-sensitive ABNF literals](https://www.rfc-editor.org/rfc/rfc7405)
- [RFC 8610: CDDL](https://www.rfc-editor.org/rfc/rfc8610)
- [FIPS 180-4: SHA-256](https://doi.org/10.6028/NIST.FIPS.180-4)
