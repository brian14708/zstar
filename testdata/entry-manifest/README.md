# Entry-manifest fixtures

`manifest.json` describes shared Rust, Go, and Python conformance checks.

The `archives` object maps relative archive paths to one `sha256` and a list of
`cases`. `schema_version` identifies the test schema; `wire_version` identifies
the archive format version.

Each case has a unique `name`. A `request` selects a file range; without one,
the case validates metadata. `range_result_hex` stores expected bytes as hex,
including an empty string for empty results. An omitted `expected_error` means
success. Optional `objects`, `verify_external`, and `limits` configure the reader.
Artifact paths are relative to this directory.

Metadata and selective-read cases exercise the checks required by each operation.

The `limits/many-extents.zstar` fixture contains 65,537 external one-byte selections
of the same `x` object. `limits/entry-byte-limit.zstar` uses 110,400 selections
and an 8,390,418-byte entry. Both are accepted without per-entry limits and
rejected under lower extent or entry-byte budgets. The 17 MiB payload in
`limits/large-frame.zstar` tests source budgets. Over-limit window cases select
a 16 MiB local window budget.
These fixtures were generated independently with Python's standard-library
`tarfile` and `compression.zstd` plus deterministic CBOR encoding; data frames
include checksums and tar streams have exactly two terminal zero records.

Checksum-mismatch cases for metadata, archive data, and external objects require
`integrity-failure`. Missing checksums and malformed frame encodings retain their
structural failure categories. Metadata checksums are mandatory; checksum-free metadata fixtures are rejected.
External-object checksums remain optional. Archives use the 44-byte `Z*01`
footer; fixture SHA-256 values identify the complete encoded fixture files.

Manifest directories use `requires`. The empty and thin archives cover empty and
nonempty declarations; `invalid/missing-requires.zstar` omits the field.
Unknown directory fields are rejected unless prefixed with `x-`.

The `pages/` fixtures cover directory range accounting, page schemas, cross-page
ancestors, optional extension pages, and corruption outside a selected read.
Opening validates the directory; the metadata operation scans every page.

Pointer fixtures cover metadata and selective reads of external extents.

Footer cases cover signature and payload errors, zero and inconsistent lengths,
checked subtraction, truncation, and trailing bytes.
Metadata-only validation does not establish the validity of the preceding data region.
