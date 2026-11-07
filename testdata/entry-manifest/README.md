# Entry-manifest fixtures

`manifest.json` describes shared Rust, Go, and Python conformance checks.

The `archives` object maps relative archive paths to one `sha256` and a list of
operation `cases`. `schema_version` identifies the test schema;
`wire_version` identifies the archive format version.

Each case has a unique `name` and an `operation`. An omitted `expected_error`
means success. Optional fields specify read requests, expected content files,
symlink targets, external objects, resource limits, and verification results.
Paths are relative to this directory.

Keep metadata and full-archive checks separate: some corruptions are deliberately
accepted by metadata-only validation and rejected by full archive validation.

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
External-object checksums remain optional. Archives use the 28-byte `Z*01`
footer; fixture SHA-256 values identify the complete encoded fixture files.

Manifest headers use `requires`. The empty and thin archives cover empty and
nonempty declarations; `invalid/missing-requires.zstar` omits the field.
Unknown header fields are rejected.

Pointer fixtures cover malformed fields, manifest agreement, JSON whitespace
without a final newline, and rejection of non-JSON and empty payloads.
Empty-payload cases exercise metadata, selective-read, and full validation scopes.

Footer cases cover signature and payload errors, zero and inconsistent lengths,
checked subtraction, truncation, and trailing bytes.
Concatenated archives are rejected by full validation; metadata-only validation
does not establish the validity of the preceding data region.
