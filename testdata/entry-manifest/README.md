# Entry-manifest fixtures

`manifest.json` describes shared Rust, Go, and Python conformance checks. It is
a test catalog, separate from the CBOR manifest inside each archive.

The `archives` object maps relative archive paths to one `sha256` and a list of
operation `cases`. The catalog keeps `schema_version` at 1.
`wire_version` identifies the archive format version independently of the catalog
schema version.

Each case has a unique `name` and an `operation`. An omitted `expected_error`
means success. Optional fields specify read requests, expected content files,
symlink targets, external objects, resource limits, and verification results.
Paths are relative to this directory. Short cases use one line for readability.

Keep metadata and full-archive checks separate: some corruptions are deliberately
accepted by metadata-only validation and rejected by full archive validation.

The `limits/many-extents.zstar` fixture contains 65,537 external one-byte selections
of the same `x` object. Its last logical byte exercises lookup beyond the former
extent-count cap. `limits/entry-byte-limit.zstar` uses 110,400 selections and an
8,390,418-byte entry. Both are accepted without per-entry limits and rejected
when a caller selects a lower extent or entry-byte budget. The 17 MiB payload in
`limits/large-frame.zstar` likewise exercises acceptance above the former frame
limit and rejection under explicit source budgets. Window fixtures retain their
historical names; over-limit cases now select a 16 MiB local window budget.
These fixtures were generated independently with Python's standard-library
`tarfile` and `compression.zstd` plus deterministic CBOR encoding; data frames
include checksums and tar streams have exactly two terminal zero records.
