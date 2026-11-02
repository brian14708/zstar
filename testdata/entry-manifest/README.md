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
