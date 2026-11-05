# Zstar for Rust

Read and write indexed tar archives with async Rust.

[Rust API reference](https://docs.rs/zstar/latest/zstar/)

```toml
[dependencies]
zstar = "0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Read and write an archive

[Create your first archive](https://brian14708.github.io/zstar/tutorials/rust/first-archive/),
read a member and byte range, and verify the result. Then
[upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/rust/s3/).
Neither tutorial needs input files.

## Features and storage

The default features provide async readers, writers, and temporary-file spooling.
Enable `fs`, `http`, or `s3` for storage adapters. See
[advanced usage](https://brian14708.github.io/zstar/guides/rust/configuration/)
for features, credentials, ownership, and memory budgets.

The [format specification](https://github.com/brian14708/zstar/blob/main/spec/draft/core.md)
defines the wire format and validation rules.

Member streams retain one decoded source within a configurable 2 MiB cache;
use `with_cache_bytes(0)` to disable it. Authenticated cursors reject modified
metadata. Filesystem extraction returns `ExtractReport`; inspect omitted metadata
before treating restoration as complete. Distinguishable frame checksum failures
return `Error::Integrity`.

The [external-content extension](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md)
defines object extents, pointer payloads, and external verification. The informative
[CAS storage appendix](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md#appendix-cas-storage)
explains object identity and storage encoding.

The draft manifest header declares mandatory extensions in `requires`. Readers
reject unsupported extensions and unknown unprefixed fields.

Standalone content references and pointers reject logical sizes exceeding uint64.

External file pointer payloads use JSON with `"version": "zstar-pointer-v1"` and
an ordered `chunks` array. This breaking draft change replaces line-based
pointers; full archive validation rejects the old payloads. Standalone content
references retain their deterministic CBOR encoding.
