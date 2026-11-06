# Zstar for Rust

Read and write indexed `.tar.zst` archives with async Rust.

[Rust API reference](https://docs.rs/zstar/latest/zstar/)

```toml
[dependencies]
zstar = "0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Read and write an archive

Start with [your first archive](https://brian14708.github.io/zstar/tutorials/rust/first-archive/)
to write an archive, read a member and byte range, and verify the result. Then
[upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/rust/s3/).
Both tutorials create their own data; no input files are needed.

Use `Archive` to read and `Builder` to write. `Builder::finish` finalizes the
archive. Writers accept entries in any order; UTF-8 path order avoids an
additional manifest sorting spool at finish.

## Features and configuration

The default features provide async readers, writers, and temporary-file spooling.
Enable `fs`, `http`, or `s3` for storage adapters. See
[advanced usage](https://brian14708.github.io/zstar/guides/rust/configuration/)
for features, credentials, member caches, filesystem restoration, and resource
budgets. Filesystem extraction returns `ExtractReport`; inspect omitted metadata
before treating restoration as complete.

See [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/)
for custom storage and chunking, ownership, cancellation, focused metadata
options, and fallible builder initialization.

## Validation and format

Opening validates metadata; reads validate the sources and extents they use.
Use `verify(VerificationScope::Archive)` for full physical validation, or
`VerificationScope::ExternalClosure` to also resolve external content. External
SHA-256 checking is opt-in through `ReaderOptions.verify_external`; metadata
hashes, frame checksums, and extent bounds are always checked. Distinguishable
frame checksum failures return `Error::Integrity`.

The [core specification](https://github.com/brian14708/zstar/blob/main/spec/draft/core.md)
and [external-content extension](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md)
define the format and validation rules. The informative
[CAS storage appendix](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md#appendix-cas-storage)
explains object identity and storage encoding.
