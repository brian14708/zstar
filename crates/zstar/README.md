# Zstar for Rust

Read and write indexed `.tar.zst` archives with async Rust.
Implements the Zstar core specification and its optional external-content extension.

Archives use the compact 28-byte `Z*01` footer with 64-bit metadata lengths.
Metadata and archive data frames require Zstandard checksums; opening validates
the complete manifest.
`ReadReport::metadata_validated` reports checksum and schema validation, not cryptographic authentication.

[Rust API reference](https://docs.rs/zstar/latest/zstar/)

```toml
[dependencies]
zstar = "0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`Archive::verify` returns `Result<()>`; external hashing follows `ReaderOptions::verify_external`.

## Get started

Use `Archive` to read and `Builder::try_new(sink).await?` to initialize a writer.
Use `Builder::try_with_options` for custom options; initialization requires Tokio
and returns configuration or storage errors immediately. `Builder::finish`
finalizes the archive. Default features include async I/O and temporary-file spooling.
Enable `fs`, `http`, or `s3` for storage adapters.

Follow [your first archive](https://brian14708.github.io/zstar/tutorials/rust/first-archive/)
to write, read, and verify an archive, then try
[S3 storage](https://brian14708.github.io/zstar/tutorials/rust/s3/).

Reader and writer options store `MetadataReaderOptions` and
`MetadataWriterOptions` in `metadata`. Set content budgets through `limits`
and the reader's `max_read_bytes`. Custom resolvers implement `get_encoded`.
Iterate members with `Archive::entries`;
wire codecs and manifest engines are internal workspace interfaces.
Create positional member cursors with `Archive::open_cursor`; each cursor is
bound to its originating archive. Read file metadata through `File::metadata`.

## Guides

- [Configuration](https://brian14708.github.io/zstar/guides/rust/configuration/):
  features, validation, credentials, caches, filesystem restoration, and limits.
- [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/):
  custom storage, chunking, ownership, and cancellation.
- [Format specifications](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md).
