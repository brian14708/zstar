# Zstar for Rust

Read and write indexed `.tar.zst` archives with async or blocking Rust.
Implements the Zstar core specification and its optional external-content extension.

Archives use the 44-byte `Z*01` footer with 64-bit metadata lengths.
Data and metadata use Zstandard frame checksums, with no separate metadata checksum.
Opening validates the metadata directory. Reads validate selected entry pages and ancestors.
`ReadReport::metadata_validated` reports metadata validation, not cryptographic authentication.

[Rust API reference](https://docs.rs/zstar/latest/zstar/)

```toml
[dependencies]
zstar = "0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Get started

The root API is asynchronous. Use `Archive` to read and `Builder::try_new(sink).await?` to initialize a writer.
Use `Builder::try_with_options` for custom options; initialization requires Tokio
and returns configuration or storage errors immediately. `Builder::finish`
finalizes the archive. The default feature enables async I/O. Metadata working buffers stay in memory.
Enable `fs`, `http`, or `s3` for storage adapters.

Follow [your first archive](https://brian14708.github.io/zstar/tutorials/rust/first-archive/)
to write and read an archive, then try
[S3 storage](https://brian14708.github.io/zstar/tutorials/rust/s3/).

Reader and writer options store `MetadataReaderOptions` and
`MetadataWriterOptions` in `metadata`. Set content budgets through `limits`
and the reader's `max_read_bytes`. Custom resolvers implement `get_encoded`.
`Archive::entry` and `entries` return immutable `EntryMetadata` with getters such
as `path()`, `kind()`, and infallible `size()`. `entries_with_prefix` selects an
exact string prefix. Open content with `open_file`; its `read_range` performs
positional reads without changing the stream position, sharing the handle's
decoded cache. Use `File::metadata` for the same logical metadata snapshot.
Physical extents live in the advanced `layout` module; wire codecs, manifest
engines, and binding cursors are internal workspace interfaces.

Enable `fs` to use `Archive::open_path` in either I/O mode. Blocking readers also
accept standard `Read + Seek` inputs through `Archive::from_reader`.
Both builders accept `append_external(header, extents)` for already stored
external content, with the declared header size checked against the extents.

## Blocking I/O

`zstar::blocking` provides supported readers, builders, and `Read + Seek` member
handles without Tokio. Disable default features for a runtime-free dependency:

```toml
zstar = { version = "0", default-features = false, features = ["fs"] }
```

```rust
use zstar::blocking::{Archive, Builder};
let mut writer = Builder::try_new(Vec::new())?;
writer.append_bytes("hello", b"world")?;
let mut archive = Archive::from_bytes(writer.finish()?)?;
assert_eq!(archive.read("hello")?, b"world");
# Ok::<(), zstar::Error>(())
```

Blocking open validates all metadata eagerly. Member handles exclusively borrow
the archive until dropped. Both modes share headers, writer options, format
validation, read planning, decoding, and provenance semantics. The `fs` feature
works alone; async local sources require `fs` and `async`.
See [blocking Rust](https://brian14708.github.io/zstar/guides/rust/blocking/)
and the `blocking_memory` and `blocking_file` examples.

## Guides

- [Configuration](https://brian14708.github.io/zstar/guides/rust/configuration/):
  features, validation, credentials, caches, filesystem restoration, and limits.
- [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/):
  custom storage, chunking, ownership, and cancellation.
- [Format specifications](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md).

Async readers validate the directory on open and load entry pages on demand.
Listing loads entry pages incrementally, validating selected entries and their
ancestors. Cached pages are evicted within the retained metadata budget.
`archive.validate_all_metadata().await?` and `entry_count().await?` scan and
validate all metadata, including optional extension pages.
Blocking readers validate all metadata on open.

Use `ReaderOptions::bounded()` or `WriterOptions::bounded()` for finite frame,
metadata page, entry, and memory budgets. Fluent `with_*` setters customize options and
headers; `validate()` checks options before I/O. The returned-read budget caps
output buffers separately from frame decoding. Resource failures carry a typed
`BudgetResource` and numeric bounds when known. Filesystem helpers return
`zstar::Result`, preserving archive errors and I/O sources.

## Cache range sources

Wrap an immutable `AsyncRangeReader` in `CachedRangeReader` to cache encoded
ranges and share concurrent fetches. Share the decorator through `Arc` and pass
it to `Archive::open`. Read-ahead is opt-in; ordinary reads still decode and
validate frames. Retain a `File` to reuse decoded frames.

See [source caching](https://brian14708.github.io/zstar/guides/rust/configuration/#cache-range-sources)
for configuration, memory limits, and snapshot requirements.
