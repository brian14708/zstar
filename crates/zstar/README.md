# Rust archive API

`zstar` is the main Rust library. Read and write indexed tar archives with
`Archive`, `Builder`, and `Header`.
Async I/O and temporary-file spooling are enabled by default. Archive operations
use a Tokio runtime; compression, hashing, and metadata processing run on
blocking workers. See the [format specification][spec] for wire details.

## Features

Run the small API examples from the repository root:

```sh
cargo run --locked -p zstar --example in_memory
cargo run --locked -p zstar --features fs --example local_range -- archive.zstar hello.txt 1 3
```


Storage support is optional and uses the same library dependency:

```toml
[dependencies]
zstar = { version = "0.1", features = ["fs", "s3"] }
```

| Feature | Purpose |
| --- | --- |
| `async` | Tokio archive readers and writers; enabled by default |
| `spooling` | Temporary-file metadata storage; enabled by default |
| `fs` | Local range sources, capture, and restoration in `zstar::fs`; enables `async` |
| `http` | Pooled HTTP range sources and object resolvers in `zstar::http`; enables `async` |
| `s3` | Native S3 reads, uploads, and presigned requests in `zstar::s3`; enables `http` |
| `compression` | Lower-level Zstandard codecs; enabled by `async` |

Use `default-features = false` for codec-independent parsing.

## Create and read an archive

```rust
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use zstar::{Archive, Builder, Header, VerificationScope};

#[tokio::main]
async fn main() -> zstar::Result<()> {
    let mut builder = Builder::new(Vec::new());
    let mut header = Header::file("hello.txt", 5);
    header.set_mode(0o640);
    builder.append_data(header, b"hello".as_slice()).await?;
    let bytes = builder.finish().await?;

    let archive = Archive::from_bytes(bytes).await?;
    assert_eq!(archive.read("hello.txt").await?, b"hello".as_slice());
    assert_eq!(
        archive.read_range("hello.txt", 1..4).await?,
        b"ell".as_slice()
    );

    let mut file = archive.open_file("hello.txt").await?;
    file.seek(SeekFrom::Start(2)).await?;
    let mut tail = String::new();
    file.read_to_string(&mut tail).await?;
    assert_eq!(tail, "llo");

    archive.verify(VerificationScope::Archive).await?;
    Ok(())
}
```

`Builder` writes to any `AsyncWrite + Unpin` sink. `append_data` reads an
`AsyncRead + Unpin` input and checks that its length matches the header's size,
including checking EOF. Use `append_bytes` for a byte slice, `append_dir` for a
directory, and `append_symlink` for a link with an exact target string.
`append_header` accepts custom metadata for directories, symlinks, and empty
files. `append_external` adds an `Entry` with external-object extents.

Supply entries in any order and include their ancestor directories. Tar members
follow insertion order; `finish` sorts the manifest and rejects duplicate paths
and missing ancestors. It writes the footer, flushes the sink, and returns it.
An append error or a cancelled append makes the builder unusable; discard its
partial output. Remote upload sinks may require a separate `shutdown` or abort
operation; see the [S3 guide][s3].

`Header` describes input metadata; the encoder assigns the output extents. Its
setters accept mode, UID, GID, nanosecond mtime, and `x-*` CBOR attributes. Long
paths, wide IDs, and fractional timestamps use local PAX records. Extension
values use `ciborium::Value`; nested maps require unique text keys. The encoder
rejects unsupported values and duplicate keys.

## Iterate over entries

`entries()` returns an owned, fused stream of `Result<File<_>>` values in path
order. It reads metadata without fetching payload bytes. Pin the stream to call
`try_next`, or consume it with stream combinators such as `try_collect`.

```rust
use futures_util::TryStreamExt;
use std::pin::pin;
use zstar::{Archive, AsyncRangeReader, Result};

async fn print_entries<R>(archive: &Archive<R>) -> Result<()>
where
    R: AsyncRangeReader + ?Sized + 'static,
{
    let mut entries = pin!(archive.entries());
    while let Some(file) = entries.try_next().await? {
        println!("{}: {} bytes", file.path(), file.size());
    }
    Ok(())
}
```

Handles expose `path`, `size`, `kind`, and `metadata`. Regular files implement
Tokio `AsyncRead` and `AsyncSeek`; directory and symlink handles expose metadata.
A handle retains the archive and one validated decoded chunk. Streams and handles
can outlive the `Archive` that created them. Seeking permits positions beyond
EOF and rejects negative or overflowing positions.

Use `entry(path)` for optional metadata, `read_link(path)` for a symlink target,
or `collect_entries()` to collect all metadata into a `Vec<Entry>`.
`entry_count()` and `root_extensions()` inspect the immutable manifest.

## Read from storage

`Archive::open` accepts an `Arc` to an immutable `AsyncRangeReader`. `Bytes`
implements this trait with exact, zero-copy slices; `from_bytes` is the in-memory
convenience. The [filesystem][fs], [HTTP][http],
and [S3][s3] adapters provide local and remote range sources.

`open_with_options` accepts `ReaderOptions` to configure metadata budgets,
concurrency, decoded caching, and an optional `Arc<dyn ObjectResolver>`. The
resolver supplies encoded external objects for all reads through that archive.
Cloning an archive shares its source, index, cache, and concurrency limits.
Keep the source bytes immutable while readers or handles use them.

`read(path)` returns a complete file as `Bytes`; `read_range(path, start..end)`
returns a half-open byte range. Both respect the configured returned-read limit,
which defaults to 64 MiB. Use `open_file` to stream larger files. `read_many`
accepts `ReadRequest` batches and groups requests for shared content.

## Verify an archive

Opening authenticates and validates the complete manifest and namespace.
Selective reads validate the selected frame checksums and apply the configured
external-object hash policy. `read_with_report` and `read_range_with_report`
return bytes together with a description of those checks.

`verify(VerificationScope::Archive)` checks every physical frame, tar member,
pointer, padding byte, and end record. It does not fetch external objects.
`VerificationScope::ExternalClosure` additionally resolves and validates every
external selection. A successful `VerificationReport` names the verified scope,
counts frames and members, and lists checked or explicitly unchecked external
hashes.

Metadata SHA-256 and data-frame checksums are mandatory. External SHA-256
verification defaults to `true`; `ReaderOptions::verify_external` controls it.

## Configure writing

`Builder::with_options` accepts `WriterOptions`:

| Option | Default | Purpose |
| --- | --- | --- |
| `frame_size` | 1 MiB | Decoded data-frame target; a multiple of 512 bytes from 512 bytes to 16 MiB |
| `compression_level` | 3 | Zstandard compression level |
| `workers` | 0 | Compression workers, from 0 (serial) to 16 |
| `manifest` | `ManifestOptions::default()` | Metadata encoding and storage budgets |
| `root_extensions` | Empty | Archive-level `x-*` CBOR values |

Smaller frames reduce the amount fetched for a small read. Parallel compression
produces the same bytes as serial compression. Bounded channels connect input,
compression, and output. Encoder records spill to automatically removed stores;
codec jobs release their Tokio blocking worker between operations.

## Budgets and caches

`ManifestOptions` controls metadata storage, parsing, compression segments, and
returned-read limits. Defaults are 8 MiB before spill, 4 GiB aggregate metadata,
64 KiB decoded segment targets, and 64 MiB returned reads. Readers retain sorted
entries in O(N) memory in addition to spool budgets.

`max_retained_entry_bytes` defaults to 1 GiB and includes spare entry-vector
capacity; `max_entries` adds a count cap (zero disables it). Temporary parsed
values, capacity-compaction copies, and codec workspaces are additional memory.

The shared `Arc<SpoolBudget>` limits logical storage across related spools;
reservations precede store I/O, and failed partial writes retain their charge
until the spool is dropped. The quota includes in-memory spools and excludes
filesystem allocation overhead, transient migration copies, decoded caches,
and caller-retained buffers. `create_spill_store` supplies a factory for
a fresh, exclusive seekable store, including when native spooling is disabled.

The default reader retains up to 16 MiB of decoded content and loads up to eight
chunks concurrently. Concurrent inline and verified-object misses share work
even when retention is disabled. Unchecked objects share work within a batch
and bypass retained and shared caches. Returned bytes and active file chunks
remain valid independently of the retained-cache budget.

`Archive::with_shared_decoded_cache` attaches a caller-owned `SharedDecodedCache`;
`MemoryDecodedCache` provides a bounded LRU implementation. Its source identity
must name the exact immutable archive snapshot, such as a full digest or
qualified object version. Metadata SHA-256 alone does not identify inline data.
Verified objects use their decoded SHA-256 across readers. Cached chunks are
immutable validation tokens. Custom cache callbacks run on bounded workers and
must not re-enter the reader.

Dropping an archive read or verification future cancels pending transport I/O.
Dropping a file handle cancels its pending source read. CPU and filesystem jobs
that have already started retain their permits until they finish.

## Format primitives

`ManifestEncoder` and `ManifestReader` support lower-level format work.
`ManifestReader::iter()` and `IntoIterator` for `&ManifestReader` yield borrowed
entries through double-ended, exact-size, fused iterators.

Disable default features for codec-independent CBOR, footer, pointer, and
tar-header parsing. Parsing alone does not establish archive validity. The
hidden `backend` module provides blocking codec support for native bindings,
CLI capture and restoration, and format tests.

[spec]: https://github.com/brian14708/zstar/blob/main/spec/v1.md
[fs]: https://github.com/brian14708/zstar/blob/main/crates/zstar/docs/fs.md
[http]: https://github.com/brian14708/zstar/blob/main/crates/zstar/docs/http.md
[s3]: https://github.com/brian14708/zstar/blob/main/crates/zstar/docs/s3.md
