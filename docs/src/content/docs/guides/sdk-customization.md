---
title: SDK customization
---

Implement a storage interface to add a database, object store, cache,
tracing wrapper, or credential policy.

## Choose an interface

| Task | Rust | Go | Python |
| --- | --- | --- | --- |
| Ordinary archive reads | `Archive` | `Reader` | `Archive` / `AsyncArchive` |
| Sequential member reads | `open_file`, `AsyncRead` / `AsyncSeek` | `OpenStream`, `io.Reader` / `io.Seeker` | `open`, `RawIOBase` / async member stream |
| Concurrent positional reads | `open_file`, `File::read_range` | `OpenFile`, `ReadAtContext` | `read(path, offset=..., length=...)`, `read_many` |
| Standard filesystem consumers | Filesystem helpers | `Reader` implements `fs.FS` | `ZstarFileSystem` implements fsspec |
| Custom archive storage | `AsyncRangeReader` | `RangeReader`; `ReaderAtSource` adapts `io.ReaderAt` | `RangeSource` / `AsyncRangeSource`; `zstar.open` |
| Custom external object reads | `ObjectResolver` | `EncodedContentResolver` | `ObjectResolver` / `AsyncObjectResolver` |
| Custom archive output | `AsyncWrite` | `io.Writer` | `BinaryWriter` / `AsyncBinaryWriter` |
| Custom external object output | `chunking::ObjectWriter` | `ChunkObjectWriter` | `ObjectStore` / `AsyncObjectStore` |
| Custom chunk boundaries | `chunking::external_chunks` | `ExternalizeChunks` | `add_external` with raw chunk iterable |
| Temporary metadata storage | Bounded in-memory buffers | `CreateSpillStore` | Bounded in-memory buffers |

## Adapter contracts

An archive source supplies an immutable snapshot, including an immutable size.
Range reads return exactly the requested bytes and reject short, reversed, and
out-of-bounds ranges. Concurrent calls must not race through a shared seek cursor.
Remote adapters must pin object identity/version, not just assume that equal
lengths mean unchanged content. Built-in HTTP sources enforce a strong ETag.

External resolvers return one complete independent encoded Zstandard object.
The address identifies decoded bytes; an encoded-size hint is advisory and must
not determine allocation or truncate a fetch. Enforce the actual encoded-byte
limit while fetching. Missing objects, transport failures, and resource-limit
failures should remain distinguishable. The archive handles decompression,
selection bounds, checksums, and the configured SHA-256 policy.

Python resolvers raise `FileNotFoundError` for unavailable objects. Rust returns
`Error::MissingObject`. Go's S3 adapter reports the `missing-external-content`
category and wraps `s3.ErrNotFound` for use with `errors.Is`.
Python and Go return owned bytes; Rust may return shared immutable `Bytes`.
Callbacks must not reenter the same writer or a serialized metadata operation.

Cache encoded ranges by archive snapshot and representation. An external digest
does not fix compressed bytes or compressed length. Never promote unchecked
content into hash-verified content merely because the address matches.

Rust's [`CachedRangeReader`](/zstar/guides/rust/configuration/#cache-range-sources)
and Go's [`NewCachedRangeReader`](/zstar/guides/go/configuration/#cache-range-sources)
provide bounded caching, shared fetches, and optional forward read-ahead around
their range-reader interfaces. Python exposes the same Rust cache through
`options={"source": {"range_cache": {"capacity_bytes": 64 << 20}}}`.
The corresponding TypedDicts live in `zstar.types`.

Archive `read_many` / Go `ReadMany` groups repeated sources and bounds concurrency.
Resolvers can implement caching, retries, and instrumentation around individual
requests.

## Ownership and cancellation

| Resource | Ownership and cleanup |
| --- | --- |
| Custom direct archive source or resolver | Borrowed/shared; archive close/drop does not close it |
| Custom direct writer sink | Python `zstar.open(sink, mode="w")` and Go borrow it; Rust `finish` returns its sink without shutdown |
| Body passed to a writer | Borrowed; caller closes it |
| Object writer/store | Borrowed; caller owns retention and publication |
| Go custom spill store | Exclusively transferred to SDK use; Go closes it |
| Python `zstar.open` with a path/URL/buffer | Archive/writer owns storage; context exit closes or aborts it |
| Native Python source/sink builder | Adapter owns storage; passing it to `open` borrows it |
| File object supplied to Python fsspec | Filesystem owns and closes it |
| Caller-supplied fsspec filesystem | Borrowed; remains usable after archive close |

Go contexts apply per operation. The context passed to `Open` or `OpenFile` does
not become a lifetime deadline. `io/fs` and plain `io.Reader` methods use a
background context; use `ReadContext` or `ReadAtContext` for remote deadlines.
A caller-owned blocking `io.ReaderAt` cannot be interrupted by a context.

Rust source futures must cancel pending work when dropped. Async writing and
builder initialization require Tokio. `Builder::try_new` and
`try_with_options` report initialization errors before returning.
Append failures/cancellation poison the writer; discard partial output.

Python async callbacks return awaitables and execute on the opening event loop
with each initiating operation's context. Keep that loop running until close.
Use explicit blocking adapters for sync objects in async APIs. Sync callbacks
execute on bounded blocking workers, with at most eight admitted globally and
one call per user object. Running callbacks cannot be interrupted; close drains
them. Reader `concurrent_chunks` also bounds native content work.

Async writers use the native async builder with bounded input/output channels.
Cancellation poisons an admitted writer mutation, cancels pending async I/O,
and retains running blocking jobs until cleanup. Callbacks must not reenter an
archive/writer or another archive sharing the active callback object.

Python filesystem destructors schedule best-effort cleanup without blocking the
collecting thread. Use explicit `close` or context managers to wait
for resource release; garbage collection must not wait inside another fsspec
operation's locks.

Objects uploaded before a failed external write may remain in storage. Finalizing
an archive does not commit an object-store transaction or garbage-collect objects.
The caller owns publication, multipart completion, and retention.

## Language-specific configuration

Use Rust's focused metadata options for reader and writer settings:

```rust
let reader = zstar::ReaderOptions {
    metadata: zstar::MetadataReaderOptions {
        max_entries: 100_000,
        ..Default::default()
    },
    ..Default::default()
};
let writer = zstar::WriterOptions::default();
// Initialize with: Builder::try_with_options(sink, writer).await?
```

Both focused types bound individual pages with `max_page_bytes`. Writers also
bound each temporary buffer with `max_buffer_bytes`; readers bound their retained
index and cached pages with `max_retained_entry_bytes`. Reader-only fields cannot be passed
through `MetadataWriterOptions`. Both outer option types store these directly
in `metadata`; content limits live in `limits` and `ReaderOptions.max_read_bytes`.
Implement Rust async storage and transport traits with
`#[async_trait::async_trait]` on each implementation. The traits support runtime
selection directly through `Arc<dyn AsyncRangeReader>`, `Arc<dyn ObjectResolver>`,
`Arc<dyn HttpTransport>`, and `Arc<dyn RequestProvider>`. External writers can use
`Box<dyn ObjectWriter>` and pass `writer.as_mut()` to chunking helpers. Futures
are boxed and `Send`; dropping them cancels pending work.

Go content controls belong in `ReaderOptions.Manifest`:

```go
options := zstar.ReaderOptions{
    Manifest: zstar.ManifestReaderOptions{ConcurrentChunks: 4, MaxReadBytes: 8 << 20},
}
```

`OpenStream` is the ordinary sequential interface; `OpenFile` is for
random access.

Python uses partial `ReaderOptions` and `WriterOptions` TypedDict dictionaries
from `zstar.types`, shared by sync and async calls. Rust supplies defaults and
validates them when opening. Pass one dictionary through `options=`, nesting
metadata policy under `metadata` and transport settings under `source` or `sink`.
Callbacks must match the selected sync or async mode. Positional reads use offset/length:

```python
from zstar import ReadRequest
from zstar.types import ReaderOptions

options = ReaderOptions(max_read_bytes=8 << 20, concurrent_chunks=4)
requests = [ReadRequest(path="data.bin", offset=10, length=20)]
# zstar.open(source, options=options)
# archive.read_many(requests)
```

Rust member ranges are half-open `start..end`. Go and Python use offset/length.
All sizes are bytes.

## Python metadata and external writes

`entry()` and iteration return immutable native `Entry` values. Use `to_header()`
for a rewrite, preserving root extensions explicitly:

```python
import zstar
from zstar import EntryKind
from zstar.types import WriterOptions

with zstar.open(
    output, options=WriterOptions(root_extensions=source.root_extensions), mode="w"
) as writer:
    for entry in source:
        if entry.kind is EntryKind.FILE:
            with source.open(entry.path) as body:
                writer.addfile(entry.to_header(), body)
        else:
            writer.addfile(entry.to_header())
```

This preserves logical metadata and exact timestamps while regenerating inline
extents. For thin output use `add_external(header, chunks, store)`. Nonempty
raw chunks must total `header.size`; empty files use no chunks. The SDK hashes
and compresses each chunk before calling `store.put_encoded`. The store returns
the positive stored encoded length, including for equivalent existing encodings.
Async writers require async chunk iterables and async stores.

## Transport and credentials

Rust `ReqwestTransport::with_client(client, max_response_bytes)` accepts pooled,
configured clients. Disable automatic response decompression and redirects;
archive range validation must see the stored representation. Use
`HttpTransport` for a different transport, `RequestProvider` for refreshed signed
requests, or configured `object_store` implementations for storage policy.

Go HTTP sources accept a configured `http.Client` (including custom
`RoundTripper`), and S3 adapters accept caller-configured AWS clients.
Python `zstar.open` accepts transport settings under `ReaderOptions.source` or
`WriterOptions.sink` in its single `options=` dictionary. All option TypedDicts,
including `SourceOptions` and `SinkOptions`, live in `zstar.types` and serve sync
and async calls. It owns opened storage; successful owned writer close finalizes and
publishes, while failure aborts. `range_cache` enables native compressed interval
caching without fsspec. Borrow a seekable binary file with `zstar.open`
or `await zstar.open(..., async_=True)`. HTTP sources accept mode-appropriate
signed-request providers. Native S3 uses explicit credentials or anonymous access.
Custom clients implement the source protocols. The optional `zstar.fsspec`
integration accepts configured fsspec filesystems through `fs=` and retains its
normal credential-provider behavior.

## Defaults and bounded-memory profiles

| Setting | Default | Meaning of zero |
| --- | --- | --- |
| Per-source frame/window and entry/extent/pointer limits | Disabled | Disables that additional limit |
| Content concurrency | 8 | Go selects default; Rust/Python require a positive value |
| Returned allocating read | 64 MiB | Rust/Python reject zero; Go selects default |
| Entry count cap | No additional cap | No additional cap |
| Rust/Python metadata page limit | 8 MiB encoded or decoded | Rejected |
| Rust/Python retained metadata | 1 GiB | Disallows retaining metadata |
| Rust/Python writer working buffer | 128 MiB each | Disallows buffering metadata |
| Go metadata encoded/decoded budgets | 4 GiB each | Selects default |
| Go metadata spool threshold | 8 MiB | Selects default |
| Go spool quota | 4 GiB | Explicit zero-byte quota disallows retention; nil selects default |
| Member decoded-source cache | 2 MiB, private | Not configurable |
| Compression workers | Serial | Serial |

Decoded caches belong to retained member handles and are released on drop or
close. Separate handles have independent completed caches; ordinary archive reads
share active loads without retaining completed frames. Member cache sizes include
bookkeeping, so a source equal to the nominal budget may not fit. Raw-byte caching and prefetching belong to caller-supplied sources
and backends; archives do not retain encoded ranges across operations. Rust and
Python keep metadata in memory and enforce page, working-buffer, and retained-index
budgets. Go also supports temporary spill storage with a separate quota.

A useful content profile is four concurrent chunks, 64 MiB encoded/decoded/window
limits per source, and an 8 MiB allocating-read limit. Configure metadata/index
budgets separately for the expected entry count. This is a local
resource policy and may reject otherwise conformant archives.

Peak memory includes the directory and cached pages, metadata buffers, up to the configured
number of encoded/decoded sources and codec workspaces, all member caches, and
all caller-retained results. There is no single aggregate process-memory cap;
multiple open readers multiply their reader-local budgets. Limit reader count,
concurrency, source size, returned buffers, and cache sizes together.
