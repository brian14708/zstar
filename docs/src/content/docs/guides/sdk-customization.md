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
| Concurrent positional reads | `open_cursor`, `FileCursor` | `OpenFile`, `ReadAtContext` | `read_range`, `read_many` |
| Standard filesystem consumers | Filesystem helpers | `Reader` implements `fs.FS` | `ZstarFileSystem` implements fsspec |
| Custom archive storage | `AsyncRangeReader` | `RangeReader`; `ReaderAtSource` adapts `io.ReaderAt` | `RangeSource` / `AsyncRangeSource`; `from_source` |
| Custom external object reads | `ObjectResolver` | `EncodedContentResolver` | `ObjectResolver` / `AsyncObjectResolver` |
| Custom archive output | `AsyncWrite` | `io.Writer` | `BinaryWriter` / `AsyncBinaryWriter` |
| Custom external object output | `chunking::ObjectWriter` | `ChunkObjectWriter` | `ObjectWriter` / `AsyncObjectWriter` |
| Custom chunk boundaries | `chunking::external_chunks` | `ExternalizeChunks` | `add_external` with raw chunk iterable |
| Temporary metadata storage | `SpillFactory` / `SpillStore` | `CreateSpillStore` | `create_spill_store` |

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
`Error::MissingObject`; Go adapters return an error matching `fs.ErrNotExist`.
Python and Go return owned bytes; Rust may return shared immutable `Bytes`.
Callbacks must not reenter the same writer or a serialized metadata operation.

Cache encoded ranges by archive snapshot and representation. An external digest
does not fix compressed bytes or compressed length. Never promote unchecked
content into hash-verified content merely because the address matches.

Archive `read_many` / Go `ReadMany` groups repeated sources and bounds concurrency.
Resolvers can implement caching, retries, and instrumentation around individual
requests.

## Ownership and cancellation

| Resource | Ownership and cleanup |
| --- | --- |
| Direct archive source or resolver | Borrowed/shared; archive close/drop does not close it |
| Direct writer sink | Borrowed in Python/Go; Rust `finish` returns its sink without shutdown |
| Body passed to a writer | Borrowed; caller closes it |
| Object writer/store | Borrowed; caller owns retention and publication |
| Custom spill store | Exclusively transferred to SDK use; Go closes it, Rust drops it, Python releases its reference |
| Path opened by Python factories | SDK owns the file and closes it |
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

Python async source/resolver methods declared with `async def` run on the event
loop active when bound. Keep that loop running until archive close. Cancellation
cancels a pending coroutine. Synchronous callbacks use blocking workers and cannot
be interrupted once running: source callbacks admit eight per source, external
resolver callbacks admit eight globally, and writer operations admit eight
globally. `concurrent_chunks` additionally bounds each reader's native work.

Async writers accept `async def read`, `write`, `flush`, and `put_encoded` methods.
Native compression remains on workers. Cancellation cancels pending async I/O
and joins native writer work before releasing output ownership. Synchronous
callbacks still require cooperative deadlines in their own implementation.
Do not suppress cancellation indefinitely inside an async storage coroutine.

Python filesystem destructors schedule best-effort cleanup without blocking the
collecting thread. Use explicit `close` / `aclose` or context managers to wait
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

Both focused types contain `SpoolOptions`. Reader-only fields cannot be passed
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
streamOptions := zstar.StreamOptions{DisableCache: true}
```

`OpenStream` is the ordinary sequential interface; `OpenFile` is for
random access. Use `Verify` with `VerifyArchive` or `VerifyExternalClosure`.

Python distinguishes synchronous `ReaderOptions` from `AsyncReaderOptions`;
synchronous readers reject coroutine-based sources/resolvers before scheduling
work. Use these TypedDicts for reusable option dictionaries and expand them into
factories with `**options`.

Use named requests to distinguish lengths from end positions:

```python
from zstar.types import ReadRequest

requests = [ReadRequest(path="data.bin", offset=10, length=20)]
# archive.read_many(requests)
```

Rust member ranges are half-open `start..end`. Go ranges and Python source/batch
requests use offset/length. Python `read_range(name, start, end)` uses a half-open
end, while `read(name, offset=..., length=...)` uses length. All sizes are bytes.

## Python metadata and external writes

Ordinary `getinfo`/`members` return standard `TarInfo`, warning when extensions
would be lost. For extension-preserving rewrites use `getmetadata` and explicitly
preserve root extensions:

```python
with ArchiveWriter(output, root_extensions=source.root_extensions) as writer:
    for name in source.names():
        metadata = source.getmetadata(name)
        if metadata.info.isfile():
            with source.open(name) as body:
                writer.addfile(metadata.info, body, extensions=metadata.extensions)
        else:
            writer.addfile(metadata.info, extensions=metadata.extensions)
```

This preserves logical metadata, including exact timestamps, but regenerates
physical layout and inline extents. For thin output, use `add_external(info,
chunks, store, extensions=...)`. `chunks` yields nonempty raw bytes totaling
`info.size`; an empty file uses an empty iterable. The SDK computes addresses,
compresses chunks, and invokes `store.put_encoded(address, encoded, raw_size)`.
The store returns a positive stored encoded length, including when reusing an
equivalent existing representation. Async writers also accept async chunk
iterables and async object writers.

## Transport and credentials

Rust `ReqwestTransport::with_client(client, max_response_bytes)` accepts pooled,
configured clients. Disable automatic response decompression and redirects;
archive range validation must see the stored representation. Use
`HttpTransport` for a different transport, `RequestProvider` for refreshed signed
requests, or configured `object_store` implementations for storage policy.

Go HTTP sources accept a configured `http.Client` (including custom
`RoundTripper`), and S3 adapters accept caller-configured AWS clients.
Python accepts configured fsspec filesystems through `fs=` for sessions,
credentials, retries, proxies, and backend caching. `AsyncArchive.from_url`
accepts an async signed-request provider. For another native client/session,
implement `RangeSource` or `AsyncRangeSource` instead of duplicating the HTTP/S3
factory. Direct native S3 factories use explicit credentials; fsspec retains its
normal credential-provider behavior.

## Defaults and bounded-memory profiles

| Setting | Default | Meaning of zero |
| --- | --- | --- |
| Per-source frame/window and entry/extent/pointer limits | Disabled | Disables that additional limit |
| Content concurrency | 8 | Go selects default; Rust/Python require a positive value |
| Returned allocating read | 64 MiB | Rust/Python allow zero bytes; Go selects default |
| Retained entry index | 1 GiB | Rust/Python allow zero bytes; Go selects default |
| Entry count cap | No additional cap | No additional cap |
| Metadata encoded/decoded budgets | 4 GiB each | Rust/Python allow zero bytes; Go selects default |
| Metadata in-memory spool threshold | 8 MiB | Rust/Python spill immediately; Go selects default |
| Spool quota | 4 GiB | An explicit zero-byte quota disallows retention; Go nil selects default |
| Member decoded-source cache | 2 MiB | Rust/Python disable; Go selects default |
| Compression workers | Serial | Serial |

Go `StreamOptions.DisableCache` explicitly disables caching. Rust/Python member
cache sizes include bookkeeping, so a source equal to the nominal budget may
not fit. A spill quota does not bound the retained index, source buffers, or
codec workspace. Custom spill factories do not guarantee bounded total memory.

A useful content profile is four concurrent chunks, 64 MiB encoded/decoded/window
limits per source, and an 8 MiB allocating-read limit. Configure metadata/index
and spool budgets separately for the expected entry count. This is a local
resource policy and may reject otherwise conformant archives.

Peak memory includes the retained index, metadata buffers, up to the configured
number of encoded/decoded sources and codec workspaces, all member caches, and
all caller-retained results. There is no single aggregate process-memory cap;
multiple open readers multiply their reader-local budgets. Limit reader count,
concurrency, source size, returned buffers, and cache sizes together.
