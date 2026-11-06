---
title: SDK customization
---

Storage is replaceable; archive encoding and validation remain shared format
behavior. Implement a storage interface to add a database, object store, cache,
tracing wrapper, or credential policy. No plugin registration is needed.

## Choose an interface

| Task | Rust | Go | Python |
| --- | --- | --- | --- |
| Ordinary archive reads | `Archive` | `Reader` | `Archive` / `AsyncArchive` |
| Sequential member reads | `open_file`, `AsyncRead` / `AsyncSeek` | `OpenStreamWithOptions`, `io.Reader` / `io.Seeker` | `open`, `RawIOBase` / async member stream |
| Concurrent positional reads | `open_cursor`, `FileCursor` | `OpenFile`, `ReadAtContext` | `read_range`, `read_many` |
| Standard filesystem consumers | Filesystem helpers | `Reader` implements `fs.FS` | `ZstarFileSystem` implements fsspec |
| Custom archive storage | `AsyncRangeReader` | `RangeReader`; `ReaderAtSource` adapts `io.ReaderAt` | `RangeSource` / `AsyncRangeSource`; `from_source` |
| Custom external object reads | `ObjectResolver` | `EncodedContentResolver` | `ObjectResolver` / `AsyncObjectResolver` |
| Custom archive output | `AsyncWrite` | `io.Writer` | `BinaryWriter` / `AsyncBinaryWriter` |
| Custom external object output | `chunking::ObjectWriter` | `ChunkObjectWriter` | `ObjectWriter` / `AsyncObjectWriter` |
| Custom chunk boundaries | `chunking::external_chunks` | `ExternalizeChunks` | `add_external` with raw chunk iterable |
| Temporary metadata storage | `SpillFactory` / `SpillStore` | `CreateSpillStore` | `create_spill_store` |

Rust/Go manifest, extent, and spool primitives are advanced interfaces. Ordinary
applications should start with the archive interfaces above. Rust's doc-hidden
`backend` module supports workspace bindings and the CLI; it is not a supported
SDK interface. Python underscored modules and native tuples remain private.

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
requests. Rust's legacy resolver batch/length methods are available to explicit
callers; the archive reader does not consult them. They are not a performance
promise, and other SDKs do not require corresponding unused hooks.

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

Rust source futures must cancel pending work when dropped. Constructing a
`Builder` is lazy and requires no runtime. Async writing requires Tokio;
`Builder::try_new` and `try_with_options` report initialization errors before
returning. Legacy `new` and `with_options` report them at the first operation.
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

Rust separates focused metadata settings from content policy while retaining the
legacy `ManifestOptions` representation for compatibility:

```rust
let reader = zstar::ReaderOptions::default()
    .with_metadata(zstar::MetadataReaderOptions {
        max_entries: 100_000,
        ..Default::default()
    });
let writer = zstar::WriterOptions::default()
    .with_metadata(zstar::MetadataWriterOptions::default());
// Initialize with: Builder::try_with_options(sink, writer).await?
```

Both focused types contain `SpoolOptions`. Reader-only fields cannot be passed
through `MetadataWriterOptions`. Direct `manifest` fields remain available for
advanced callers. Implement Rust async storage and transport traits with
`#[async_trait::async_trait]` on each implementation. The traits support runtime
selection directly through `Arc<dyn AsyncRangeReader>`, `Arc<dyn ObjectResolver>`,
`Arc<dyn HttpTransport>`, and `Arc<dyn RequestProvider>`. External writers can use
`Box<dyn ObjectWriter>` and pass `writer.as_mut()` to chunking helpers. Futures
are boxed and `Send`; dropping them cancels pending work.

Go content controls belong on `ReaderOptions`:

```go
options := zstar.ReaderOptions{ConcurrentChunks: 4, MaxReadBytes: 8 << 20}
streamOptions := zstar.StreamOptions{DisableCache: true}
```

Nonzero top-level controls override the legacy nested `Manifest` fields.
`OpenStreamWithOptions` is the ordinary sequential interface; `OpenFile` is for
random access. Use `VerifyScope` with `VerifyArchive` or `VerifyExternalClosure`;
`Verify(ctx, bool)` is retained for compatibility.

Python distinguishes synchronous `ReaderOptions` from `AsyncReaderOptions`;
synchronous readers reject coroutine-based sources/resolvers before scheduling
work. Python also offers `ContentOptions`, `MetadataOptions`, and `SpoolOptions` TypedDicts
for reusable groups; expand them as keyword arguments into factories. Existing
flat keywords continue to work. `Archive.from_bytes` mirrors the async factory.
Use named requests to distinguish lengths from end positions:

```python
from zstar.types import ReadRequest

requests = [ReadRequest(path="data.bin", offset=10, length=20)]
# archive.read_many(requests); legacy three-tuples also work.
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
iterables and async object writers. No native tuples are required.

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
| Member decoded-source cache | 2 MiB | Rust/Python disable; legacy Go size argument selects default |
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

Fixed Zstandard encoding, mandatory validation, unknown-required-extension
rejection, and source immutability remain format guarantees. `x-*` fields carry
application metadata and do not register executable extensions.
