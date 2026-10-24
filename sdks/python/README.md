# Zstar for Python

Install `zstar-py` and import `zstar`. The SDK uses the Rust archive engine for
metadata authentication, indexed reads, writing, and verification.

## Read an archive asynchronously

`AsyncArchive` reads bytes, local files, custom range sources, HTTP URLs, and S3
objects. Await its constructors and use `async with` to release the reader.

```python
import asyncio
from zstar import AsyncArchive, pack_file


async def main() -> None:
    encoded = pack_file("hello.txt", b"hello world")
    async with await AsyncArchive.from_bytes(encoded) as archive:
        async for member in archive:
            print(member.name, member.size)
        info = archive.getmember("hello.txt")
        file = archive.extractfile(info)
        assert file is not None
        async with file:
            await file.seek(6)
            assert await file.read(5) == b"world"
        await archive.verify()


asyncio.run(main())
```

| Constructor | Source |
| --- | --- |
| `await AsyncArchive.from_bytes(data, **options)` | Owned encoded byte snapshot |
| `await AsyncArchive.open(name, **options)` | Owned local file handle |
| `await AsyncArchive.open(fileobj=file, **options)` | Borrowed seekable binary stream |
| `await AsyncArchive.from_source(source, size, **options)` | Synchronous `read_range(offset, length)` callbacks on bounded workers |
| `Archive.from_url(url, headers=..., **options)` | Synchronous native HTTP range reader |
| `await AsyncArchive.from_url(url, headers=..., **options)` | Fixed or renewable presigned HTTP requests |
| `await AsyncArchive.from_s3(bucket, key, **options)` | Native S3 client; environment credentials and optional `endpoint`, `region`, `object_prefix` |

Member names and metadata follow `tarfile`: `getmember(name)` returns an owned
`TarInfo` snapshot and raises `KeyError` for missing names. It exposes `name`,
`size`, `mode`, `uid`, `gid`, `mtime`, `linkname`, and the usual `isfile()`,
`isdir()`, and `issym()` predicates. Zstar's `TarInfo` extends `tarfile.TarInfo`
with exact `mtime_ns` and an owned `extensions` dictionary for `x-*` CBOR values.

Iteration is lazy and yields members in path order without fetching payloads.
Unlike `tarfile`, `getmembers()` and `getnames()` return iterators:

```python
async for member in archive.getmembers():
    print(member.name)
names = [name async for name in archive.getnames()]  # Explicit collection.
```

`extractfile(name_or_tarinfo)` creates a member stream. On `AsyncArchive`,
`getmember()` and `extractfile()` are coroutines (`await archive.getmember(name)`)
because metadata lookup may fetch from a slow source and must not block the event loop. Directories return `None`; symlinks resolve only within the archive,
and cycles raise `tarfile.ExtractError`. Stream `read`, `readinto`, `seek`, `tell`,
and `close` are awaitable. Closing a member stream leaves its archive open;
closing the archive invalidates its member streams and iterators.

Opening authenticates and validates metadata and the complete namespace. Reads
fetch and validate selected frames. `await archive.verify()` checks every
physical frame, tar member, pointer, padding byte, and end record.
`await archive.verify(external=True)` also resolves and validates external content.
Verification streams content independently of the returned-read limit. Both
reader classes return the same report schema, with lists of checked and unchecked
external object addresses.

`read(name, offset=0, length=None)` returns owned `bytes`. `read_many(requests)`
accepts a bounded sequence of `(name, offset, length)` tuples and returns a list
in request order. Duplicate and overlapping requests share chunk loads within
a batch. Batches permit at most 4,096 requests, 65,536 planned range copies, and
the configured aggregate returned-read budget. Invalid requests fail before
payload I/O.

`read_with_report(name, offset=0, length=None)` returns `(bytes, report)` describing
metadata authentication, checked data frames, and checked or unchecked external
object addresses. It does not claim full physical archive validation.

Async transports use native Tokio operations; local filesystem work and
synchronous Python callbacks run on bounded workers. Cancelling an await drops
pending transport I/O. Already-started callbacks and codec jobs finish under
their capacity limits. `await archive.close()` rejects new operations, cancels
pending native work, and waits for admitted operations to drain. Concurrent
closes join the same drain. Close is idempotent and returned bytes stay valid.
Caller-owned streams, sources, spill stores, and shared caches stay open.

## Synchronous reads and filesystem helpers

`Archive(data)`, `Archive.open(name)`, `Archive.open(fileobj=file)`, and
`Archive.from_source(source, size)` use the same metadata and verification policy
as `AsyncArchive`. Member streams implement `io.RawIOBase`, including `readinto`,
line iteration, and seeking. Use `io.BufferedReader` for buffered line reads.

```python
from zstar import Archive, pack_file

with Archive(pack_file("hello.txt", b"hello world")) as archive:
    assert list(archive.getnames()) == ["hello.txt"]
    file = archive.extractfile("hello.txt")
    assert file is not None
    with file:
        file.seek(6)
        assert file.read() == b"world"
    archive.verify()
```

Path constructors own their file handles; `fileobj=` borrows a seekable binary
stream and preserves its cursor during sizing and ranged reads. Keep borrowed
streams and custom sources open and immutable while their archive is open.
Sources implement synchronous `read_range(offset, length)` and return exactly
the requested encoded bytes.

`pack_directory`, `extract`, and `materialize` use the Rust filesystem helpers on
Linux/macOS. They perform blocking native work while releasing the GIL.

```python
from zstar import pack_directory, extract, materialize

encoded = pack_directory("input")
extract(encoded, "restored")
inline = materialize(encoded, objects="local-objects")
```

`pack_directory` sorts UTF-8 paths and defaults to modes 0644 for files (0755
when executable), 0755 for directories, and 0777 for symlinks. UID, GID, and
mtime default to zero; special bits are cleared. `preserve_metadata=True`
retains source metadata. Link targets are stored verbatim and never followed.

Restoration creates files before links, holds directory handles, refuses
collisions and a symlink destination root, and applies directory metadata last.
It restores mtime/modes without ownership, strips setuid/setgid unless
`allow_special_bits=True`, and can leave a partial destination on failure.
Materialization streams external members into a portable inline archive.

## Write an archive

`ArchiveWriter` is a synchronous streaming writer accepting standard
`tarfile.TarInfo` metadata:

```python
from io import BytesIO
from tarfile import TarInfo
from zstar import Archive, ArchiveWriter

output = BytesIO()
with ArchiveWriter(output) as writer:
    info = TarInfo("hello.txt")
    info.size = 5
    writer.addfile(info, BytesIO(b"hello"))
with Archive(output.getvalue()) as archive:
    assert archive.read("hello.txt") == b"hello"
```

`addfile(tarinfo, fileobj=None)` writes a regular file, directory, or symlink.
Nonempty files require a binary source and use `tarinfo.size` as their exact
body length. Include ancestor directory members for nested paths, as required
by the format. Other tar member types are unsupported.

`add(name, arcname=None, recursive=True)` captures a filesystem path with its
metadata, recursively adding sorted descendants and directory members. Its
default archive name is the source basename. Symlinks are stored without being
followed. Use `ArchiveWriter.open(name, mode="w")` to own an output file or
`mode="x"` to require that it does not exist:

```python
with ArchiveWriter.open("backup.zstar", mode="x") as writer:
    writer.add("input", arcname="backup")
```

Normal context exit finalizes and flushes the archive. Exceptional exit discards
the unfinished writer; discard partial output. A directly supplied sink stays
open. `close()` finalizes explicitly and closes any writer-owned output file.
Entries may arrive in any order; physical tar members follow insertion order
and finalization sorts manifest records. Duplicate paths are rejected.

For format tooling, `zstar.format` contains `EntryArchiveWriter`,
`IndexedArchive`, `ManifestArchive`, and restricted CBOR codecs. Those APIs use
[wire entry tuples](../../spec/v1.md#manifest-schema); ordinary readers and writers use
`TarInfo` instead.

## Limits, caches, and external objects

| Option | Default | Purpose |
| --- | --- | --- |
| `memory_bytes` | 8 MiB | Metadata bytes retained before spill |
| `max_encoded_bytes`, `max_decoded_bytes` | 4 GiB each | Metadata parsing/storage budgets |
| `max_spool_bytes` | 4 GiB | Aggregate logical storage charged across spools |
| `max_read_bytes` | 64 MiB | Returned bytes per allocating read or batch, including member `read()`/`readall()` |
| `max_retained_entry_bytes` | 1 GiB | Estimated typed index bytes, including spare vector capacity |
| `max_entries` | 0 | Separate entry-count cap; zero disables it |
| `cache_bytes` | 16 MiB | Retained decoded content; zero disables retention |

The validated sorted index uses O(N) memory beyond the spool threshold. Active
chunks and returned bytes add to retained cache capacity. Read large files through
bounded ranges or repeated member-stream reads. Reading an entire member at once
is still subject to `max_read_bytes`. Member `readinto()` can fill larger
caller-owned buffers using bounded native reads. Every member stream retains at
most one validated decoded frame/object plus its extent seek index, independently
of cache capacity. The retained index cap also respects the existing four-times-decoded
metadata ceiling; transient parsed items, capacity-compaction copies, and codec
workspaces are additional memory. See [custom spill stores and shared caches](docs/advanced.md).

External resolvers implement `get_encoded("sha256:<hex>", hint, max_bytes)`.
Raise `FileNotFoundError` for missing objects. The hint describes encoded size;
it does not constrain decoded size. Both reader classes accept `resolver=` at construction for
all reads and external verification. Per-read overrides remain available.
Callbacks must be synchronous and support concurrent calls; native workers bound
their concurrency.

SHA-256 verification is enabled by default. `verify_external=False` skips only
external-object hashing; metadata authentication, archive checksums, and codec
bounds still apply. The policy is fixed at reader construction. Unchecked objects bypass retained
and shared caches but may share loads within a batch. Filesystem extraction and
materialization also accept this opt-out. Object files use
`<lowercase raw SHA-256>.zst`; `objects=` supplies their local directory.

Format failures raise `ZstarError`, a `ValueError` subclass with a stable
`category` matching the conformance corpus. Argument/type errors retain ordinary
Python exception types. Verification makes no inline or reconstructed-file
SHA-256 claims.

## HTTP and S3

`from_url` accepts additional signed headers and discovers size with GET, without
requiring HEAD permission. Both sync and async HTTP readers require a valid
strong ETag; subsequent requests enforce `If-Match`, including renewed URLs.
Weak ETags, missing validators and Last-Modified-only responses are rejected.
For renewable async requests, pass `provider=` instead of
`url` and `headers`:

```python
from typing import Literal
from zstar import AsyncArchive, put_presigned
from zstar.types import SignedRequest


async def provider(method: Literal["GET", "PUT"], force_refresh: bool) -> SignedRequest:
    signed = await signing_service.sign(method, refresh=force_refresh)
    return {"url": signed.url, "headers": signed.headers, "expires_in": 300}


async def transfer(encoded: bytes) -> bytes:
    async with await AsyncArchive.from_url(provider=provider) as archive:
        await put_presigned(encoded, provider=provider)
        return await archive.read("hello.txt")
```

Providers run on the originating asyncio loop with a fresh copy of the captured
context. They receive `GET` or `PUT`, may run concurrently, and return a URL plus
optional headers and expiry. `expires_in` must be finite and between zero and
seven days. Expiry triggers refresh; a 401/403 permits one forced refresh and
retry. Provider and transport errors redact signed URLs and callback details.
Cancellation cancels pending providers and network operations.

`from_s3` uses the maintained Rust S3 client for credentials, signing, connection
pooling, and retries. With `object_prefix`, external objects use
`<prefix>/<raw SHA-256>.zst`. Credentials, endpoints, and keys remain storage policy.
`put_presigned(data, url, headers=...)` accepts immutable bytes up to 64 MiB,
including empty uploads, and replays the same snapshot on authorization retry.
Python does not expose multipart uploads yet.

For completed encoded archives, `s3_archive_metadata(data)` inspects only the
footer and returns the three optional S3 user metadata keys. For example:

```python
from zstar import pack_file, s3_archive_metadata

data = pack_file("hello.txt", b"hello")
s3_client.put_object(
    Bucket="archives",
    Key="example.zstar",
    Body=data,
    Metadata=s3_archive_metadata(data),
)
```

`AsyncArchive.from_s3` uses the position from its pinned HEAD to fetch small
manifests and their footer together in one GET. The actual footer remains
authoritative for decoded size and digest; invalid or large hints use standard
discovery. See the [metadata key definitions](../../crates/zstar/docs/s3.md#discovery-key-definitions).

## fsspec integration

Install `zstar-py[fsspec]` for a read-only filesystem view. The adapter loads when
requested; ordinary `import zstar` does not import fsspec.

```python
from zstar import ZstarFileSystem

fs = ZstarFileSystem()
try:
    with fs.open("example.zstar!/hello.txt") as member:
        member.seek(6)
        assert member.read(5) == b"world"
finally:
    fs.close()
```

Local archive paths use retained native file handles, so replacing a path does
not retarget an open reader. HTTP URLs use native ranged GETs and require a
valid strong ETag; `storage_options` supports `headers` for HTTP. S3 locations
use s3fs version-pinned ranges or a retained S3File enforcing its pinned ETag;
backend credentials/options go in `storage_options`. Backends that cannot enforce
snapshot identity are rejected. An optional `opener(location)` may return encoded
bytes or a seekable binary stream that stays immutable for the reader's lifetime.
Byte openers receive full physical validation; other backends authenticate metadata.

`max_open_archives` defaults to eight and `cache_bytes` to 16 MiB per reader.
Open member files pin their archive; unpinned readers are evicted in least-recently
used order. Opening beyond capacity while all readers are pinned raises `OSError`.
Eviction and `fs.close()` close native readers and custom streams. Close attempts
every resource and groups multiple cleanup failures in an `ExceptionGroup`.
`fs.close()` is terminal: it rejects new work, waits for admitted archive loads to
finish cleanup, and invalidates existing members. Distinct pending loads are also
bounded by `max_open_archives`; custom openers must return for close to complete.
Directory symlink listings preserve the caller's alias and use the sorted index's
prefix range. Final and ancestor symlinks resolve only within the archive, with a
40-hop limit; dangling targets and cycles fail `info`/`open`. `resolver=` and `verify_external=` apply across readers. Installed packages register
`fsspec.filesystem("zstar")` and `zstar://archive.zstar!/member` URLs.

## Free-threading

The extension supports free-threaded CPython (3.13t/3.14t). Archives, native
cursors, caches, and `ZstarFileSystem` are safe to share between threads;
`AsyncArchiveFile` belongs to one event loop (use
`asyncio.run_coroutine_threadsafe` from other threads); callbacks must not
re-enter the archive that invoked them. Free-threaded wheels are not abi3 and
need `maturin build --no-default-features`. See `docs/advanced.md` for the
per-type guarantees and build details.

## Type checking

The package ships `py.typed` and native-extension stubs. Readers, lazy iterators,
member streams, and writers have concrete types; options and reports use
`TypedDict`. Storage protocols and callback types are available in `zstar.types`.
Custom storage classes satisfy these protocols structurally, without inheritance.

Narrow the result of `extractfile()` before using its stream: directories return
`None`. Extension values and low-level CBOR codecs use the recursive `CborValue`
type, supporting integers, booleans, text, bytes, arrays, and text-keyed maps.
Use `isinstance()` to narrow these values before accessing a particular shape.

`just lint` runs strict Pyrefly checks on the SDK and a consumer of the
installed wheel, including calls that must be rejected by the type checker.

## Development

The SDK uses a standard Python source layout:

```text
sdks/python/
├── pyproject.toml      # Python package and test dependencies
├── Cargo.toml          # Private native binding crate
├── src/zstar/          # Public exports and private Python implementation
├── rust/               # PyO3 bindings
├── tests/
└── docs/
```

Import the archive API from `zstar`. `zstar.format` provides format tooling,
`zstar.types` provides storage protocols and option/report types, and
`zstar.fsspec` provides the optional filesystem adapter. Modules prefixed with
an underscore are implementation details.

From the repository root, run `just check` for installed-wheel tests using
`uv sync --locked --package zstar-py --group test --no-editable` with the system
Python interpreter.
For an editable development install, run `uv sync --package zstar-py` from the
repository root inside `nix develop`.

Local RustFS tests accept only loopback endpoints and disposable credentials.
Start the root compose service and set `ZSTAR_S3_ENDPOINT=http://127.0.0.1:9000`.
CI tests the system Python on Linux/macOS and installed Windows wheels.
Wheels use the CPython 3.11 stable ABI (`cp311-abi3`) for compatibility with later CPython versions.
See [RELEASING.md](../../RELEASING.md) for package release requirements.
