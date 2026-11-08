# Zstar for Python

Read and write indexed `.tar.zst` archives with Python 3.11 or later.

```sh
pip install zstar-py
# Optional filesystem integration:
pip install 'zstar-py[fsspec]'
```

```python
import zstar
from zstar import RangeCacheOptions, SourceOptions

with zstar.open("example.tar.zst", mode="w") as writer:
    writer.writestr("hello.txt", b"Hello from Zstar")

with zstar.open("example.tar.zst") as archive:
    print(archive.entry("hello.txt").size)
    print(archive.read("hello.txt", offset=0, length=5))

with zstar.open(
    "https://example.com/archive.tar.zst",
    storage_options=SourceOptions(range_cache=RangeCacheOptions(capacity_bytes=64 << 20)),
) as archive:
    data = archive.read("hello.txt")
```

`zstar.open(file, mode="r", *, async_=False, ...)` is the archive entry point.
Use `"r"` to read, `"w"` to write, or `"x"` for exclusive local creation.

Use `zstar.open(source)` for local paths, file/HTTP/S3 URLs, bytes-like archive
data, seekable binary files, or custom range sources. Buffers become immutable
snapshots. `Path` objects always mean local paths, including URL-like filenames.
Use `zstar.open(destination, mode="w")` for paths, file/S3 URLs, or binary writers.
For memory output, pass `io.BytesIO()`; the buffer stays open after finalization.
Open member streams with `archive.open(path)`.

Use `get_entry(path, default=None)` for optional metadata lookup and
`has_entry(path)` or `path in archive` for existence checks. These use indexed
lookup. `read(name, offset=..., length=...)` selects an exact range; omit
`length` to read through EOF. `read_many([ReadRequest("hello.txt")])` batches full members;
`ReadRequest(path, offset)` reads through EOF and an explicit length selects an
exact range.

```python
import zstar
from zstar import Header

with zstar.open("example.tar.zst", atomic=True, mode="w") as writer:
    writer.ensure_directory("docs/nested")
    writer.writestr(Header.file("docs/nested/hello.txt", 5, mode=0o600), b"hello")
```

`ensure_directory` creates missing ancestors and reuses directories already
appended or captured, preserving their metadata. File headers passed to
`writestr` must declare the exact buffer length.

```python
import zstar


async def create_and_read() -> bytes:
    async with zstar.open("example.tar.zst", mode="w", async_=True) as writer:
        await writer.writestr("hello.txt", b"hello")
    async with zstar.open("example.tar.zst", async_=True) as archive:
        return await archive.read("hello.txt")
```

Use `archive = await zstar.open(path, async_=True)` when managing the lifetime
explicitly. With `async_=True`, results are single-use awaitable context managers;
acquisition starts when awaited or entered.

Paths, URLs, and buffer snapshots own their native storage. Successful writer close
finalizes and flushes before closing output; S3 close publishes the upload. Failure
or exceptional context exit aborts owned output. Local `mode="w"` truncates on
opening; `mode="x"` exclusively creates a local file. Failed local writes remove
unfinished output while its identity matches; they cannot restore previous contents.
Set `atomic=True` on `zstar.open(..., "w")` to stage beside
the destination and publish after successful finalization. Aborting or failing
before publication preserves the previous file. Atomic `mode="x"` publishes only
if the destination is still absent; exclusive conflicts may also occur at close.
Atomic replacement is supported for local files.
Native sessions retain the terminal result. Repeated close or abort calls join the
same operation and replay its result; cancelling an async waiter leaves cleanup running.

Caller-supplied range sources and binary writers remain borrowed and open.
Use `zstar.open(file)` or `await zstar.open(file, async_=True)`
for a borrowed seekable binary file. Bodies, resolvers, and external object stores
also remain borrowed. Sync calls release the GIL while waiting on Rust and need
no Python event loop. Async objects belong to their opening loop. Use context
managers or explicit `close()` for deterministic cleanup; await it on async objects.

`storage_options` configures transports and the native compressed range cache for
path and URL inputs;
`options` configures archive policy. Native S3 requires a region and explicit
credentials or anonymous access. The cache is optional: omit `range_cache` to
disable it, or pass `RangeCacheOptions()` to enable the default cache. Cache hits reuse one immutable
snapshot without contacting the remote object. fsspec backend caches and member
streams' fixed decoded cache are separate layers.

Entries, headers, and extents are immutable owned values. Metadata keeps exact
nanosecond timestamps and defensive copies of extensions. Use `Header.file` and
`writer.addfile(header, fileobj=body)` or `writer.writestr(header, data)` for custom metadata.
Options are partial TypedDict dictionaries: `ReaderOptions`, `AsyncReaderOptions`,
`WriterOptions`, and nested budget types. For example, `options={"concurrent_chunks": 4}`
sets only concurrency. Rust supplies defaults, validates values, rejects unknown keys,
and copies the options when opening.
Storage protocols live in `zstar.types`; sources implement only `size()` and
`read_at(offset, length)`. Async protocols return awaitables. Sync callbacks run on bounded blocking workers.

Opening validates the metadata directory. Lookup validates selected pages and
ancestors; listing scans metadata lazily. Reads validate selected extents and
Zstandard checksums. Set `verify_external=True` in reader options to check external
object SHA-256 digests. Archive bytes use the 44-byte `Z*01` footer.

Capture filesystem paths with `writer.add(path, arcname=...)`, write bytes or UTF-8
text with `writer.writestr(name, data)`, and stream a declared-size body with
`writer.addfile(header, fileobj=body)`. Optional integrations are explicit:
`zstar.tar` for TarInfo conversion and `zstar.fsspec` for the filesystem. Core imports load
neither fsspec nor tarfile. fsspec backends retain their own credential and cache
configuration; native S3 uses explicit credentials or anonymous access.

- [Python guide](https://brian14708.github.io/zstar/guides/python/configuration/)
- [API reference](https://brian14708.github.io/zstar/api/python/)
- [Format specification](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md)
