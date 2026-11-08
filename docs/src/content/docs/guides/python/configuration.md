---
title: Python advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/python/first-archive/) for a runnable example.

## Direct archive interface

`zstar.open(file, mode="r", *, async_=False, ...)` is the archive entry point.

| Mode | Result | With `async_=True` |
| --- | --- | --- |
| `"r"` (default) | `Archive` | `AsyncArchive` |
| `"w"` | `ArchiveWriter` | `AsyncArchiveWriter` |
| `"x"` | `ArchiveWriter`, exclusive local creation | `AsyncArchiveWriter`, exclusive local creation |

Async opening supports `async with` or `await`. Append and update modes are unsupported.
The classes describe returned objects; open archives through `zstar.open`.

Direct use has no fsspec dependency. `zstar.open(source)` accepts local paths,
file/HTTP/S3 URLs, bytes-like archive data, seekable binary files, and custom range
sources. Buffers become immutable snapshots; caller-supplied files and sources
remain borrowed. `Path` objects always denote local paths, including names that
look like URLs. Unknown URL schemes are rejected. HTTP sinks are unsupported.
Native HTTP checks exact
ranges, identity encoding, and strong ETag consistency. S3 pins conditional reads.

```python
import zstar
from zstar import ReaderOptions

options: ReaderOptions = {
    "max_read_bytes": 8 << 20,
    "concurrent_chunks": 4,
    "limits": {"max_frame_decoded_bytes": 64 << 20},
}
with zstar.open("archive.tar.zst", options=options) as archive:
    for entry in archive:
        print(entry.path, entry.kind, entry.size)
    print(archive.read("docs/readme.txt", offset=0, length=5))
```

Use `entry(path)` for a snapshot and `entries(prefix=None)` for lazy listing.
`get_entry(path, default=None)` returns a default for absent paths;
`has_entry(path)` and sync `path in archive` use indexed lookup without listing.
Async lookup methods are awaited.
Entries own their metadata and remain usable after closure. Extents stay in Rust;
Python wrappers are created as a read-only sequence is accessed. Extension getters
return defensive copies, including nested containers. `Header` describes writer
input without physical packing. `entry.to_header()` preserves logical metadata.

`read(path)` returns the full member. `read(path, offset=..., length=...)` selects
an exact byte range; omit `length` to read from the offset through EOF. `read_many([ReadRequest(...)])` shares selected frames/objects and
preserves request order. `ReadRequest(path)` selects a complete member;
`ReadRequest(path, offset)` reads through EOF. Exact ranges and complete members
can share one batch, subject to the aggregate returned-read budget.
Opening and empty reads do not certify unread content. Missing paths raise
`KeyError`, directories raise `IsADirectoryError` for content operations, and
symlink reads are rejected. Inspect `entry.linkname` for exact target text.

Use `archive.open(path)` for independent member cursors. Sync streams implement
`io.RawIOBase`. Reads, seeks, tell, and close are awaitable on async streams;
use `async with archive.open(path) as member` or await it for explicit
lifetime management. Allocating reads obey the 64 MiB
returned-read budget. `readinto` can fill a larger writable contiguous caller
buffer in bounded chunks. Partial progress can remain in the buffer on failure;
the cursor advances only on success. Each stream serializes its cursor operations
and privately retains at most one decoded source within 2 MiB. Archive positional
reads can run concurrently; sync user callbacks are serialized per object.

The async facade uses the same Rust archive and builder. It accepts async protocols
whose methods return awaitables, including decorated methods. It confines callbacks
to the opening loop and copies the initiating operation's `contextvars`.
Custom async storage implements the protocols in `zstar.types`.
Use `await zstar.open(file, async_=True)` to borrow a blocking seekable file.
Sync callbacks execute on bounded workers and must support running away from the
calling thread. Reentry into the object invoking a callback is rejected.

Use `async_=True` for awaitable context management:

```python
import zstar

async with zstar.open("archive.tar.zst", async_=True) as archive:
    data = await archive.read("docs/readme.txt")
```

Use `archive = await zstar.open(..., async_=True)` to manage its lifetime
explicitly. With `async_=True`, results are single-use awaitable context managers;
acquisition starts on awaiting or entering.

`zstar.open(destination, mode="w")` accepts local paths, file/S3 URLs, and binary
writers. Opening a path or URL owns the storage. Supplied file objects, sources,
sinks, bodies, resolvers, and stores remain borrowed:

```python
import zstar
from zstar import Header

with zstar.open("output.tar.zst", mode="x") as writer:
    writer.ensure_directory("docs/nested")
    writer.writestr(Header.file("docs/nested/readme.txt", 5, mode=0o600), b"hello")
    writer.add("local-data", "data")
```

The writer validates exact declared body sizes, including at most one excess byte.
Partial sink writes are supported; zero progress and oversized counts fail.
Use `ensure_directory(path)` to create a directory and missing ancestors, retaining
the metadata of directories already appended or captured. Call it before writing
members beneath that path. File headers passed to `writestr` must declare
the exact buffer length; mismatches fail before mutation. Finish rejects duplicates
or ancestors omitted from explicit appends.
Pre-mutation argument errors leave the writer usable; mutation failure or
cancellation leaves only cleanup available. Successful context exit writes the
footer and flushes. Owned writers then close their sink and publish an S3 upload;
closure succeeds only after publication succeeds. Exceptional exit and admitted
mutation failure abort owned output. Borrowed sinks remain open for caller-managed
publication. Native file sinks handle exclusive creation,
no-follow opening, and identity-checked cleanup. S3 sinks complete multipart uploads
on successful close and abort on exceptional exit. Uploaded external objects may
remain after writer failure; retention belongs to the caller.

For local atomic replacement, use `zstar.open(path, atomic=True, mode="w")` or
`await zstar.open(path, atomic=True, mode="w", async_=True)`. Output is staged in the same
directory, flushed and synchronized, then published after successful finalization.
Until publication, an existing destination keeps its original contents; failures
discard the staging file. `mode="x", atomic=True` also checks exclusivity when
publishing and never replaces a concurrent creator. New destinations remain
absent until publication. File URLs use the same `mode` and `atomic` arguments. Atomic
replacement creates a new inode with ordinary file-creation permissions and is
supported for local files only.

Cancellation cancels pending native I/O and Python awaitables. Running blocking
callbacks cannot be interrupted: their permits remain held until they finish, and
close/abort drains them before releasing borrowed resources. Use context managers
or explicit `close` and `abort` to wait for cleanup. Await these methods on async objects.
A cancelled owned close retains its cleanup operation; another close or abort
joins it. The first cleanup operation determines whether to publish or abort.
Once publication starts it continues despite cancellation of its waiter. A remote
publication can succeed before a transport failure is reported; abort cannot undo
an already published object. Primary and cleanup failures are reported together.

## Filesystem interface

Open a filesystem with `mode="r"` to read (the default) or `mode="w"` to write.
Closing a writer finalizes the archive. Both modes support sync and async code.

```python
import fsspec

with fsspec.filesystem("zstar", fo="archive.zstar", mode="w") as fs:
    fs.makedirs("docs")
    with fs.open("docs/readme.txt", "wt", encoding="utf-8") as member:
        member.write("Hello from Zstar")
    fs.pipe_file("data.bin", b"raw bytes")

with fsspec.filesystem("zstar", fo="archive.zstar") as fs:
    print(fs.ls("docs", detail=False))
    assert fs.cat_file("docs/readme.txt") == b"Hello from Zstar"
```

Standard helpers such as `glob`, `find`, `cat`, `get(..., recursive=True)`, and
`get_mapper()` work with archive-relative paths. Local downloads reject
preexisting symlinks at every destination component and require POSIX
no-follow descriptor operations. Unsupported hosts fail explicitly. Downloads
copy regular-file bytes; they do not restore archived ownership, modes, timestamps,
or symlinks. Symlink downloads are rejected by policy. `get_file()` also accepts a
borrowed binary output stream and fsspec progress callbacks. `cat_ranges()` batches
member ranges in Rust, supports negative offsets, and follows fsspec's per-range
error handling. Pass `reader_options=AsyncReaderOptions(concurrent_chunks=4)` to
tune native reader limits. Use `external=` for external content and
`AsyncReaderOptions(verify_external=True)` for SHA-256 checking. Writer policy uses
`writer_options=WriterOptions(...)`; both integrations share the direct API types.
Custom resolvers use the direct archive API.

Use `Path` objects or strings for local archives. `fo` may also be an owned
binary sink, or a path understood by another fsspec filesystem. Pass a configured
filesystem with `fs=` when credentials or sessions need to be managed by the caller:

```python
s3 = fsspec.filesystem("s3", profile="work")
with fsspec.filesystem("zstar", fo="bucket/archive.zstar", fs=s3, mode="w") as fs:
    fs.put("local-data/", "data", recursive=True)
```

S3 credentials, endpoints, retries, and caching come from `s3fs`. Configure an
in-memory block cache through its options:

```python
with fsspec.filesystem(
    "zstar",
    fo="s3://bucket/archive.zstar",
    target_options={
        "profile": "work",
        "default_cache_type": "blockcache",
        "default_block_size": 4 << 20,
    },
) as fs:
    data = fs.cat_file("docs/readme.txt")
```

Set `default_cache_type="none"` to fetch only requested encoded ranges. For a
persistent, disk-backed block cache, compose standard fsspec filesystems:

```python
fs, path = fsspec.core.url_to_fs(
    "zstar://docs/readme.txt::blockcache::s3://bucket/archive.zstar",
    blockcache={"cache_storage": "/path/to/zstar-cache", "check_files": True},
    s3={"profile": "work", "default_block_size": 4 << 20},
)
with fs, fs.open(path) as member:
    data = member.read()
```

The disk cache reuses compressed blocks across opens. Close the filesystem to
save cache metadata, reuse the same block size, and manage the cache directory's
disk usage. `check_files=True` checks source identity before reuse. Keep source
objects immutable while an archive is open.

Each member stream privately retains at most one validated decoded source within
2 MiB, including cache bookkeeping. Entry metadata and returned buffers are separate.
Sources larger than the budget are decoded without retention.
Backends configured with `asynchronous=True` use direct async range I/O and bypass
stream caching; use a synchronous backend or disk-cache wrapper for source caching.

Direct HTTP reads use the native range transport; chained HTTP URLs use fsspec
and require a separately installed `fsspec[http]`.

For external content, pass a filesystem rooted at the object directory:

```python
objects = fsspec.filesystem("dir", path="objects-bucket/chunks", fs=s3)
with fsspec.filesystem("zstar", fo="bucket/archive.zstar", fs=s3, external=objects) as fs:
    data = fs.cat_file("example.txt")
```

Objects are named `<lowercase sha256>.zst`. Files of 16 MiB or larger are split
with FastCDC (2 MiB minimum, 8 MiB target, 16 MiB maximum). The external filesystem
can use different storage or credentials and remains caller-owned. Zstar checks
object availability and extent lengths; set
`reader_options=AsyncReaderOptions(verify_external=True)` to check SHA-256 too. Use low-level readers for custom `resolver=` implementations. Raw
`ArchiveWriter.add_external` also accepts custom raw chunk iterables and object
writers. See [SDK customization](/zstar/guides/sdk-customization/).

A writer accepts `wb`/`wt` member streams, `mkdir`, `makedirs`, `pipe_file`, and
`put(..., recursive=True)`. There is one active member at a time. Parent directories
are created automatically, duplicate members are rejected, and append/update modes are
unsupported. Bulk writes accept fsspec's `mode="create"` or `mode="overwrite"`;
both reject existing members because an archive cannot replace entries.
Closing a filesystem with an unfinished member or after an exception
aborts the transaction when the backend supports atomic commit/discard. Async writers
use `async with` and `await fs.open_async(...)`:

```python
async with fsspec.filesystem(
    "zstar", fo="s3://bucket/archive.zstar", mode="w", asynchronous=True
) as fs:
    async with await fs.open_async("hello.txt", "wb") as member:
        await member.write(b"hello")
```

For async backends, use their native async filesystem and pass `asynchronous=True`.
Native compression and archive work run on Rust workers; backend stream operations are
awaited on the owning event loop.

On failure, writers close owned files before removing unfinished output, including
on Windows. Transactional fsspec outputs are discarded without committing them;
caller-owned sinks passed to `ArchiveWriter` remain open. Use explicit `close()` /
`await close()` or context managers to wait for owned resources to close; garbage
collection cleanup is nonblocking and best effort.

## Configuration and storage

Use partial TypedDict dictionaries from `zstar`: `Limits`, `MetadataReaderOptions`,
`MetadataWriterOptions`, `ReaderOptions`, `AsyncReaderOptions`, and `WriterOptions`.
`metadata.max_page_bytes` limits each encoded or decoded metadata page to 8 MiB
by default. Total archive metadata may exceed that limit. Reader retained metadata
is limited to 1 GiB by default; full scans also use that budget
for their temporary metadata buffer. Writer `metadata.max_buffer_bytes` defaults
to 128 MiB for each in-memory working buffer or sorting index. Metadata buffers
never spill to files. Content concurrency defaults to eight. Zero frame/window
limits disable those additional limits; page and memory budgets still apply.

Omit keys to use Rust defaults. Rust validates values and rejects unknown keys,
including in nested dictionaries, before storage I/O. Opening copies option values,
root extensions, and optional extension pages into owned Rust values; subsequent
dictionary changes do not affect the open archive. Complete listing validates optional
pages skipped by selective reads. Compression workers and frame settings are shared by both modes.

Transport options are exported from `zstar`, separate from archive budgets.
S3 requires an explicit region and either `Credentials` or `anonymous=True`; native
transports do not discover ambient credentials. Configure `endpoint` for compatible
services. HTTP sources accept headers or a mode-appropriate `request_provider`.
Providers return a signed URL, optional headers, and validity duration; the native
transport refreshes expiring or denied requests.

Use `zstar.tar.from_tarinfo` and `to_tarinfo` for explicit TarInfo conversion.
PAX metadata preserves nanoseconds; conversion warns when entry extensions are
omitted. Root extensions remain separate. Filesystem capture uses `writer.add(name, arcname=...)`
and reuses native sorted no-follow traversal.

See [SDK customization](/zstar/guides/sdk-customization/) for protocol examples.

### Storage options

`zstar.open` accepts keyword-only `storage_options` separately from archive `options`.
Storage options are partial TypedDict dictionaries exported from `zstar`:

| Policy | Fields |
| --- | --- |
| `SourceOptions` | `headers`, sync `request_provider`, `s3`, `range_cache` |
| `AsyncSourceOptions` | Same fields, with an async `request_provider` |
| `SinkOptions` | `s3`; shared by both writers |
| `S3Options` | Required `region`; optional `endpoint`, `anonymous`, `credentials` |
| `Credentials` | Required `access_key_id`, `secret_access_key`; optional `session_token` |
| `RangeCacheOptions` | Integer fields in the cache table below |

S3 requires exactly one of explicit credentials or `anonymous=True`. No environment,
profile, or instance-role discovery occurs. HTTP providers supply their own headers,
so they cannot be combined with nonempty `headers`. HTTP policy applies only to
HTTP sources, S3 policy only to S3 locations. Inapplicable settings fail before I/O.
HTTP uploads and exclusive S3 creation are unsupported.
Writer opening flags are direct arguments: `mode="w"` or `"x"`, and
`atomic=True` for local output. Supplied sinks reject exclusive and atomic
opening flags because the caller manages their storage. `storage_options` applies
only to path and URL inputs; borrowed objects manage their own transport settings.

```python
import zstar
from zstar import Credentials, S3Options, SinkOptions, SourceOptions

s3 = S3Options(
    region="us-east-1",
    credentials=Credentials(access_key_id="...", secret_access_key="..."),
)
with zstar.open(
    "s3://bucket/archive.tar.zst", storage_options=SinkOptions(s3=s3), mode="w"
) as writer:
    writer.writestr("hello.txt", b"hello")
with zstar.open("s3://bucket/archive.tar.zst", storage_options=SourceOptions(s3=s3)) as archive:
    assert archive.read("hello.txt") == b"hello"
```

### Native compressed range cache

Omit `range_cache` for no additional cache decorator; `RangeCacheOptions()` enables
the default cache. This cache also works without fsspec installed.

| Key | Default | Meaning |
| --- | --- | --- |
| `capacity_bytes` | `16 << 20` | Maximum retained compressed payload |
| `max_entries` | `1024` | Maximum retained intervals |
| `read_ahead_bytes` | `0` | Extra forward bytes on a miss |
| `max_fetch_bytes` | `8 << 20` | Maximum expanded fetch length |
| `max_in_flight` | `8` | Maximum outstanding fetch intervals |

```python
import zstar
from zstar import RangeCacheOptions, SourceOptions

with zstar.open(
    "https://example.com/archive.tar.zst",
    storage_options=SourceOptions(
        headers={"Authorization": "Bearer ..."},
        range_cache=RangeCacheOptions(capacity_bytes=64 << 20, read_ahead_bytes=256 << 10),
    ),
) as archive:
    data = archive.read("hello.txt", offset=0, length=5)
```

Values must be nonnegative integers fitting native `usize`; booleans are rejected.
`max_entries` and `max_fetch_bytes` must be positive, and `max_in_flight` must be
between 1 and 1024. Zero capacity disables completed-range retention while keeping
in-flight sharing. Requests exceeding `max_fetch_bytes` are fetched exactly,
without expansion. Keep expansion within native transport bounds: 64 MiB for S3,
and 256 MiB plus eight bytes for HTTP responses.

Containing intervals serve smaller ranges; arbitrary overlapping intervals are
not stitched together. The cache shares in-flight fetches and evicts retained
intervals in LRU order. Cached hits do not revalidate remote objects. Keep source
contents immutable while open; actual requests preserve HTTP/S3 snapshot checks.

Capacity covers retained compressed payload only. Outstanding fetches, returned
buffers, bookkeeping, metadata, codec workspaces, and decoded member caches use
additional memory. `concurrent_chunks` limits content work; `max_in_flight` limits
fetches through this source decorator. External object resolvers remain separate.
Native compressed caching, fsspec backend caches (including persistent disk caches),
and each member stream's fixed 2 MiB decoded cache are independent layers.

### Ownership and async opening

| Input | Storage ownership |
| --- | --- |
| Local path, URL, or reader buffer snapshot | Owned by the archive/writer |
| Range source, seekable binary file, or binary writer | Borrowed, including native handles |

```python
import zstar


async def round_trip() -> bytes:
    async with zstar.open("archive.tar.zst", mode="w", async_=True) as writer:
        await writer.writestr("hello.txt", b"hello")
    async with zstar.open("archive.tar.zst", async_=True) as archive:
        return await archive.read("hello.txt")
```

With `async_=True`, opening starts when awaited or entered and retains ownership
across cancellation during acquisition. Borrow seekable binary files with `zstar.open(file)`
or `await zstar.open(file, async_=True)`. Custom storage implements the sync or
async protocols in `zstar.types`. Native storage binds directly to Rust.

`Path` inputs are local even when they contain `://` or start with `file:`. String
inputs recognize file, HTTP(S), and S3 URL schemes. Filesystem
symlink and `..` traversal retain their normal meaning. Mode `"w"` selects writing.
Without `atomic=True`, this truncates an existing file immediately;
failure cleanup cannot restore prior contents. Set `atomic=True` to publish only
after finalization. Use `"x"` for exclusive creation. Cleanup removes an unfinished
name only while its file identity matches.

Borrowed readers preserve the file cursor and read the complete source from byte
zero. Borrowed writers start at the current output position and do not truncate
existing trailing data. When embedding an archive, reopen the exact archive slice
rather than the containing file. `io.BytesIO` output stays available after writer
finalization. Pass the source or destination directly to `zstar.open`; use
`archive.open(path)` for files inside an archive.
