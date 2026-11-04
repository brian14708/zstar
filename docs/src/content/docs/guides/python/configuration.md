---
title: Python advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/python/first-archive/) for a runnable example.

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
`get_mapper()` work with archive-relative paths. `get_file()` also accepts a
borrowed binary output stream and fsspec progress callbacks. `cat_ranges()` batches
member ranges in Rust, supports negative offsets, and follows fsspec's per-range
error handling. Pass `reader_options={"concurrent_chunks": 4, ...}` to tune native
reader limits; use `external=` for external content.

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

Member streams read exact logical ranges without a decoded-content cache.
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
object availability and extent lengths; set `verify_external=True` to check
SHA-256 too. Use low-level readers for custom `resolver=` implementations.

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
awaited on the owning event loop. `aiofiles` and `aiofile` objects are not required.

## Raw archive interface

Use `Archive.from_file`, `Archive.from_source`, or `Archive.from_url` for sync
reads. For async reads, await the corresponding `AsyncArchive` factories.
Readers expose standard `tarfile.TarInfo` metadata. Lookup and file reads never
follow symlinks. Opening a symlink as a member stream raises `tarfile.ExtractError`;
use `read_link()` to inspect its exact stored target. The fsspec adapter likewise
reports links as links and rejects opening them or traversing through them.

Use `getinfo(name)` for one `TarInfo`, `members(prefix=None)` to iterate metadata,
and `names(prefix=None)` to iterate filenames. Prefixes select paths by their UTF-8
byte prefix. Results are owned snapshots; changing them does not change the archive.
`read_link()` returns a symlink's exact stored target, and
`read_range(name, start, end)` reads a half-open member range.

Opening completes metadata authentication and validation; use `verify()` for
physical archive or external-closure validation. Reader options include
`concurrent_chunks`; writer options include `workers`. Public option, report,
and storage protocol types live in `zstar.types`.

Write file metadata with `ArchiveWriter.addfile(tarinfo, body)` or its async
counterpart. Manifest tuples, CBOR values, extent references, and raw extension
metadata are private implementation details.

```python
from pathlib import Path
import zstar

with zstar.Archive.from_file(Path("archive.zstar")) as archive:
    for info in archive.members():
        print(info.name, info.size)
    print(archive.read("docs/readme.txt"))
```

Create a writer with `ArchiveWriter.from_file(...)` or
`await AsyncArchiveWriter.from_file(...)`. Import both classes from `zstar`.

Archive member streams and backend streams use structural byte-stream protocols. A
reader needs `read()`, and a seekable source additionally needs `seek()` and `tell()`;
a writer sink needs `write()` and `flush()`. Core readers borrow supplied streams;
`ZstarFileSystem` owns and closes supplied file objects. Caller-supplied filesystems
remain available after the archive closes.

Raw S3 reads require explicit configuration. They do not read AWS environment
variables or discover credentials from profiles, containers, or instance metadata:

```python
from zstar import Archive

with Archive.from_s3(
    "bucket",
    "archive.zstar",
    region="us-east-1",
    access_key_id=credentials.access_key,
    secret_access_key=credentials.secret_key,
    session_token=credentials.token,
) as archive:
    data = archive.read("hello.txt")
```

Use `anon=True` instead of credentials for public objects. `endpoint` selects a
custom S3 endpoint; `object_prefix` optionally resolves external content. The
async factory accepts the same keywords. fsspec/s3fs retains its normal credential
chain and is configured independently through `target_options` or `fs=`.

## Performance

Keep the archive or filesystem open across reads to reuse parsed metadata. Use
`read_many()` or fsspec's `cat_ranges()` to share work across related ranges.
Decoded content is not cached between calls.

Both interfaces use Rust for decoding, validation, and compression. Direct path
reads use native file I/O; fsspec and supplied Python streams add callbacks.
Sync calls release the GIL while waiting but still block the calling thread.
Use `AsyncArchive` in event-loop code. Closing async readers waits for active work.

Use `put_file()` for local files and `pipe_file()` for bytes to stream known-size
inputs directly. Writable `open()` members spool until their size is known;
`member_spool_bytes` sets when they spill to disk. `addfile(TarInfo, body)` avoids
filesystem dispatch for direct writes. Members are written serially, so more
async uploads do not speed up insertion into one archive.

`AsyncArchiveWriter` runs native work off the event loop, with at most eight
active jobs across writers. Cancellation waits for running work before cleaning
up. The `workers` option controls compression parallelism separately.

The index and manifest are stored inside the archive. To set storage object
metadata, configure the output sink through your backend before passing it to a
writer.

## Resource limits

Opening reads the complete manifest and retains an index within the metadata
budget. Spooling metadata to disk does not reduce index memory.

Pass `limits` to bound frame and window bytes, entry bytes, extent counts, and
pointer bytes. Zero or omission disables each additional limit; metadata and
index budgets are separate. For example,
`limits={"max_frame_decoded_bytes": 64 << 20, "max_extents": 100_000}` sets limits
for your application without changing which archives are valid.

The [format specification](/zstar/reference/spec/) defines the format and validation rules.
