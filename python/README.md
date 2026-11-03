# Zstar for Python

Python bindings for indexed `.tar.zst` archives. Python 3.11 or later is required;
`fsspec` is a required dependency and provides the uniform filesystem interface.
Install optional fsspec backends separately for the protocols you use.
Direct native HTTP readers do not require additional dependencies.

## Installation

```sh
pip install zstar-py
# Optional: fsspec HTTP backends and chained HTTP URLs
pip install 'fsspec[http]>=2026.9.0'
# Optional: fsspec S3 backends and chained S3 URLs
pip install 's3fs>=2026.9.0'
```

Other fsspec protocols work with their corresponding backend packages installed.
Zstar does not bundle optional backend dependencies or expose extras for them.

The distribution is named `zstar-py`; the import is `zstar`. Run `just check` from
the repository root for the complete development test suite.

The stable public surface is intentionally small:

- `zstar.Archive` and `zstar.AsyncArchive` read archives.
- `zstar.ArchiveWriter` and `zstar.AsyncArchiveWriter` create archives.
- `zstar.ZstarError` reports archive and storage failures.
- `fsspec.filesystem("zstar", ...)` provides the standard fsspec filesystem API.
- `zstar.types` contains protocols and typed option dictionaries for low-level APIs.

The `_native` module, underscored Python modules, and underscored methods are
implementation details. Reader factories share native option parsing; metadata
uses the lossless entry tuples behind the Python `TarInfo` view. Native bindings
are organized by reader, writer, manifest, storage, and callback support.
Use the fsspec filesystem interface for external-object writing; the archive
writers expose `addfile()`, `add_entry()`, and `write_bytes()`
for supported direct writes.

## Filesystem interface

`ZstarFileSystem` is the public file API. It follows fsspec conventions and supports
sync and native async use. A filesystem is either a reader (`mode="r"`, the default)
or a writer (`mode="w"`). Closing a writer finalizes the archive. Writable member
streams are staged; known-size `put_file()` and `pipe_file()` inputs stream directly.

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

The adapter uses fsspec for S3 storage; it does not create a native S3 client.
S3 credentials, endpoints, retries, and transport caching come from `s3fs`.
Source streams honor the backend's caching defaults. For example, configure
in-memory S3 block caching through standard s3fs options:

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

The disk cache stores compressed source blocks in sparse files and reuses them
across opens. Close the archive filesystem to persist cache metadata. Reopen
partially cached objects with the same block size and manage the cache directory's
disk usage. `check_files=True` checks the source identity before reuse; keep source
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

Objects are named `<lowercase sha256>.zst`. Files of 16 MiB or larger are
externalized using FastCDC (2 MiB minimum, 8 MiB target, 16 MiB maximum). The
external filesystem can use a
different backend or credentials. It remains caller-owned. Zstar checks object
availability and extent lengths; pass `verify_external=True` to verify SHA-256. Custom `resolver=` implementations are supported by the
low-level readers only.

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

The lower-level factories are useful when fsspec's filesystem methods do not fit. Use
`Archive.from_file`, `Archive.from_source`, or `Archive.from_url` for synchronous
readers and await the corresponding `AsyncArchive` factories for native async reads.
Readers expose standard `tarfile.TarInfo` metadata. Lookup and file reads never
follow symlinks. Opening a symlink as a member stream raises `tarfile.ExtractError`;
use `read_link()` to inspect its exact stored target. The fsspec adapter likewise
reports links as links and rejects opening them or traversing through them.

For metadata that does not fit `TarInfo`, `entry()` and `iter_entries()` return
the lossless manifest tuples, including entry attributes and extent references.
`root_extensions` and `read_link()` expose archive-level metadata and symlink
targets. Opening completes metadata authentication and validation; use `verify()`
for physical archive or external-closure validation. Reader options also
include `concurrent_chunks`; writer options include `workers` and
`root_extensions`.

`ArchiveWriter.add_entry(entry, body=None, size=0)` and its async counterpart
write lossless entry tuples, including extension attributes. Supply a body and its
size to encode new inline extents, or omit the body for entries referencing external
objects. Existing inline offsets belong to their source archive and cannot be copied.
`read_range(name, start, end)` reads a half-open member range.

```python
from pathlib import Path
import zstar

with zstar.Archive.from_file(Path("archive.zstar")) as archive:
    for info in archive.members():
        print(info.name, info.size)
    print(archive.read("docs/readme.txt"))
```

The public writer classes provide direct stream control:
`ArchiveWriter.from_file(...)` and `await AsyncArchiveWriter.from_file(...)`.
Import them from `zstar`. Use `ZstarFileSystem` for standard filesystem operations,
or the raw interface for metadata, batched ranges, and direct sized-body writes.

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

`Archive`, `AsyncArchive`, and fsspec share Rust reader sessions, range resolution,
and member cursors. Member opens inspect metadata once. Rust handles seek bounds
and bounded `readinto()` loops; fsspec fetches use positional reads.
Python provides file interfaces and commits stream positions after successful
reads. Synchronous calls enter the native runtime directly, while asynchronous
calls support cancellation and drain admitted operations when closing.
The two interfaces share source setup, result conversion, and metadata iteration
in Rust. Synchronous calls release the GIL while waiting on the shared Tokio
runtime; they do not create a Python event loop. They still block the calling
thread, so use `AsyncArchive` in event-loop code. fsspec adapts `AsyncArchive` for
both its synchronous and asynchronous interfaces.

Writers share one synchronous Rust encoder. `AsyncArchiveWriter` offloads calls
to native blocking workers, with at most eight admitted jobs across writers.
Queued calls await capacity without blocking the event loop. Running calls retain
their capacity until completion, even if cancelled; cancellation waits for the
call to finish before cleaning up its output. The `workers` writer option
controls compression parallelism separately from this admission limit.

Both interfaces use Rust for archive decoding, validation, and compression. The
raw path reader uses native positional file I/O. The fsspec reader adds Python
filesystem dispatch, async scheduling, and backing-stream callbacks. Passing a
Python stream to the raw reader also incurs callbacks; it is not the same path as
native file I/O.

A local warm-cache benchmark measured the interfaces before the shared-reader
refactor as follows:

| Workload | Raw Python API | fsspec sync API |
| --- | ---: | ---: |
| Read one 32 MiB member | 34.83 ms | 54.86 ms |
| Read 128 × 8 KiB members | 0.93 ms | 28.75 ms |
| Write one 32 MiB member | 24.85 ms | 20.04 ms |
| Write 128 × 8 KiB members | 1.06 ms | 13.66 ms |

These compare Python APIs backed by Rust, not a standalone Rust application.
The write paths also differ: fsspec uses a native worker and a backend transaction,
while the raw writer calls the encoder synchronously. Results do not establish
remote S3 performance.


Keep the filesystem open across reads to reuse parsed metadata. For related ranges,
use `read_many()` or fsspec's `cat_ranges()` to share work within one operation.
Decoded content is not retained between calls.

Use `put_file()` for local files and `pipe_file()` for bytes: known-size inputs
stream straight to the encoder. Writable `open()` members spool because the tar
header requires their final size; `member_spool_bytes` controls when they spill to
disk. A native staged-writer session tracks members, creates parent directories,
serializes writes, and prevents finalization after a member failure. Python owns
temporary-file creation and backend publication. The raw writer's
`addfile(TarInfo, body)` avoids filesystem dispatch when
inserting many small members. Increasing async upload concurrency cannot parallelize
insertion into a single archive.

The fsspec writer always emits the archive index and manifest inside the object.
Configure storage object metadata through your backend or storage SDK when
opening an output sink, then pass that sink to an archive writer.

The [format specification](../spec/v1.md) defines the wire format and validation rules.

Opening processes the complete manifest and retains an index within the configured
metadata budget. Temporary spooling does not make the index disk-backed. Pass a
`limits` dictionary to readers and writers to bound frame and window
bytes, entry bytes, extent counts, and pointer bytes. Zero or omission disables
each additional limit; aggregate metadata and index budgets remain independent.
For example, `limits={"max_frame_decoded_bytes": 64 << 20, "max_extents": 100_000}`
sets caller policy without changing archive validity.
