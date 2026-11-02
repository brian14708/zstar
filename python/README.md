# Zstar for Python

Python bindings for indexed `.tar.zst` archives. Python 3.11 or later is required;
`fsspec` is a required dependency and provides the uniform filesystem interface.
Install the optional `http` extra for chained HTTP URLs and explicit HTTP backends.
Direct native HTTP readers do not require it.

## Installation

```sh
pip install zstar-py
# Optional: fsspec HTTP backends and chained HTTP URLs
pip install 'zstar-py[http]'
```

The distribution is named `zstar-py`; the import is `zstar`. Run `just check` from
the repository root for the complete development test suite.

The stable public surface is intentionally small:

- `zstar.Archive` and `zstar.AsyncArchive` read archives.
- `zstar.ArchiveWriter` and `zstar.AsyncArchiveWriter` create archives.
- `zstar.ZstarError` reports archive and storage failures.
- `fsspec.filesystem("zstar", ...)` provides the standard fsspec filesystem API.
- `zstar.types` contains protocols and typed option dictionaries for low-level APIs.

The `_native` module, underscored Python modules, and underscored methods are
implementation details. Use the fsspec filesystem interface for packed-object
writing; the archive writers expose `addfile()`, `add_entry()`, and `write_bytes()`
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

For backends that expose fsspec's `setxattr()` operation, pass string metadata:
the archive is uploaded first, then the backend updates object metadata. With s3fs
this is an S3 `CopyObject` request, so it can create a new object version and is not
part of the upload transaction. Its single-copy operation supports objects up to
5 GB. If the metadata update fails, close raises and the completed archive remains
stored.

```python
with fsspec.filesystem(
    "zstar",
    fo="s3://bucket/archive.zstar",
    mode="w",
    target_options={"key": key, "secret": secret},
    metadata={"application": "zstar"},
) as fs:
    fs.pipe_file("hello.txt", b"hello")
```

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
and require the `http` extra.

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
Readers expose standard `tarfile.TarInfo` metadata.

For metadata that does not fit `TarInfo`, `entry()` and `iter_entries()` return
the lossless manifest tuples, including entry attributes and extent references.
`root_extensions`, `read_link()`, and `validate_metadata()` expose the matching
archive-level and validation operations from the Rust reader. Reader options also
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

Both interfaces use Rust for archive decoding, validation, and compression. The
raw path reader uses native positional file I/O. The fsspec reader adds Python
filesystem dispatch, async scheduling, and backing-stream callbacks. Passing a
Python stream to the raw reader also incurs callbacks; it is not the same path as
native file I/O.

A local warm-cache benchmark measured the current interfaces as follows:

| Workload | Raw Python API | fsspec sync API |
| --- | ---: | ---: |
| Read one 32 MiB member | 34.83 ms | 54.86 ms |
| Read 128 × 8 KiB members | 0.93 ms | 28.75 ms |
| Write one 32 MiB member | 24.85 ms | 20.04 ms |
| Write 128 × 8 KiB members | 1.06 ms | 13.66 ms |

These compare Python APIs backed by Rust, not a standalone Rust application.
The write paths also differ: fsspec uses a native worker and a backend transaction,
while the raw writer calls the encoder synchronously. Results do not establish
remote S3 performance. See the [benchmark methodology](../tools/bench/README.md).


Keep the filesystem open across reads to reuse parsed metadata. For related ranges,
use `read_many()` or fsspec's `cat_ranges()` to share work within one operation.
Decoded content is not retained between calls.

Use `put_file()` for local files and `pipe_file()` for bytes: known-size inputs
stream straight to the encoder. Writable `open()` members spool because the tar
header requires their final size; `member_spool_bytes` controls when they spill to
disk. The raw writer's `addfile(TarInfo, body)` avoids filesystem dispatch when
inserting many small members. Increasing async upload concurrency cannot parallelize
insertion into a single archive.

The fsspec writer always emits the archive index and manifest inside the object.
S3 user metadata is added only when `metadata=` is supplied and the backend supports
`setxattr()`; those optional headers are separate from the authenticated archive
metadata.

The [format specification](../spec/v1.md) defines the wire format and validation rules.
