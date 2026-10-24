# Zstar for Python

Python bindings for reading and writing
[Zstar](https://github.com/brian14708/zstar) indexed tar archives, with synchronous,
asyncio, and optional fsspec APIs. Python 3.11 or later is required.

## Installation

```sh
pip install zstar-py
# Include the optional fsspec integration:
pip install 'zstar-py[fsspec]'
```

The distribution is named `zstar-py`; the import is `zstar`. For a local build
and the installed-wheel test suite, run `just check` from the repository root.

## Read an archive

```python
from zstar import Archive, pack_file

encoded = pack_file("hello.txt", b"hello world")
with Archive(encoded) as archive:
    assert archive.read("hello.txt", offset=6, length=5) == b"world"
```

Use `Archive.open("archive.zstar")` for a local file,
`Archive.open(fileobj=source)` for a borrowed seekable binary object, or
`Archive.from_url(url)` for HTTP ranges. HTTP sources require a strong ETag.
Keep borrowed sources immutable and open until the reader closes.

Opening authenticates the complete manifest. Reads validate selected frames;
use `archive.verify()` for full archive validation. External content requires a
resolver; use `archive.verify(external=True)` to validate the external closure. Digests establish internal
consistency, not authenticity.

## Write an archive

```python
from io import BytesIO
from tarfile import TarInfo
from zstar import ArchiveWriter

info = TarInfo("hello.txt")
info.size = 5
info.mode = 0o644
with ArchiveWriter.open("archive.zstar", mode="x") as writer:
    writer.addfile(info, BytesIO(b"hello"))
```

Successful close finalizes the manifest and footer. Mode `x` refuses an existing
file; mode `w` replaces it. `ArchiveWriter(fileobj)` borrows a binary sink and
leaves it open. Paths use `/` separators and require explicit ancestor-directory
entries. Use `writer.add(path, arcname=...)` to capture filesystem entries, or
`pack_directory(path)` to collect a complete archive in memory.

## Async reads

```python
import asyncio
from zstar import AsyncArchive, pack_file


async def main():
    encoded = pack_file("hello.txt", b"hello")
    async with await AsyncArchive.from_bytes(encoded) as archive:
        assert await archive.read("hello.txt") == b"hello"


asyncio.run(main())
```

`AsyncArchive.open`, `from_source`, `from_url`, and `from_s3` are also async
constructors. Await metadata lookups such as `getmember`; iterate `getmembers()` with `async for`.

## fsspec

Install the `fsspec` extra, then open a member through a filesystem:

```python
from zstar import ZstarFileSystem

fs = ZstarFileSystem(fo="archive.zstar")
try:
    with fs.open("hello.txt", "rb") as member:
        data = member.read()
finally:
    fs.close()
```

`ZstarFileSystem(fo=...)` accepts an archive path or binary file object and
provides read-only listing, globbing, and member streams. Close the filesystem
when finished. Generic remote chains use the selected backend's snapshot and
cache policies.

Reader, writer, source, cache, and filesystem classes document their ownership,
limits, and thread-safety contracts in their docstrings and type definitions.
The [format specification][spec] defines wire rules and validation scopes.
[spec]: https://github.com/brian14708/zstar/blob/main/spec/v1.md
