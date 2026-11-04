# Zstar for Python

Read and write indexed `.tar.zst` archives with Python 3.11 or later.

```sh
pip install zstar-py
```

Import `zstar` for archive readers and writers, or use fsspec for filesystem
operations. The package includes fsspec; install optional storage backends
separately.

Start with [your first archive](https://brian14708.github.io/zstar/tutorials/python/first-archive/),
then [upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/python/s3/).
Both tutorials create their own data; no input files are needed.

## Filesystem interface

Use `fsspec.filesystem("zstar", fo=...)` for standard filesystem operations,
or import `ZstarFileSystem` directly with `from zstar import ZstarFileSystem`.
See [filesystem and storage configuration](https://brian14708.github.io/zstar/guides/python/configuration/#filesystem-interface)
for async use, caching, S3 backends, and external objects.

## Raw archive interface

Use `Archive` and `AsyncArchive` to read, or `ArchiveWriter` and
`AsyncArchiveWriter` to write. `ZstarError` reports failures, and `zstar.types`
defines options, reports, file metadata, and storage protocols. See the
[archive interface guide](https://brian14708.github.io/zstar/guides/python/configuration/#raw-archive-interface).
Metadata uses standard `tarfile.TarInfo` objects through `getinfo()` and `members()`;
`names()` yields filenames. Writers accept `TarInfo` through `addfile()`.
CBOR values, manifest tuples, and extent layouts are private implementation details.
Underscored modules and methods are implementation details.

On failure, writers close owned files before removing unfinished output, including
on Windows. Transactional fsspec outputs are discarded without committing them;
caller-owned sinks passed to `ArchiveWriter` remain open.

## Performance

Keep readers open to reuse metadata and batch related ranges. Filename and member
iteration uses bounded native batches and avoids constructing Python extent or
extension objects. Prefer `names()` when only filenames are needed. See
[performance guidance](https://brian14708.github.io/zstar/guides/python/configuration/#performance).

## Resource limits

Opening retains the complete index in memory. See
[resource limits](https://brian14708.github.io/zstar/guides/python/configuration/#resource-limits)
for metadata, frame, and extent budgets.
