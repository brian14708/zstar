# Zstar for Python

Read and write indexed `.tar.zst` archives with Python 3.11 or later.

```sh
pip install zstar-py
```

Use fsspec for filesystem operations. The package includes fsspec; install optional
storage backends separately. Import `zstar` for direct archive readers and writers.

Start with [your first archive using fsspec](https://brian14708.github.io/zstar/tutorials/python/first-archive/),
then [upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/python/s3/).
Both tutorials create their own data; no input files are needed.

## Filesystem interface

Use `fsspec.filesystem("zstar", fo=...)` for standard filesystem operations,
or import `ZstarFileSystem` directly with `from zstar import ZstarFileSystem`.
See [filesystem and storage configuration](https://brian14708.github.io/zstar/guides/python/configuration/#filesystem-interface)
for async use, caching, S3 backends, and external objects.

## Raw archive interface

See [advanced archive creation](https://brian14708.github.io/zstar/tutorials/python/advanced-archive/)
for a runnable example using the direct reader and writer APIs.

Use `Archive` and `AsyncArchive` to read, or `ArchiveWriter` and
`AsyncArchiveWriter` to write. `ZstarError` reports failures, and `zstar.types`
defines options, reports, file metadata, and storage protocols. See the
[archive interface guide](https://brian14708.github.io/zstar/guides/python/configuration/#raw-archive-interface).
Metadata uses standard `tarfile.TarInfo` objects through `getinfo()` and `members()`;
`names()` yields filenames. Writers accept `TarInfo` through `addfile()`.
Use `getmetadata()` and `root_extensions` for extension metadata.
Manifest tuples, extent layouts, and underscored APIs are private.

## Configuration

Keep readers open to reuse metadata and batch related ranges. Prefer `names()`
when only filenames are needed. See
[performance guidance](https://brian14708.github.io/zstar/guides/python/configuration/#performance)
for member caching, iteration, and writer ordering.

Opening retains the complete index in memory. See
[resource limits](https://brian14708.github.io/zstar/guides/python/configuration/#resource-limits)
for metadata, frame, and extent budgets.

See [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/)
for custom storage and chunking, ownership, cancellation, and extension-preserving
rewrites. Use explicit `close()` / `aclose()` or context managers to wait for
owned resources to close.

## Validation and format

Opening validates metadata; reads validate the sources and extents they use.
Use `verify()` for full physical validation or `verify(external=True)` to also
resolve external content. External SHA-256 checking is opt-in through
`verify_external`; metadata hashes, frame checksums, and extent bounds are always
checked. Checksum mismatches report `integrity-failure`.

The [core specification](https://github.com/brian14708/zstar/blob/main/spec/draft/core.md)
and [external-content extension](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md)
define the format and validation rules. The informative
[CAS storage appendix](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md#appendix-cas-storage)
explains object identity and storage encoding.
