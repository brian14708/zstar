# Zstar for Python

Read and write indexed `.tar.zst` archives with Python 3.11 or later.
Implements the Zstar core specification and its optional external-content extension.

Archives use the 44-byte `Z*01` footer with 64-bit metadata lengths.
Data and metadata use Zstandard frame checksums, with no separate metadata checksum.
Opening validates
the metadata directory. Reads validate selected entry pages and ancestors.
`ReadReport["metadata_validated"]` reports metadata validation, not cryptographic authentication.

```sh
pip install zstar-py
```

The package includes fsspec; install optional storage backends separately.
Use context managers or explicit `close()` / `await aclose()` for cleanup.


## Get started

```python
import fsspec

with fsspec.filesystem("zstar", fo="example.zstar", mode="w") as fs:
    fs.pipe_file("hello.txt", b"Hello from Zstar")

with fsspec.filesystem("zstar", fo="example.zstar") as fs:
    print(fs.cat_file("hello.txt"))
```

Follow [your first archive](https://brian14708.github.io/zstar/tutorials/python/first-archive/),
then try [S3 storage](https://brian14708.github.io/zstar/tutorials/python/s3/).
For direct sync and async APIs, see
[advanced archive creation](https://brian14708.github.io/zstar/tutorials/python/advanced-archive/).

For fsspec, use `verify_external=True` to check external SHA-256 hashes.
`reader_options` accepts budgets and concurrency settings only. Use
`ReaderOptions`, `AsyncReaderOptions`, `FileSystemReaderOptions`, and `WriterOptions`
from `zstar.types` for typed option dictionaries.

## Guides

- [Configuration](https://brian14708.github.io/zstar/guides/python/configuration/):
  filesystem and raw APIs, validation, caching, storage, and limits.
- [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/):
  custom storage, chunking, ownership, cancellation, and metadata preservation.
- [API reference](https://brian14708.github.io/zstar/api/python/).
- [Format specifications](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md).

Readers validate the directory on open and load entry pages on demand.
`archive.entry_count` scans all metadata and can raise a validation error.
For `AsyncArchive`, use `await archive.entry_count`. Listing also validates every page.
