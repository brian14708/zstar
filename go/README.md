# Zstar for Go

Read and write indexed `.tar.zst` archives with Go.
Implements the Zstar core specification and its optional external-content extension.

Archives use the 44-byte `Z*01` footer with 64-bit metadata lengths.
Data and metadata use Zstandard frame checksums, with no separate metadata checksum.
Opening validates
the metadata directory. Reads validate selected entry pages and ancestors.
`ReadReport.MetadataValidated` reports metadata validation, not cryptographic authentication.

[Go API reference](https://pkg.go.dev/github.com/brian14708/zstar/go) ·
[S3 API](https://pkg.go.dev/github.com/brian14708/zstar/go/s3)

```sh
go get github.com/brian14708/zstar/go
```

Import `github.com/brian14708/zstar/go` as `zstar`; S3 adapters are in
`github.com/brian14708/zstar/go/s3`.


## Get started

Use `Open` to read and `NewWriter` to write. Close readers to release resources
and writers to finalize archives. The caller owns sources and destinations.

Follow [your first archive](https://brian14708.github.io/zstar/tutorials/go/first-archive/),
then try [S3 storage](https://brian14708.github.io/zstar/tutorials/go/s3/).

Set `MaxReadBytes` and `ConcurrentChunks` through `ReaderOptions.Manifest`.
Use `Entries` to enumerate and `OpenStream`
for member streams. Manifest engines are private.
Writer headers and reader entries use `EntryType`: `TypeFile`, `TypeDirectory`,
or `TypeSymlink`. Set `Header.Type`; its zero value is a regular file.

## Guides

- [Configuration](https://brian14708.github.io/zstar/guides/go/configuration/):
  options, validation, streams, S3 storage, errors, and limits.
- [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/):
  custom storage, chunking, ownership, and cancellation.
- [Format specifications](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md).

Readers validate the directory on open and load entry pages on demand.
`reader.EntryCount()` returns `(uint64, error)` and scans all metadata; listing
also validates every page.

## Cache range sources

Wrap an immutable `RangeReader` with `NewCachedRangeReader` and
`DefaultRangeCacheOptions()` to cache encoded ranges and share concurrent fetches.
Read-ahead is opt-in. Returned byte slices remain caller-owned; ordinary archive
reads still decode and validate frames. Containing cached intervals replace
redundant subranges without counting as capacity evictions. Retain a `File` or
`Stream` to reuse decoded sources; close it to release its cache.

See [source caching](https://brian14708.github.io/zstar/guides/go/configuration/#cache-range-sources)
for configuration, memory limits, and snapshot requirements.
