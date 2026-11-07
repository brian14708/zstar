# Zstar for Go

Read and write indexed `.tar.zst` archives with Go.
Implements the Zstar core specification and its optional external-content extension.

Archives use the compact 28-byte `Z*01` footer with 64-bit metadata lengths.
Metadata and archive data frames require Zstandard checksums; opening validates
the complete manifest.
`ReadReport.MetadataValidated` reports checksum and schema validation, not cryptographic authentication.

[Go API reference](https://pkg.go.dev/github.com/brian14708/zstar/go) ·
[S3 API](https://pkg.go.dev/github.com/brian14708/zstar/go/s3)

```sh
go get github.com/brian14708/zstar/go
```

Import `github.com/brian14708/zstar/go` as `zstar`; S3 adapters are in
`github.com/brian14708/zstar/go/s3`.

`Reader.Verify` returns an error on failure; external hashing follows `ReaderOptions.VerifyExternalHash`.

## Get started

Use `Open` to read and `NewWriter` to write. Close readers to release resources
and writers to finalize archives. The caller owns sources and destinations.

Follow [your first archive](https://brian14708.github.io/zstar/tutorials/go/first-archive/),
then try [S3 storage](https://brian14708.github.io/zstar/tutorials/go/s3/).

Set `MaxReadBytes` and `ConcurrentChunks` through `ReaderOptions.Manifest`.
Use `Entries` to enumerate, `Verify` to verify, and `OpenStream`
for member streams. Manifest engines are private.
Writer headers and reader entries use `EntryType`: `TypeFile`, `TypeDirectory`,
or `TypeSymlink`. Set `Header.Type`; its zero value is a regular file.

## Guides

- [Configuration](https://brian14708.github.io/zstar/guides/go/configuration/):
  options, validation, streams, S3 storage, errors, and limits.
- [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/):
  custom storage, chunking, ownership, and cancellation.
- [Format specifications](https://github.com/brian14708/zstar/blob/main/spec/draft/README.md).
