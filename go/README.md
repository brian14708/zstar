# Zstar for Go

Read and write indexed `.tar.zst` archives with Go.

[Go API reference](https://pkg.go.dev/github.com/brian14708/zstar/go) ·
[S3 API](https://pkg.go.dev/github.com/brian14708/zstar/go/s3)

```sh
go get github.com/brian14708/zstar/go
```

Import `github.com/brian14708/zstar/go` as `zstar`; S3 adapters are in
`github.com/brian14708/zstar/go/s3`.

Start with [your first archive](https://brian14708.github.io/zstar/tutorials/go/first-archive/),
then [upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/go/s3/).
Both tutorials create their own data; no input files are needed.

## Read and write an archive

Use `Open` and `NewWriter` for readers and writers. Close readers to release
resources and writers to finalize archives. Sources and destinations remain
owned by the caller.

## Configuration

See [advanced usage](https://brian14708.github.io/zstar/guides/go/configuration/)
for options, streams, S3 storage, errors, and memory budgets.

See [SDK customization](https://brian14708.github.io/zstar/guides/sdk-customization/)
for custom storage and chunking, ownership, cancellation, and bounded-memory
profiles.

## Validation and format

Opening validates metadata; reads validate the sources and extents they use.
Use `VerifyScope(ctx, zstar.VerifyArchive)` for full physical validation or
`zstar.VerifyExternalClosure` to also resolve external selections. External
SHA-256 checking is opt-in through `ReaderOptions.VerifyExternalHash`; codec
checksums remain mandatory. Checksum mismatches report `integrity-failure`.
`Manifest` and `EntryArchiveWriter` are advanced interfaces; ordinary callers
can use `Reader`, `Writer`, and `Header`.

The [core specification](https://github.com/brian14708/zstar/blob/main/spec/draft/core.md)
and [external-content extension](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md)
define archive validation and external object semantics. The informative
[CAS storage appendix](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md#appendix-cas-storage)
explains object identity and storage encoding.
