# Go SDK

Import `github.com/brian14708/zstar/go` as `zstar`. S3 adapters are in
`github.com/brian14708/zstar/go/s3`.

Use `Open` for default reader settings, or `OpenWithOptions` with
`ReaderOptions`. Create archives with `NewWriter` and `WriterOptions`.
Close readers to release SDK resources and close writers to finalize archives;
the caller retains ownership of sources and destinations.

Options are specific to their operation:

| Operation | Options |
| --- | --- |
| `ReaderOptions.Manifest`, `OpenManifest` | `ManifestReaderOptions` |
| `WriterOptions.Manifest`, `NewManifestWriter`, `NewEntryArchiveWriter` | `ManifestWriterOptions` |
| `PackWriterOptions.Spool` | `SpoolOptions` |

Zero-valued option fields select defaults. Storage budgets and spill factories
can be configured independently for each operation. Pass the same `SpoolBudget`
pointer to share a quota across owners.

The former combined `ManifestOptions` type has been replaced by these operation
specific types. Reader fields such as `MaxReadBytes` belong in
`ManifestReaderOptions`; encoding fields such as `CompressionLevel`,
`RootExtensions`, and `BlockTarget` belong in `ManifestWriterOptions`.
