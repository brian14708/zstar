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

Zero-valued option fields select defaults. Storage budgets and spill factories
can be configured independently for each operation. Pass the same `SpoolBudget`
pointer to share a quota across owners.

The former combined `ManifestOptions` type has been replaced by these operation
specific types. Reader fields such as `MaxReadBytes` belong in
`ManifestReaderOptions`; encoding fields such as `CompressionLevel`,
`RootExtensions`, and `BlockTarget` belong in `ManifestWriterOptions`.

Opening authenticates and validates all metadata before returning; `Verify`
checks physical content and, optionally, external closure. The complete retained
index must fit `MaxRetainedEntryBytes`; temporary spooling does not bound index
memory.

Writers accept arbitrary input order and sort the manifest. Optional `Limits`
in reader/writer manifest options bound frame and window bytes,
entry bytes, extent counts, and pointer bytes. Zero disables each additional
limit. Known-size inputs that cannot fit configured extent or metadata budgets
are rejected before payload I/O.
