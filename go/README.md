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
| `WriterOptions.Manifest`, `NewManifestWriter` | `ManifestWriterOptions` |
| `NewWriter`, `NewEntryArchiveWriter` | `WriterOptions` |

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

For repeated small sequential reads, use `Reader.OpenStream(ctx, path, cacheBytes)`.
The returned `Stream` implements `io.ReadSeekCloser` and also provides
`ReadContext`. It retains at most one validated decoded source; zero selects a
2 MiB cache budget including fixed stream/cache bookkeeping. Sources exceeding
the cache budget are decoded normally but not retained. Close the stream to
release its cache, and keep the parent reader open while using it. The stream
owns entry metadata outside the cache budget. `OpenFile` remains the concurrent
`io.ReaderAt` view without retained content; its `WriteToContext` streams whole
members with bounded parallel reads.

Use `Reader.Entries(prefix)` for lazy Go iteration over owned entry metadata:

```go
for entry, err := range reader.Entries("dir/") {
    if err != nil {
        return err
    }
    fmt.Println(entry.Path)
}
```

`Manifest.Entries` offers the same `iter.Seq2[Entry, error]` API. Prefixes match
literal UTF-8 bytes; `"dir/"` selects descendants, `"dir"` also matches names
such as `"directory"`, and `""` selects everything. Iteration seeks with binary
search, retains one bounded batch and releases it on early loop exit.
`CursorPrefix` remains available for callers needing explicit batch sizes.

`WriterOptions.CompressionLevel` controls data frames independently of
`WriterOptions.Manifest.CompressionLevel`. Zero selects the codec default
(Zstandard level 3). `WriterOptions.Workers` selects 0 (serial, default) or 1–16
ordered compression workers. Each worker has one outstanding independent frame;
tar order and frame emission order remain unchanged. Both `NewWriter(output,
WriterOptions)` and `NewEntryArchiveWriter(output, WriterOptions)` accept the same
options; the older three-argument constructor and `WithOptions` variant are removed.

The unused `BatchEncodedContentResolver`, `EncodedContentIndex`, and
`ObjectRequest` declarations have been removed. Resolvers implement `GetEncoded`;
`ReadMany` still groups repeated sources and loads distinct sources concurrently.

S3 `BlobStoreOptions.CheckExists` optionally checks HEAD before compressing and
uploading an object. This saves work for repeated content and adds a request for
new objects. The returned extent uses the existing encoded length as its hint;
object existence does not establish integrity. Conditional uploads still prevent
replacement. `BlobStore` reuses one idle encoder for objects with at most 1 MiB
history and retains at most 2 MiB of encoded output, plus codec bookkeeping.
Call `BlobStore.Close` to release that workspace; it refuses new operations,
allows active calls to finish, and leaves the SDK client open.

Memory budgets apply to separate owners. Reader peak memory includes the retained
entry index, up to `ConcurrentChunks` encoded inputs, decoded sources and codec
workspaces, batch planning, returned buffers, and each open stream's cache and
metadata. Frame/window limits bound individual content jobs; their zero defaults
do not provide a finite aggregate content-memory ceiling. One idle decoder is
retained after reads. Metadata spool quotas exclude the parsed index and codec
workspaces. A parallel writer holds at most `Workers + 1` raw frame buffers and
`Workers` encoded buffers and codec workspaces, in addition to metadata storage.
The serial writer holds one raw and one encoded buffer. S3 active work is bounded
by `BlobStoreOptions.Concurrency`; each upload owns its raw input, codec and
encoded output until completion. Cancelled noninterruptible work retains its
capacity until it finishes. Close writers to join workers and release buffers.

File and stream errors retain their causes through `errors.Is` and `errors.As`.
S3 operations after `BlobStore.Close` match `fs.ErrClosed`, truncated responses
match `io.ErrUnexpectedEOF`, and cancellation preserves the context error.
Response-body and encoder cleanup failures are returned alongside the primary
operation error. Backend diagnostics remain redacted.
