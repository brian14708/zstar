---
title: Go advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/go/first-archive/) for a runnable example.

## Options

Options are specific to their operation:

| Operation | Options |
| --- | --- |
| `ReaderOptions.Manifest` | `ManifestReaderOptions` |
| `WriterOptions.Manifest` | `ManifestWriterOptions` |
| `NewWriter` | `WriterOptions` |

Zero-valued option fields select defaults. Storage budgets and spill factories
can be configured independently for each operation. Pass the same `SpoolBudget`
pointer to share a quota across owners.

Set `MaxReadBytes`, `ConcurrentChunks`, and metadata/index/spool budgets in
`ReaderOptions.Manifest` (`ManifestReaderOptions`). Put encoding
settings such as `CompressionLevel`, `RootExtensions`, and `BlockTarget` in
`ManifestWriterOptions`.

## Validation

Use `Open`/`OpenWithOptions` to read and `NewWriter` with `Header` to write.

Opening validates the directory; lookups validate selected pages and ancestors.
External SHA-256 checking defaults to disabled; opt in through
`ReaderOptions.VerifyExternalHash`. Zstandard frame checksums, and extent bounds are always checked.
Checksum mismatches report `integrity-failure`. The directory and metadata page cache
require budgets separate from temporary spools.

Writers accept arbitrary input order and sort the manifest. Optional `Limits`
in reader/writer manifest options bound frame and window bytes,
entry bytes, extent counts, and pointer bytes. Zero disables each additional
limit. Known-size inputs that cannot fit configured extent or metadata budgets
are rejected before payload I/O.

## Member streams

For sequential reads, use `Reader.OpenStream(ctx, path)`.
The returned `Stream` implements `io.ReadSeekCloser` and also provides
`ReadContext`. It privately retains at most one validated decoded source within
2 MiB including fixed bookkeeping. Sources exceeding
the cache budget are decoded normally but not retained. Close the stream to
release its cache, and keep the parent reader open while using it. The stream
owns entry metadata outside the cache budget.

`OpenFile` provides concurrent `io.ReaderAt` access and retains one decoded source
under the same private budget. Its `WriteToContext` streams whole members with
bounded parallel reads and uses the same cache. Call `File.Close()` to release
retained decoded bytes and reject further reads; it leaves the archive and other
handles open. Dropping a handle also allows its cache to be reclaimed.

Streams and filesystem handles reuse this file-level cache. Separate member
handles have independent completed caches; ordinary archive reads do not retain
decoded frames between operations. Concurrent active loads can still share
fetching and decoding. Cache hits check cancellation, archive lifetime, and each
extent's full bounds.

## Cache range sources

Wrap an immutable `RangeReader` with `NewCachedRangeReader` to cache encoded
ranges across archive operations:

```go
options := zstar.DefaultRangeCacheOptions()
source, err := zstar.NewCachedRangeReader(upstream, options)
if err != nil {
    return err
}
reader, err := zstar.Open(ctx, source)
if err != nil {
    return err
}
defer reader.Close()
```

Share the same decorator to share its cache. Defaults retain up to 16 MiB across
1,024 intervals, permit eight outstanding fetches, and disable read-ahead.
Set `ReadAheadBytes` to fetch extra forward bytes on a miss. Expansion stops at
EOF and `MaxFetchBytes` (8 MiB by default); larger explicit requests are fetched
exactly. Read-ahead can waste bandwidth on scattered access. Partial overlaps
are not stitched, and arbitrary adjacent requests are not merged.

Start with `DefaultRangeCacheOptions()` and customize its fields; the zero-valued
options struct is invalid. Set `CapacityBytes` to zero to disable retention while
keeping shared in-flight fetches. `MaxEntries` bounds retained interval count.
Inspect `Stats()` for hits, shared reads, source requests and bytes, and storage.
Capacity counts retained payload lengths; bookkeeping, in-flight buffers, and
caller-owned output copies require additional memory. Returned `[]byte` buffers
can be modified without affecting cached data.

Bind each decorator to one immutable source snapshot. Cache hits do not
revalidate identity. Source errors pass through unchanged and are not cached.
No background goroutines are started; if a fetch leader is cancelled, waiting
readers can retry with their own contexts. The decorator never closes the source.

This cache avoids repeated encoded-range fetches. Ordinary archive reads still
decode and validate frames; retain a member `File` or `Stream` for decoded-source reuse.

## Iteration

Use `Reader.Entries(prefix)` for lazy Go iteration over owned entry metadata:

```go
for entry, err := range reader.Entries("dir/") {
    if err != nil {
        return err
    }
    fmt.Println(entry.Path)
}
```

Prefixes match
literal UTF-8 bytes; `"dir/"` selects descendants, `"dir"` also matches names
such as `"directory"`, and `""` selects everything. Iteration seeks with binary
search, retains one bounded batch and releases it on early loop exit.

## Writing

`WriterOptions.CompressionLevel` controls data frames independently of
`WriterOptions.Manifest.CompressionLevel`. Zero selects the codec default
(Zstandard level 3). `WriterOptions.Workers` selects 0 (serial, default) or 1–16
ordered compression workers. Each worker has one outstanding independent frame;
tar order and frame emission order remain unchanged.

Resolvers implement `GetEncoded`. `ReadMany` groups repeated sources and loads
distinct sources concurrently.

## S3 objects

S3 `BlobStoreOptions.CheckExists` optionally checks HEAD before compressing and
uploading an object. This saves work for repeated content and adds a request for
new objects. The returned extent uses the existing encoded length as its hint;
object existence does not establish integrity. Conditional uploads still prevent
replacement. `BlobStore` reuses one idle encoder for objects with at most 1 MiB
history and retains at most 2 MiB of encoded output, plus codec bookkeeping.
Call `BlobStore.Close` to release that workspace; it refuses new operations,
allows active calls to finish, and leaves the SDK client open.

## Resource limits

Memory budgets apply to separate owners. Reader peak memory includes the retained
directory and cached pages, up to `ConcurrentChunks` encoded inputs, decoded sources and codec
workspaces, batch planning, returned buffers, and each open stream's cache and
metadata. Frame/window limits bound individual content jobs; their zero defaults
do not provide a finite aggregate content-memory ceiling. One idle decoder is
retained after reads. Metadata spool quotas exclude the directory and cached pages and codec
workspaces. A parallel writer holds at most `Workers + 1` raw frame buffers and
`Workers` encoded buffers and codec workspaces, in addition to metadata storage.
The serial writer holds one raw and one encoded buffer. S3 active work is bounded
by `BlobStoreOptions.Concurrency`; each upload owns its raw input, codec and
encoded output until completion. Cancelled noninterruptible work retains its
capacity until it finishes. Close writers to join workers and release buffers.

## Errors

File and stream errors retain their causes through `errors.Is` and `errors.As`.
S3 operations after `BlobStore.Close` match `fs.ErrClosed`, truncated responses
match `io.ErrUnexpectedEOF`, and cancellation preserves the context error.
Response-body and encoder cleanup failures are returned alongside the primary
operation error. Backend diagnostics remain redacted.

See [SDK customization](/zstar/guides/sdk-customization/) for custom storage and
chunkers, ownership, cancellation, zero-value semantics, and budget profiles.

Optional extension pages can be written with `ManifestWriterOptions.ExtensionPages`, a map from `x-` group
names to lists of CBOR values. Each value is encoded as a separate page.
Readers skip unknown optional groups during selective reads and validate them
during complete metadata scans.

Native S3 readers discover size and snapshot identity with a suffix GET and
retain up to 128 KiB for metadata and content reads. Small archives can therefore
open and read with a single GET and no HEAD. This prioritizes fewer round trips
over minimum transferred bytes; uncached ranges still use the pinned identity.
`Source.WithMaxRangeBytes` also caps the initial prefetch.
Archive discovery uses the terminal footer; no Zstar-specific S3 object metadata
is written or required.
Local file readers read the footer and directory separately, then load entry
pages on demand using positional reads. Raw-byte caching belongs to the supplied
source; the archive does not retain encoded ranges across operations.
