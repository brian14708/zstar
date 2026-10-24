# Zstar for Go

The native SDK uses pure-Go Zstandard, `io.ReaderAt`, `io.Writer`, and
`context.Context`; it needs no Rust runtime or cgo. The default APIs implement
[the entry manifest](../../spec/v1.md), an incompatible pre-freeze replacement
for the previous frame-table format.

```sh
go get github.com/brian14708/zstar/sdks/go
```

```go
import zstar "github.com/brian14708/zstar/sdks/go"
```

## Write an archive

```go
var output bytes.Buffer
writer, err := zstar.NewWriter(&output, zstar.WriterOptions{
    Manifest: zstar.ManifestOptions{CompressionLevel: 3},
})
if err != nil { return err }
err = writer.Add(ctx, zstar.Header{
    Path: "hello.txt", Mode: 0o644, Size: 5,
}, strings.NewReader("hello"))
if err != nil { return err }
if err = writer.Close(); err != nil { return err }
```

Supply paths in arbitrary insertion order with explicit ancestor directory entries.
Tar order follows insertion order. Close externally sorts the manifest and rejects
duplicates and missing ancestors.
`Kind` defaults to `KindFile`; `KindDirectory` and `KindSymlink` are also supported.
Non-files have size zero and no body. Symlink `Target` text is preserved exactly;
readers never follow it. Long paths, link targets, wide ownership IDs, and signed
nanosecond times use local PAX when required. `Header.Extensions` and
`WriterOptions.Manifest.RootExtensions` preserve typed `x-*` metadata. Values
support integers, booleans, UTF-8 text, bytes, arrays and text-keyed maps;
null and floating point are rejected. Writers snapshot caller metadata before
consuming payloads.

`Add` consumes exactly `Size` bytes and rejects extra or truncated input.
Validation, cancellation and output failures are sticky. `Close` finalizes the
checksummed tar frames, a segmented CBOR sequence and footer, and
releases temporary storage. It never closes the caller's
destination. Output can be partial on failure; stage it when publication matters.

`FrameSize` is a multiple of 512 in [512, 16 MiB], default 1 MiB. Small files
share frames and large files split across them. Payload codec windows follow
the bounded frame target. Encoder metadata and transient frame records spill
to disk; no permanent frame table or whole-file digest is produced.

External files supply `Header.Extents`, size zero and no body. An extent is
`Extent{Size, Source, EncodedSize, Offset}`: a decoded length, an absolute archive
frame offset or canonical `sha256:<hex>` object address, the encoded chunk size,
and an optional decoded offset. The encoder assigns inline extents and generates
external pointer text. External encoded sizes are hints that may change after recompression. [The S3 uploader](s3/README.md) returns a ready-to-use complete-object extent.
Standalone references use the same tuples through `EncodeContentReference` and
`EncodeContentPointer`.
`Extent.Source` is an immutable, comparable `ExtentSource`. Use
`ArchiveSource(offset)` for an encoded frame offset, `ObjectSource(digest)` for
a decoded SHA-256 digest, or `ParseObjectSource(address)` for a canonical address.
`ArchiveOffset()` and `ContentAddress()` identify the source kind without type
assertions. Entries expose `TypeFile`, `TypeDirectory`, and `TypeSymlink` constants.

## Read a local or remote archive

```go
stat, err := file.Stat()
if err != nil { return err }
archive, err := zstar.Open(ctx, zstar.ReaderAtSource{
    Reader: file, Length: stat.Size(),
})
if err != nil { return err }
defer archive.Close()
data, err := archive.Read(ctx, "hello.txt", 1, 3) // "ell"
```

Keep the source immutable for the reader's lifetime; the caller owns it.
`HTTPRangeReader` supports native URLs and renewable presigned GET providers,
with pooled HTTP, exact bounded ranges, a required strong ETag enforced through
`If-Match`, size pinning and URL redaction. Missing/weak validators are rejected. Its size
probe uses GET, so it needs no HEAD permission. Set context deadlines for remote
I/O. The [native S3 source](s3/README.md) pins ETag or version through credentialed
HEAD and uses conditional ranges.

Opening authenticates the concatenated decoded segments and validates the complete
canonical manifest, namespace and required features. Sorted typed entries remain in
memory for binary path search. `Entry(path)` returns owned metadata and an error;
`EntryCount` avoids listing. `Cursor().Next(limit)` returns at most 4,096 entries.
`VisitEntries` permits callbacks to read the reader without holding its metadata
lock. Physical tar order may differ from the manifest array.

Like `archive/zip.Reader`, `Reader` implements `fs.FS`: `archive.Open(name)`
returns a closeable `fs.File`, and directories support `fs.ReadDirFile`.
Use `fs.ReadFile`, `fs.Stat`, `fs.ReadDir`, `fs.WalkDir`, or `fs.Sub` directly.
`Reader` also implements `fs.ReadLinkFS` for `fs.ReadLink` and `fs.Lstat`.
Paths use forward slashes and the virtual root is `"."`. Symlinks are preserved
in metadata and never followed by `Open`; `ReadLink` returns their exact target.
`Entry.FileInfo()` converts owned metadata to `fs.FileInfo`, including Go mode
bits, signed timestamps, and an owned `*Entry` from `Sys()`.

Missing paths in `Entry`, file opens, reads, and `ReadLink` return `*fs.PathError`
matching `fs.ErrNotExist`; invalid paths match `fs.ErrInvalid`. Closed readers
and filesystem handles match `fs.ErrClosed`. Closing a member handle leaves the
archive open. Standard filesystem reads use a background context; for remote
I/O with deadlines use the explicit context-aware APIs below.

`OpenFile` returns a concurrent `io.ReaderAt`/`io.WriterTo` member view. Retained
views binary-search an extent prefix index; `WriteTo` streams complete selected
extents without whole-file buffering or a reconstructed-file hash. Keep the
reader open while using views. `ReadLink` returns the exact stored target.
The `Open`/`OpenFile` context applies only to opening. File views do not retain
it: use `ReadAtContext(ctx, buffer, offset)` or `WriteToContext(ctx, output)` for
per-operation cancellation. Standard `ReadAt` and `WriteTo` use a background
context. `ReadAt` copies bounded decoded chunks directly into the caller's
buffer and can exceed the budget for SDK-returned allocations.

`ReaderOptions.Manifest` configures memory, spill, segment, read and worker budgets.
Defaults are 8 MiB in-memory metadata spool, 4 GiB aggregate encoded/decoded
metadata, a charged 16 MiB content LRU, eight concurrent chunk workers, and
64 MiB returned content. `ReaderOptions.DisableContentCache` disables content
retention. Typed entries use O(N) memory beyond the spool threshold. Returned
entries/results, active workers and codec state are additional memory.
`ManifestOptions.MaxRetainedEntryBytes` defaults to 1 GiB and includes spare
entry-slice capacity; `MaxEntries` adds an optional count cap (zero disables it).
Temporary parsed values and capacity-compaction copies are additional memory.
`ReadAheadBytes` defaults to 1 MiB and is capped at 17 MiB for sequential metadata
and physical validation reads. Smaller windows trade memory for more requests.
Metadata ingestion and each physical validation reuse a decoder. Content workers
retain at most one idle decoder workspace per reader, with active decoders bounded
by `ConcurrentChunks`; codec workspaces are separate from the content cache budget.
Large files can be streamed past the returned-read budget. `CreateSpillStore`
transfers an empty append/read-at store to SDK ownership until `Close`; its bytes
must remain immutable except for SDK writes. `DisableDisk` rejects required
spill when no custom store is supplied.

`ManifestOptions.SpoolBudget` limits retained logical storage across frame,
entry, metadata and validation spools. A nil value creates a 4 GiB budget for that
reader/writer. Pass `NewSpoolBudget(limit)` to share a quota across owners;
`NewSpoolBudget(0)` disallows retaining any spool bytes. `Used()` reports current
usage. Reservations precede store I/O, and failed partial writes conservatively
retain their charge until `Close`, including when cleanup reports an error.
The quota includes memory spools and excludes filesystem allocation overhead,
transient migration copies, decoded caches and caller-owned outputs.

Concurrent misses share fetch/decode work, including with caching disabled.
A canceled waiter stops waiting without canceling remaining waiters; when all
waiters cancel, the underlying operation is canceled. Callbacks that ignore
cancellation retain their bounded work slots until completion. Closing a reader
cancels active loads, wakes waiters, releases spools and clears caches. It leaves
the caller's source/resolver open.

External cache and miss keys include the resolver namespace as well as the
decoded content address; recompression-dependent size hints do not identify
objects. Pointer resolvers provide stable namespaces and are retained by cache
entries until eviction/close. Function or value resolvers cannot establish that
identity safely and share content only within one operation; pass a pointer
(including a pointer to a resolver function) to enable reuse across operations.
Unchecked bytes from another resolver never satisfy a read or hash-promotion
request. A failed promotion preserves the original unchecked provenance.

For cross-reader reuse, pass a caller-owned cache and immutable source identity:

```go
shared := zstar.NewMemoryDecodedCache(32 << 20)
archive, err := zstar.OpenWithOptions(ctx, source, zstar.ReaderOptions{
    SharedContentCache: shared,
    SourceIdentity: []byte("bucket/key@immutable-version"),
})
if err != nil { return err }
defer archive.Close()
```

The identity names the exact immutable encoded snapshot. Inline keys also bind
the authenticated metadata digest and encoded locator; verified external keys
use the content digest. Cache failures and eviction fall back to source reads.
Lower-level manifests also expose `SetSharedContentCache`; configure it before
concurrent reads. Readers create opaque `CachedChunk` values only after codec validation and any
required object hash verification.

For restartable reuse, combine a bounded memory tier with a private local
directory. `NewTieredDecodedCache` recovers valid tokens after a process restart,
promotes disk hits into memory, and evicts by least recent use in each tier:

```go
cache, err := zstar.NewTieredDecodedCache(32<<20, zstar.DiskDecodedCacheOptions{
    Directory: "/var/cache/my-app/zstar",
    Capacity:  512 << 20,
})
```

The disk budget includes each chunk's key, provenance, and integrity digest.
The disk tier validates the stored key and integrity digest and preserves the
reader-created token’s codec and hash-verification provenance. Readers accept
only matching verified tokens; disk hits do not decompress the source again. Corrupt,
stale, oversized, and interrupted entries are discarded and loaded again from
the source. Keep the directory private to one cache instance, including across processes; its digest detects
accidental corruption but does not authenticate an untrusted directory. Cache
failures are advisory and fall back to source reads. Disk stores decoded bytes,
which can be much larger than compressed sources. Temporary writes need up to
one extra chunk beyond the configured disk budget; filesystem block overhead
and in-memory index entries are separate. Entries are atomically renamed on
filesystems that support replacement rename, but directory metadata is not
fsynced for power-loss durability. `MemoryUsedBytes()` and `DiskUsedBytes()`
report each tier’s retained charge. `NewDiskDecodedCache` also exposes the disk
tier directly through the same `DecodedContentCache` interface.

## External objects and grouped reads

Set `ReaderOptions.Resolver` to an `EncodedContentResolver`. It returns a complete
compressed object bounded by the supplied maximum, independently of the size
hint. The reader owns decoding and verification. Object SHA-256 is checked by
default; `SkipExternalHash` explicitly skips it. Checksums, codec limits and
selected decoded ranges always apply. Cache keys exclude hints, and unchecked
bytes become checked only after successful hashing.

```go
results, err := archive.ReadMany(ctx, []zstar.ReadRequest{
    {Path: "hello.txt", Offset: 0, Length: 5},
    {Path: "hello.txt", Offset: 1, Length: 3},
})
```

Batches validate every range and the aggregate budget before content I/O.
Limits are 4,096 requests, 65,536 extent copies and `MaxReadBytes` total output.
Each selected frame/object is grouped once per batch, including with caching
disabled. Results own compact independent buffers in caller order. Failure
cancels other jobs, joins batch workers and returns no results. Sources and
resolvers must support concurrent calls.

## Validation scopes

`ReadWithReport` and `ReadManyWithReport` describe selected frames and checked or
unchecked external objects. They authenticate metadata but never establish the
validity of unread headers, pointers, padding or end markers.
`ValidateMetadata(ctx)` confirms the complete sequence and namespace validation
completed at open.
`Verify(ctx, false)` also validates every physical data frame, effective tar/PAX
header, inline mapping, pointer recipe, padding and exact end markers.
`Verify(ctx, true)` additionally checks the external closure through the resolver.
Full reports retain aggregate counts to bound memory for many-entry archives.

`VisitPayloads` fuses physical validation with borrowed payload callbacks. Their
output is provisional until success. External payloads are pointer text; empty
files yield an empty callback, and non-files yield none. Entry metadata is
isolated from validation and reused for each member's chunks. Ordered physical
members consume metadata sequentially; arbitrary order uses a bounded temporary
rank sort to prove exact member coverage. No inline SHA-256 is computed.

`ErrorCategory` exposes stable format/resource/missing-object/cancellation
categories. Transport causes remain available through `errors.Is`/`errors.As`.
The lower-level `OpenManifest`, `NewManifestWriter` and `NewEntryArchiveWriter`
APIs expose authenticated entries, sequence encoding and explicit per-read resolver
policies when building custom integrations.

## API migration and development checks

`RangeReaderAt` is renamed to `RangeReader`, matching its `ReadRange` method.
The S3 adapter's `Client` and `BlobOptions` are renamed to `BlobClient` and
`BlobStoreOptions` to identify the consumer and match `NewBlobStore`.

This pre-freeze update replaces raw `uint64`/`string` extent sources with
`ArchiveSource`, `ObjectSource`, or `ParseObjectSource`; the wire format is unchanged.
`Header.Kind` and `Entry.Type` now have named types and constants. `Entry` returns
a missing-path error instead of `(nil, nil)`, and canceling the opening context
no longer cancels later file-view operations.
`Manifest.ValidateMetadata` takes a context, matching `Reader.ValidateMetadata`;
it replaces both the no-context method and `ValidateMetadataContext`.
The historical `Manifest.Spilled` diagnostic is removed; integrations can
observe spill creation and cleanup through `ManifestOptions.CreateSpillStore`.

Run `golangci-lint fmt ./...` from `sdks/go` to apply gofumpt and goimports.
`just lint` enforces formatting, strict error checking, exported documentation,
modern Go idioms, and correctness rules. `just check` runs the Go race tests. Lint suppressions must name the rule and explain why it
is intentional; advisory cache and response cleanup failures are documented locally.
