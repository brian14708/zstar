---
title: Rust advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/rust/first-archive/) for a runnable example.

## Archive lifecycle

`Builder::try_new` and `try_with_options` initialize writers asynchronously and
return initialization errors before exposing a builder. `Builder::finish`
finalizes the manifest and footer. File paths are relative,
use `/` separators, and require explicit entries for ancestor directories.
Add entries in any order; the writer sorts the manifest. UTF-8 path order avoids
an additional manifest sorting spool at finish. Filesystem packing uses this
path automatically.
`ReaderOptions.limits` and `WriterOptions.limits` bound frames, windows, metadata
items, and extent counts. Zero disables each additional limit. Known-size inputs that
cannot fit configured extent or metadata budgets are rejected before payload I/O.

Opening validates and retains the directory; entry pages are cached on demand.
Reads validate selected pages, ancestor directories, and the frames they use.
These checks establish consistency; they do not prove who created the archive.

Use `open_cursor()` for reusable positional member reads. A `FileCursor` borrows
the archive on each read and supports concurrent reads without changing a stream
position. Cursors are bound to the archive that created them.

## Features and storage

| Feature | Provides | Enabled by default |
| --- | --- | --- |
| `async` | `Archive`, `Builder`, and async member streams | Yes |
| `disk` | Temporary-file spill storage | Yes |
| `fs` | Local range sources, filesystem capture, and restoration | No |
| `http` | HTTP range sources and presigned requests | No |
| `s3` | S3 storage; also enables `http` | No |

Enable adapters through Cargo features, for example
`zstar = { version = "0", features = ["fs"] }`.
See the module documentation for `zstar::fs`, `zstar::http`, and `zstar::s3`
for configuration, ownership, resource limits, and cancellation behavior.
The core S3 builder requires an explicit region and either static credentials or
anonymous access. It never reads environment variables, profiles, credential
files, metadata services, or proxy settings; applications can resolve those
policies before constructing the builder.
`ReaderOptions` and `WriterOptions` store focused options in `metadata`.
Configure source and entry budgets through `limits` and allocating reads through
`ReaderOptions.max_read_bytes`. Keep caller-owned sources immutable while a
reader is in use.

Member files and cursors retain at most one decoded source within a 2 MiB
private cache budget including bookkeeping. Entry metadata and returned buffers are
separate. Larger sources are decoded and validated but are not retained. The cache
belongs to the retained handle and is released when it is dropped; separate
handles do not share completed decoded caches.

`fs::extract`, `extract_file`, and their options variants return `ExtractReport`.
Inspect `is_lossless()` and `omitted`: ownership is never applied, and unauthorized
set-id bits and unsupported symlink metadata are reported by field and count.

External SHA-256 checking defaults to disabled. Opt in with
`ReaderOptions { verify_external: true, ..Default::default() }`; metadata validation,
frame checksums, and extent bounds are always checked.

## Resource limits

Frame and window limits default to zero (disabled), so defaults do not impose a
finite aggregate content-memory ceiling. Peak content memory includes up to
`concurrent_chunks` encoded inputs, complete decoded sources and codec workspaces,
plus stream caches and returned buffers. Metadata spools, the directory, and cached pages
have separate budgets. For example:

```rust
let mut options = zstar::ReaderOptions::default();
options.concurrent_chunks = 4;
options.limits.max_frame_encoded_bytes = 64 << 20;
options.limits.max_frame_decoded_bytes = 64 << 20;
options.limits.max_window_bytes = 64 << 20;
options.max_read_bytes = 8 << 20;
```

These are local resource policies, independent of wire validity. Spool limits
exclude directory/page memory and codec workspaces.

## Metadata extensions

Write optional extension pages with `MetadataWriterOptions.extension_pages`,
a map from `x-` group names to lists of CBOR values. Each value is encoded as a
separate page.
Readers skip unknown optional groups during selective reads and validate them
during complete metadata scans.

## Storage reads

Native S3 readers discover size and snapshot identity with a suffix GET and
retain up to 128 KiB for metadata and content reads. Small archives can therefore
open and read with a single GET and no HEAD. This prioritizes fewer round trips
over minimum transferred bytes; uncached ranges still use the pinned identity.
`S3RangeSource::with_max_range_bytes` also caps the initial prefetch.
Archive discovery uses the terminal footer; no Zstar-specific S3 object metadata
is written or required.

Async local file readers read the footer and directory separately, then load entry
pages on demand using positional reads.

## Cache range sources

`CachedRangeReader` decorates any immutable `AsyncRangeReader`. It retains fetched
intervals in a bounded LRU cache and shares fetches for concurrent contained
ranges. For example, with the `fs` feature:

```rust
use std::sync::Arc;
use zstar::{Archive, CachedRangeReader, RangeCacheOptions};
use zstar::fs::AsyncLocalFileSource;

let source = Arc::new(AsyncLocalFileSource::open("archive.zstar").await?);
let source = Arc::new(CachedRangeReader::new(source, RangeCacheOptions::default())?);
let archive = Archive::open(source.clone()).await?;
let selected = archive.read_range("example.txt", 0..5).await?;
let counters = source.stats();
```

The defaults retain up to 16 MiB across 1,024 intervals, allow eight outstanding
fetch intervals, and disable read-ahead. Inspect `stats()` for cache hits, shared
fetches, source requests and bytes, and retained storage.

Set `read_ahead_bytes` to fetch extra forward bytes on a miss, for example
`256 << 10` for 256 KiB. Expansion stays within `max_fetch_bytes` (8 MiB by default) and EOF, so nearby requests can
reuse that interval. Scattered access can waste bandwidth through read-ahead.
The cache does not stitch partial overlaps or merge arbitrary adjacent requests.
Set `max_fetch_bytes` within your source's range limits.
Larger explicitly requested ranges bypass expansion. `max_in_flight` bounds
outstanding intervals; `max_entries` bounds bookkeeping even for tiny reads.
Set `capacity_bytes` to zero to disable retention while keeping shared fetches.

Cache capacity counts retained payload lengths. In-flight response buffers and
returned `Bytes` slices held by callers are additional memory. Eviction does
not invalidate a slice already returned to a caller.

Keep each decorator bound to one immutable snapshot. A cached hit does not
revalidate the source. HTTP snapshot pinning still applies to underlying misses;
source errors are propagated unchanged and are not cached. Reads create no
detached network tasks. Cancelling a leader releases its I/O future, and a
waiting reader may take over and retry it.

This caches encoded ranges, so frames are still decoded and checked for every
ordinary archive read. Use `read_many` for grouped access and retained member
cursors for decoded-frame locality.

See [SDK customization](/zstar/guides/sdk-customization/) for custom storage,
metadata options, and ownership and cancellation contracts. The
[format specification](/zstar/reference/spec/core/) defines wire rules and
validation scopes.
