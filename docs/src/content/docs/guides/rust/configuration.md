---
title: Rust advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/rust/first-archive/) for a runnable example.

## Archive lifecycle

`Builder::finish` finalizes the manifest and footer. File paths are relative,
use `/` separators, and require explicit entries for ancestor directories.
Add entries in any order; the writer sorts the manifest. UTF-8 path order avoids
an additional manifest sorting spool at finish. Filesystem packing uses this
path automatically.
`ReaderOptions.limits` and `WriterOptions.limits` bounds frames, windows, metadata items,
and extent counts. Zero disables each additional limit. Known-size inputs that
cannot fit configured extent or metadata budgets are rejected before payload I/O.

Opening validates the complete manifest and retains an index within the memory
budget. Spooling metadata to disk does not reduce index memory. Reads validate
the frames they use. Call `verify(VerificationScope::Archive)` to check the tar
stream and payload mapping, or use external-closure verification to check
referenced objects too. Verification checks consistency; it does not prove who
created the archive.

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
cache budget including bookkeeping. Use `with_cache_bytes(0)` to disable
retention or select another budget. Entry metadata and returned buffers are
separate. Larger sources are decoded and validated but are not retained.

`fs::extract`, `extract_file`, and their options variants return `ExtractReport`.
Inspect `is_lossless()` and `omitted`: ownership is never applied, and unauthorized
set-id bits and unsupported symlink metadata are reported by field and count.

External SHA-256 checking defaults to disabled. Opt in with
`ReaderOptions { verify_external: true, ..Default::default() }`; metadata hashes,
frame checksums, and extent bounds are always checked. Verification returns
`Result<()>` under the selected scope and hash policy.

## Resource limits

Frame and window limits default to zero (disabled), so defaults do not impose a
finite aggregate content-memory ceiling. Peak content memory includes up to
`concurrent_chunks` encoded inputs, complete decoded sources and codec workspaces,
plus stream caches and returned buffers. Metadata spools and the retained index
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
exclude index memory and codec workspaces.

The [format specification][spec] defines the wire rules and validation scopes.

[spec]: /zstar/reference/spec/core/

See [SDK customization](/zstar/guides/sdk-customization/) for focused
`MetadataReaderOptions` / `MetadataWriterOptions`, fallible writer initialization,
custom object writers and chunk streams, and ownership/cancellation contracts.
