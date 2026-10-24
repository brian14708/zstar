# Zstar filesystem support

`zstar::fs` provides async local archive sources and blocking capture, restoration,
and materialization helpers shared by the CLI and Python bindings.

## Read a local archive

Enable the `fs` feature to use local sources:

```rust,no_run
use std::sync::Arc;
use zstar::Archive;
use zstar::fs::AsyncLocalFileSource;

#[tokio::main]
async fn main() -> zstar::Result<()> {
    let source = Arc::new(AsyncLocalFileSource::open("archive.zstar").await?);
    let archive = Archive::open(source).await?;
    let bytes = archive.read_range("member", 0..4096).await?;
    println!("Read {} bytes", bytes.len());
    Ok(())
}
```

`AsyncLocalFileSource` opens files and performs reads on blocking workers. It
allows eight concurrent reads by default. `AsyncLocalFileSource::new(source,
concurrency)` accepts 1–1,024 slots and shares the limit across clones. Configure
the underlying `LocalFileSource` before wrapping it to change its allocation limit.

`LocalFileSource` holds a regular-file handle and snapshots its length. Reads
are positional and exact: reversed or out-of-bounds ranges, short reads, and
allocations above the default 64 MiB per-range limit return errors. Set a larger
limit with `with_max_range_bytes`. Unix uses `pread`; Windows supplies an offset
for each read. Concurrent reads do not share a seek cursor.

Keep file contents immutable while using an archive or its cached bytes.
Replacing the path leaves the open handle targeting the original file;
in-place truncation produces a short-read error when accessed. The source
provides bytes; the core archive reader validates checksums and hashes.

Empty, invalid, and over-budget requests do not acquire worker slots.
Cancellation while waiting starts no I/O. Jobs that have already started retain
their permits and buffers until completion, because regular-file syscalls are
not interruptible. Source I/O and archive decoding have independent limits.
The range sources support Unix and Windows.

## Capture and restore files

These helpers perform blocking filesystem I/O:

```rust,no_run
use std::path::Path;
use zstar::{SliceReader, backend::BlockingArchive};

fn main() -> zstar::fs::AnyResult<()> {
    let encoded = zstar::fs::pack(Path::new("input"))?;
    let mut archive = BlockingArchive::open(SliceReader::new(&encoded), Default::default())?;
    zstar::fs::extract(&mut archive, Path::new("restored"), None, false)?;
    Ok(())
}
```

Capture sorts UTF-8 paths and preserves exact link targets without following
links. Default modes are files `0o644` (`0o755` when executable), directories
`0o755`, and symlinks `0o777`. UID, GID, and mtime default to zero; special mode
bits are cleared. `PackOptions::preserve_metadata` retains source metadata.

Restoration holds directory handles, creates files exclusively, refuses
collisions, writes files before links, and applies directory metadata last.
A symlink at the destination root is rejected. Absolute, dangling, cyclic,
and `..` link targets are preserved verbatim and never accessed.

Restoration applies mtime and modes, strips setuid/setgid by default, and does
not apply ownership. Symlink modes the platform cannot set (all non-0777 modes
on Linux) are skipped rather than failing. `extract_with_report` and
`extract_file_with_report` return an `ExtractReport` with entry counts and an
`omitted` list (`OmittedField::{Ownership, SpecialModeBits, SymlinkMode,
SymlinkTimes}`, each with a count and example paths) so callers can see exactly
what was not applied; `extract`/`extract_with_options` discard it.

A destination that does not exist is extracted into a fresh sibling directory
(`.zstar-tmp-*`) and renamed into place on success; failure removes it, so no
partial tree remains. An existing destination, or one without a file name such
as `.` or `/`, is extracted in place: existing entries collide as errors and are
never modified, and on failure the files, links and directories this call
created are removed. A crash can still leave a staging directory or partial
in-place content. Errors are prefixed with the archive path. Capture and
restoration support Linux and macOS Unix filesystems.

`pack_to_with_options` streams payloads and sorts captured metadata through a
bounded spool. Defaults are 1 MiB frames, compression level 3, and up to two
compression workers. `pack` collects the complete encoded archive into memory.

Extraction validates physical tar structure while streaming inline payloads.
`extract_file_with_options` accepts `ExtractOptions`, including metadata spool
limits and `max_directory_handles` (default 64, including the destination
root). Handles beyond the limit are evicted least-recently-used and reopened
with no-follow opens from the nearest retained ancestor, so the limit bounds
file descriptors rather than tree size (minimum 2). CLI extraction uses `LocalFileSource` and inherits its per-range limit.

## External objects and materialization

Pass `LocalObjects::open(path)` to restore files that reference external objects.
Objects are named `<raw lowercase SHA-256>.zst`. Actual encoded lengths and
decoded bounds are checked independently of size hints. Raw object hashes are
verified by default; `ExtractOptions::verify_external = false` skips that hash
check. Archive checksums, metadata authentication, and physical validation remain
mandatory. Inline frames and reconstructed files have no separate SHA-256 field.

`materialize_to` streams a portable archive, preserving entry metadata and root
extensions. `materialize` collects the result into a `Vec`. Neither imposes an
aggregate logical-file byte limit.

For remote indexed reads, see the [HTTP][http] and
[S3][s3] adapters.

[http]: https://github.com/brian14708/zstar/blob/main/crates/zstar/docs/http.md
[s3]: https://github.com/brian14708/zstar/blob/main/crates/zstar/docs/s3.md
