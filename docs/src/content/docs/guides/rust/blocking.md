---
title: Blocking Rust APIs
---

Use `zstar::blocking` in synchronous applications. The root `Archive`, `Builder`,
and `File` APIs are asynchronous. Both modes use the same archive format, writer
engine, frame decoders, read planning, extent validation, and verification policy.
Choose the async API for concurrent source requests and owned concurrent member
handles; choose blocking for sequential standard I/O without a runtime.

## Memory and local files

Blocking APIs are available without default features or Tokio:

```toml
[dependencies]
zstar = { version = "0", default-features = false }
```

```rust
use zstar::blocking::{Archive, Builder};

let mut writer = Builder::try_new(Vec::new())?;
writer.append_dir("data")?;
writer.append_bytes("data/hello.txt", b"hello world")?;
let mut archive = Archive::from_bytes(writer.finish()?)?;
assert_eq!(archive.read_range("data/hello.txt", 6..11)?, b"world");
for entry in archive.entries() {
    println!("{}", entry?.path());
}
```

Enable `fs` for local positional reads, capture, and restoration. `fs` alone does
not enable Tokio. `AsyncLocalFileSource` requires both `fs` and `async`.

```toml
zstar = { version = "0", default-features = false, features = ["fs"] }
```

```rust
use zstar::blocking::Archive;
let mut archive = Archive::open_path("archive.zstar")?;
let bytes = archive.read("data/hello.txt")?;
```

`Archive::from_reader(reader)` accepts any `Read + Seek` input. It records
the size once and owns the seek position. Sources must represent immutable bytes
throughout the archive lifetime. `MemorySource` can own or borrow a byte buffer.

## Member streams

```rust
use std::io::{Read, Seek, SeekFrom};

let mut file = archive.open_file("data/hello.txt")?;
file.seek(SeekFrom::Start(6))?;
let first = file.read_range(0..5)?; // stream position is still 6
assert_eq!(file.metadata().size(), 11);
let mut output = Vec::new();
file.read_to_end(&mut output)?;
drop(file);
let first = archive.read_range("data/hello.txt", 0..5)?;
```

One member handle exclusively borrows its archive and source until dropped.
Seeks past EOF are valid and reads clamp at EOF; negative or overflowing seeks
fail without moving the cursor. Streaming keeps bounded decoded retention and
does not allocate the whole file. A content failure makes that handle unusable.

## Custom storage and external objects

Implement `blocking::RangeReader` with `len(&self) -> u64` and
`read_range(&mut self, Range<u64>) -> zstar::Result<Vec<u8>>`. Return exactly the
requested half-open range; reject invalid ranges and short reads. Sources and
resolvers need not implement `Send` or `Sync`.

Implement `blocking::ObjectResolver::get_encoded(&mut self, address, hint,
max_bytes)` for external content. Configure it once on the archive:

```rust
use zstar::blocking::{Archive, ReaderOptions};

let options = ReaderOptions {
    resolver: Some(Box::new(my_resolver)),
    verify_external: true,
    max_read_bytes: 8 << 20,
    ..Default::default()
};
let mut archive = Archive::open_with_options(my_source, options)?;
let (bytes, report) = archive.read_range_with_report("data.bin", 0..100)?;
```

The encoded-size hint is advisory. Enforce `max_bytes` while fetching; the reader
checks complete decoded content and optional external SHA-256 verification.
HTTP and S3 adapters currently expose async interfaces; blocking applications can
provide their own source and resolver implementations.

## Validation and resources

Blocking open eagerly validates every metadata page and the complete namespace.
`validate_all_metadata()` therefore succeeds immediately. Both modes enumerate
immutable `EntryMetadata` snapshots with infallible size getters.
Async open validates the directory and loads selected pages and ancestors on
demand. Both reject malformed content when it is accessed. `ReadReport` records
performed checks and does not authenticate metadata or certify unread content.

`blocking::ReaderOptions` shares metadata, limits, the 64 MiB default returned-read
budget, and verification defaults with the async API. Blocking execution is
sequential. `read_many` validates metadata and aggregate returned bytes before
fetching content, preserves request order, and shares selected sources per batch.

Both builders use the same `Header` and `WriterOptions`, and emit identical bytes
for equal inputs and settings. `append_data` takes `std::io::Read`, checks the
declared length, and probes one extra byte to reject excess input. `finish`
writes the footer, flushes, and returns the sink. Remote publication remains the
sink owner's responsibility.

The default feature is `async`; disable it for standard I/O without Tokio.
Metadata remains in memory and exceeding a configured page, buffer, or retained
index limit returns `Error::Budget`. Configure filesystem
packing with `fs::PackOptions.writer` and local archive opening with
`fs::ExtractOptions.reader`.
