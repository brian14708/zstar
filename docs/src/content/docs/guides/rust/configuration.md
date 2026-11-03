---
title: Rust advanced usage
---

Use this guide to configure storage, ownership, and resource limits. Start with the
[first archive tutorial](/zstar/tutorials/rust/first-archive/) for a runnable example.

## Archive lifecycle

`Builder::finish` finalizes the manifest and footer. File paths are relative,
use `/` separators, and require explicit entries for ancestor directories.
Add entries in any order; the writer sorts the manifest.
`ManifestOptions.limits` bounds frames, windows, metadata items,
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
position. After inspecting an entry, `cursor_from_entry()` avoids looking it up
again; supply the unmodified entry from the same archive.

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
`ReaderOptions`, `WriterOptions`, and `ManifestOptions` configure reader,
writer, and metadata budgets. Keep caller-owned sources immutable while a
reader is in use.

Manifest primitives and spool types are exported from the crate root.

From the repository root, run the examples with:

```sh
cargo run --locked -p zstar --features fs --example tutorial_file
cargo run --locked -p zstar --features fs --example local_range -- example.zstar example.txt 0 5
```

The [format specification][spec] defines the wire rules and validation scopes.

[spec]: /zstar/reference/spec/
