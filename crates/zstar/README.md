# Zstar for Rust

An async Rust library for reading and writing
[Zstar](https://github.com/brian14708/zstar) indexed tar archives. Readers
authenticate the complete manifest before using its index and fetch only the
frames needed for a requested file range.

## Read and write an archive

Add `zstar` and Tokio to your project:

```toml
[dependencies]
zstar = "0.0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use zstar::{Archive, Builder, VerificationScope};

#[tokio::main]
async fn main() -> zstar::Result<()> {
    let mut builder = Builder::new(Vec::new());
    builder.append_bytes("hello.txt", b"hello").await?;
    let encoded = builder.finish().await?;

    let archive = Archive::from_bytes(encoded).await?;
    assert_eq!(archive.read_range("hello.txt", 1..4).await?, b"ell".as_slice());
    archive.verify(VerificationScope::Archive).await?;
    Ok(())
}
```

`Builder::finish` finalizes the manifest and footer. File paths are relative,
use `/` separators, and require explicit entries for ancestor directories.
Entries may be added in any order; the writer sorts the manifest.

Opening validates metadata. Selective reads validate the frames they use;
`verify(VerificationScope::Archive)` additionally checks the physical tar stream
and payload mapping. External-closure verification also resolves external
content. These checks establish internal consistency, not authenticity.

## Features and storage

| Feature | Provides | Enabled by default |
| --- | --- | --- |
| `async` | `Archive`, `Builder`, and async member streams | Yes |
| `disk` | Temporary-file spill storage | Yes |
| `fs` | Local range sources, filesystem capture, and restoration | No |
| `http` | HTTP range sources and presigned requests | No |
| `s3` | S3 storage and packed objects; also enables `http` | No |

Enable adapters through Cargo features, for example
`zstar = { version = "0.0.1", features = ["fs"] }`.
See the module documentation for `zstar::fs`, `zstar::http`, and `zstar::s3`
for configuration, ownership, resource limits, and cancellation behavior.
`ReaderOptions`, `WriterOptions`, and `ManifestOptions` configure reader,
writer, and metadata budgets. Keep caller-owned sources immutable while a
reader or its cached bytes are in use.

From the repository root, run the examples with:

```sh
cargo run --locked -p zstar --example in_memory
cargo run --locked -p zstar --features fs --example local_range -- archive.zstar hello.txt 0 5
```

The [format specification][spec] defines the wire rules and validation scopes.

[spec]: https://github.com/brian14708/zstar/blob/main/spec/v1.md
