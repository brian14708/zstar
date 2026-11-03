# Zstar for Rust

Read and write indexed tar archives with async Rust.

[Rust API reference](https://docs.rs/zstar/latest/zstar/)

```toml
[dependencies]
zstar = "0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Read and write an archive

[Create your first archive](https://brian14708.github.io/zstar/tutorials/rust/first-archive/),
read a member and byte range, and verify the result. Then
[upload and read an S3 archive](https://brian14708.github.io/zstar/tutorials/rust/s3/).
Neither tutorial needs input files.

## Features and storage

The default features provide async readers, writers, and temporary-file spooling.
Enable `fs`, `http`, or `s3` for storage adapters. See
[advanced usage](https://brian14708.github.io/zstar/guides/rust/configuration/)
for features, credentials, ownership, and memory budgets.

The [format specification](https://github.com/brian14708/zstar/blob/main/spec/v1.md)
defines the wire format and validation rules.
