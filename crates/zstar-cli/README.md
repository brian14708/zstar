# Zstar CLI

Pack, inspect, and extract [Zstar](https://github.com/brian14708/zstar)
indexed tar archives.

## Build and run

From the repository root:

```sh
cargo build --locked -p zstar-cli
./target/debug/zstar --help
```

Use `zstar <command> --help` for all options. The examples below assume the
built binary is on `PATH`.

## Portable archives

```sh
zstar pack input archive.zstar
zstar inspect archive.zstar
zstar extract archive.zstar restored
```

The pack output must be outside the input directory. Packing refuses an existing
output unless `--force` is supplied. Capture preserves symlinks and normalizes
modes, ownership, and timestamps by default; `--preserve-metadata` retains the
source metadata. Compression and metadata budgets are configurable through
`pack --help`.

`inspect` validates metadata and prints manifest offsets, sizes, and entry count.
Extraction also validates physical tar structure and refuses destination
collisions. Ownership is not restored; special mode bits require
`--allow-special-bits`. Omitted metadata is reported on stderr. The Rust
library's `zstar::fs` module documentation describes platform support and
restoration behavior.

## S3 storage

`pack` outputs and `inspect`/`extract` inputs also accept `s3://bucket/key` URLs.
The CLI uses the AWS credential chain and selected `AWS_PROFILE`. Remote
inspection and extraction stream the archive to a temporary file first. Uploads
use bounded buffers and multipart transfers for large archives. Both small and
multipart uploads publish conditionally unless `--force` is supplied, preserving
output created by another writer. S3-compatible services must support conditional
writes for this protection.

Service endpoints configured in the selected AWS profile honor its
`s3.addressing_style`: path-style endpoints keep their host unchanged;
virtual-hosted endpoints include the bucket in the host.

## External content

External objects can be supplied to extraction with `--objects <directory>`.
Their hashes are not checked by default; use `--verify-external-hash` to opt into
SHA-256 verification. Manifest validation and data checksums remain required.
The [archive specification][spec] defines validation scopes.

[spec]: https://github.com/brian14708/zstar/blob/main/spec/draft/core.md

See the [external-content extension](https://github.com/brian14708/zstar/blob/main/spec/draft/external-content.md)
for object references and verification rules.
