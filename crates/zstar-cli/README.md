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

`pack` outputs and `inspect`/`extract` inputs also accept `s3://bucket/key` URLs.
The CLI uses the AWS credential chain and selected `AWS_PROFILE`. Remote
inspection and extraction download the archive to a temporary file first.

## Packed external objects

Create a local store directory outside the input tree, then capture a thin
archive into it:

```sh
mkdir -p object-store
zstar pack-objects input thin.zstar catalog.cbor --store object-store
zstar materialize-objects thin.zstar catalog.cbor portable.zstar --store object-store
zstar extract portable.zstar restored
```

`pack-objects` supports `--chunking fixed` (default) and `--chunking fastcdc`.
It publishes a generation descriptor as well as the archive and catalog.
`materialize-generation <generation-key> <output> --store <directory>` restores
a portable archive from a store-relative descriptor key.

To export complete external frames for extraction without a catalog:

```sh
mkdir -p exported
zstar export-objects catalog.cbor --store object-store --destination exported
zstar extract thin.zstar restored --objects exported/objects
```

External-object hashes are checked by default. `--skip-external-hash` skips only
that check; manifest authentication and data checksums remain required.
The Rust library module documentation describes the filesystem restoration
policy; the [specification][spec] defines the packed storage profile and
validation scopes.

[spec]: https://github.com/brian14708/zstar/blob/main/spec/v1.md
