# Zstar

Zstar is an indexed `.tar.zst` archive format with deterministic CBOR metadata
and independently compressed data frames. The self-contained
[specification](spec/v1.md) includes the wire rules, implementation profiles,
and formal schema and grammar.

This repository contains the Rust library and CLI plus Go and Python SDKs:

- [`crates/zstar`](crates/zstar/README.md) — Rust library and storage adapters
- [`crates/zstar-cli`](crates/zstar-cli/README.md) — `zstar` command
- [`sdks/python`](sdks/python/README.md) — Python and fsspec APIs
- [`sdks/go`](sdks/go/README.md) — Go reader, writer, and storage APIs

Use the pinned development shell and run the full check suite:

```sh
nix develop
just
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development checks and
[`RELEASING.md`](RELEASING.md) for package releases.
