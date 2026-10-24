# Zstar

Zstar is an indexed `.tar.zst` archive format for reading individual files and
byte ranges without decompressing the entire archive. It combines independently
compressed Zstandard frames with deterministic CBOR metadata. Archives can keep
all content inline or reference external objects by SHA-256.

The [v1 specification](spec/v1.md) is a working draft. It defines the wire
format, validation rules, implementation profiles, schema, and grammar. Standard
Zstandard and tar tools can decode the tar stream; resolving external content
requires a Zstar reader.

## Libraries and CLI

| Component | Documentation | Source |
| --- | --- | --- |
| Rust library | [Reader, writer, and feature guide](crates/zstar/README.md) | `crates/zstar/` |
| CLI | [Packing, inspection, and extraction](crates/zstar-cli/README.md) | `crates/zstar-cli/` |
| Python SDK | [Sync, async, and fsspec APIs](python/README.md) | `python/` |
| Go SDK | [Package and API documentation](go/) | `go/` |

The Rust storage adapters are documented in the generated API documentation
for `zstar::fs`, `zstar::http`, and `zstar::s3`.
Shared conformance fixtures under `testdata/` exercise the SDKs against the same
valid and invalid archives.

## Development

Use the pinned development shell and run the standard lint and test suite:

```sh
nix develop
just
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites, individual checks,
integration tests, and packaging.
