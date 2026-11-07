# Zstar

**Compressed archives. Direct access.**

Zstar specifies an indexed `.tar.zst` archive format for reading individual
files and byte ranges without decompressing the entire archive. The
[core specification](spec/draft/core.md) defines self-contained archives with
independent Zstandard frames and deterministic CBOR metadata. Standard
Zstandard and tar tools can decode their contents.

Optional extensions build on the core format. The
[external-content extension](spec/draft/external-content.md) adds references to
external objects identified by SHA-256. The separate
[catalog proposal](spec/draft/catalog.md) organizes archives into collections.
See the [draft specifications](spec/draft/README.md) for scope and status.

## Libraries and CLI

| Component | Documentation | Source |
| --- | --- | --- |
| Rust library | [Reader, writer, and feature guide](crates/zstar/README.md) | `crates/zstar/` |
| CLI | [Packing, inspection, and extraction](crates/zstar-cli/README.md) | `crates/zstar-cli/` |
| Python SDK | [Sync, async, and fsspec APIs](python/README.md) | `python/` |
| Go SDK | [Package and API documentation](go/README.md) | `go/` |

The [SDK customization guide](https://brian14708.github.io/zstar/guides/sdk-customization/)
describes custom storage, ownership, cancellation, and resource policies.

## SDK capabilities

Rust, Go, and Python implement the core archive format and the optional
external-content extension. They validate the metadata directory on open and selected
entry pages, ancestors, sources, and extents during reads.

| Operation | Rust | Go | Python |
| --- | --- | --- | --- |
| External SHA-256 opt-in | `verify_external` | `VerifyExternalHash` | `verify_external` |
| Filesystem restoration | Unix, with metadata report | No | fsspec byte downloads; links rejected |

External SHA-256 checking defaults to disabled. Metadata validation, Zstandard frame
checksums, and extent bounds are always checked.
Selective reads do not establish full physical validity, and these checks establish consistency rather
than publisher authenticity. See each SDK's configuration guide for resource
budgets.

## Development

Use the pinned development shell and run the standard lint and test suite:

```sh
nix develop
just
```

The [justfile](justfile) defines the check recipes. `just lint` and `just check`
prepare an installed Python wheel before running Python checks. Shared fixtures
under `testdata/` cover valid and invalid archives across SDKs.

Run `just check-integration` for RustFS-backed S3 tests. It starts a disposable
service on local port 9000, which must be available; the Nix shell supplies
RustFS on Linux. Run `just package` from a clean Git tree to build release
artifacts. See [docs/README.md](docs/README.md) for documentation builds and previews.
