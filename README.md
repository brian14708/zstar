# Zstar

**Compressed archives. Direct access.**

Zstar is an indexed `.tar.zst` archive format for reading individual files and
byte ranges without decompressing the entire archive. It combines independently
compressed Zstandard frames with deterministic CBOR metadata. Archives can keep
all content inline or reference external objects by SHA-256.

Standard Zstandard and tar tools can decode the tar stream; resolving external
content requires a Zstar reader.

The [draft specifications](spec/draft/README.md) define the core format and
external-content extension. The separate catalog proposal describes named
collections of archive layers; catalog operations are not yet implemented.

## Libraries and CLI

| Component | Documentation | Source |
| --- | --- | --- |
| Rust library | [Reader, writer, and feature guide](crates/zstar/README.md) | `crates/zstar/` |
| CLI | [Packing, inspection, and extraction](crates/zstar-cli/README.md) | `crates/zstar-cli/` |
| Python SDK | [Sync, async, and fsspec APIs](python/README.md) | `python/` |
| Go SDK | [Package and API documentation](go/README.md) | `go/` |

The [SDK customization guide](https://brian14708.github.io/zstar/guides/sdk-customization/)
describes custom storage, ownership, cancellation, and resource policies.

## Validation support

| Operation | Rust | Go | Python |
| --- | --- | --- | --- |
| Metadata validation on open | Yes | Yes | Yes |
| Selective source and extent validation | Yes | Yes | Yes |
| Full physical archive validation | `verify(Archive)` | `VerifyScope(VerifyArchive)` | `verify()` |
| External closure validation | `verify(ExternalClosure)` | `VerifyScope(VerifyExternalClosure)` | `verify(external=True)` |
| External SHA-256 opt-in | `verify_external` | `VerifyExternalHash` | `verify_external` |
| Portable and thin writing | Yes | Yes | Yes, raw and fsspec APIs |
| Filesystem restoration | Unix, with metadata report | No | fsspec byte downloads; links rejected |

External SHA-256 checking defaults to disabled. Metadata digests, frame
checksums, and extent bounds are always checked. Selective reads do not
establish full physical validity, and these checks establish consistency rather
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
