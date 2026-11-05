# Zstar

**Compressed archives. Direct access.**

Zstar is an indexed `.tar.zst` archive format for reading individual files and
byte ranges without decompressing the entire archive. It combines independently
compressed Zstandard frames with deterministic CBOR metadata. Archives can keep
all content inline or reference external objects by SHA-256.

The [draft specifications](spec/draft/README.md) are working drafts. The [core
specification](spec/draft/core.md) defines archive framing, the manifest,
validation, and implementation profiles. The standalone [external-content
extension](spec/draft/external-content.md) defines thin archives and content
references. Standard Zstandard and tar tools can decode the tar stream;
resolving external content requires a Zstar reader.

External-file pointer payloads are JSON documents using `zstar-pointer-v1`;
archive manifests and standalone content references use deterministic CBOR.
The [catalog proposal](spec/draft/catalog.md) defines named JSON collections of
CAS-stored archive layers with nesting and whiteout merging. Catalog operations
are not yet implemented by the SDKs.

The informative [CAS storage appendix](spec/draft/external-content.md#appendix-cas-storage) explains how object
identity remains independent of compressed storage representations.

## Libraries and CLI

| Component | Documentation | Source |
| --- | --- | --- |
| Rust library | [Reader, writer, and feature guide](crates/zstar/README.md) | `crates/zstar/` |
| CLI | [Packing, inspection, and extraction](crates/zstar-cli/README.md) | `crates/zstar-cli/` |
| Python SDK | [Sync, async, and fsspec APIs](python/README.md) | `python/` |
| Go SDK | [Package and API documentation](go/) | `go/` |

The Rust storage adapters are documented in the generated API documentation for
`zstar::fs`, `zstar::http`, and `zstar::s3`. Shared conformance fixtures under
`testdata/` exercise the SDKs against the same valid and invalid archives.

## Validation support

| Operation | Rust | Go | Python |
| --- | --- | --- | --- |
| Metadata validation on open | Yes | Yes | Yes |
| Selective source and extent validation | Yes | Yes | Yes |
| Full physical archive validation | `verify(Archive)` | `VerifyScope(VerifyArchive)` | `verify()` |
| External closure validation | `verify(ExternalClosure)` | `VerifyScope(VerifyExternalClosure)` | `verify(external=True)` |
| External SHA-256 opt-in | `verify_external` | `VerifyExternalHash` | `verify_external` |
| Portable and thin writing | Yes | Yes | Portable raw writer; thin fsspec writer |
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

See [justfile](justfile) for lint, test, integration, and packaging recipes,
and [docs/README.md](docs/README.md) for documentation builds and previews.
