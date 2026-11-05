# Zstar draft specifications

The core specification defines the archive format. The external-content extension
adds thin archives and standalone content references. Its informative [CAS storage appendix](external-content.md#appendix-cas-storage)
explains object identity and storage encoding.

Archive metadata uses deterministic CBOR. Physical external-file pointers use
JSON with `"version": "zstar-pointer-v1"`; standalone content references retain
their CBOR encoding. The proposed catalog is a separate JSON document with
`"version": "zstar-catalog-v1"`, not a Zstar archive or core manifest extension.

| Document | Scope | Status |
| --- | --- | --- |
| [Core](core.md) | Archive framing, tar profile, manifest, inline extents, validation, and implementation profiles | Working draft |
| [External content](external-content.md) | `external-content`, object extents, thin pointers, standalone references, and closure verification | Working draft; SDK support documented per package |
| [Catalog](catalog.md) | Named JSON catalogs, collection paths, CAS layers, delegation, and whiteout merging | Design proposal; not implemented |

Core-only implementations may reject `external-content`. Implementations accepting
it apply both core and extension rules. External object addresses identify complete
raw content, independently of its compressed storage representation.

Rust, Go, and Python implement core and external-content archive operations,
including JSON pointer writing and full-validation parsing. Catalog conformance
is separate and remains unimplemented. See each document's conformance classes
and the SDK READMEs for the checks an operation actually establishes.
