# Zstar draft specifications

The core specification defines a self-contained indexed archive format.
Optional extensions add capabilities such as external-object references.
The catalog proposal defines a separate document for organizing archives.

| Document | Scope | Status |
| --- | --- | --- |
| [Core](core.md) | Archive framing, tar profile, paged metadata, inline extents, validation, and implementation profiles | Working draft |
| [External content](external-content.md) | `external-content`, object extents, thin pointers, standalone references | Working draft; SDK support documented per package |
| [Catalog](catalog.md) | Named JSON catalogs, collection paths, CAS layers, delegation, and whiteout merging | Design proposal; not implemented |

Implementations can support core alone or add optional extensions. Rust, Go,
and Python implement core and external-content archive operations.
See the SDK READMEs for supported operations and validation scopes.
