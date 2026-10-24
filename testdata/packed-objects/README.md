# Packed-object interoperability fixtures

The root fixtures were written by Rust; `go/` and `python/` were written through
their respective SDK APIs. All reconstruct `payload.bin` from unchanged v1
external-object archives. The Python fixture uses explicit five-byte fixed chunks;
other producers feed complete `first`, `second`, `third`, `first` chunks. Every
producer writes independently compressed complete frames, a deterministic catalog,
and a published generation. Local snapshot ETags quote the complete encoded SHA-256.

Rust, Go and Python read each producer's catalog, pack, archive and generation.
`manifest.json` records rejection categories for malformed catalog fixtures.
Chunking algorithm vectors are separate implementation tests in `../chunking/`.
