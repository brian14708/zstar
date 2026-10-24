# Rust HTTP transport

`zstar::http` provides pooled async range reads for the archive API.
Enable the `http` feature to use this module.

## Read a signed URL

```rust,no_run
use std::sync::Arc;
use zstar::Archive;
use zstar::http::{HttpRangeSource, PresignedRequest, ReqwestTransport};

#[tokio::main]
async fn main() -> zstar::Result<()> {
    let transport = Arc::new(ReqwestTransport::new()?);
    let source = Arc::new(HttpRangeSource::new(
        transport,
        PresignedRequest::get("https://example.com/archive.zstar?signature=..."),
    ));
    let archive = Archive::open(source).await?;
    let bytes = archive.read_range("hello.txt", 0..5).await?;
    println!("Read {} bytes", bytes.len());
    Ok(())
}
```

## Transport behavior

`ReqwestTransport` reuses a rustls HTTP/1.1/HTTP/2 client, with connection pooling,
a 10-second connect timeout, a 60-second request timeout, and redirects disabled.
GET ranges require exact `206`, `Content-Range` with total size, identity encoding,
and a complete bounded body. The default transport limit is 256 MiB + 8 bytes;
the indexed reader applies its own smaller metadata and output budgets.

`PresignedRequest` holds an exact method, URL, and additional signed headers.
Its `Debug` representation redacts URL and header values. Transport errors
remove request URLs. Avoid logging public URL/header fields yourself.

`HttpRangeSource` implements `zstar::AsyncRangeReader` and caches discovered
size. It rejects size changes on subsequent responses. It also pins the first
response's `ETag` (else `Last-Modified`) and sends `If-Match` (or `If-Range`) on
every later range; a 412, a 200 reply to a conditional range, or a differing
validator fails with an `InvalidData` I/O error carrying `http::SourceChanged`
(test with `http::is_source_changed`). This catches same-size replacement.
Servers sending no validator are protected only by the size check. Weak `ETag`s
cannot be used with `If-Match` and fall back to `Last-Modified`. Custom
`HttpTransport`s opt in by implementing `get_range_validated`.
`HttpEncodedObjectResolver` uses one source per object, so its size probe and
body read share one pinned snapshot.

Construct the source with a fixed GET request or a `RequestProvider`. Providers
receive the method and an explicit `force_refresh` flag. Near expiry triggers
refresh; a 401/403 permits one retry only when the provider can produce
different credentials (`RequestProvider::can_refresh`, false for fixed
requests). Other status failures are returned to the caller; 200 (Range
ignored), 401/403, 404, 412 and 416 errors carry an explanatory hint. Provider errors must also
avoid credentials and signed URLs in their messages.

`HttpRangeSource::put` accepts shared `Bytes` with no full-buffer clone and uses
the same refresh behavior. A fixed GET request cannot authorize PUT; supply a
provider supporting both methods or a fixed PUT request for uploads. Presigned
multipart orchestration is not yet implemented.

## External objects

`HttpEncodedObjectResolver` implements `zstar::ObjectResolver` for complete
external objects addressed by their 32-byte SHA-256 digest. Its provider closure
maps each digest to a `RequestProvider` (for example, a signed object URL). It
probes object length with one ranged byte, rejects zero-length or over-budget
objects before fetching the rest, and applies a shared concurrency limit. The
encoded size hint never acts as a decoded-size bound; the archive reader checks
the decoded object and hash policy. HTTP 404 maps to `MissingObject`; codec and
digest validation remain the archive reader's job.

## Cancellation and testing

Dropping a future cancels its request. `with_client` permits custom client and
response budgets; callers then own timeout, redirect, and encoding policy.

Run `nix develop --command cargo test -p zstar --features http --test transport` for real loopback HTTP tests.
