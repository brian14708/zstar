# Native Go S3 storage

This package accepts the AWS SDK for Go v2 `*s3.Client`; configure credentials,
region, HTTP pooling, endpoint, and retry policy using the SDK. The adapter never
puts that configuration into archive metadata. `NewSource` pins size and ETag or
version from HEAD and uses exact conditional ranges. `NewBlobStore` reads/writes
independent compressed external objects. The archive reader handles decoding,
SHA-256 policy, concurrent miss sharing and bounded decoded caching.

```go
import (
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
    zstar "github.com/brian14708/zstar/sdks/go"
    zstars3 "github.com/brian14708/zstar/sdks/go/s3"
)

client := awss3.NewFromConfig(awsConfig)
blobs, err := zstars3.NewBlobStore(client, "bucket", zstars3.BlobStoreOptions{
    Prefix: "objects", Concurrency: 8,
})
// Check err, then reuse this source/client for the reader's lifetime.
archive, err := zstar.OpenWithOptions(ctx,
    zstars3.NewSource(client, "bucket", "data.zstar"),
    zstar.ReaderOptions{Resolver: blobs, Manifest: zstar.ManifestOptions{ContentCacheBytes: 16 << 20}},
)
defer archive.Close()
bytes, err := archive.Read(ctx, "external.bin", 0, 4096)
```

The default object key is `<prefix>/<lowercase raw SHA-256>.zst`.
`BlobStoreOptions.Key` supplies a different digest-to-key convention. Whole files,
split files, and slices from objects merged across files all work. Objects are
bounded to 16 MiB raw / 17 MiB encoded. `Concurrency` bounds adapter requests and upload codec work. `GetEncoded` enforces
actual input limits independently of the manifest hint, and returns encoded bytes
without hashing them. `Put(ctx, raw)` returns a complete-object `zstar.Extent`,
including its canonical object address and actual encoded-size hint:

```go
extent, err := blobs.Put(ctx, contents)
if err != nil { return err }
err = writer.Add(ctx, zstar.Header{
    Path: "external.bin", Mode: 0o644, Extents: []zstar.Extent{extent},
}, nil)
```

Adjust `extent.Size` and `extent.Offset` to select a subrange. No reconstructed
whole-file OID or digest is stored. `ReaderOptions.SkipExternalHash` controls
object hashing consistently across native, presigned and custom resolvers.

Each object uses a checksum-enabled codec window bounded to its decoded length,
rounded up to a supported power of two. This reduces unused encoder history
without excluding any match within the object.

Use the AWS SDK's `s3.NewPresignClient(client).PresignGetObject` for a signed GET
URL, then pass its URL and signed headers to `zstar.HTTPRangeReader`. Its size
probe uses a GET range rather than requiring HEAD authorization. The native S3
source itself uses credentialed HEAD.
`NewSource` accepts the minimal `SourceClient` interface containing only HEAD
and GET, so read-only adapters need no PUT implementation. `NewBlobStore` accepts
`BlobClient`, which adds PUT. Both interfaces are satisfied by the AWS SDK v2 client.

These consumer-defined interfaces follow the AWS SDK v2's
[documented client-mocking pattern](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/unit-testing.html).
The official SDK supports S3-compatible endpoints and supplies credentials,
signing, retries and connection pooling. There is no SDK v2 replacement for the
old, broad SDK v1 `s3iface.S3API`; keep interfaces limited to the operations each
adapter consumes. MinIO's `minio-go/v7` is a well-known alternative S3 client.
Go Cloud's `gocloud.dev/blob` is useful for provider-neutral storage, but its
portable reader options do not expose ETag/version preconditions; preserving
this adapter's pinned reads would need provider-specific integration.

For general-purpose multipart transfers, the official SDK's
[`feature/s3/manager`](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/sdk-utilities-s3.html)
provides upload/download managers. Zstar's multipart adapter additionally
enforces its documented buffer limits, error redaction and cancellation cleanup.

`Upload(ctx, client, bucket, key, input, options)` streams a caller-owned
`io.Reader` into parallel multipart uploads. Defaults are 8 MiB parts and four
in-flight parts. Part buffers are bounded by part size × concurrency, excluding
SDK/HTTP buffers. Part size is configurable from 5 to 64 MiB; concurrency from
one to 32. Empty input uses a zero-length PUT and aborts its empty multipart
session. Failures and context cancellation attempt abort with a separate
10-second cleanup deadline. Cancellation cannot interrupt a blocking custom
`io.Reader`; close its pipe or otherwise unblock it. Input is never closed by
the adapter. Native multipart is supported; presigned multipart remains pending.

Completed archive inputs implementing both `io.ReaderAt` and `io.Seeker`
(including files and `bytes.Reader`) automatically receive three S3 user metadata
keys: `zstar-version`, `zstar-manifest-position` (`offset:encoded_size`) and
`zstar-manifest-sha256` (lowercase decoded manifest SHA-256). Probing reads only the
footer and restores the initial input position before upload. Generic streams and
non-archive inputs continue without inferred hints. `UploadOptions.Metadata` adds
caller metadata; options are copied, and inferred archive values replace reserved
keys. `ArchiveMetadata(footer, objectSize)` generates the same mapping for direct
SDK PUTs or streaming uploads with a known completed footer.

Native readers get the position hint from the pinned HEAD and fetch manifest plus
footer together when the whole tail fits `ManifestOptions.ReadAheadBytes` (default
1 MiB) and the encoded metadata budget. Invalid/stale/large hints fall back to
standard discovery. Footer length/digest checks and snapshot conditions always
apply; decoded size stays in the footer. See the
[discovery key definitions](../../../crates/zstar/docs/s3.md#discovery-key-definitions).

Storage errors preserve missing-object and context cancellation signals while
redacting backend messages and signed URL details. `ErrNotFound` works with
`errors.Is`. Caller-provided AWS logging must also avoid credentials/URLs.

Tests use only disposable local RustFS:

```sh
ZSTAR_S3_ENDPOINT=http://127.0.0.1:9000 go test -race -tags=integration ./...
```

Run from `sdks/go` with the root compose service already healthy. Unit tests
check multipart resource bounds, ordering, empty uploads, redaction, failure
cleanup, and cancellation cleanup without a storage service.
