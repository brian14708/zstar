//! Native S3-compatible storage and verified external objects.
//!
//! Conditional archive ranges pin snapshots. Storage keys are adapter policy,
//! never wire-format fields.
//!
//! Use [`AmazonS3Builder`](crate::s3::AmazonS3Builder) for credentials, region,
//! endpoint, retry policy, and connection options. Presigned requests use
//! [`S3RequestProvider`](crate::s3::S3RequestProvider) with the
//! pooled transport from [`crate::http`].
//!
//! [`S3RangeSource`](crate::s3::S3RangeSource) pins object size and version
//! validators for conditional range reads. [`S3BlobStore`](crate::s3::S3BlobStore)
//! stores complete independently compressed, content-addressed objects and
//! verifies their decoded SHA-256. Keys, credentials, and endpoints stay
//! outside the archive format.

use crate::{AsyncRangeReader, Digest, Error, Extent, Result, Source};
use bytes::{Bytes, BytesMut};
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, GetRange, GetResult, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutResult,
};
use std::time::{Duration, Instant};
use std::{num::NonZeroUsize, ops::Range, sync::Arc};
use tokio::sync::{OnceCell, Semaphore};

pub use object_store;
pub use object_store::{aws::AmazonS3, path::Path};

/// Explicit S3 configuration for the core adapter.
///
/// This deliberately has no `from_env` constructor. Region and either static
/// credentials or anonymous access must be supplied by the caller; the core
/// library never consults environment variables, profiles, metadata services,
/// or other ambient configuration.
#[must_use]
pub struct AmazonS3Builder {
    inner: object_store::aws::AmazonS3Builder,
    region: Option<String>,
    credentials: bool,
    anonymous: bool,
}

impl AmazonS3Builder {
    pub fn new() -> Self {
        Self {
            inner: object_store::aws::AmazonS3Builder::new(),
            region: None,
            credentials: false,
            anonymous: false,
        }
    }

    pub fn with_bucket_name(mut self, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_bucket_name(value);
        self
    }

    #[expect(
        clippy::missing_panics_doc,
        reason = "Unwraps enforce internal invariants established before this operation."
    )]
    pub fn with_region(mut self, value: impl Into<String>) -> Self {
        self.region = Some(value.into());
        self.inner = self.inner.with_region(self.region.as_ref().unwrap());
        self
    }

    pub fn with_endpoint(mut self, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_endpoint(value);
        self
    }

    pub fn with_allow_http(mut self, value: bool) -> Self {
        self.inner = self.inner.with_allow_http(value);
        self
    }

    pub fn with_virtual_hosted_style_request(mut self, value: bool) -> Self {
        self.inner = self.inner.with_virtual_hosted_style_request(value);
        self
    }

    pub fn with_access_key_id(mut self, value: impl Into<String>) -> Self {
        self.credentials = true;
        self.inner = self.inner.with_access_key_id(value);
        self
    }

    pub fn with_secret_access_key(mut self, value: impl Into<String>) -> Self {
        self.credentials = true;
        self.inner = self.inner.with_secret_access_key(value);
        self
    }

    pub fn with_token(mut self, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_token(value);
        self
    }

    pub fn with_skip_signature(mut self, value: bool) -> Self {
        self.anonymous = value;
        self.inner = self.inner.with_skip_signature(value);
        self
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub fn build(self) -> std::result::Result<AmazonS3, object_store::Error> {
        if self.region.as_deref().is_none_or(str::is_empty) {
            return Err(object_store::Error::Generic {
                store: "S3 configuration",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "region is required",
                )),
            });
        }
        if !self.credentials && !self.anonymous {
            return Err(object_store::Error::Generic {
                store: "S3 configuration",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "explicit credentials or anonymous access are required",
                )),
            });
        }
        self.inner.build()
    }
}

impl Default for AmazonS3Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod builder_tests {
    use super::AmazonS3Builder;

    #[test]
    fn requires_explicit_configuration() {
        assert!(
            AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .build()
                .is_err()
        );
        assert!(
            AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .with_skip_signature(true)
                .build()
                .is_ok()
        );
        assert!(
            AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .with_access_key_id("key")
                .with_secret_access_key("secret")
                .build()
                .is_ok()
        );
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
// Backend errors can contain full signed URLs, so retain the category without
// forwarding their source chains or credential-bearing Display/Debug output.
pub(crate) fn storage_error(error: object_store::Error) -> Error {
    use object_store::Error as E;
    let (kind, message) = match error {
        E::NotFound { .. } => (std::io::ErrorKind::NotFound, "storage object not found"),
        E::PermissionDenied { .. } | E::Unauthenticated { .. } => (
            std::io::ErrorKind::PermissionDenied,
            "storage authorization failed",
        ),
        E::Precondition { .. } => (std::io::ErrorKind::InvalidData, "storage object changed"),
        _ => (std::io::ErrorKind::Other, "storage operation failed"),
    };
    Error::Io(std::io::Error::new(kind, message))
}

pub(crate) fn content_attributes(content_type: &'static str, immutable: bool) -> Attributes {
    [
        (Attribute::ContentType, AttributeValue::from(content_type)),
        (
            Attribute::CacheControl,
            AttributeValue::from(if immutable {
                crate::http::IMMUTABLE_CACHE_CONTROL
            } else {
                crate::http::MUTABLE_CACHE_CONTROL
            }),
        ),
    ]
    .into_iter()
    .collect()
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
/// Upload completed archive bytes with content type and cache policy headers.
/// Only the footer is inspected here; readers still validate the manifest.
pub async fn put_archive(
    store: &dyn ObjectStore,
    path: &Path,
    encoded: Bytes,
) -> Result<PutResult> {
    if encoded.len() < crate::FOOTER_SIZE {
        return Err(Error::NotZstar);
    }
    let footer = crate::parse_footer(&encoded[encoded.len() - crate::FOOTER_SIZE..])?;
    footer.metadata_range(encoded.len() as u64)?;
    let attributes = content_attributes(crate::http::ZSTD_CONTENT_TYPE, false);
    store
        .put_opts(
            path,
            encoded.into(),
            PutOptions {
                attributes,
                ..Default::default()
            },
        )
        .await
        .map_err(storage_error)
}

struct PinnedMeta {
    object: ObjectMeta,
    tail: Bytes,
}

/// Exact ranged reads pinned to the first observed version/ETag and size.
///
/// The first operation fetches and retains up to 128 KiB from the object suffix,
/// discovering size and identity without a separate HEAD. Metadata and content
/// ranges reuse those bytes; uncached ranges remain conditional on that identity.
/// Reuse this source for the lifetime of an indexed reader.
pub struct S3RangeSource {
    /// A range reader over one object in an S3-compatible store.
    store: Arc<dyn ObjectStore>,
    path: Path,
    meta: OnceCell<PinnedMeta>,
    max_range_bytes: u64,
}
impl S3RangeSource {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            meta: OnceCell::new(),
            max_range_bytes: 64 << 20,
        }
    }
    /// Override the transport allocation bound independently of reader budgets.
    /// This also caps the initial suffix prefetch.
    #[must_use]
    pub const fn with_max_range_bytes(mut self, limit: u64) -> Self {
        self.max_range_bytes = limit;
        self
    }
    async fn metadata(&self) -> Result<&PinnedMeta> {
        self.meta
            .get_or_try_init(|| async {
                let suffix = self.max_range_bytes.min(128 << 10);
                if suffix == 0 {
                    return Err(Error::Budget("S3 range size"));
                }
                let result = self
                    .store
                    .get_opts(
                        &self.path,
                        GetOptions {
                            range: Some(GetRange::Suffix(suffix)),
                            ..Default::default()
                        },
                    )
                    .await;
                // Some stores reject suffix ranges for empty objects. Confirm an
                // empty object with HEAD on a generic backend error only; never
                // substitute a nonempty object's metadata for a failed range.
                let result = match result {
                    Ok(result) => result,
                    Err(error @ object_store::Error::Generic { .. }) => {
                        match self
                            .store
                            .get_opts(
                                &self.path,
                                GetOptions {
                                    head: true,
                                    ..Default::default()
                                },
                            )
                            .await
                        {
                            Ok(result) if result.meta.size == 0 => result,
                            _ => return Err(storage_error(error)),
                        }
                    }
                    Err(error) => return Err(storage_error(error)),
                };
                if result.range != (result.meta.size.saturating_sub(suffix)..result.meta.size) {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "storage suffix range mismatch",
                    )));
                }
                let meta = result.meta.clone();
                if meta.e_tag.as_deref().is_none_or(str::is_empty)
                    && meta
                        .version
                        .as_deref()
                        .is_none_or(|v| v.is_empty() || v == "null")
                {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "storage object has no immutable identity",
                    )));
                }
                let tail = read_exact(result, meta.size.min(suffix)).await?;
                Ok(PinnedMeta { object: meta, tail })
            })
            .await
    }
}
#[async_trait::async_trait]
impl AsyncRangeReader for S3RangeSource {
    async fn size(&self) -> Result<u64> {
        Ok(self.metadata().await?.object.size)
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end {
            return Err(Error::Range);
        }
        let pinned = self.metadata().await?;
        let meta = &pinned.object;
        crate::checked_range(range.start, range.end - range.start, meta.size)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        if range.end - range.start > self.max_range_bytes {
            return Err(Error::Budget("S3 range size"));
        }
        let tail_start = meta.size - pinned.tail.len() as u64;
        if range.start >= tail_start {
            let start = usize::try_from(range.start - tail_start).map_err(|_| Error::Range)?;
            let end = usize::try_from(range.end - tail_start).map_err(|_| Error::Range)?;
            return Ok(pinned.tail.slice(start..end));
        }
        let requested = range.clone();
        let range = range.start..range.end.min(tail_start);
        let result = self
            .store
            .get_opts(
                &self.path,
                GetOptions {
                    range: Some(range.clone().into()),
                    if_match: meta.e_tag.clone(),
                    version: meta.version.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        if result.range != range
            || result.meta.size != meta.size
            || result.meta.e_tag != meta.e_tag
            || result.meta.version != meta.version
        {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "storage range or identity changed",
            )));
        }
        let bytes = read_exact(result, range.end - range.start).await?;
        if requested.end > range.end {
            let length =
                usize::try_from(requested.end - requested.start).map_err(|_| Error::Range)?;
            let suffix = usize::try_from(requested.end - tail_start).map_err(|_| Error::Range)?;
            let mut out = BytesMut::with_capacity(length);
            out.extend_from_slice(&bytes);
            out.extend_from_slice(&pinned.tail[..suffix]);
            return Ok(out.freeze());
        }
        Ok(bytes)
    }
}

// Stream into an explicitly bounded allocation even with custom stores.
pub(crate) async fn read_exact(result: GetResult, count: u64) -> Result<Bytes> {
    use futures_util::StreamExt;
    let mut stream = result.into_stream();
    let count = usize::try_from(count).map_err(|_| Error::Budget("S3 response size"))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count)
        .map_err(|_| Error::Budget("S3 response allocation"))?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(storage_error)?;
        if chunk.len() > count - bytes.len() {
            return Err(Error::Budget("oversized storage response"));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != count {
        return Err(Error::MalformedFrame("short storage response".into()));
    }
    Ok(Bytes::from(bytes))
}

/// One independent Zstandard frame per raw content digest. The default key is
///
/// `<prefix>/<lowercase sha256>.zst`; a custom key mapper can implement any
/// deployment layout. Whole files, file pieces, and merged objects all work.
pub struct S3BlobStore {
    store: Arc<dyn ObjectStore>,
    key: Arc<dyn Fn([u8; 32]) -> Path + Send + Sync>,
    permits: Arc<Semaphore>,
}
impl S3BlobStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, concurrency: NonZeroUsize) -> Self {
        Self::with_key_mapper(
            store,
            move |digest| {
                let hex: String = digest.iter().fold(String::new(), |mut output, byte| {
                    use std::fmt::Write as _;
                    write!(output, "{byte:02x}").expect("writing to String");
                    output
                });
                prefix.clone().join(format!("{hex}.zst"))
            },
            concurrency,
        )
    }
    pub fn with_key_mapper(
        store: Arc<dyn ObjectStore>,
        key: impl Fn([u8; 32]) -> Path + Send + Sync + 'static,
        concurrency: NonZeroUsize,
    ) -> Self {
        Self {
            store,
            key: Arc::new(key),
            permits: Arc::new(Semaphore::new(concurrency.get())),
        }
    }
    #[must_use]
    pub fn object_path(&self, digest: [u8; 32]) -> Path {
        (self.key)(digest)
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Fetch one bounded encoded object. The encoded-size hint does not limit
    /// fetches; decoding and address verification belong to the reader.
    pub async fn get_encoded(&self, digest: [u8; 32], _hint: u64, max_bytes: u64) -> Result<Bytes> {
        let limit = max_bytes.min(isize::MAX as u64 - 1);
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| Error::Budget("object store closed"))?;
        // Content addresses are verified by the reader, so a single bounded
        // GET suffices without a separate metadata probe or version pin.
        let result = match self.store.get(&(self.key)(digest)).await {
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::MissingObject(digest));
            }
            result => result.map_err(storage_error)?,
        };
        let size = result.meta.size;
        if size == 0 || size > limit {
            return Err(Error::Budget("encoded object size"));
        }
        if result.range != (0..size) {
            return Err(Error::MalformedFrame(
                "partial storage object response".into(),
            ));
        }
        let bytes = read_exact(result, size).await?;
        Ok(bytes)
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub async fn put(&self, raw: Bytes) -> Result<Extent> {
        if raw.is_empty() {
            return Err(Error::Budget("external object size"));
        }
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Budget("object store closed"))?;
        let raw_for_digest = raw.clone();
        let (digest, permit) = tokio::task::spawn_blocking(move || {
            (*Digest::sha256(&raw_for_digest).as_bytes(), permit)
        })
        .await
        .map_err(|_| Error::Io(std::io::Error::other("object hasher failed")))?;
        let path = (self.key)(digest);
        // Keys are content addresses: an existing non-empty object under this
        // key is the same content, so skip the encode and upload. Its size is
        // only a hint (another encoder may have written it); readers always
        // decode and verify the SHA-256, so a damaged object fails loudly.
        match self.store.head(&path).await {
            Ok(meta) if meta.size > 0 => {
                drop(permit);
                return Ok(Extent {
                    size: raw.len() as u64,
                    source: Source::Object(digest),
                    encoded_size: meta.size,
                    offset: 0,
                });
            }
            Ok(_) => return Err(Error::Budget("encoded object size")),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(storage_error(error)),
        }
        let raw_for_worker = raw.clone();
        let (encoded, permit) = tokio::task::spawn_blocking(move || {
            crate::encode_frame(&raw_for_worker, 3).map(|encoded| (Bytes::from(encoded), permit))
        })
        .await
        .map_err(|_| Error::Io(std::io::Error::other("object encoder failed")))??;
        let uploaded_size = encoded.len() as u64;
        let encoded_size = match self
            .store
            .put_opts(
                &path,
                encoded.into(),
                PutOptions {
                    mode: PutMode::Create,
                    attributes: content_attributes(crate::http::ZSTD_CONTENT_TYPE, true),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => uploaded_size,
            // A concurrent writer won the race after our HEAD. Keep its
            // encoding and return the stored size as the extent's hint.
            Err(object_store::Error::AlreadyExists { .. }) => {
                let meta = self.store.head(&path).await.map_err(storage_error)?;
                if meta.size == 0 {
                    return Err(Error::Budget("encoded object size"));
                }
                meta.size
            }
            Err(error) => return Err(storage_error(error)),
        };
        drop(permit);
        Ok(Extent {
            size: raw.len() as u64,
            source: Source::Object(digest),
            encoded_size,
            offset: 0,
        })
    }
}
#[async_trait::async_trait]
impl crate::chunking::ObjectWriter for S3BlobStore {
    async fn put_chunk(&mut self, raw: Bytes) -> Result<Extent> {
        self.put(raw).await
    }
}
/// Signs GET and PUT requests on demand, using the client's credential provider.
/// URL lifetime is an adapter setting; it is never serialized into an archive.
pub struct S3RequestProvider {
    store: Arc<AmazonS3>,
    path: Path,
    lifetime: Duration,
}
impl S3RequestProvider {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub fn new(store: Arc<AmazonS3>, path: Path, lifetime: Duration) -> std::io::Result<Self> {
        if lifetime <= Duration::from_secs(5) || lifetime > Duration::from_hours(168) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "S3 URL lifetime must be >5 seconds and <=7 days",
            ));
        }
        Ok(Self {
            store,
            path,
            lifetime,
        })
    }
}
#[async_trait::async_trait]
impl crate::http::RequestProvider for S3RequestProvider {
    async fn request(
        &self,
        method: &str,
        _force_refresh: bool,
    ) -> std::io::Result<crate::http::ExpiringRequest> {
        use object_store::signer::Signer;
        let method = match method {
            "GET" => reqwest::Method::GET,
            "PUT" => reqwest::Method::PUT,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "only GET and PUT may be signed",
                ));
            }
        };
        let expires_at = Instant::now() + self.lifetime;
        let url = self
            .store
            .signed_url(method.clone(), &self.path, self.lifetime)
            .await
            .map_err(|error| match storage_error(error) {
                Error::Io(error) => error,
                _ => unreachable!(),
            })?;
        Ok(crate::http::ExpiringRequest {
            request: crate::http::PresignedRequest {
                method: method.to_string(),
                url: url.into(),
                headers: vec![],
            },
            expires_at: Some(expires_at),
        })
    }
}

#[async_trait::async_trait]
impl crate::ObjectResolver for S3BlobStore {
    async fn get_encoded(&self, digest: [u8; 32], hint: u64, max_bytes: u64) -> Result<Bytes> {
        Self::get_encoded(self, digest, hint, max_bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_put_keeps_its_permit_until_the_hash_worker_finishes() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let blobs = S3BlobStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                Path::from("objects"),
                NonZeroUsize::new(1).unwrap(),
            );
            // Queue the hasher behind a blocked worker so cancellation happens
            // while spawn_blocking still owns unfinished work.
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || wait.recv().unwrap());
            let mut put = Box::pin(blobs.put(Bytes::from_static(b"raw object")));
            let pending = futures_util::poll!(&mut put).is_pending();
            drop(put);
            let retained = blobs.permits.available_permits() == 0;
            release.send(()).unwrap();
            blocker.await.unwrap();
            let permit = tokio::time::timeout(Duration::from_secs(5), blobs.permits.acquire())
                .await
                .unwrap()
                .unwrap();
            drop(permit);
            assert!(pending);
            assert!(
                retained,
                "cancelled hasher released its concurrency slot early"
            );
        });
    }
}
