//! Native S3-compatible storage, conditional archive ranges, and verified
//! external objects. Storage keys are adapter policy, never wire-format fields.
//!
//! Use [`AmazonS3Builder`](crate::s3::AmazonS3Builder) for credentials, region,
//! endpoint, retry policy, and connection options. Presigned requests use
//! [`S3RequestProvider`](crate::s3::S3RequestProvider) with the
//! pooled transport from [`crate::http`].

#![doc = include_str!("../docs/s3.md")]

use crate::{AsyncRangeReader, Digest, Error, Extent, Result, Source};
use bytes::Bytes;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, GetResult, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutOptions, PutResult,
};
use std::time::{Duration, Instant};
use std::{num::NonZeroUsize, ops::Range, sync::Arc};
use tokio::sync::{OnceCell, Semaphore};

pub use object_store;
pub use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    buffered::BufWriter,
    path::Path,
};

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

/// S3 user metadata identifying the archive wire version.
pub const VERSION_METADATA_KEY: &str = "zstar-version";
/// S3 user metadata containing `offset:encoded_size`, including segment headers.
pub const MANIFEST_POSITION_METADATA_KEY: &str = "zstar-manifest-position";
/// S3 user metadata containing the decoded manifest SHA-256 as lowercase hex.
pub const MANIFEST_SHA256_METADATA_KEY: &str = "zstar-manifest-sha256";

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

/// Generate archive content headers and optional discovery metadata from a validated footer.
/// The digest and decoded length in the actual footer remain authoritative.
pub fn archive_attributes(footer: &crate::Footer, source_size: u64) -> Result<Attributes> {
    if footer.toc_offset == 0
        || footer.toc_encoded_len <= 8
        || footer.toc_decoded_len == 0
        || footer.toc_offset.checked_add(footer.toc_encoded_len)
            != source_size.checked_sub(crate::FOOTER_SIZE as u64)
    {
        return Err(Error::MalformedFooter("manifest coverage"));
    }
    let mut digest = String::with_capacity(64);
    for byte in &footer.toc_sha256 {
        use std::fmt::Write as _;
        write!(digest, "{byte:02x}").expect("writing to String");
    }
    let mut attributes = content_attributes(crate::http::ZSTD_CONTENT_TYPE, false);
    for (key, value) in [
        (
            Attribute::Metadata(VERSION_METADATA_KEY.into()),
            AttributeValue::from("1"),
        ),
        (
            Attribute::Metadata(MANIFEST_POSITION_METADATA_KEY.into()),
            AttributeValue::from(format!("{}:{}", footer.toc_offset, footer.toc_encoded_len)),
        ),
        (
            Attribute::Metadata(MANIFEST_SHA256_METADATA_KEY.into()),
            AttributeValue::from(digest),
        ),
    ] {
        attributes.insert(key, value);
    }
    Ok(attributes)
}

/// Upload completed archive bytes with their optional S3 discovery metadata.
/// Only the footer is inspected here; readers still authenticate the manifest.
pub async fn put_archive(
    store: &dyn ObjectStore,
    path: &Path,
    encoded: Bytes,
) -> Result<PutResult> {
    if encoded.len() < crate::FOOTER_SIZE {
        return Err(Error::NotZstar);
    }
    let footer = crate::parse_footer(&encoded[encoded.len() - crate::FOOTER_SIZE..])?;
    let attributes = archive_attributes(&footer, encoded.len() as u64)?;
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

pub(crate) fn response_cache_policy(attributes: &Attributes) -> crate::http::CachePolicy {
    crate::http::CachePolicy::from_headers(
        attributes.get(&Attribute::CacheControl).map(AsRef::as_ref),
        None,
    )
}

struct PinnedMeta {
    object: ObjectMeta,
    manifest: Option<Range<u64>>,
}

fn manifest_hint(attributes: &Attributes) -> Option<Range<u64>> {
    if attributes
        .get(&Attribute::Metadata(VERSION_METADATA_KEY.into()))
        .is_some_and(|version| version.as_ref() != "1")
    {
        return None;
    }
    let value = attributes.get(&Attribute::Metadata(MANIFEST_POSITION_METADATA_KEY.into()))?;
    let (offset, length) = value.as_ref().split_once(':')?;
    let decimal = |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    if !decimal(offset) || !decimal(length) {
        return None;
    }
    let offset = offset.parse::<u64>().ok()?;
    let length = length.parse::<u64>().ok()?;
    Some(offset..offset.checked_add(length)?)
}

/// Exact ranged reads pinned to the first observed version/ETag and size.
/// Reuse this source for the lifetime of an indexed reader.
pub struct S3RangeSource {
    store: Arc<dyn ObjectStore>,
    path: Path,
    meta: OnceCell<PinnedMeta>,
    max_range_bytes: u64,
    cache_policy: std::sync::Mutex<crate::http::CachePolicy>,
}
impl S3RangeSource {
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self {
            store,
            path,
            meta: OnceCell::new(),
            max_range_bytes: 64 << 20,
            cache_policy: std::sync::Mutex::new(crate::http::CachePolicy::default()),
        }
    }
    /// Override the transport allocation bound independently of reader budgets.
    #[must_use]
    pub fn with_max_range_bytes(mut self, limit: u64) -> Self {
        self.max_range_bytes = limit;
        self
    }
    async fn metadata(&self) -> Result<&PinnedMeta> {
        self.meta
            .get_or_try_init(|| async {
                let result = self
                    .store
                    .get_opts(
                        &self.path,
                        GetOptions {
                            head: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(storage_error)?;
                self.cache_policy
                    .lock()
                    .unwrap()
                    .observe(response_cache_policy(&result.attributes));
                let manifest = manifest_hint(&result.attributes);
                let meta = result.meta;
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
                Ok(PinnedMeta {
                    object: meta,
                    manifest,
                })
            })
            .await
    }
}
#[async_trait::async_trait]
impl AsyncRangeReader for S3RangeSource {
    fn cache_allowed(&self) -> bool {
        self.cache_policy.lock().unwrap().allows_reuse()
    }
    async fn size(&self) -> Result<u64> {
        Ok(self.metadata().await?.object.size)
    }
    async fn manifest_range(&self) -> Result<Option<Range<u64>>> {
        let meta = self.metadata().await?;
        Ok(meta.manifest.clone().filter(|range| {
            range.start <= meta.object.size
                && meta.object.size - range.start <= self.max_range_bytes
        }))
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end {
            return Err(Error::Range);
        }
        let meta = &self.metadata().await?.object;
        crate::checked_range(range.start, range.end - range.start, meta.size)?;
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        if range.end - range.start > self.max_range_bytes {
            return Err(Error::Budget("S3 range size"));
        }
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
        let policy = response_cache_policy(&result.attributes);
        let bytes = read_exact(result, range.end - range.start).await?;
        self.cache_policy.lock().unwrap().observe(policy);
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
/// `<prefix>/<lowercase sha256>.zst`; a custom key mapper can implement any
/// deployment layout. Whole files, file pieces, and merged objects all work.
pub struct S3BlobStore {
    store: Arc<dyn ObjectStore>,
    key: Arc<dyn Fn([u8; 32]) -> Path + Send + Sync>,
    permits: Arc<Semaphore>,
    policies: std::sync::Mutex<lru::LruCache<[u8; 32], crate::http::CachePolicy>>,
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
            policies: std::sync::Mutex::new(lru::LruCache::new(NonZeroUsize::new(1024).unwrap())),
        }
    }
    #[must_use]
    pub fn object_path(&self, digest: [u8; 32]) -> Path {
        (self.key)(digest)
    }
    /// Fetch one bounded encoded object. The encoded-size hint does not limit
    /// fetches; decoding and address verification belong to the reader.
    pub async fn get_encoded(&self, digest: [u8; 32], _hint: u64, max_bytes: u64) -> Result<Bytes> {
        let limit = max_bytes.min(17 << 20);
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
        let policy = response_cache_policy(&result.attributes);
        let bytes = read_exact(result, size).await?;
        let mut policies = self.policies.lock().unwrap();
        if let Some(previous) = policies.get_mut(&digest) {
            previous.observe(policy);
        } else {
            policies.put(digest, policy);
        }
        Ok(bytes)
    }
    pub async fn put(&self, raw: Bytes) -> Result<Extent> {
        if raw.is_empty() || raw.len() > crate::DATA_FRAME_MAX {
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
            Ok(meta) if meta.size > 0 && meta.size <= 17 << 20 => {
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
                if meta.size == 0 || meta.size > 17 << 20 {
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
    fn cache_allowed(&self, digest: [u8; 32]) -> bool {
        self.policies
            .lock()
            .unwrap()
            .get(&digest)
            .is_some_and(crate::http::CachePolicy::allows_reuse)
    }
    async fn get_encoded(&self, digest: [u8; 32], hint: u64, max_bytes: u64) -> Result<Bytes> {
        S3BlobStore::get_encoded(self, digest, hint, max_bytes).await
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
