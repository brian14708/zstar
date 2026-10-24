//! Pooled asynchronous HTTP range reads and presigned requests.
//!
//! [`HttpRangeSource`](crate::http::HttpRangeSource) pins a strong `ETag` and
//! object length for the reader lifetime. Ranges must be exact `206` responses
//! with identity encoding; changed snapshots are reported as
//! [`SourceChanged`](crate::http::SourceChanged). Request debug output
//! and transport errors redact signed URLs and header values. Dropping a
//! future cancels an in-flight request.

use crate::AsyncRangeReader;
use bytes::Bytes;
use std::{
    fmt,
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

pub(crate) const ZSTD_CONTENT_TYPE: &str = "application/zstd";
pub(crate) const MUTABLE_CACHE_CONTROL: &str = "private, no-cache, no-transform";
pub(crate) const IMMUTABLE_CACHE_CONTROL: &str =
    "private, max-age=31536000, immutable, no-transform";

pub(crate) fn artifact_content_type(key: &str) -> &'static str {
    let name = key.rsplit('/').next().unwrap_or_default();
    let extension = name.rsplit_once('.').map_or("", |(_, extension)| extension);
    if ["zstar", "zst", "zstp"]
        .iter()
        .any(|suffix| extension.eq_ignore_ascii_case(suffix))
    {
        ZSTD_CONTENT_TYPE
    } else if extension.eq_ignore_ascii_case("cbor") {
        "application/cbor"
    } else {
        "application/octet-stream"
    }
}

/// Cache admission for a private decoded-content cache. An absent policy keeps
/// the reader's immutable-snapshot caching behavior. Restrictive directives
/// bypass retention; stale content is fetched again with the pinned validator.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CachePolicy {
    denied: bool,
    expires: Option<Instant>,
}
impl CachePolicy {
    /// Parse a native object response Cache-Control value for a private cache.
    #[must_use]
    pub fn from_cache_control(control: &str) -> Self {
        Self::from_headers(Some(control), None)
    }
    pub(crate) fn from_headers(control: Option<&str>, age: Option<&str>) -> Self {
        let mut policy = Self::default();
        let mut max_age = None;
        for directive in control.unwrap_or_default().split(',') {
            let (name, value) = directive
                .trim()
                .split_once('=')
                .unwrap_or((directive.trim(), ""));
            let name = name.trim();
            if name.eq_ignore_ascii_case("no-store") || name.eq_ignore_ascii_case("no-cache") {
                policy.denied = true;
            } else if name.eq_ignore_ascii_case("max-age") {
                let value = value.trim().trim_matches('"').parse::<u64>();
                if max_age.is_some() || value.is_err() {
                    policy.denied = true;
                }
                max_age = value.ok();
            }
        }
        if let Some(seconds) = max_age {
            let age = age.unwrap_or("0").parse::<u64>().unwrap_or(u64::MAX);
            policy.expires =
                Instant::now().checked_add(Duration::from_secs(seconds.saturating_sub(age)));
            if policy.expires.is_none() {
                policy.denied = true;
            }
        }
        policy
    }
    fn from_response(headers: &reqwest::header::HeaderMap, requested: Instant) -> Self {
        let header = |name| headers.get(name).and_then(|v| v.to_str().ok());
        let now = std::time::SystemTime::now();
        let date = header(reqwest::header::DATE).and_then(|v| httpdate::parse_http_date(v).ok());
        let apparent_age = date
            .and_then(|v| now.duration_since(v).ok())
            .unwrap_or_default();
        let age = header(reqwest::header::AGE)
            .unwrap_or("0")
            .parse::<u64>()
            .unwrap_or(u64::MAX);
        let corrected_age = Duration::from_secs(age)
            .saturating_add(requested.elapsed())
            .max(apparent_age);
        let control = headers
            .get_all(reqwest::header::CACHE_CONTROL)
            .iter()
            .map(|v| v.to_str().unwrap_or("no-store"))
            .collect::<Vec<_>>()
            .join(",");
        let mut policy = Self::from_headers(Some(&control), None);
        if let Some(expiry) = policy.expires {
            policy.expires = Some(
                expiry
                    .checked_sub(corrected_age)
                    .unwrap_or_else(Instant::now),
            );
        }
        if policy.expires.is_none()
            && !policy.denied
            && let Some(expires) = header(reqwest::header::EXPIRES)
        {
            policy.expires = Some(Instant::now());
            if let Ok(expires) = httpdate::parse_http_date(expires) {
                let lifetime = expires
                    .duration_since(date.unwrap_or(now))
                    .unwrap_or_default();
                policy.expires = Instant::now().checked_add(lifetime.saturating_sub(corrected_age));
                policy.denied = policy.expires.is_none();
            }
        }
        if headers.get_all(reqwest::header::VARY).iter().any(|v| {
            v.to_str()
                .unwrap_or("*")
                .split(',')
                .any(|v| v.trim() == "*")
        }) {
            policy.denied = true;
        }
        policy
    }
    /// Observe another response for the same source. Retain restrictive
    /// directives for its lifetime so concurrent reads cannot relax admission.
    pub fn observe(&mut self, mut response: Self) {
        response.denied |= self.denied;
        *self = response;
    }
    /// Whether decoded bytes may be retained and reused without another fetch.
    #[must_use]
    pub fn allows_reuse(&self) -> bool {
        !self.denied && self.expires.is_none_or(|expiry| Instant::now() < expiry)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PresignedRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
}
impl PresignedRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: "GET".into(),
            url: url.into(),
            headers: vec![],
        }
    }
    pub fn put(url: impl Into<String>) -> Self {
        Self {
            method: "PUT".into(),
            url: url.into(),
            headers: vec![],
        }
    }
}
impl fmt::Debug for PresignedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PresignedRequest")
            .field("method", &self.method)
            .field("url", &"[redacted]")
            .field("header_count", &self.headers.len())
            .finish()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeResponse {
    pub bytes: Bytes,
    pub content_range: Range<u64>,
    pub total_size: u64,
}
#[derive(Debug)]
pub struct HttpStatusError {
    pub status: u16,
}
impl fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP {}", self.status)?;
        match self.status {
            200 => f.write_str(" (server ignored the Range request; ranged reads are required)"),
            401 | 403 => f.write_str(" (authorization failed or the signed URL expired)"),
            404 => f.write_str(" (object not found)"),
            412 => f.write_str(" (precondition failed; the object changed)"),
            416 => f.write_str(" (range not satisfiable; the object may be empty or truncated)"),
            _ => Ok(()),
        }
    }
}
/// The remote object was replaced or modified after this source pinned its
/// first-observed strong `ETag` validator. Carried inside an
/// `InvalidData` [`std::io::Error`]; test with [`is_source_changed`].
#[derive(Debug)]
pub struct SourceChanged;
impl fmt::Display for SourceChanged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP object changed during session")
    }
}
impl std::error::Error for SourceChanged {}
fn source_changed() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, SourceChanged)
}
/// True when `error` reports a pinned-snapshot violation.
#[must_use]
pub fn is_source_changed(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<SourceChanged>().is_some())
}
/// Response validators used to pin one immutable object snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RangeValidator {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// Response policy governing reuse of decoded chunks.
    pub cache_policy: CachePolicy,
}
impl RangeValidator {
    fn pin(self) -> std::io::Result<Self> {
        if !self.etag.as_deref().is_some_and(strong_etag) {
            return Err(invalid("HTTP archive requires a strong ETag"));
        }
        Ok(self)
    }
    /// Header that makes the server refuse (412) or answer 200 on change.
    fn condition(&self) -> Option<(&'static str, &str)> {
        match (&self.etag, &self.last_modified) {
            // If-Match requires strong comparison; weak tags never match.
            (Some(tag), _) if strong_etag(tag) => Some(("If-Match", tag)),
            _ => None,
        }
    }
    fn matches(&self, seen: &Self) -> bool {
        self.etag == seen.etag
    }
}
fn strong_etag(tag: &str) -> bool {
    tag.len() >= 2
        && tag.starts_with('"')
        && tag.ends_with('"')
        && tag.as_bytes()[1..tag.len() - 1]
            .iter()
            .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte) || *byte >= 0x80)
}
impl std::error::Error for HttpStatusError {}
fn invalid(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
fn redacted(error: reqwest::Error) -> std::io::Error {
    std::io::Error::other(error.without_url())
}
fn status_error(status: reqwest::StatusCode) -> std::io::Error {
    std::io::Error::other(HttpStatusError {
        status: status.as_u16(),
    })
}
fn refreshable(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|error| error.downcast_ref::<HttpStatusError>())
        .is_some_and(|error| matches!(error.status, 401 | 403))
}

/// A conditional request answered 412 (If-Match) or 200 (If-Range ignored the
/// range because the validator no longer matches) means the object changed.
fn map_conditional(error: std::io::Error, condition: Option<(&str, &str)>) -> std::io::Error {
    let status = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<HttpStatusError>())
        .map(|inner| inner.status);
    match (status, condition) {
        (Some(412), Some(_)) | (Some(200), Some(("If-Range", _))) => source_changed(),
        _ => error,
    }
}
fn provider_failure(error: std::io::Error) -> std::io::Error {
    // Provider implementations may include signed URLs or credentials in
    // their error text. Preserve cancellation-like kinds for callers while
    // replacing the message and all other implementation details.
    match error.kind() {
        std::io::ErrorKind::Interrupted | std::io::ErrorKind::TimedOut => {
            std::io::Error::new(error.kind(), "zstar: presigned provider failed")
        }
        _ => std::io::Error::other("zstar: presigned provider failed"),
    }
}
#[async_trait::async_trait]
pub trait HttpTransport: Send + Sync {
    async fn get_range(
        &self,
        request: PresignedRequest,
        range: Range<u64>,
    ) -> std::io::Result<RangeResponse>;
    async fn put(&self, request: PresignedRequest, body: Bytes) -> std::io::Result<()>;
    /// Like [`get_range`](Self::get_range) but also reports the response's
    /// validators. Archive sources require a strong `ETag`; the default reports
    /// none and therefore cannot be used by an indexed HTTP archive source.
    async fn get_range_validated(
        &self,
        request: PresignedRequest,
        range: Range<u64>,
    ) -> std::io::Result<(RangeResponse, RangeValidator)> {
        Ok((
            self.get_range(request, range).await?,
            RangeValidator::default(),
        ))
    }
}
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
    max_response_bytes: usize,
}
impl ReqwestTransport {
    pub fn new() -> std::io::Result<Self> {
        let client = reqwest::Client::builder()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .http2_adaptive_window(true)
            .pool_max_idle_per_host(32)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(redacted)?;
        Ok(Self {
            client,
            max_response_bytes: 256 * 1024 * 1024 + 8,
        })
    }
    /// The supplied client must disable automatic response decompression so
    /// range validation sees the stored representation and its headers.
    #[must_use]
    pub fn with_client(client: reqwest::Client, max_response_bytes: usize) -> Self {
        Self {
            client,
            max_response_bytes,
        }
    }
}
#[async_trait::async_trait]
impl HttpTransport for ReqwestTransport {
    async fn get_range(
        &self,
        request: PresignedRequest,
        range: Range<u64>,
    ) -> std::io::Result<RangeResponse> {
        self.get_range_validated(request, range)
            .await
            .map(|(response, _)| response)
    }
    async fn get_range_validated(
        &self,
        request: PresignedRequest,
        range: Range<u64>,
    ) -> std::io::Result<(RangeResponse, RangeValidator)> {
        if range.start >= range.end || request.method != "GET" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "nonempty GET range required",
            ));
        }
        let count = usize::try_from(range.end - range.start)
            .map_err(|_| invalid("range exceeds address space"))?;
        if count > self.max_response_bytes {
            return Err(invalid("response exceeds local budget"));
        }
        let request_control = request
            .headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("cache-control"))
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let request_policy = CachePolicy::from_cache_control(&request_control);
        let has_cache_control = request
            .headers
            .iter()
            .any(|(key, _)| key.eq_ignore_ascii_case("cache-control"));
        let mut req = self.client.get(&request.url);
        for (key, value) in request.headers {
            req = req.header(key, value);
        }
        req = req
            .header(
                reqwest::header::RANGE,
                format!("bytes={}-{}", range.start, range.end - 1),
            )
            .header(reqwest::header::ACCEPT_ENCODING, "identity");
        if !has_cache_control {
            req = req.header(reqwest::header::CACHE_CONTROL, "no-transform");
        }
        let requested = Instant::now();
        let mut response = req.send().await.map_err(redacted)?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(status_error(response.status()));
        }
        if response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(invalid("encoded range response"));
        }
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(String::from)
        };
        let mut validator = RangeValidator {
            etag: header(reqwest::header::ETAG),
            last_modified: header(reqwest::header::LAST_MODIFIED),
            cache_policy: CachePolicy::from_response(response.headers(), requested),
        };
        validator.cache_policy.denied |= request_policy.denied;
        let (content_range, total_size) = response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range)
            .ok_or_else(|| invalid("invalid Content-Range"))?;
        if content_range != range {
            return Err(invalid("mismatched Content-Range"));
        }
        if response
            .content_length()
            .is_some_and(|length| length != count as u64)
        {
            return Err(invalid("response length mismatch"));
        }
        let mut output = Vec::new();
        output
            .try_reserve_exact(count)
            .map_err(|_| invalid("response allocation budget"))?;
        while let Some(chunk) = response.chunk().await.map_err(redacted)? {
            if chunk.len() > count - output.len() {
                return Err(invalid("oversized range response"));
            }
            output.extend_from_slice(&chunk);
        }
        if output.len() != count {
            return Err(invalid("short range response"));
        }
        Ok((
            RangeResponse {
                bytes: Bytes::from(output),
                content_range,
                total_size,
            },
            validator,
        ))
    }
    async fn put(&self, request: PresignedRequest, body: Bytes) -> std::io::Result<()> {
        if request.method != "PUT" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "PUT request required",
            ));
        }
        let length = body.len();
        let is_archive = body
            .get(length.saturating_sub(crate::FOOTER_SIZE)..)
            .and_then(|bytes| crate::parse_footer(bytes).ok())
            .is_some_and(|footer| {
                footer.toc_offset.checked_add(footer.toc_encoded_len)
                    == (length as u64).checked_sub(crate::FOOTER_SIZE as u64)
            });
        let mut req = self.client.put(&request.url).body(body);
        for (key, value) in request.headers {
            req = req.header(key, value);
        }
        // Explicit framing also covers empty signed PUTs; do not rely on the
        // client's empty-body optimization to supply Content-Length: 0.
        req = req.header(reqwest::header::CONTENT_LENGTH, length);
        let mut request = req.build().map_err(redacted)?;
        let content_type = if is_archive {
            ZSTD_CONTENT_TYPE
        } else {
            artifact_content_type(request.url().path())
        };
        request
            .headers_mut()
            .entry(reqwest::header::CONTENT_TYPE)
            .or_insert(reqwest::header::HeaderValue::from_static(content_type));
        request
            .headers_mut()
            .entry(reqwest::header::CACHE_CONTROL)
            .or_insert(reqwest::header::HeaderValue::from_static(
                MUTABLE_CACHE_CONTROL,
            ));
        let response = self.client.execute(request).await.map_err(redacted)?;
        if !response.status().is_success() {
            return Err(status_error(response.status()));
        }
        Ok(())
    }
}
fn parse_content_range(value: &str) -> Option<(Range<u64>, u64)> {
    fn decimal(value: &str) -> Option<u64> {
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        value.parse().ok()
    }
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let (start, end, total) = (decimal(start)?, decimal(end)?, decimal(total)?);
    if start > end || end >= total {
        return None;
    }
    Some((start..end.checked_add(1)?, total))
}

#[derive(Clone)]
pub struct ExpiringRequest {
    pub request: PresignedRequest,
    pub expires_at: Option<Instant>,
}
/// Resolve URLs without putting storage credentials into the archive format.
/// `force_refresh` is true only after expiry or a 401/403 response.
#[async_trait::async_trait]
pub trait RequestProvider: Send + Sync {
    async fn request(&self, method: &str, force_refresh: bool) -> std::io::Result<ExpiringRequest>;
    /// Whether a forced refresh can yield different credentials. A provider
    /// returning false avoids a pointless second request after 401/403.
    fn can_refresh(&self) -> bool {
        true
    }
}
struct FixedRequest(PresignedRequest);
#[async_trait::async_trait]
impl RequestProvider for FixedRequest {
    async fn request(&self, method: &str, _: bool) -> std::io::Result<ExpiringRequest> {
        if self.0.method != method {
            return Err(invalid("provider does not support requested method"));
        }
        Ok(ExpiringRequest {
            request: self.0.clone(),
            expires_at: None,
        })
    }
    fn can_refresh(&self) -> bool {
        false
    }
}
#[derive(Default)]
struct State {
    initialized: bool,
    size: Option<u64>,
    pin: Option<RangeValidator>,
}
/// Pooled GET source with one-byte size discovery, expiry refresh, and one
/// bounded authentication retry. A GET URL never needs HEAD authorization.
///
/// A strong `ETag` is required and pinned. Later ranges send `If-Match`; a 412,
/// missing/differing `ETag`, or changed size fails. Weak tags and Last-Modified
/// alone cannot establish the exact immutable snapshot required by the reader.
pub struct HttpRangeSource {
    transport: Arc<dyn HttpTransport>,
    provider: Arc<dyn RequestProvider>,
    state: Mutex<State>,
    cache_policy: std::sync::Mutex<CachePolicy>,
}
impl HttpRangeSource {
    pub fn new(transport: Arc<dyn HttpTransport>, request: PresignedRequest) -> Self {
        Self::with_provider(transport, Arc::new(FixedRequest(request)))
    }
    pub fn with_provider(
        transport: Arc<dyn HttpTransport>,
        provider: Arc<dyn RequestProvider>,
    ) -> Self {
        Self {
            transport,
            provider,
            state: Mutex::new(State::default()),
            cache_policy: std::sync::Mutex::new(CachePolicy::default()),
        }
    }
    async fn request(&self, method: &str, force: bool) -> std::io::Result<PresignedRequest> {
        let mut value = self
            .provider
            .request(method, force)
            .await
            .map_err(provider_failure)?;
        if value
            .expires_at
            .is_some_and(|expiry| expiry <= Instant::now() + Duration::from_secs(5))
        {
            value = self
                .provider
                .request(method, true)
                .await
                .map_err(provider_failure)?;
            if value
                .expires_at
                .is_some_and(|expiry| expiry <= Instant::now())
            {
                return Err(invalid("provider returned expired URL"));
            }
        }
        if value.request.method != method {
            return Err(invalid("provider returned wrong method"));
        }
        Ok(value.request)
    }
    async fn fetch_raw(
        &self,
        range: Range<u64>,
        pin: Option<&RangeValidator>,
    ) -> std::io::Result<(RangeResponse, RangeValidator)> {
        let refreshable_provider = self.provider.can_refresh();
        for attempt in 0..2 {
            let mut request = self.request("GET", attempt != 0).await?;
            let condition = pin.and_then(RangeValidator::condition);
            if let Some((name, value)) = condition {
                request
                    .headers
                    .retain(|(key, _)| !key.eq_ignore_ascii_case(name));
                request.headers.push((name.into(), value.into()));
            }
            match self
                .transport
                .get_range_validated(request, range.clone())
                .await
            {
                Err(error) if attempt == 0 && refreshable_provider && refreshable(&error) => {}
                Err(error) => return Err(map_conditional(error, condition)),
                Ok(result) => return Ok(result),
            }
        }
        unreachable!()
    }
    async fn fetch(&self, range: Range<u64>) -> std::io::Result<RangeResponse> {
        let mut state = self.state.lock().await;
        if state.initialized {
            let pin = state.pin.clone();
            drop(state);
            let (response, seen) = self.fetch_raw(range, pin.as_ref()).await?;
            if pin.as_ref().is_some_and(|pin| !pin.matches(&seen)) {
                return Err(source_changed());
            }
            let state = self.state.lock().await;
            if state.size.is_some_and(|size| size != response.total_size) {
                return Err(source_changed());
            }
            self.cache_policy.lock().unwrap().observe(seen.cache_policy);
            return Ok(response);
        }
        // Hold the lock so concurrent first reads cannot pin different versions.
        let (response, seen) = self.fetch_raw(range, None).await?;
        self.cache_policy
            .lock()
            .unwrap()
            .observe(seen.cache_policy.clone());
        let pin = seen.pin()?;
        state.size = Some(response.total_size);
        state.pin = Some(pin);
        state.initialized = true;
        Ok(response)
    }
    pub async fn put(&self, body: Bytes) -> std::io::Result<()> {
        for attempt in 0..2 {
            let request = self.request("PUT", attempt != 0).await?;
            match self.transport.put(request, body.clone()).await {
                Err(error) if attempt == 0 && refreshable(&error) => {}
                result => return result,
            }
        }
        unreachable!()
    }
}
#[async_trait::async_trait]
impl crate::AsyncRangeReader for HttpRangeSource {
    fn cache_allowed(&self) -> bool {
        self.cache_policy.lock().unwrap().allows_reuse()
    }
    async fn size(&self) -> crate::Result<u64> {
        if let Some(size) = self.state.lock().await.size {
            return Ok(size);
        }
        Ok(self.fetch(0..1).await?.total_size)
    }
    async fn read_range(&self, range: Range<u64>) -> crate::Result<Bytes> {
        if range.start > range.end {
            return Err(crate::Error::Range);
        }
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        Ok(self.fetch(range).await?.bytes)
    }
}

/// Bounded resolver for complete encoded external objects over HTTP ranges.
/// The URL provider maps a content address to a signed GET request; URLs and
/// credentials are never retained in archive metadata.
pub struct HttpEncodedObjectResolver {
    transport: Arc<dyn HttpTransport>,
    provider: Arc<dyn Fn([u8; 32]) -> Arc<dyn RequestProvider> + Send + Sync>,
    permits: Semaphore,
    max_object_bytes: u64,
    policies: std::sync::Mutex<lru::LruCache<[u8; 32], CachePolicy>>,
}
impl HttpEncodedObjectResolver {
    pub fn new(
        transport: Arc<dyn HttpTransport>,
        provider: impl Fn([u8; 32]) -> Arc<dyn RequestProvider> + Send + Sync + 'static,
        concurrency: std::num::NonZeroUsize,
        max_object_bytes: u64,
    ) -> std::io::Result<Self> {
        if max_object_bytes == 0 || max_object_bytes > 17 << 20 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "encoded object limit must be between 1 and 17 MiB",
            ));
        }
        Ok(Self {
            transport,
            provider: Arc::new(provider),
            permits: Semaphore::new(concurrency.get()),
            max_object_bytes,
            policies: std::sync::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(1024).unwrap(),
            )),
        })
    }
}
#[async_trait::async_trait]
impl crate::ObjectResolver for HttpEncodedObjectResolver {
    fn cache_allowed(&self, address: [u8; 32]) -> bool {
        self.policies
            .lock()
            .unwrap()
            .get(&address)
            .is_some_and(CachePolicy::allows_reuse)
    }
    async fn get_encoded(
        &self,
        address: [u8; 32],
        _hint: u64,
        max_bytes: u64,
    ) -> crate::Result<Bytes> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| crate::Error::Budget("HTTP object resolver closed"))?;
        let limit = max_bytes.min(self.max_object_bytes);
        if limit == 0 {
            return Err(crate::Error::Budget("encoded object size"));
        }
        let source =
            HttpRangeSource::with_provider(self.transport.clone(), (self.provider)(address));
        let size = match source.size().await {
            Err(crate::Error::Io(error))
                if error
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<HttpStatusError>())
                    .is_some_and(|status| status.status == 404) =>
            {
                return Err(crate::Error::MissingObject(address));
            }
            result => result?,
        };
        if size == 0 || size > limit {
            return Err(crate::Error::Budget("encoded object size"));
        }
        let bytes = source.read_range(0..size).await?;
        let policy = source.cache_policy.lock().unwrap().clone();
        let mut policies = self.policies.lock().unwrap();
        if let Some(previous) = policies.get_mut(&address) {
            previous.observe(policy);
        } else {
            policies.put(address, policy);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_cache_respects_directives_age_and_expiry() {
        for control in [
            "no-store",
            "No-Cache=\"Set-Cookie\"",
            "max-age=0",
            "max-age=oops",
            "max-age=60, max-age=120",
        ] {
            assert!(
                !CachePolicy::from_headers(Some(control), None).allows_reuse(),
                "{control}"
            );
        }
        assert!(
            CachePolicy::from_headers(
                Some("private, max-age=60, immutable, s-maxage=0"),
                Some("5")
            )
            .allows_reuse()
        );
        assert!(!CachePolicy::from_headers(Some("max-age=60"), Some("60")).allows_reuse());
        let mut policy = CachePolicy::from_headers(Some("max-age=60"), None);
        policy.expires = Some(Instant::now());
        assert!(!policy.allows_reuse());
        let mut headers = reqwest::header::HeaderMap::new();
        headers.append(
            reqwest::header::CACHE_CONTROL,
            "max-age=60".parse().unwrap(),
        );
        headers.append(reqwest::header::CACHE_CONTROL, "no-store".parse().unwrap());
        assert!(!CachePolicy::from_response(&headers, Instant::now()).allows_reuse());
        headers.remove(reqwest::header::CACHE_CONTROL);
        headers.insert(
            reqwest::header::DATE,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::EXPIRES,
            "Wed, 21 Oct 2015 07:29:00 GMT".parse().unwrap(),
        );
        assert!(!CachePolicy::from_response(&headers, Instant::now()).allows_reuse());
    }
    #[test]
    fn content_range() {
        assert_eq!(parse_content_range("bytes 2-4/9"), Some((2..5, 9)));
        for value in [
            "bytes 4-2/9",
            "bytes 2-4/4",
            "bytes 2-4/*",
            "bytes 2-4/+9",
            "bytes 2-4/9junk",
        ] {
            assert!(parse_content_range(value).is_none());
        }
    }
    #[test]
    fn request_debug_redacts_secrets() {
        let request = PresignedRequest {
            method: "GET".into(),
            url: "http://secret?sig=credential".into(),
            headers: vec![("Authorization".into(), "secret".into())],
        };
        assert!(!format!("{request:?}").contains("secret"));
        assert!(!format!("{request:?}").contains("credential"));
    }
}
