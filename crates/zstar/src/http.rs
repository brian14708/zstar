//! Pooled async HTTP ranges and presigned request sources. Request Debug and
//! transport errors redact URLs and header values. Futures cancel by dropping.

#![doc = include_str!("../docs/http.md")]

use crate::AsyncRangeReader;
use bytes::Bytes;
use std::{
    fmt,
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};

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
        let mut response = req.send().await.map_err(redacted)?;
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(status_error(response.status()));
        }
        if response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .is_some_and(|value| value != "identity")
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
        let validator = RangeValidator {
            etag: header(reqwest::header::ETAG),
            last_modified: header(reqwest::header::LAST_MODIFIED),
        };
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
        let mut req = self.client.put(&request.url).body(body);
        for (key, value) in request.headers {
            req = req.header(key, value);
        }
        // Explicit framing also covers empty signed PUTs; do not rely on the
        // client's empty-body optimization to supply Content-Length: 0.
        req = req.header(reqwest::header::CONTENT_LENGTH, length);
        let response = req.send().await.map_err(redacted)?;
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
            return Ok(response);
        }
        // Hold the lock so concurrent first reads cannot pin different versions.
        let (response, seen) = self.fetch_raw(range, None).await?;
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
        })
    }
}
#[async_trait::async_trait]
impl crate::ObjectResolver for HttpEncodedObjectResolver {
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
        source.read_range(0..size).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
