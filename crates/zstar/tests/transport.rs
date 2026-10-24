use bytes::Bytes;
use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zstar::http::{
    ExpiringRequest, HttpEncodedObjectResolver, HttpRangeSource, HttpTransport, PresignedRequest,
    RequestProvider, ReqwestTransport,
};
use zstar::{Archive, AsyncRangeReader, Builder, ObjectResolver};

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(mut reply: impl FnMut(&str) -> Vec<u8> + Send + 'static) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/archive?signature=secret",
        listener.local_addr().unwrap()
    );
    let requests = Arc::new(Mutex::new(vec![]));
    let calls = requests.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = vec![];
            loop {
                let mut byte = [0];
                if socket.read(&mut byte).await.unwrap_or(0) == 0 {
                    break;
                }
                input.push(byte[0]);
                if input.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(input.len() < 16384);
            }
            let input = String::from_utf8(input).unwrap();
            calls.lock().unwrap().push(input.clone());
            let response = reply(&input);
            let _ = socket.write_all(&response).await;
        }
    });
    Server {
        url,
        requests,
        task,
    }
}
fn response(status: u16, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut out =
        format!("HTTP/1.1 {status} Test\r\nConnection: close\r\n{headers}\r\n").into_bytes();
    out.extend_from_slice(body);
    out
}
fn ranged(request: &str, data: &[u8]) -> Vec<u8> {
    assert!(request.starts_with("GET "));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("accept-encoding: identity")
    );
    let range = request
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("range: bytes=")
                .map(String::from)
        })
        .unwrap();
    let (start, end) = range.split_once('-').unwrap();
    let (start, end) = (
        start.parse::<usize>().unwrap(),
        end.parse::<usize>().unwrap(),
    );
    response(
        206,
        &format!(
            "ETag: \"test-snapshot\"\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\n",
            data.len(),
            end - start + 1
        ),
        &data[start..=end],
    )
}

#[tokio::test]
async fn presigned_get_opens_archive_without_head_and_cache_avoids_payload_requests() {
    let mut writer = Builder::new(Vec::new());
    writer.append_bytes("hello", b"hello world").await.unwrap();
    let data = writer.finish().await.unwrap();
    let server = server(move |request| ranged(request, &data)).await;
    let source = Arc::new(HttpRangeSource::new(
        Arc::new(ReqwestTransport::new().unwrap()),
        PresignedRequest::get(&server.url),
    ));
    let reader = Archive::open(source).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 3);
    assert_eq!(
        reader.read_range("hello", 6..11).await.unwrap(),
        b"world"[..]
    );
    assert_eq!(
        reader.read_range("hello", 0..5).await.unwrap(),
        b"hello"[..]
    );
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn http_encoded_object_resolver_fetches_bounded_complete_object() {
    let encoded = zstar::encode_frame(b"external object bytes", 3).unwrap();
    let body = encoded.clone();
    let server = server(move |request| ranged(request, &body)).await;
    let url = server.url.clone();
    let resolver = HttpEncodedObjectResolver::new(
        Arc::new(ReqwestTransport::new().unwrap()),
        move |_| {
            Arc::new(FixedProvider(PresignedRequest::get(url.clone()))) as Arc<dyn RequestProvider>
        },
        NonZeroUsize::new(2).unwrap(),
        1 << 20,
    )
    .unwrap();
    let address = [7; 32];
    assert_eq!(
        resolver
            .get_encoded(address, u64::MAX, encoded.len() as u64)
            .await
            .unwrap(),
        Bytes::from(encoded.clone())
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2); // one byte for size, then the full object
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains("range: bytes=0-0")
    );
    assert!(
        requests[1]
            .to_ascii_lowercase()
            .contains(&format!("range: bytes=0-{}", encoded.len() - 1))
    );
    assert!(matches!(
        resolver
            .get_encoded(address, 1, (encoded.len() - 1) as u64)
            .await,
        Err(zstar::Error::Budget("encoded object size"))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 3); // only the size probe
    assert!(matches!(
        resolver.get_encoded(address, 1, 0).await,
        Err(zstar::Error::Budget("encoded object size"))
    ));
    assert_eq!(server.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn http_encoded_object_resolver_maps_missing_and_rejects_invalid_limits() {
    let server = server(|request| {
        assert!(request.starts_with("GET "));
        response(404, "Content-Length: 0\r\n", &[])
    })
    .await;
    let url = server.url.clone();
    let resolver = HttpEncodedObjectResolver::new(
        Arc::new(ReqwestTransport::new().unwrap()),
        move |_| {
            Arc::new(FixedProvider(PresignedRequest::get(url.clone()))) as Arc<dyn RequestProvider>
        },
        NonZeroUsize::new(1).unwrap(),
        1 << 20,
    )
    .unwrap();
    let error = resolver.get_encoded([9; 32], 0, 1 << 20).await.unwrap_err();
    assert!(matches!(error, zstar::Error::MissingObject(digest) if digest == [9; 32]));
    assert!(
        HttpEncodedObjectResolver::new(
            Arc::new(ReqwestTransport::new().unwrap()),
            |_| Arc::new(FixedProvider(PresignedRequest::get("http://unused")))
                as Arc<dyn RequestProvider>,
            NonZeroUsize::new(1).unwrap(),
            0,
        )
        .is_err()
    );
}

struct ChangingSizeTransport(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl HttpTransport for ChangingSizeTransport {
    async fn get_range(
        &self,
        _: PresignedRequest,
        range: std::ops::Range<u64>,
    ) -> std::io::Result<zstar::http::RangeResponse> {
        let call = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let total_size = if call == 0 { 4 } else { 5 };
        Ok(zstar::http::RangeResponse {
            bytes: Bytes::from(vec![0; (range.end - range.start) as usize]),
            content_range: range,
            total_size,
        })
    }
    async fn get_range_validated(
        &self,
        request: PresignedRequest,
        range: std::ops::Range<u64>,
    ) -> std::io::Result<(zstar::http::RangeResponse, zstar::http::RangeValidator)> {
        Ok((
            self.get_range(request, range).await?,
            zstar::http::RangeValidator {
                etag: Some("\"test-snapshot\"".into()),
                last_modified: None,
            },
        ))
    }
    async fn put(&self, _: PresignedRequest, _: Bytes) -> std::io::Result<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn http_encoded_object_resolver_rejects_size_change_between_probe_and_fetch() {
    let resolver = HttpEncodedObjectResolver::new(
        Arc::new(ChangingSizeTransport(std::sync::atomic::AtomicUsize::new(
            0,
        ))),
        |_| {
            Arc::new(FixedProvider(PresignedRequest::get("http://unused")))
                as Arc<dyn RequestProvider>
        },
        NonZeroUsize::new(1).unwrap(),
        1 << 20,
    )
    .unwrap();
    let error = resolver.get_encoded([3; 32], 4, 10).await.unwrap_err();
    assert!(matches!(error, zstar::Error::Io(error) if zstar::http::is_source_changed(&error)));
}

struct CancelFirstTransport {
    entered: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl HttpTransport for CancelFirstTransport {
    async fn get_range(
        &self,
        _: PresignedRequest,
        range: std::ops::Range<u64>,
    ) -> std::io::Result<zstar::http::RangeResponse> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            std::future::pending().await
        } else {
            Ok(zstar::http::RangeResponse {
                bytes: Bytes::from(vec![1; (range.end - range.start) as usize]),
                content_range: range,
                total_size: 1,
            })
        }
    }
    async fn get_range_validated(
        &self,
        request: PresignedRequest,
        range: std::ops::Range<u64>,
    ) -> std::io::Result<(zstar::http::RangeResponse, zstar::http::RangeValidator)> {
        Ok((
            self.get_range(request, range).await?,
            zstar::http::RangeValidator {
                etag: Some("\"test-snapshot\"".into()),
                last_modified: None,
            },
        ))
    }
    async fn put(&self, _: PresignedRequest, _: Bytes) -> std::io::Result<()> {
        unreachable!()
    }
}

#[tokio::test]
async fn cancelled_http_object_fetch_releases_concurrency_permit() {
    let transport = Arc::new(CancelFirstTransport {
        entered: tokio::sync::Notify::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let provider = |_| {
        Arc::new(FixedProvider(PresignedRequest::get("http://unused"))) as Arc<dyn RequestProvider>
    };
    let resolver = Arc::new(
        HttpEncodedObjectResolver::new(
            transport.clone(),
            provider,
            NonZeroUsize::new(1).unwrap(),
            1 << 20,
        )
        .unwrap(),
    );
    let cancelled = {
        let resolver = resolver.clone();
        tokio::spawn(async move { resolver.get_encoded([1; 32], 1, 10).await })
    };
    transport.entered.notified().await;
    cancelled.abort();
    let _ = cancelled.await;
    assert_eq!(
        resolver.get_encoded([2; 32], 1, 10).await.unwrap(),
        Bytes::from_static(&[1])
    );
}

struct FixedProvider(PresignedRequest);
#[async_trait::async_trait]
impl RequestProvider for FixedProvider {
    async fn request(&self, method: &str, _: bool) -> std::io::Result<ExpiringRequest> {
        assert_eq!(method, "GET");
        Ok(ExpiringRequest {
            request: self.0.clone(),
            expires_at: None,
        })
    }
}
#[tokio::test]
async fn rejects_ignored_wrong_short_oversized_or_encoded_ranges() {
    let cases = [
        response(200, "Content-Length: 2\r\n", b"ab"),
        response(
            206,
            "Content-Range: bytes 1-2/4\r\nContent-Length: 2\r\n",
            b"ab",
        ),
        response(206, "Content-Range: bytes 0-1/4\r\n", b"a"),
        response(206, "Content-Range: bytes 0-1/4\r\n", b"abc"),
        response(
            206,
            "Content-Range: bytes 0-1/4\r\nContent-Length: 2\r\nContent-Encoding: gzip\r\n",
            b"ab",
        ),
        response(
            206,
            "Content-Range: bytes 0-1/*\r\nContent-Length: 2\r\n",
            b"ab",
        ),
    ];
    for reply in cases {
        let server = server(move |_| reply.clone()).await;
        let error = ReqwestTransport::new()
            .unwrap()
            .get_range(PresignedRequest::get(&server.url), 0..2)
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("signature=secret"));
    }
}
struct Provider {
    url: String,
    calls: Mutex<Vec<bool>>,
    expires: bool,
}

struct FailingProvider;
#[async_trait::async_trait]
impl RequestProvider for FailingProvider {
    async fn request(&self, _: &str, _: bool) -> std::io::Result<ExpiringRequest> {
        Err(std::io::Error::other(
            "signed URL https://private.invalid/?signature=secret",
        ))
    }
}
#[async_trait::async_trait]
impl RequestProvider for Provider {
    async fn request(&self, method: &str, refresh: bool) -> std::io::Result<ExpiringRequest> {
        assert_eq!(method, "GET");
        self.calls.lock().unwrap().push(refresh);
        Ok(ExpiringRequest {
            request: PresignedRequest::get(&self.url),
            expires_at: Some(
                Instant::now()
                    + if self.expires && !refresh {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(60)
                    },
            ),
        })
    }
}
#[tokio::test]
async fn refreshes_once_on_auth_failure_but_not_server_failure() {
    for (status, expected) in [
        (403, vec![false, true]),
        (401, vec![false, true]),
        (503, vec![false]),
    ] {
        let mut statuses = VecDeque::from([status, 206]);
        let server = server(move |request| match statuses.pop_front().unwrap() {
            206 => ranged(request, b"abcd"),
            status => response(status, "Content-Length: 0\r\n", b""),
        })
        .await;
        let provider = Arc::new(Provider {
            url: server.url.clone(),
            calls: Mutex::new(vec![]),
            expires: false,
        });
        let source = HttpRangeSource::with_provider(
            Arc::new(ReqwestTransport::new().unwrap()),
            provider.clone(),
        );
        assert_eq!(source.size().await.is_ok(), status != 503);
        assert_eq!(*provider.calls.lock().unwrap(), expected);
    }
    let server = server(|_| response(403, "Content-Length: 0\r\n", b"")).await;
    let provider = Arc::new(Provider {
        url: server.url.clone(),
        calls: Mutex::new(vec![]),
        expires: false,
    });
    let source = HttpRangeSource::with_provider(
        Arc::new(ReqwestTransport::new().unwrap()),
        provider.clone(),
    );
    assert!(source.size().await.is_err());
    assert_eq!(*provider.calls.lock().unwrap(), [false, true]);
}

#[tokio::test]
async fn provider_failures_redact_signed_url_details() {
    let source = HttpRangeSource::with_provider(
        Arc::new(ReqwestTransport::new().unwrap()),
        Arc::new(FailingProvider),
    );
    let error = source.size().await.unwrap_err();
    let text = format!("{error:?}");
    assert!(text.contains("presigned provider failed"));
    assert!(!text.contains("private.invalid"));
    assert!(!text.contains("secret"));
}
#[tokio::test]
async fn expiry_refresh_and_object_size_consistency() {
    let mut calls = 0;
    let server = server(move |request| {
        calls += 1;
        ranged(request, if calls == 1 { b"abcd" } else { b"abcde" })
    })
    .await;
    let provider = Arc::new(Provider {
        url: server.url.clone(),
        calls: Mutex::new(vec![]),
        expires: true,
    });
    let source = HttpRangeSource::with_provider(
        Arc::new(ReqwestTransport::new().unwrap()),
        provider.clone(),
    );
    assert_eq!(source.size().await.unwrap(), 4);
    assert_eq!(*provider.calls.lock().unwrap(), [false, true]);
    assert!(source.read_range(0..2).await.is_err());
}
#[tokio::test]
async fn rejects_local_budget_before_io_and_redacts_connection_error() {
    let server = server(|_| panic!("invalid requests must not reach server")).await;
    let transport = ReqwestTransport::with_client(reqwest::Client::new(), 1);
    assert!(
        transport
            .get_range(PresignedRequest::get(&server.url), 0..2)
            .await
            .is_err()
    );
    assert!(
        transport
            .get_range(PresignedRequest::put(&server.url), 0..1)
            .await
            .is_err()
    );
    assert!(
        transport
            .put(PresignedRequest::get(&server.url), Bytes::new())
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
    let error = transport
        .get_range(
            PresignedRequest::get("invalid://private?secret=token"),
            0..1,
        )
        .await
        .unwrap_err();
    assert!(!format!("{error:?}").contains("token"));
    assert!(!format!("{error:?}").contains("private"));
}
#[tokio::test]
async fn dropping_range_future_closes_pending_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/?secret", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        socket
            .write_all(
                b"HTTP/1.1 206 OK\r\nContent-Range: bytes 0-1/2\r\nContent-Length: 2\r\n\r\n",
            )
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let transport = ReqwestTransport::new().unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            transport.get_range(PresignedRequest::get(url), 0..2)
        )
        .await
        .is_err()
    );
    task.await.unwrap();
}
