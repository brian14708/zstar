//! Snapshot pinning and error behavior for the pooled HTTP range source.
use std::{
    fmt::Write as _,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use zstar::http::{
    HttpEncodedObjectResolver, HttpRangeSource, PresignedRequest, RequestProvider,
    ReqwestTransport, is_source_changed,
};
use zstar::{AsyncRangeReader, ObjectResolver};

/// `(etag, last_modified, body)`; swapped by the test to simulate replacement.
type Object = Arc<Mutex<(String, String, Vec<u8>)>>;

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
fn header(request: &str, name: &str) -> Option<String> {
    request.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim().into())
    })
}
async fn server(object: Object, status: Option<u16>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/o?sig=secret", listener.local_addr().unwrap());
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
            }
            let input = String::from_utf8(input).unwrap();
            calls.lock().unwrap().push(input.clone());
            let (etag, modified, data) = object.lock().unwrap().clone();
            let reply = |code: u16, extra: &str, body: &[u8]| {
                let mut out = format!(
                    "HTTP/1.1 {code} T\r\nConnection: close\r\nContent-Length: {}\r\n{extra}\r\n",
                    body.len()
                )
                .into_bytes();
                out.extend_from_slice(body);
                out
            };
            let out = if let Some(code) = status {
                reply(code, "", b"")
            } else if header(&input, "if-match").is_some_and(|m| m != etag) {
                reply(412, "", b"")
            } else if header(&input, "if-range").is_some_and(|m| m != modified) {
                reply(200, "", &data)
            } else {
                let range = header(&input, "range").unwrap();
                let (a, b) = range
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let (a, b) = (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap());
                let mut extra = format!("Content-Range: bytes {a}-{b}/{}\r\n", data.len());
                if !etag.is_empty() {
                    let _ = write!(extra, "ETag: {etag}\r\n");
                }
                if !modified.is_empty() {
                    let _ = write!(extra, "Last-Modified: {modified}\r\n");
                }
                reply(206, &extra, &data[a..=b])
            };
            let _ = socket.write_all(&out).await;
        }
    });
    Server {
        url,
        requests,
        task,
    }
}
fn object(etag: &str, modified: &str, data: &[u8]) -> Object {
    Arc::new(Mutex::new((etag.into(), modified.into(), data.to_vec())))
}
fn source(url: &str) -> HttpRangeSource {
    HttpRangeSource::new(
        Arc::new(ReqwestTransport::new().unwrap()),
        PresignedRequest::get(url),
    )
}
fn changed(error: zstar::Error) -> bool {
    matches!(error, zstar::Error::Io(e) if is_source_changed(&e))
}

#[tokio::test]
async fn same_size_replacement_with_etag_is_detected() {
    let o = object("\"v1\"", "", b"aaaaaaaa");
    let server = server(o.clone(), None).await;
    let source = source(&server.url);
    assert_eq!(source.size().await.unwrap(), 8);
    assert_eq!(&source.read_range(0..4).await.unwrap()[..], b"aaaa");
    *o.lock().unwrap() = ("\"v2\"".into(), String::new(), b"bbbbbbbb".to_vec());
    assert!(changed(source.read_range(4..8).await.unwrap_err()));
    let requests = server.requests.lock().unwrap();
    assert_eq!(header(&requests[1], "if-match").as_deref(), Some("\"v1\""));
}

#[tokio::test]
async fn weak_missing_and_invalid_validators_are_rejected_before_snapshot_use() {
    for tag in ["", "W/\"same\"", "unquoted", "\"embedded\"quote\""] {
        let server = server(
            object(tag, "Wed, 21 Oct 2015 07:28:00 GMT", b"abcdefgh"),
            None,
        )
        .await;
        let source = source(&server.url);
        for _ in 0..2 {
            let error = source.size().await.unwrap_err();
            assert!(error.to_string().contains("strong ETag"), "{error}");
        }
    }
}

#[tokio::test]
async fn resolver_probe_and_read_share_one_snapshot() {
    // The object is replaced after the size probe: the body read must fail.
    let o = object("\"v1\"", "", b"0123456789");
    let server = server(o.clone(), None).await;
    let url = server.url.clone();
    let flipper = o.clone();
    struct Flip(String, Object);
    #[async_trait::async_trait]
    impl RequestProvider for Flip {
        async fn request(&self, _: &str, _: bool) -> std::io::Result<zstar::http::ExpiringRequest> {
            // Swap on the second provider call (the read after the probe).
            static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            if CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
                *self.1.lock().unwrap() = ("\"v2\"".into(), String::new(), b"9876543210".to_vec());
            }
            Ok(zstar::http::ExpiringRequest {
                request: PresignedRequest::get(&self.0),
                expires_at: None,
            })
        }
    }
    let resolver = HttpEncodedObjectResolver::new(
        Arc::new(ReqwestTransport::new().unwrap()),
        move |_| Arc::new(Flip(url.clone(), flipper.clone())) as Arc<dyn RequestProvider>,
        NonZeroUsize::new(1).unwrap(),
        1 << 20,
    )
    .unwrap();
    assert!(changed(
        resolver.get_encoded([1; 32], 10, 100).await.unwrap_err()
    ));
}

#[tokio::test]
async fn fixed_url_is_not_retried_on_403_and_statuses_are_explained() {
    for (code, hint) in [
        (403, "HTTP 403"),
        (416, "range not satisfiable"),
        (200, "ignored the Range"),
    ] {
        let server = server(object("", "", b"x"), Some(code)).await;
        let error = source(&server.url).size().await.unwrap_err().to_string();
        assert!(error.contains(hint), "{error}");
        assert!(!error.contains("secret"));
        assert_eq!(server.requests.lock().unwrap().len(), 1, "status {code}");
    }
}
