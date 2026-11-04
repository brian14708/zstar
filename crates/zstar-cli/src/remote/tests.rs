use super::{PART_BYTES, download, upload};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use zstar::s3::object_store::{RetryConfig, aws::AmazonS3Builder, path::Path};

#[derive(Default)]
struct State {
    object: Option<Vec<u8>>,
    parts: Vec<Vec<u8>>,
    requests: Vec<(String, bool, usize)>,
    conflict_on_complete: bool,
    completion_error: bool,
    aborted: bool,
}

struct Server {
    store: zstar::s3::AmazonS3,
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn store(endpoint: &str) -> zstar::s3::AmazonS3 {
    AmazonS3Builder::new()
        .with_bucket_name("bucket")
        .with_region("us-east-1")
        .with_access_key_id("test")
        .with_secret_access_key("test")
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..Default::default()
        })
        .build()
        .unwrap()
}

fn reply(state: &mut State, request: &str, body: Vec<u8>) -> (u16, Vec<u8>) {
    let line = request.lines().next().unwrap();
    let create = request.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v.trim() == "*")
    });
    state.requests.push((line.into(), create, body.len()));
    if line.starts_with("GET ") {
        return (200, state.object.clone().unwrap());
    }
    if line.starts_with("DELETE ") {
        state.aborted = true;
        return (204, vec![]);
    }
    if line.starts_with("POST ") && line.contains("uploads") {
        state.parts.clear();
        return (200, b"<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>".to_vec());
    }
    if line.starts_with("PUT ") && line.contains("partNumber=") {
        state.parts.push(body);
        return (200, vec![]);
    }
    if line.starts_with("POST ") {
        let xml = String::from_utf8(body).unwrap();
        assert!(xml.contains("<PartNumber>1</PartNumber>"));
        assert!(xml.contains("<PartNumber>2</PartNumber>"));
        // ETags must be XML-escaped and retain the original part ordering.
        assert!(xml.contains("&amp;"));
        if state.conflict_on_complete {
            state.object = Some(b"concurrent writer".to_vec());
        }
        if state.completion_error {
            return (200, b"<Error><Code>InternalError</Code></Error>".to_vec());
        }
        if create && state.object.is_some() {
            return (412, vec![]);
        }
        state.object = Some(state.parts.concat());
        return (200, b"<CompleteMultipartUploadResult><ETag>completed</ETag></CompleteMultipartUploadResult>".to_vec());
    }
    assert!(line.starts_with("PUT "), "unexpected request: {line}");
    if create && state.object.is_some() {
        return (412, vec![]);
    }
    state.object = Some(body);
    (200, vec![])
}

async fn server() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(State::default()));
    let shared = state.clone();
    let task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                request.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            let length = request
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.unwrap();
            let (status, body) = reply(&mut shared.lock().unwrap(), &request, body);
            let headers = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nETag: \"part&etag\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    Server {
        store: store(&endpoint),
        state,
        task,
    }
}

fn source(size: u64) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let bytes = vec![42; usize::try_from(size).unwrap()];
    file.write_all(&bytes).unwrap();
    file
}

#[tokio::test]
async fn small_upload_is_atomic_and_force_can_overwrite() {
    let server = server().await;
    let key = Path::from("archive.zstar");
    let mut file = source(1024);
    server.state.lock().unwrap().object = Some(b"existing".to_vec());
    let error = upload(&server.store, &key, file.as_file_mut(), false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("--force"));
    assert_eq!(
        server.state.lock().unwrap().object.as_deref(),
        Some(b"existing".as_slice())
    );
    upload(&server.store, &key, file.as_file_mut(), true)
        .await
        .unwrap();
    assert_eq!(
        server.state.lock().unwrap().object.as_ref().unwrap().len(),
        1024
    );
    server.state.lock().unwrap().object = None;
    upload(&server.store, &key, file.as_file_mut(), false)
        .await
        .unwrap();
    let state = server.state.lock().unwrap();
    assert_eq!(
        state.requests.iter().map(|r| r.1).collect::<Vec<_>>(),
        [true, false, true]
    );
    assert!(state.requests.iter().all(|r| r.0.starts_with("PUT ")));
    drop(state);
}

#[tokio::test]
async fn multipart_upload_is_bounded_and_publishes_conditionally() {
    let server = server().await;
    let key = Path::from("archive.zstar");
    let mut file = source(PART_BYTES + 1024);
    upload(&server.store, &key, file.as_file_mut(), false)
        .await
        .unwrap();
    {
        let state = server.state.lock().unwrap();
        assert_eq!(
            state.parts.iter().map(Vec::len).collect::<Vec<_>>(),
            [usize::try_from(PART_BYTES).unwrap(), 1024]
        );
        assert!(state.requests.last().unwrap().1);
        assert_eq!(
            state.object.as_ref().unwrap().len(),
            usize::try_from(PART_BYTES + 1024).unwrap()
        );
    }
    upload(&server.store, &key, file.as_file_mut(), true)
        .await
        .unwrap();
    assert!(!server.state.lock().unwrap().requests.last().unwrap().1);
}

#[tokio::test]
async fn multipart_conflict_preserves_concurrent_output_and_aborts() {
    let server = server().await;
    let key = Path::from("archive.zstar");
    server.state.lock().unwrap().conflict_on_complete = true;
    let mut file = source(PART_BYTES + 1024);
    let error = upload(&server.store, &key, file.as_file_mut(), false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("--force"));
    let state = server.state.lock().unwrap();
    assert_eq!(
        state.object.as_deref(),
        Some(b"concurrent writer".as_slice())
    );
    assert!(state.aborted);
    drop(state);
}

#[tokio::test]
async fn multipart_embedded_error_is_not_success_and_aborts() {
    let server = server().await;
    let key = Path::from("archive.zstar");
    server.state.lock().unwrap().completion_error = true;
    let mut file = source(PART_BYTES + 1024);
    let error = upload(&server.store, &key, file.as_file_mut(), false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("did not return an object ETag"));
    let state = server.state.lock().unwrap();
    assert!(state.object.is_none());
    assert!(state.aborted);
    drop(state);
}

#[tokio::test]
async fn download_writes_before_the_full_response_arrives() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = store(&format!("http://{}", listener.local_addr().unwrap()));
    let (finish, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            if line == "\r\n" {
                break;
            }
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nETag: \"test\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\nConnection: close\r\n\r\nabc").await.unwrap();
        ready.await.unwrap();
        socket.write_all(b"def").await.unwrap();
    });
    let mut output = tempfile::tempfile().unwrap();
    let mut observer = output.try_clone().unwrap();
    let transfer = tokio::spawn(async move {
        download(&store, &Path::from("archive.zstar"), &mut output)
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while observer.metadata().unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("download must stream to disk before the body is complete");
    finish.send(()).unwrap();
    transfer.await.unwrap();
    server.await.unwrap();
    observer.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    observer.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"abcdef");
}
