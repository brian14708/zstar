//! Streaming archive publication with bounded concurrent multipart parts.

use super::{AmazonS3, Path, content_attributes, storage_error};
use crate::{Error, Result};
use bytes::BytesMut;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use object_store::{
    MultipartId, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    multipart::{MultipartStore, PartId},
    signer::{SignedUrlOptions, Signer},
};
use reqwest::{Method, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

/// Default and minimum multipart part size.
pub const MIN_PART_BYTES: usize = 8 << 20;
const MAX_PARTS: usize = 10_000;
const MAX_OBJECT_BYTES: u64 = 5 << 40;
// With growing parts, part size doubles every tier. Ten tiers of 8 MiB-based
// parts exceed the 5 TiB object limit within the 10,000-part limit.
const PARTS_PER_TIER: usize = 1_000;
const MAX_IN_FLIGHT: usize = 4;

/// Whether publication may replace an existing object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadMode {
    /// Replace any existing object.
    Overwrite,
    /// Fail with [`std::io::ErrorKind::AlreadyExists`] if the object exists.
    Create,
}

/// Both the publication failure and its failed multipart cleanup.
#[derive(Debug, thiserror::Error)]
#[error("archive publication and cleanup failed")]
pub struct UploadCleanupError {
    /// Original publication error.
    #[source]
    pub publication: Error,
    /// Error returned by the abort request.
    pub cleanup: Error,
}

type PartJob = BoxFuture<'static, (usize, object_store::Result<PartId>)>;

/// Streams one archive to S3 with archive content headers.
///
/// Archives smaller than one part ([`MIN_PART_BYTES`], or the part size chosen
/// by [`Self::with_size_hint`]) are published with a single PUT. Larger archives
/// upload up to four parts concurrently, and nothing is visible until
/// [`Self::finish`] succeeds.
///
/// Parts are equal-sized by default, as some S3-compatible stores require.
/// Without a size hint this limits an archive to 10,000 parts of 8 MiB (about
/// 78 GiB) unless [`Self::with_growing_parts`] is selected.
///
/// [`Self::finish`] aborts a failed multipart publication itself. Call
/// [`Self::abort`] after a failed [`Self::write`] to release uploaded parts.
/// Once finishing or aborting starts, further writes and finalization are rejected.
pub struct ArchiveUpload {
    store: Arc<AmazonS3>,
    path: Path,
    mode: UploadMode,
    fixed_part: Option<usize>,
    growing: bool,
    buffer: BytesMut,
    written: u64,
    multipart: Option<MultipartId>,
    parts: Vec<Option<PartId>>,
    pending: FuturesUnordered<PartJob>,
    closed: bool,
}

impl ArchiveUpload {
    #[must_use]
    pub fn new(store: Arc<AmazonS3>, path: Path, mode: UploadMode) -> Self {
        Self {
            store,
            path,
            mode,
            fixed_part: None,
            growing: false,
            buffer: BytesMut::new(),
            written: 0,
            multipart: None,
            parts: Vec::new(),
            pending: FuturesUnordered::new(),
            closed: false,
        }
    }

    /// Uses equal parts sized to stay within the S3 part-count limit.
    ///
    /// # Errors
    /// Rejects sizes above the S3 object size limit.
    pub fn with_size_hint(mut self, size: u64) -> Result<Self> {
        if size > MAX_OBJECT_BYTES {
            return Err(crate::Error::budget(
                crate::BudgetResource::UploadBytes,
                "S3 object size",
            ));
        }
        let part = usize::try_from(size.div_ceil(MAX_PARTS as u64)).map_err(|_| Error::Range)?;
        self.fixed_part = Some(part.max(MIN_PART_BYTES));
        Ok(self)
    }

    /// Without a size hint, doubles the part size every 1,000 parts so streams
    /// can reach the S3 object size limit. Stores that require equal part sizes
    /// (such as Cloudflare R2) reject such uploads. Ignored with a size hint.
    #[must_use]
    pub const fn with_growing_parts(mut self) -> Self {
        self.growing = true;
        self
    }

    fn part_size(&self) -> usize {
        self.fixed_part.unwrap_or_else(|| {
            if self.growing {
                MIN_PART_BYTES << (self.parts.len() / PARTS_PER_TIER).min(10)
            } else {
                MIN_PART_BYTES
            }
        })
    }

    fn check_open(&self) -> Result<()> {
        if self.closed {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "S3 archive upload closed",
            )));
        }
        Ok(())
    }

    /// Buffers bytes, uploading each completed part.
    ///
    /// # Errors
    /// Returns size-limit or storage errors, or `InvalidInput` if closed.
    pub async fn write(&mut self, mut data: &[u8]) -> Result<()> {
        self.check_open()?;
        self.written = self
            .written
            .checked_add(data.len() as u64)
            .filter(|size| *size <= MAX_OBJECT_BYTES)
            .ok_or(crate::Error::budget(
                crate::BudgetResource::UploadBytes,
                "S3 object size",
            ))?;
        while !data.is_empty() {
            let part = self.part_size();
            if self.buffer.is_empty() {
                self.buffer.reserve(part);
            }
            let count = data.len().min(part - self.buffer.len());
            self.buffer.extend_from_slice(&data[..count]);
            data = &data[count..];
            if self.buffer.len() == part {
                self.send_part().await?;
            }
        }
        Ok(())
    }

    async fn send_part(&mut self) -> Result<()> {
        if self.parts.len() >= MAX_PARTS {
            return Err(crate::Error::budget(
                crate::BudgetResource::UploadParts,
                "S3 part count",
            ));
        }
        let id = if let Some(id) = &self.multipart {
            id.clone()
        } else {
            let id = self
                .store
                .create_multipart_opts(
                    &self.path,
                    PutMultipartOptions {
                        attributes: archive_attributes(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(storage_error)?;
            self.multipart = Some(id.clone());
            id
        };
        while self.pending.len() >= MAX_IN_FLIGHT {
            self.collect().await?;
        }
        let index = self.parts.len();
        self.parts.push(None);
        let data = self.buffer.split().freeze();
        let (store, path) = (self.store.clone(), self.path.clone());
        self.pending.push(Box::pin(async move {
            (index, store.put_part(&path, &id, index, data.into()).await)
        }));
        Ok(())
    }

    async fn collect(&mut self) -> Result<()> {
        if let Some((index, result)) = self.pending.next().await {
            self.parts[index] = Some(result.map_err(storage_error)?);
        }
        Ok(())
    }

    /// Uploads remaining bytes and publishes the archive.
    ///
    /// A failed multipart publication attempts abort before returning. If that
    /// also fails, the I/O error contains [`UploadCleanupError`] with both errors;
    /// [`Self::abort`] can retry cleanup using the retained upload ID.
    ///
    /// # Errors
    /// Returns storage errors, `AlreadyExists` in [`UploadMode::Create`], or
    /// `InvalidInput` if finishing or aborting has already started.
    pub async fn finish(&mut self) -> Result<()> {
        self.check_open()?;
        self.closed = true;
        let Some(id) = self.multipart.clone() else {
            let mode = match self.mode {
                UploadMode::Overwrite => PutMode::Overwrite,
                UploadMode::Create => PutMode::Create,
            };
            self.store
                .put_opts(
                    &self.path,
                    self.buffer.split().freeze().into(),
                    PutOptions {
                        mode,
                        attributes: archive_attributes(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(publication_error)?;
            return Ok(());
        };
        if let Err(publication) = self.complete(&id).await {
            if let Err(cleanup) = self.abort().await {
                return Err(Error::Io(std::io::Error::other(UploadCleanupError {
                    publication,
                    cleanup,
                })));
            }
            return Err(publication);
        }
        Ok(())
    }

    async fn complete(&mut self, id: &MultipartId) -> Result<()> {
        if !self.buffer.is_empty() {
            self.send_part().await?;
        }
        while !self.pending.is_empty() {
            self.collect().await?;
        }
        let parts = std::mem::take(&mut self.parts)
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(Error::Unsupported("missing multipart part"))?;
        match self.mode {
            UploadMode::Overwrite => {
                self.store
                    .complete_multipart(&self.path, id, parts)
                    .await
                    .map_err(publication_error)?;
            }
            UploadMode::Create => complete_create(&self.store, &self.path, id, &parts).await?,
        }
        self.multipart = None;
        Ok(())
    }

    /// Cancels in-flight parts and aborts any multipart upload.
    ///
    /// # Errors
    /// Returns storage errors from the abort request.
    pub async fn abort(&mut self) -> Result<()> {
        self.closed = true;
        self.pending.clear();
        self.parts.clear();
        self.buffer.clear();
        if let Some(id) = self.multipart.clone() {
            self.store
                .abort_multipart(&self.path, &id)
                .await
                .map_err(storage_error)?;
            self.multipart = None;
        }
        Ok(())
    }
}

fn archive_attributes() -> object_store::Attributes {
    content_attributes(crate::http::ZSTD_CONTENT_TYPE, false)
}

fn already_exists() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "storage object already exists",
    ))
}

fn publication_error(error: object_store::Error) -> Error {
    if matches!(
        error,
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    ) {
        already_exists()
    } else {
        storage_error(error)
    }
}

fn transport(error: reqwest::Error) -> Error {
    Error::Io(std::io::Error::other(error.without_url()))
}

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct Completion<'a> {
    #[serde(rename = "Part")]
    parts: Vec<CompletionPart<'a>>,
}

#[derive(Serialize)]
struct CompletionPart<'a> {
    #[serde(rename = "PartNumber")]
    number: usize,
    #[serde(rename = "ETag")]
    etag: &'a str,
}

/// The root element selects the variant, so an embedded `<Error>` is rejected.
#[derive(Deserialize)]
enum Completed {
    CompleteMultipartUploadResult {
        #[serde(rename = "ETag")]
        etag: String,
    },
}

fn completed_etag(body: &[u8]) -> Option<String> {
    match quick_xml::de::from_reader(body).ok()? {
        Completed::CompleteMultipartUploadResult { etag } => Some(etag),
    }
}

async fn complete_create(store: &AmazonS3, path: &Path, id: &str, parts: &[PartId]) -> Result<()> {
    // object_store's multipart completion always overwrites. Sign a conditional
    // completion instead, so a concurrent writer cannot be silently replaced.
    let options = SignedUrlOptions::default()
        .with_query([("uploadId", id)])
        .with_signed_header(header::IF_NONE_MATCH, header::HeaderValue::from_static("*"));
    let url = store
        .signed_url_opts(Method::POST, path, Duration::from_secs(300), &options)
        .await
        .map_err(storage_error)?;
    let body = quick_xml::se::to_string(&Completion {
        parts: parts
            .iter()
            .enumerate()
            .map(|(index, part)| CompletionPart {
                number: index + 1,
                etag: &part.content_id,
            })
            .collect(),
    })
    .map_err(|error| Error::Io(std::io::Error::other(error)))?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(transport)?;
    let mut response = client
        .post(url)
        .header(header::IF_NONE_MATCH, "*")
        .header(header::CONTENT_TYPE, "application/xml")
        .body(body)
        .send()
        .await
        .map_err(transport)?;
    if response.status() == StatusCode::PRECONDITION_FAILED {
        return Err(already_exists());
    }
    response = response.error_for_status().map_err(transport)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport)? {
        if body.len() + chunk.len() > 64 << 10 {
            return Err(crate::Error::budget(
                crate::BudgetResource::ResponseBytes,
                "S3 upload completion response",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    // S3 can return HTTP 200 with an embedded XML error. Require a completed
    // object's ETag rather than treating the HTTP status alone as success.
    match completed_etag(&body) {
        Some(etag) if !etag.is_empty() => Ok(()),
        Some(_) => Err(Error::Io(std::io::Error::other(
            "S3 upload completion returned an empty ETag",
        ))),
        None => Err(Error::Io(std::io::Error::other(
            "S3 upload completion did not return an object ETag",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_abort_retains_multipart_id_for_retry() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for status in ["403 Forbidden", "204 No Content"] {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                    let count = socket.read(&mut buffer).unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&buffer[..count]);
                }
                assert!(String::from_utf8_lossy(&request).starts_with("DELETE "));
                write!(
                    socket,
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
        });
        let store = Arc::new(
            super::super::AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("test")
                .with_access_key_id("key")
                .with_secret_access_key("secret")
                .with_endpoint(endpoint)
                .with_allow_http(true)
                .build()
                .unwrap(),
        );
        let mut upload = ArchiveUpload::new(store, Path::from("archive"), UploadMode::Overwrite);
        upload.multipart = Some("upload-id".into());
        assert!(upload.abort().await.is_err());
        assert_eq!(upload.multipart.as_deref(), Some("upload-id"));
        upload.abort().await.unwrap();
        assert!(upload.multipart.is_none());
        server.join().unwrap();
    }

    #[test]
    fn completion_etag_requires_a_result_document() {
        assert_eq!(
            completed_etag(
                b"<?xml version=\"1.0\"?>\n<CompleteMultipartUploadResult><ETag>x</ETag></CompleteMultipartUploadResult>"
            )
            .as_deref(),
            Some("x")
        );
        assert_eq!(
            completed_etag(b"<Error><Code>InternalError</Code><ETag>x</ETag></Error>"),
            None
        );
        let body = quick_xml::se::to_string(&Completion {
            parts: vec![CompletionPart {
                number: 1,
                etag: "\"a&b\"",
            }],
        })
        .unwrap();
        assert!(body.contains("<ETag>\"a&amp;b\"</ETag>"), "{body}");
    }

    #[test]
    fn parts_are_equal_unless_growth_is_selected() {
        let store = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .build()
                .unwrap(),
        );
        let upload = |growing: bool| {
            let mut upload =
                ArchiveUpload::new(store.clone(), Path::from("a"), UploadMode::Overwrite);
            if growing {
                upload = upload.with_growing_parts();
            }
            upload.parts = vec![None; 2 * PARTS_PER_TIER];
            upload
        };
        assert_eq!(upload(false).part_size(), MIN_PART_BYTES);
        assert_eq!(upload(true).part_size(), MIN_PART_BYTES << 2);
        let hinted = upload(true).with_size_hint(1 << 40).unwrap();
        assert_eq!(hinted.part_size(), (1_usize << 40).div_ceil(MAX_PARTS));
    }
}
