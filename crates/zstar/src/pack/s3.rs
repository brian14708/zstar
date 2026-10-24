//! Native S3 pack storage with exact conditional ranges and bounded uploads.
use super::{PackStore, Snapshot};
use crate::s3::{
    AmazonS3, Path,
    object_store::{
        GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions,
    },
};
use crate::{Error, Result, manifest_io::Spool};
use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{ops::Range, sync::Arc};

pub struct S3PackStore {
    store: Arc<AmazonS3>,
    root: Path,
    part_size: usize,
    concurrency: usize,
    client: reqwest::Client,
    policies: std::sync::Mutex<lru::LruCache<String, crate::http::CachePolicy>>,
}
impl S3PackStore {
    pub fn new(
        store: Arc<AmazonS3>,
        root: Path,
        part_size: usize,
        concurrency: usize,
    ) -> Result<Self> {
        if !(5 << 20..=64 << 20).contains(&part_size) || !(1..=32).contains(&concurrency) {
            return Err(Error::Unsupported("invalid pack upload settings"));
        }
        Ok(Self {
            store,
            root,
            part_size,
            concurrency,
            policies: std::sync::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(1024).unwrap(),
            )),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .map_err(|_| Error::Unsupported("pack HTTP client failed"))?,
        })
    }
    fn path(&self, key: &str) -> Path {
        if self.root.as_ref().is_empty() {
            Path::from(key)
        } else {
            Path::from(format!("{}/{key}", self.root.as_ref()))
        }
    }
}
struct AbortUpload {
    store: Arc<AmazonS3>,
    path: Path,
    id: Option<String>,
}
impl Drop for AbortUpload {
    fn drop(&mut self) {
        use crate::s3::object_store::multipart::MultipartStore;
        if let Some(id) = self.id.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let store = self.store.clone();
            let path = self.path.clone();
            runtime.spawn(async move {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    store.abort_multipart(&path, &id),
                )
                .await;
            });
        }
    }
}
fn completion_body(parts: &[crate::s3::object_store::multipart::PartId]) -> String {
    use std::fmt::Write;
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (index, part) in parts.iter().enumerate() {
        write!(
            xml,
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            index + 1,
            quick_xml::escape::escape(&part.content_id)
        )
        .unwrap();
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}
fn completion_succeeded(body: &[u8]) -> bool {
    use quick_xml::{Reader, events::Event};
    let mut reader = Reader::from_reader(body);
    let mut depth = 0;
    let mut finished = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(root)) => {
                if depth == 0
                    && (finished || root.local_name().as_ref() != b"CompleteMultipartUploadResult")
                {
                    return false;
                }
                depth += 1;
            }
            Ok(Event::End(_)) => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
                if depth == 0 {
                    finished = true;
                }
            }
            Ok(Event::Eof) => return finished && depth == 0,
            Ok(Event::Empty(_)) if depth == 0 => return false,
            Ok(Event::Text(text))
                if depth == 0 && text.as_ref().iter().any(|b| !b.is_ascii_whitespace()) =>
            {
                return false;
            }
            Err(_) => return false,
            _ => {}
        }
    }
}
fn storage_error(error: crate::s3::object_store::Error) -> Error {
    if matches!(error, crate::s3::object_store::Error::Precondition { .. }) {
        Error::ChangedPack
    } else {
        crate::s3::storage_error(error)
    }
}
impl PackStore for S3PackStore {
    fn cache_allowed(&self, snapshot: &Snapshot) -> bool {
        self.policies
            .lock()
            .unwrap()
            .get(&snapshot.key)
            .is_some_and(crate::http::CachePolicy::allows_reuse)
    }
    async fn put(&self, key: String, mut data: Spool) -> Result<Snapshot> {
        let length = data.len();
        let path = self.path(&key);
        let attributes =
            crate::s3::content_attributes(crate::http::artifact_content_type(&key), true);
        if length == 0 {
            return Err(Error::Unsupported("empty packed artifact"));
        }
        let result = if length < self.part_size as u64 {
            let raw = tokio::task::spawn_blocking(move || data.read(0, length))
                .await
                .map_err(|_| Error::Unsupported("pack spool worker failed"))??;
            match self
                .store
                .put_opts(
                    &path,
                    raw.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: attributes.clone(),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(result) => result,
                Err(crate::s3::object_store::Error::AlreadyExists { .. }) => {
                    let meta = self.store.head(&path).await.map_err(storage_error)?;
                    if meta.size != length {
                        return Err(Error::ChangedPack);
                    }
                    crate::s3::object_store::PutResult {
                        e_tag: meta.e_tag,
                        version: meta.version,
                        extensions: Default::default(),
                    }
                }
                Err(error) => return Err(storage_error(error)),
            }
        } else {
            use crate::s3::object_store::{
                multipart::MultipartStore,
                signer::{SignedUrlOptions, Signer},
            };
            if length.div_ceil(self.part_size as u64) > 10_000 {
                return Err(Error::Budget("S3 multipart part count"));
            }
            // Complete directly with a destination precondition. Generic
            // ObjectStore completion overwrites, and conditional multipart
            // copy discards attributes set on the staging object.
            let id = self
                .store
                .create_multipart_opts(
                    &path,
                    PutMultipartOptions {
                        attributes,
                        ..Default::default()
                    },
                )
                .await
                .map_err(storage_error)?;
            let mut upload = AbortUpload {
                store: self.store.clone(),
                path: path.clone(),
                id: Some(id.clone()),
            };
            let mut jobs = FuturesUnordered::new();
            let mut completed = Vec::new();
            let mut offset = 0;
            let mut index = 0;
            while offset < length {
                if jobs.len() == self.concurrency {
                    completed.push(jobs.next().await.unwrap()?);
                }
                let size = (length - offset).min(self.part_size as u64);
                let (next, bytes) = tokio::task::spawn_blocking(move || {
                    data.read(offset, size).map(|bytes| (data, bytes))
                })
                .await
                .map_err(|_| Error::Unsupported("pack spool worker failed"))??;
                data = next;
                offset += size;
                let store = self.store.clone();
                let path = path.clone();
                let id = id.clone();
                jobs.push(async move {
                    let part = store
                        .put_part(&path, &id, index, bytes.into())
                        .await
                        .map_err(storage_error)?;
                    Ok::<_, Error>((index, part))
                });
                index += 1;
            }
            while let Some(part) = jobs.next().await {
                completed.push(part?);
            }
            completed.sort_unstable_by_key(|(index, _)| *index);
            let parts: Vec<_> = completed.into_iter().map(|(_, part)| part).collect();
            let options = SignedUrlOptions::default()
                .with_query([("uploadId", id)])
                .with_signed_header(
                    reqwest::header::IF_NONE_MATCH,
                    reqwest::header::HeaderValue::from_static("*"),
                )
                .with_signed_header(
                    reqwest::header::CONTENT_TYPE,
                    reqwest::header::HeaderValue::from_static("application/xml"),
                );
            let url = self
                .store
                .signed_url_opts(
                    reqwest::Method::POST,
                    &path,
                    std::time::Duration::from_secs(300),
                    &options,
                )
                .await
                .map_err(storage_error)?;
            let mut response = self
                .client
                .post(url)
                .headers(options.signed_headers)
                .body(completion_body(&parts))
                .send()
                .await
                .map_err(|_| Error::Unsupported("S3 multipart completion failed"))?;
            if response.status() != reqwest::StatusCode::PRECONDITION_FAILED {
                if !response.status().is_success() {
                    return Err(Error::Unsupported("S3 multipart completion failed"));
                }
                // S3 can return an XML error with HTTP 200. Bound and inspect
                // the response before accepting publication.
                let mut body = Vec::new();
                while let Some(bytes) = response
                    .chunk()
                    .await
                    .map_err(|_| Error::Unsupported("S3 multipart completion failed"))?
                {
                    if body.len() + bytes.len() > 64 << 10 {
                        return Err(Error::Budget("S3 multipart response"));
                    }
                    body.extend_from_slice(&bytes);
                }
                if !completion_succeeded(&body) {
                    return Err(Error::Unsupported("S3 multipart completion failed"));
                }
                upload.id = None;
            }
            let meta = self.store.head(&path).await.map_err(storage_error)?;
            if meta.size != length {
                return Err(Error::ChangedPack);
            }
            crate::s3::object_store::PutResult {
                e_tag: meta.e_tag,
                version: meta.version,
                extensions: Default::default(),
            }
        };
        let snapshot = Snapshot {
            key,
            length,
            version: result.version.unwrap_or_default(),
            etag: result.e_tag.unwrap_or_default(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
    async fn get_range(&self, snapshot: &Snapshot, range: Range<u64>) -> Result<Bytes> {
        snapshot.validate()?;
        let size = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, size, snapshot.length)?;
        if size == 0 {
            return Ok(Bytes::new());
        }
        if size > 256 << 20 {
            return Err(Error::Budget("packed S3 range"));
        }
        let version = (!snapshot.version.is_empty() && snapshot.version != "null")
            .then(|| snapshot.version.clone());
        let etag = (!snapshot.etag.is_empty()).then(|| snapshot.etag.clone());
        let result = self
            .store
            .get_opts(
                &self.path(&snapshot.key),
                GetOptions {
                    range: Some(range.clone().into()),
                    version: version.clone(),
                    if_match: etag.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        if result.meta.size != snapshot.length
            || result.range != range
            || etag.is_some() && result.meta.e_tag != etag
            || version.is_some() && result.meta.version != version
        {
            return Err(Error::ChangedPack);
        }
        let policy = crate::s3::response_cache_policy(&result.attributes);
        let bytes = crate::s3::read_exact(result, size).await?;
        let mut policies = self.policies.lock().unwrap();
        if let Some(previous) = policies.get_mut(&snapshot.key) {
            previous.observe(policy);
        } else {
            policies.put(snapshot.key.clone(), policy);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multipart_response_requires_complete_success_xml() {
        assert!(completion_succeeded(b" <CompleteMultipartUploadResult><ETag>&quot;etag&quot;</ETag></CompleteMultipartUploadResult>"));
        for body in [
            b"<Error><Code>InternalError</Code></Error>".as_slice(),
            b"<CompleteMultipartUploadResult>",
            b"<CompleteMultipartUploadResult></CompleteMultipartUploadResult><Error/>",
            b"",
            b"<CompleteMultipartUploadResult><ETag></CompleteMultipartUploadResult>",
        ] {
            assert!(!completion_succeeded(body));
        }
        let body = completion_body(&[crate::s3::object_store::multipart::PartId {
            content_id: "\"a&<b>\"".into(),
        }]);
        assert!(body.contains("&amp;&lt;b&gt;"));
        assert!(body.contains("<PartNumber>1</PartNumber>"));
    }
}
