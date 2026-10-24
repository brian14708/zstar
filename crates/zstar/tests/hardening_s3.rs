use bytes::Bytes;
use futures_util::stream::{self, BoxStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{num::NonZeroUsize, sync::Arc};
use zstar::Source;
use zstar::s3::{
    Path, S3BlobStore, S3RangeSource,
    object_store::{
        self, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, memory::InMemory,
    },
};
use zstar::{AsyncRangeReader, Error};

#[derive(Debug, Default)]
struct TestStore {
    inner: InMemory,
    heads: AtomicUsize,
    gets: AtomicUsize,
    hide_first_head: bool,
    no_identity: bool,
    response: Option<Response>,
}

#[derive(Clone, Copy, Debug)]
enum Response {
    Partial,
    Short,
    Oversized,
    BackendError,
}

impl std::fmt::Display for TestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("test store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for TestStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if options.head {
            let count = self.heads.fetch_add(1, Ordering::SeqCst);
            if self.hide_first_head && count == 0 {
                return Err(object_store::Error::NotFound {
                    path: path.to_string(),
                    source: "concurrent creation".into(),
                });
            }
        } else {
            self.gets.fetch_add(1, Ordering::SeqCst);
        }
        let head = options.head;
        let mut result = self.inner.get_opts(path, options).await?;
        if self.no_identity {
            result.meta.e_tag = Some(String::new());
            result.meta.version = Some("null".into());
        }
        if !head {
            match self.response {
                Some(Response::Partial) => result.range.start += 1,
                Some(Response::Short) => {
                    result.payload =
                        GetResultPayload::Stream(Box::pin(stream::iter([Ok(Bytes::new())])));
                }
                Some(Response::Oversized) => {
                    let bytes = Bytes::from(vec![0; result.meta.size as usize + 1]);
                    result.payload = GetResultPayload::Stream(Box::pin(stream::iter([Ok(bytes)])));
                }
                Some(Response::BackendError) => {
                    result.payload = GetResultPayload::Stream(Box::pin(stream::iter([Err(
                        object_store::Error::Generic {
                            store: "test",
                            source: "https://example.com/?secret=credential".into(),
                        },
                    )])));
                }
                None => {}
            }
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn blobs(store: Arc<dyn ObjectStore>) -> S3BlobStore {
    S3BlobStore::new(store, Path::from("objects"), NonZeroUsize::new(1).unwrap())
}

#[tokio::test]
async fn concurrent_creation_preserves_winning_encoding_and_size_hint() {
    let store = Arc::new(TestStore {
        hide_first_head: true,
        ..Default::default()
    });
    let blobs = blobs(store.clone());
    let raw = Bytes::from_static(b"same bytes");
    let digest = *zstar::Digest::sha256(&raw).as_bytes();
    let path = blobs.object_path(digest);
    let alternate = Bytes::from(zstar::encode_frame(&raw, 19).unwrap());
    store
        .inner
        .put(&path, alternate.clone().into())
        .await
        .unwrap();
    let extent = blobs.put(raw.clone()).await.unwrap();
    assert_eq!(extent.source, Source::Object(digest));
    assert_eq!(extent.size, raw.len() as u64);
    assert_eq!(extent.encoded_size, alternate.len() as u64);
    assert_eq!(
        store.inner.get(&path).await.unwrap().bytes().await.unwrap(),
        alternate
    );
    assert_eq!(store.heads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn blob_fetch_uses_one_get_and_enforces_actual_response_bounds() {
    for response in [
        None,
        Some(Response::Partial),
        Some(Response::Short),
        Some(Response::Oversized),
        Some(Response::BackendError),
    ] {
        let store = Arc::new(TestStore {
            response,
            no_identity: true,
            ..Default::default()
        });
        let blobs = blobs(store.clone());
        let digest = [1; 32];
        store
            .inner
            .put(
                &blobs.object_path(digest),
                Bytes::from_static(b"encoded").into(),
            )
            .await
            .unwrap();
        let result = blobs.get_encoded(digest, 1, 7).await;
        match response {
            None => assert_eq!(result.unwrap(), b"encoded"[..]),
            Some(Response::Partial | Response::Short) => {
                assert!(matches!(result, Err(Error::MalformedFrame(_))));
            }
            Some(Response::Oversized) => assert!(matches!(result, Err(Error::Budget(_)))),
            Some(Response::BackendError) => {
                let error = result.unwrap_err();
                assert!(!format!("{error:?}").contains("secret"));
                assert!(matches!(error, Error::Io(_)));
            }
        }
        assert_eq!(store.heads.load(Ordering::SeqCst), 0);
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
        assert!(matches!(
            blobs.get_encoded(digest, 100, 6).await,
            Err(Error::Budget(_))
        ));
        assert!(matches!(
            blobs.get_encoded(digest, 100, 0).await,
            Err(Error::Budget(_))
        ));
    }
}

#[tokio::test]
async fn archive_source_rejects_empty_etag_and_null_version() {
    let store = Arc::new(TestStore {
        no_identity: true,
        ..Default::default()
    });
    let path = Path::from("archive");
    store
        .inner
        .put(&path, Bytes::from_static(b"archive").into())
        .await
        .unwrap();
    let source = S3RangeSource::new(store.clone(), path);
    assert!(source.size().await.is_err());
    assert!(source.read_range(0..1).await.is_err());
    assert_eq!(store.gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn put_rejects_invalid_existing_object_without_overwriting() {
    let store = Arc::new(InMemory::new());
    let blobs = blobs(store.clone());
    let raw = Bytes::from_static(b"same bytes");
    let digest = *zstar::Digest::sha256(&raw).as_bytes();
    let path = blobs.object_path(digest);
    store.put(&path, Bytes::new().into()).await.unwrap();
    assert!(matches!(blobs.put(raw).await, Err(Error::Budget(_))));
    assert_eq!(store.head(&path).await.unwrap().size, 0);
}

#[tokio::test]
async fn blob_put_skips_upload_when_content_addressed_object_exists() {
    let store = Arc::new(InMemory::new());
    let blobs = S3BlobStore::new(
        store.clone(),
        Path::from("objects"),
        NonZeroUsize::new(1).unwrap(),
    );
    let first = blobs.put(Bytes::from_static(b"same bytes")).await.unwrap();
    let Source::Object(digest) = first.source else {
        panic!()
    };
    let path = blobs.object_path(digest);
    // Distinguishable-but-equivalent stored bytes prove no overwrite happens.
    let alt = zstar::encode_frame(b"same bytes", 19).unwrap();
    store
        .put(&path, Bytes::from(alt.clone()).into())
        .await
        .unwrap();
    let second = blobs.put(Bytes::from_static(b"same bytes")).await.unwrap();
    assert_eq!(second.source, first.source);
    assert_eq!(second.size, first.size);
    let after = store.head(&path).await.unwrap();
    assert_eq!(after.size as usize, alt.len());
    assert_eq!(second.encoded_size, after.size);
    // A missing object is still uploaded.
    let other = blobs.put(Bytes::from_static(b"other")).await.unwrap();
    let Source::Object(d) = other.source else {
        panic!()
    };
    assert!(store.head(&blobs.object_path(d)).await.is_ok());
}

#[tokio::test]
async fn manifest_metadata_hints_save_a_get_without_changing_validation() {
    use object_store::{Attribute, Attributes, PutOptions};
    use zstar::s3::{MANIFEST_POSITION_METADATA_KEY, VERSION_METADATA_KEY, archive_attributes};
    let bytes = Bytes::from_static(include_bytes!(
        "../../../testdata/entry-manifest/inline.zstar"
    ));
    let footer = zstar::parse_footer(&bytes[bytes.len() - 80..]).unwrap();
    let good = archive_attributes(&footer, bytes.len() as u64).unwrap();
    for (position, version, expected) in [
        (None, None, 2),
        (
            Some(format!("{}:{}", footer.toc_offset, footer.toc_encoded_len)),
            None,
            1,
        ),
        (Some("0:1".into()), None, 2),
        (Some("+1:123".into()), None, 2),
        (Some("18446744073709551616:10".into()), None, 2),
        (Some("1:18446744073709551615".into()), None, 2),
        (
            Some(format!("{}:{}", footer.toc_offset, footer.toc_encoded_len)),
            Some("2"),
            2,
        ),
        // Plausible coverage but stale start: candidate footer fails agreement,
        // so opening retries through the standard footer path.
        (
            Some(format!(
                "{}:{}",
                footer.toc_offset - 1,
                footer.toc_encoded_len + 1
            )),
            None,
            3,
        ),
    ] {
        let store = Arc::new(TestStore::default());
        let path = Path::from("hint.zstar");
        let mut attributes = Attributes::new();
        if let Some(position) = position {
            attributes.insert(
                Attribute::Metadata(MANIFEST_POSITION_METADATA_KEY.into()),
                position.into(),
            );
        }
        if let Some(version) = version {
            attributes.insert(
                Attribute::Metadata(VERSION_METADATA_KEY.into()),
                version.into(),
            );
        }
        store
            .put_opts(
                &path,
                bytes.clone().into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let archive = zstar::Archive::open(Arc::new(S3RangeSource::new(store.clone(), path)))
            .await
            .unwrap();
        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), expected);
        assert_eq!(archive.read("a-b").await.unwrap(), b"hello"[..]);
    }
    assert_eq!(good.len(), 5);
    assert!(archive_attributes(&footer, bytes.len() as u64 + 1).is_err());
}

#[tokio::test]
async fn completed_archive_put_writes_metadata_and_hints_preserve_budgets_and_hashes() {
    use object_store::{Attribute, GetOptions};
    use zstar::s3::{MANIFEST_POSITION_METADATA_KEY, put_archive};
    let bytes = Bytes::from_static(include_bytes!(
        "../../../testdata/entry-manifest/inline.zstar"
    ));
    let store = Arc::new(TestStore::default());
    let path = Path::from("put.zstar");
    put_archive(store.as_ref(), &path, bytes.clone())
        .await
        .unwrap();
    let head = store
        .inner
        .get_opts(
            &path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(head.attributes.len(), 5);
    assert_eq!(
        head.attributes
            .get(&Attribute::ContentType)
            .unwrap()
            .as_ref(),
        "application/zstd"
    );
    assert_eq!(
        head.attributes
            .get(&Attribute::CacheControl)
            .unwrap()
            .as_ref(),
        "private, no-cache, no-transform"
    );
    assert!(head.attributes.get(&Attribute::ContentEncoding).is_none());
    assert!(
        head.attributes
            .get(&Attribute::Metadata(MANIFEST_POSITION_METADATA_KEY.into()))
            .is_some()
    );
    let source = Arc::new(S3RangeSource::new(store.clone(), path.clone()));
    let result = zstar::Archive::open_with_options(
        source,
        zstar::ReaderOptions {
            manifest: zstar::ManifestOptions {
                max_encoded_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    assert!(matches!(result, Err(Error::Budget(_))));
    assert_eq!(store.gets.load(Ordering::SeqCst), 1); // Footer only: oversized hint wasn't fetched.
    let mut corrupted = bytes.to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    put_archive(store.as_ref(), &path, corrupted.into())
        .await
        .unwrap();
    assert!(matches!(
        zstar::Archive::open(Arc::new(S3RangeSource::new(store.clone(), path))).await,
        Err(Error::Integrity)
    ));
    assert!(
        put_archive(
            store.as_ref(),
            &Path::from("bad"),
            Bytes::from_static(b"bad")
        )
        .await
        .is_err()
    );
}
