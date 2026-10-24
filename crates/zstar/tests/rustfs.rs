//! Opt-in disposable `RustFS` only: never exercises real AWS credentials/endpoints.
use bytes::Bytes;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncWriteExt;
use zstar::backend::BlockingBuilder;
use zstar::http::{HttpRangeSource, ReqwestTransport};
use zstar::s3::{
    AmazonS3, AmazonS3Builder, BufWriter, Path, S3BlobStore, S3RangeSource, S3RequestProvider,
    object_store::{ObjectStoreExt, signer::Signer},
};
use zstar::{Archive, AsyncRangeReader, Entry, ReaderOptions, Source};

#[tokio::test]
#[ignore = "requires disposable local RustFS; set ZSTAR_S3_ENDPOINT and run --ignored"]
async fn native_s3_presigned_ranges_objects_and_multipart() {
    let endpoint =
        std::env::var("ZSTAR_S3_ENDPOINT").expect("explicit local RustFS endpoint required");
    let parsed = reqwest::Url::parse(&endpoint).unwrap();
    assert_eq!(parsed.scheme(), "http");
    assert!(
        matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")),
        "integration must use local RustFS"
    );
    assert!(parsed.username().is_empty() && parsed.password().is_none());
    let bucket = format!(
        "zstar-rust-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(bucket)
            .with_access_key_id("zstar-test")
            .with_secret_access_key("zstar-test-secret")
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    bucket_request(&client, &store, reqwest::Method::PUT).await;

    let blobs = S3BlobStore::new(
        store.clone(),
        Path::from("objects"),
        NonZeroUsize::new(4).unwrap(),
    );
    let object = Bytes::from_static(b"other-file:remote contents:third-file");
    let whole = blobs.put(object.clone()).await.unwrap();
    assert_eq!(blobs.put(object.clone()).await.unwrap(), whole);
    let Source::Object(digest) = whole.source else {
        panic!("object source")
    };
    let encoded = blobs
        .get_encoded(digest, whole.encoded_size, 17 << 20)
        .await
        .unwrap();
    assert_eq!(zstar::decode_frame(&encoded, object.len()).unwrap(), object);
    let mut writer =
        BlockingBuilder::with_options(Vec::new(), 1024, 3, Default::default()).unwrap();
    writer
        .add_file(Entry::file("inline"), 12, &mut b"hello RustFS".as_slice())
        .unwrap();
    let mut link = Entry::file("link");
    link.kind = zstar::EntryKind::Symlink;
    link.mode = 0o777;
    link.linkname = Some(".././inline".into());
    writer.add_metadata(link).unwrap();
    let mut remote = Entry::file("remote");
    remote.extents = vec![zstar::Extent {
        size: 15,
        offset: 11,
        ..whole
    }];
    writer.add_metadata(remote).unwrap();
    let archive = Bytes::from(writer.finish().unwrap());
    let path = Path::from("archive.zstar");
    zstar::s3::put_archive(store.as_ref(), &path, archive.clone())
        .await
        .unwrap();
    let native = Archive::open_with_options(
        Arc::new(S3RangeSource::new(store.clone(), path.clone())),
        ReaderOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        native.read_range("inline", 0..12).await.unwrap(),
        b"hello RustFS"[..]
    );
    assert_eq!(
        native
            .read_range_with_resolver("remote", 0, 15, Some(&blobs))
            .await
            .unwrap(),
        b"remote contents"[..]
    );
    assert_eq!(native.read_link("link").await.unwrap(), ".././inline");

    let transport = Arc::new(ReqwestTransport::new().unwrap());
    let presigned_path = Path::from("presigned.zstar");
    let remote = Arc::new(HttpRangeSource::with_provider(
        transport,
        Arc::new(
            S3RequestProvider::new(
                store.clone(),
                presigned_path.clone(),
                Duration::from_secs(60),
            )
            .unwrap(),
        ),
    ));
    remote.put(archive.clone()).await.unwrap();
    let indexed = Archive::open_with_options(remote, ReaderOptions::default())
        .await
        .unwrap();
    assert_eq!(
        indexed.read_range("inline", 6..12).await.unwrap(),
        b"RustFS"[..]
    );
    assert_eq!(
        indexed
            .read_range_with_resolver("remote", 7, 8, Some(&blobs))
            .await
            .unwrap(),
        b"contents"[..]
    );

    // More than two 5 MiB parts exercises initiation, concurrent parts, completion.
    let multipart_path = Path::from("multipart.bin");
    let mut upload = BufWriter::with_capacity(store.clone(), multipart_path.clone(), 5 << 20)
        .with_max_concurrency(2);
    let chunk = Bytes::from(vec![0x5a; 1 << 20]);
    for _ in 0..11 {
        upload.put(chunk.clone()).await.unwrap();
    }
    upload.shutdown().await.unwrap();
    let source = S3RangeSource::new(store.clone(), multipart_path.clone());
    assert_eq!(source.size().await.unwrap(), 11 << 20);
    assert_eq!(
        source
            .read_range((5 << 20) - 4..(5 << 20) + 4)
            .await
            .unwrap(),
        vec![0x5a; 8]
    );
    let aborted = Path::from("aborted.bin");
    let mut upload =
        BufWriter::with_capacity(store.clone(), aborted.clone(), 5 << 20).with_max_concurrency(2);
    for _ in 0..6 {
        upload.put(chunk.clone()).await.unwrap();
    }
    upload.abort().await.unwrap();
    assert!(store.head(&aborted).await.is_err());

    // Replacing an archive with the same length must invalidate pinned ranges.
    let pinned = S3RangeSource::new(store.clone(), path.clone());
    assert_eq!(pinned.size().await.unwrap(), archive.len() as u64);
    let mut replacement = archive.to_vec();
    replacement[0] ^= 1;
    store
        .put(&path, Bytes::from(replacement).into())
        .await
        .unwrap();
    assert!(matches!(
        pinned.read_range(0..1).await,
        Err(zstar::Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));

    for key in [
        path,
        presigned_path,
        multipart_path,
        blobs.object_path(digest),
    ] {
        store.delete(&key).await.unwrap();
    }
    bucket_request(&client, &store, reqwest::Method::DELETE).await;
}
async fn bucket_request(client: &reqwest::Client, store: &AmazonS3, method: reqwest::Method) {
    let url = store
        .signed_url(method.clone(), &Path::default(), Duration::from_secs(60))
        .await
        .unwrap();
    let response = client.request(method, url).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "bucket operation returned {}",
        response.status()
    );
}
