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
    object_store::{ObjectStore, ObjectStoreExt, signer::Signer},
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
    assert_content_headers(&store, &blobs.object_path(digest), "application/zstd", true).await;
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

    test_packed_generation(store.clone()).await;

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

async fn test_packed_generation(store: Arc<AmazonS3>) {
    use zstar::manifest_io::{ManifestOptions, Spool};
    use zstar::pack::{
        Catalog, CatalogLimits, Generation, PackStore, PackWriter, PackWriterOptions,
        PackedObjectResolver, ResolverOptions, S3PackStore, publish_generation,
    };
    use zstar::{ObjectRequest, ObjectResolver};
    let packs =
        Arc::new(S3PackStore::new(store.clone(), Path::from("packed"), 5 << 20, 2).unwrap());
    let mut writer = PackWriter::new(
        packs.clone(),
        Catalog::default(),
        PackWriterOptions::default(),
    )
    .unwrap();
    let mut extents = Vec::new();
    let mut state = 123_u64;
    for _ in 0..3 {
        let mut raw = Vec::with_capacity(4 << 20);
        for _ in 0..(4 << 20) / 8 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            raw.extend_from_slice(&state.to_le_bytes());
        }
        extents.push(writer.put_chunk(Bytes::from(raw)).await.unwrap());
    }
    let catalog = writer.finish().await.unwrap();
    let snapshot = &catalog.packs().values().next().unwrap().snapshot;
    assert!(snapshot.length > 10 << 20);
    assert_content_headers(
        &store,
        &Path::from(format!("packed/{}", snapshot.key)),
        "application/zstd",
        true,
    )
    .await;
    let resolver = PackedObjectResolver::new(
        packs.clone(),
        catalog.clone(),
        ResolverOptions {
            max_range_bytes: 16 << 20,
            ..Default::default()
        },
    )
    .unwrap();
    let requests: Vec<_> = extents
        .iter()
        .map(|e| ObjectRequest {
            address: match e.source {
                Source::Object(digest) => digest,
                Source::Archive(_) => panic!("object source"),
            },
            hint: 0,
            max_bytes: 17 << 20,
        })
        .collect();
    let objects = resolver.get_encoded_many(&requests).await.unwrap();
    for (e, encoded) in extents.iter().zip(&objects) {
        let raw = zstar::decode_frame(encoded, e.size as usize).unwrap();
        zstar::verify_sha256(
            &raw,
            match e.source {
                Source::Object(digest) => digest,
                Source::Archive(_) => panic!("object source"),
            },
        )
        .unwrap();
    }
    // Re-uploading the identical physical pack with a different multipart part
    // size must leave the existing immutable snapshot intact.
    let alternative = S3PackStore::new(store.clone(), Path::from("packed"), 8 << 20, 2).unwrap();
    let bytes = packs.get_range(snapshot, 0..snapshot.length).await.unwrap();
    let mut spool = Spool::new(ManifestOptions::default());
    spool.append(&bytes).unwrap();
    let reused = alternative.put(snapshot.key.clone(), spool).await.unwrap();
    assert_eq!(&reused, snapshot);
    let mut builder = BlockingBuilder::new(Vec::new()).unwrap();
    let mut entry = Entry::file("packed");
    entry.extents = extents;
    builder.add_metadata(entry).unwrap();
    let archive = builder.finish().unwrap();
    let mut spool = Spool::new(ManifestOptions::default());
    spool.append(&archive).unwrap();
    let (descriptor, g) = publish_generation(
        packs.as_ref(),
        "run",
        spool,
        &catalog,
        zstar::chunking::Chunking::default(),
        CatalogLimits::default(),
    )
    .await
    .unwrap();
    for (snapshot, kind) in [
        (&descriptor, "application/cbor"),
        (&g.archive, "application/zstd"),
        (&g.catalog, "application/cbor"),
    ] {
        assert_content_headers(
            &store,
            &Path::from(format!("packed/{}", snapshot.key)),
            kind,
            true,
        )
        .await;
    }
    let encoded = packs
        .get_range(&descriptor, 0..descriptor.length)
        .await
        .unwrap();
    assert_eq!(Generation::decode(&encoded).unwrap().archive, g.archive);
    let changed = Path::from(format!("packed/{}", snapshot.key));
    store
        .put(
            &changed,
            Bytes::from(vec![0; snapshot.length as usize]).into(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resolver.get_encoded_many(&requests).await,
        Err(zstar::Error::ChangedPack)
    ));
    use futures_util::TryStreamExt;
    let keys: Vec<_> = store
        .list(Some(&Path::from("packed")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();
    for key in keys {
        store.delete(&key).await.unwrap();
    }
}

async fn assert_content_headers(store: &AmazonS3, path: &Path, kind: &str, immutable: bool) {
    use zstar::s3::object_store::{Attribute, GetOptions};
    let head = store
        .get_opts(
            path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        head.attributes
            .get(&Attribute::ContentType)
            .unwrap()
            .as_ref(),
        kind
    );
    assert_eq!(
        head.attributes
            .get(&Attribute::CacheControl)
            .unwrap()
            .as_ref(),
        if immutable {
            "private, max-age=31536000, immutable, no-transform"
        } else {
            "private, no-cache, no-transform"
        }
    );
    assert!(head.attributes.get(&Attribute::ContentEncoding).is_none());
}
