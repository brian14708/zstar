use bytes::Bytes;
use std::{num::NonZeroUsize, sync::Arc};
use zstar::s3::{
    Path, S3BlobStore, S3RangeSource,
    object_store::{ObjectStoreExt, memory::InMemory},
};
use zstar::{Archive, AsyncRangeReader, Builder, Error, ReaderOptions, Source};

#[tokio::test]
async fn conditional_ranges_reject_replaced_objects_and_enforce_budgets() {
    let store = Arc::new(InMemory::new());
    let path = Path::from("archive");
    store
        .put(&path, Bytes::from_static(b"original").into())
        .await
        .unwrap();
    let source = S3RangeSource::new(store.clone(), path.clone()).with_max_range_bytes(3);
    assert_eq!(source.size().await.unwrap(), 8);
    assert_eq!(source.read_range(0..3).await.unwrap(), b"ori"[..]);
    assert!(matches!(
        source.read_range(0..4).await,
        Err(Error::Budget(_))
    ));
    assert!(matches!(source.read_range(8..9).await, Err(Error::Range)));
    store
        .put(&path, Bytes::from_static(b"replaced").into())
        .await
        .unwrap();
    assert!(source.read_range(0..3).await.is_err());
}
#[tokio::test]
async fn encoded_objects_use_actual_sizes_and_reader_owned_verification() {
    let store = Arc::new(InMemory::new());
    let blobs = Arc::new(S3BlobStore::new(
        store.clone(),
        Path::from("objects"),
        NonZeroUsize::new(2).unwrap(),
    ));
    let extent = blobs.put(Bytes::from_static(b"aaaa")).await.unwrap();
    let Source::Object(digest) = extent.source else {
        panic!("object source")
    };
    let encoded = blobs.get_encoded(digest, 1, 17 << 20).await.unwrap();
    assert_eq!(encoded.len() as u64, extent.encoded_size);
    assert_eq!(zstar::decode_frame(&encoded, 4).unwrap(), b"aaaa");
    assert!(matches!(
        blobs.get_encoded(digest, extent.encoded_size, 1).await,
        Err(Error::Budget(_))
    ));
    let mut writer = Builder::new(Vec::new());
    let mut entry = zstar::Entry::file("remote");
    entry.extents = vec![extent];
    entry.extents[0].encoded_size = 1;
    writer.append_external(entry).await.unwrap();
    let archive = Bytes::from(writer.finish().await.unwrap());
    let archive_path = Path::from("archive");
    store.put(&archive_path, archive.into()).await.unwrap();
    let reader = Archive::open_with_options(
        Arc::new(S3RangeSource::new(store.clone(), archive_path.clone())),
        ReaderOptions {
            resolver: Some(blobs.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (raw, report) = reader.read_range_with_report("remote", 0..4).await.unwrap();
    assert_eq!(raw, b"aaaa"[..]);
    assert_eq!(report.external_objects_sha256_checked, vec![digest]);
    assert!(reader.cache_len_bytes() > 0);
    store
        .put(
            &blobs.object_path(digest),
            Bytes::from(zstar::encode_frame(b"xxxx", 3).unwrap()).into(),
        )
        .await
        .unwrap();
    assert_eq!(
        reader.read_range("remote", 0..4).await.unwrap(),
        b"aaaa"[..]
    );
    let uncached = Archive::open_with_options(
        Arc::new(S3RangeSource::new(store.clone(), archive_path.clone())),
        ReaderOptions {
            cache_bytes: 0,
            resolver: Some(blobs.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        uncached.read_range("remote", 0..4).await,
        Err(Error::Integrity)
    ));
    let unchecked = Archive::open_with_options(
        Arc::new(S3RangeSource::new(store.clone(), archive_path)),
        ReaderOptions {
            cache_bytes: 0,
            resolver: Some(blobs.clone()),
            verify_external: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let (raw, report) = unchecked
        .read_range_with_report("remote", 0..4)
        .await
        .unwrap();
    assert_eq!(raw, b"xxxx"[..]);
    assert_eq!(report.external_objects_unchecked, vec![digest]);
    assert_eq!(report.external_objects_sha256_checked, [] as [[u8; 32]; 0]);
    store.delete(&blobs.object_path(digest)).await.unwrap();
    assert!(matches!(
        unchecked.read_range("remote", 0..4).await,
        Err(Error::MissingObject(_))
    ));
}
