#![cfg(feature = "async")]
use bytes::Bytes;
use futures_util::TryStreamExt;
use std::sync::{Arc, Mutex};
use zstar::{Archive, AsyncRangeReader, Error, Result};

struct Source {
    bytes: Bytes,
    ranges: Mutex<Vec<std::ops::Range<u64>>>,
}
#[async_trait::async_trait]
impl AsyncRangeReader for Source {
    async fn size(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    async fn read_range(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        self.ranges.lock().unwrap().push(range.clone());
        self.bytes.read_range(range).await
    }
}
fn fixture(name: &str) -> Bytes {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/entry-manifest/pages")
            .join(format!("{name}.zstar")),
    )
    .unwrap()
    .into()
}

#[tokio::test]
async fn page_limit_does_not_limit_total_archive_metadata() {
    let mut builder = zstar::blocking::Builder::try_with_options(
        Vec::new(),
        zstar::WriterOptions::default().with_metadata(zstar::MetadataWriterOptions {
            block_target: 512,
            max_page_bytes: 4096,
            ..Default::default()
        }),
    )
    .unwrap();
    for i in 0..100 {
        builder
            .append_header(zstar::Header::file(format!("f{i:03}"), 0).with_attributes(
                std::collections::BTreeMap::from([(
                    "x-padding".into(),
                    ciborium::Value::Bytes(vec![7; 2048]),
                )]),
            ))
            .unwrap();
    }
    let bytes = builder.finish().unwrap();
    let footer =
        zstar::backend::parse_footer(&bytes[bytes.len() - zstar::backend::FOOTER_SIZE..]).unwrap();
    assert!(footer.toc_decoded_len > 4096);
    let metadata = zstar::MetadataReaderOptions::default().with_max_page_bytes(4096);
    let blocking = zstar::blocking::Archive::open_with_options(
        zstar::blocking::MemorySource::new(&bytes),
        zstar::blocking::ReaderOptions::default().with_metadata(metadata.clone()),
    )
    .unwrap();
    assert_eq!(blocking.entry_count(), 100);
    let archive = Archive::open_with_options(
        Arc::new(Bytes::from(bytes)),
        zstar::ReaderOptions::default().with_metadata(metadata),
    )
    .await
    .unwrap();
    assert_eq!(
        archive
            .entries()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        100
    );
    archive.validate_all_metadata().await.unwrap();
    assert_eq!(archive.entry_count().await.unwrap(), 100);
}

#[tokio::test]
async fn oversized_entry_page_is_rejected_before_fetching_it() {
    let mut builder = zstar::blocking::Builder::try_new(Vec::new()).unwrap();
    builder
        .append_header(zstar::Header::file("large", 0).with_attributes(
            std::collections::BTreeMap::from([(
                "x-padding".into(),
                ciborium::Value::Bytes(vec![7; 128 << 10]),
            )]),
        ))
        .unwrap();
    let bytes = builder.finish().unwrap();
    let metadata = zstar::MetadataReaderOptions::default().with_max_page_bytes(64 << 10);
    assert!(matches!(
        zstar::blocking::Archive::open_with_options(
            zstar::blocking::MemorySource::new(&bytes),
            zstar::blocking::ReaderOptions::default().with_metadata(metadata.clone()),
        ),
        Err(Error::Budget(zstar::BudgetError {
            resource: zstar::BudgetResource::MetadataPageBytes,
            ..
        }))
    ));
    let source = Arc::new(Source {
        bytes: bytes.into(),
        ranges: Mutex::new(Vec::new()),
    });
    let archive = Archive::open_with_options(
        source.clone(),
        zstar::ReaderOptions::default().with_metadata(metadata),
    )
    .await
    .unwrap();
    let count = source.ranges.lock().unwrap().len();
    assert!(matches!(
        archive.entry("large").await,
        Err(Error::Budget(zstar::BudgetError {
            resource: zstar::BudgetResource::MetadataPageBytes,
            limit: Some(65536),
            ..
        }))
    ));
    assert_eq!(source.ranges.lock().unwrap().len(), count);
}
#[tokio::test]
async fn selected_pages_and_ancestors_ignore_unselected_corruption_until_complete_metadata_validation()
 {
    for name in [
        "cross-page-ancestors",
        "unselected-extension-checksum",
        "unselected-entry-checksum",
        "unused-external-declaration",
    ] {
        let source = Arc::new(Source {
            bytes: fixture(name),
            ranges: Mutex::new(Vec::new()),
        });
        let reader = Archive::open(source.clone()).await.unwrap();
        assert_eq!(
            reader.read_range("a/file", 0..5).await.unwrap(),
            &[0, 1, 2, 3, 4][..]
        );
        assert!(reader.entry("zzzz-not-present").await.unwrap().is_none());
        let count = source.ranges.lock().unwrap().len();
        assert_eq!(reader.read_range("a/file", 0..0).await.unwrap().len(), 0);
        assert_eq!(source.ranges.lock().unwrap().len(), count);
        let result = reader.entry_count().await;
        assert_eq!(
            result.is_ok(),
            name == "cross-page-ancestors",
            "{name}: {result:?}"
        );
    }
}
#[tokio::test]
async fn selected_entries_require_cross_page_directory_ancestors() {
    let reader = Archive::open(Arc::new(fixture("missing-ancestor")))
        .await
        .unwrap();
    assert!(matches!(
        reader.entry("a/file").await,
        Err(Error::MalformedToc(_))
    ));
}
#[tokio::test]
async fn malformed_directory_is_rejected_before_page_use() {
    for name in [
        "entry-page-gap",
        "entry-page-overlap",
        "separator-order",
        "reference-trailing",
        "reference-empty-attributes",
        "reference-overflow",
        "declared-decoded-sum",
    ] {
        assert!(
            Archive::open(Arc::new(fixture(name))).await.is_err(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn opening_reads_only_footer_and_directory() {
    let mut state = 1_u64;
    let padding = (0..(256 << 10))
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            state.to_le_bytes()[7]
        })
        .collect();
    let mut header = zstar::Header::file("file", 0);
    header.set_attributes(std::collections::BTreeMap::from([(
        "x-padding".into(),
        ciborium::Value::Bytes(padding),
    )]));
    let mut writer = zstar::blocking::Builder::try_new(Vec::new()).unwrap();
    writer.append_header(header).unwrap();
    let source = Arc::new(Source {
        bytes: writer.finish().unwrap().into(),
        ranges: Mutex::new(Vec::new()),
    });
    assert!(source.bytes.len() > 128 << 10);
    let reader = Archive::open(source.clone()).await.unwrap();
    let footer_start = source.bytes.len() as u64 - zstar::backend::FOOTER_SIZE as u64;
    {
        let ranges = source.ranges.lock().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], footer_start..source.bytes.len() as u64);
        assert_eq!(ranges[1].end, footer_start);
    }
    assert_eq!(reader.entry_count().await.unwrap(), 1);
    let ranges = source.ranges.lock().unwrap();
    assert!(ranges.len() > 2);
    assert_eq!(ranges[2], ranges[0]);
    assert!(ranges[3..].iter().all(|r| r.end <= footer_start));
    drop(ranges);
}

#[tokio::test]
async fn listing_is_lazy_and_prefixes_skip_unrelated_corruption() {
    for name in ["unselected-extension-checksum", "unselected-entry-checksum"] {
        let source = Arc::new(Source {
            bytes: fixture(name),
            ranges: Mutex::new(Vec::new()),
        });
        let archive = Archive::open(source.clone()).await.unwrap();
        let mut entries = archive.entries_with_prefix("a/");
        assert_eq!(source.ranges.lock().unwrap().len(), 2);
        let first = entries.try_next().await.unwrap().unwrap();
        assert_eq!(first.path(), "a/file");
        assert!(entries.try_next().await.unwrap().is_none());
        let selected: Vec<_> = archive
            .entries_with_prefix("a/")
            .try_collect()
            .await
            .unwrap();
        assert_eq!(selected, vec![first]);
        let missing: Vec<_> = archive
            .entries_with_prefix("zzzz")
            .try_collect()
            .await
            .unwrap();
        assert_eq!(missing, Vec::<zstar::EntryMetadata>::new());
        let all = archive.entries().try_collect::<Vec<_>>().await;
        assert_eq!(all.is_ok(), name == "unselected-extension-checksum");
        assert!(archive.validate_all_metadata().await.is_err());
    }
    let archive = Archive::from_bytes(fixture("missing-ancestor"))
        .await
        .unwrap();
    assert!(matches!(
        archive
            .entries_with_prefix("a/")
            .try_collect::<Vec<_>>()
            .await,
        Err(Error::MalformedToc(_))
    ));
}

#[tokio::test]
async fn listing_evicts_old_pages_and_does_not_recount_revisited_entries() {
    let options = zstar::WriterOptions::default()
        .with_metadata(zstar::MetadataWriterOptions::default().with_block_target(512));
    let mut writer = zstar::blocking::Builder::try_with_options(Vec::new(), options).unwrap();
    for i in 0..64 {
        writer
            .append_header(
                zstar::Header::file(format!("file-{i:03}"), 0).with_attributes(
                    std::collections::BTreeMap::from([(
                        "x-padding".into(),
                        ciborium::Value::Bytes(vec![7; 8192]),
                    )]),
                ),
            )
            .unwrap();
    }
    let bytes: Bytes = writer.finish().unwrap().into();
    let options = zstar::ReaderOptions::default().with_metadata(
        zstar::MetadataReaderOptions::default()
            .with_max_retained_entry_bytes(128 << 10)
            .with_max_entries(64),
    );
    let source = Arc::new(Source {
        bytes: bytes.clone(),
        ranges: Mutex::new(Vec::new()),
    });
    let archive = Archive::open_with_options(source.clone(), options.clone())
        .await
        .unwrap();
    let mut stream = archive.entries();
    assert_eq!(source.ranges.lock().unwrap().len(), 2);
    assert_eq!(stream.try_next().await.unwrap().unwrap().path(), "file-000");
    assert_eq!(source.ranges.lock().unwrap().len(), 3);
    assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap().len(), 63);
    assert_eq!(
        archive
            .entries()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        64
    );
    assert!(matches!(
        archive.validate_all_metadata().await,
        Err(Error::Budget(_))
    ));
    let limited = Archive::open_with_options(
        Arc::new(bytes),
        options.with_metadata(
            zstar::MetadataReaderOptions::default()
                .with_max_retained_entry_bytes(128 << 10)
                .with_max_entries(63),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        limited.entries().try_collect::<Vec<_>>().await,
        Err(Error::Budget(_))
    ));
}
