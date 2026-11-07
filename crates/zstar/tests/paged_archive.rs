#![cfg(feature = "async")]
use bytes::Bytes;
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
    let mut entry = zstar::Entry::file("file");
    entry
        .attributes
        .insert("x-padding".into(), ciborium::Value::Bytes(padding));
    let mut writer = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    writer.add_file(entry, 0, &mut &[][..]).unwrap();
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
