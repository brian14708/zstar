//! Independently generated entry-manifest operations and corruption scopes.
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use zstar::blocking::{
    Archive as BlockingArchive, MemorySource, ObjectResolver as EncodedObjectResolver,
};
use zstar::{Error, Result};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/entry-manifest")
}
fn artifact(name: &str) -> Vec<u8> {
    let path = root().join(name).canonicalize().unwrap();
    assert!(path.starts_with(root().canonicalize().unwrap()));
    std::fs::read(path).unwrap()
}
fn cases() -> Vec<Value> {
    let manifest: Value = serde_json::from_slice(&artifact("manifest.json")).unwrap();
    assert_eq!(manifest["schema_version"], 2);
    assert_eq!(manifest["format"], "entry-manifest");
    manifest["archives"]
        .as_object()
        .unwrap()
        .iter()
        .flat_map(|(path, archive)| {
            archive["cases"].as_array().unwrap().iter().map(|case| {
                let mut case = case.clone();
                case["archive"] = Value::String(path.clone());
                case["archive_sha256"] = archive["sha256"].clone();
                case
            })
        })
        .collect()
}
struct Objects(HashMap<[u8; 32], Vec<u8>>);
impl Objects {
    fn from_case(case: &Value) -> Self {
        Self(
            case["objects"]
                .as_object()
                .into_iter()
                .flat_map(|m| m.iter())
                .map(|(address, path)| {
                    (
                        zstar::backend::manifest::parse_object_address(address).unwrap(),
                        artifact(path.as_str().unwrap()),
                    )
                })
                .collect(),
        )
    }
}
impl EncodedObjectResolver for Objects {
    fn get_encoded(&mut self, address: [u8; 32], _: u64, _: u64) -> Result<Vec<u8>> {
        self.0
            .get(&address)
            .cloned()
            .ok_or(Error::MissingObject(address))
    }
}
fn options(case: &Value) -> zstar::blocking::ReaderOptions {
    let mut options = zstar::blocking::ReaderOptions::default();
    if let Some(value) = case["limits"]["memory_bytes"].as_u64() {
        options.metadata.max_retained_entry_bytes = value;
        options.metadata.max_page_bytes = value;
    }
    if let Some(value) = case["limits"]["max_read_bytes"].as_u64() {
        options.max_read_bytes = value;
    }
    if let Some(value) = case["limits"]["max_decoded_bytes"].as_u64() {
        options.metadata.max_page_bytes = value;
    }
    if let Some(limits) = case["limits"]["limits"].as_object() {
        for (name, value) in limits {
            let value = value.as_u64().unwrap();
            match name.as_str() {
                "max_frame_encoded_bytes" => options.limits.max_frame_encoded_bytes = value,
                "max_frame_decoded_bytes" => options.limits.max_frame_decoded_bytes = value,
                "max_window_bytes" => options.limits.max_window_bytes = value,
                "max_entry_bytes" => options.limits.max_entry_bytes = value,
                "max_extents" => options.limits.max_extents = value,
                "max_pointer_bytes" => options.limits.max_pointer_bytes = value,
                _ => panic!("unknown fixture limit: {name}"),
            }
        }
    }
    options
}
fn request(case: &Value) -> (&str, u64, u64) {
    (
        case["request"]["path"].as_str().unwrap(),
        case["request"]["offset"].as_u64().unwrap(),
        case["request"]["length"].as_u64().unwrap(),
    )
}
fn compare_outcome(case: &Value, result: Result<()>) {
    match result {
        Err(error) => assert_eq!(
            Some(error.category()),
            case["expected_error"].as_str(),
            "{}: {error}",
            case["name"]
        ),
        Ok(()) => assert!(
            case["expected_error"].is_null(),
            "expected rejection: {}",
            case["name"]
        ),
    }
}
fn check_hash(case: &Value, bytes: &[u8]) {
    assert_eq!(
        hex::encode(zstar::backend::Digest::sha256(bytes).as_bytes()),
        case["archive_sha256"].as_str().unwrap()
    );
}
#[test]
fn entry_manifest_operations_match_independent_corpus() {
    for case in cases() {
        let bytes = artifact(case["archive"].as_str().unwrap());
        check_hash(&case, &bytes);
        let result = (|| {
            let mut config = options(&case);
            config.resolver = Some(Box::new(Objects::from_case(&case)));
            config.verify_external = case["verify_external"].as_bool().unwrap_or(false);
            let mut reader = BlockingArchive::open_with_options(MemorySource::new(&bytes), config)?;
            if case["request"].is_null() {
                return Ok(());
            }
            let (path, offset, length) = request(&case);
            let (actual, report) = reader.read_range_with_report(
                path,
                offset..offset.checked_add(length).ok_or(Error::Range)?,
            )?;
            assert!(report.metadata_validated);
            assert_eq!(
                actual,
                hex::decode(case["range_result_hex"].as_str().unwrap()).unwrap(),
                "{}",
                case["name"]
            );
            Ok(())
        })();
        compare_outcome(&case, result);
    }
}

#[cfg(feature = "async")]
mod asynchronous {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use zstar::{Archive, ObjectResolver, ReaderOptions};
    struct Source(Bytes);
    #[async_trait::async_trait]
    impl zstar::AsyncRangeReader for Source {
        async fn size(&self) -> Result<u64> {
            Ok(self.0.len() as u64)
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
        )]
        async fn read_range(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
            Ok(self.0.slice(range.start as usize..range.end as usize))
        }
    }
    #[async_trait::async_trait]
    impl ObjectResolver for Objects {
        async fn get_encoded(&self, address: [u8; 32], _: u64, _: u64) -> Result<Bytes> {
            self.0
                .get(&address)
                .cloned()
                .map(Bytes::from)
                .ok_or(Error::MissingObject(address))
        }
    }
    #[tokio::test]
    async fn indexed_entry_operations_match_independent_corpus() {
        for case in cases() {
            let bytes = artifact(case["archive"].as_str().unwrap());
            check_hash(&case, &bytes);
            let result = async {
                let config = options(&case);
                let reader = Archive::open_with_options(
                    Arc::new(Source(bytes.into())),
                    ReaderOptions {
                        limits: config.limits,
                        max_read_bytes: config.max_read_bytes,
                        metadata: config.metadata,
                        resolver: Some(Arc::new(Objects::from_case(&case))),
                        verify_external: case["verify_external"].as_bool().unwrap_or(false),
                        ..Default::default()
                    },
                )
                .await?;
                if case["request"].is_null() {
                    reader.entry_count().await?;
                    return Ok(());
                }
                let (path, offset, length) = request(&case);
                let (actual, report) = reader
                    .read_range_with_report(
                        path,
                        offset..offset.checked_add(length).ok_or(Error::Range)?,
                    )
                    .await?;
                assert!(report.metadata_validated);
                assert_eq!(
                    actual.as_ref(),
                    hex::decode(case["range_result_hex"].as_str().unwrap()).unwrap()
                );
                if case["objects"]
                    .as_object()
                    .is_some_and(|objects| !objects.is_empty())
                {
                    if case["verify_external"].as_bool().unwrap_or(false) {
                        assert_ne!(report.external_objects_sha256_checked, [] as [[u8; 32]; 0]);
                        assert_eq!(report.external_objects_unchecked.len(), 0);
                    } else {
                        assert_ne!(report.external_objects_unchecked, [] as [[u8; 32]; 0]);
                        assert_eq!(report.external_objects_sha256_checked.len(), 0);
                    }
                }
                Ok(())
            }
            .await;
            compare_outcome(&case, result);
        }
    }
}
