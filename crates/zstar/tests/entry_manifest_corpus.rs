#![cfg(feature = "compression")]
//! Independently generated entry-manifest operations and corruption scopes.
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use zstar::backend::{BlockingArchive, EncodedObjectResolver};
use zstar::{Error, ManifestOptions, Result, SliceReader};

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
    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["format"], "entry-manifest");
    manifest["cases"].as_array().unwrap().clone()
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
                        zstar::manifest::parse_object_address(address).unwrap(),
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
fn options(case: &Value) -> ManifestOptions {
    let mut options = ManifestOptions::default();
    if let Some(value) = case["limits"]["max_read_bytes"].as_u64() {
        options.max_read_bytes = value;
    }
    if let Some(value) = case["limits"]["max_decoded_bytes"].as_u64() {
        options.max_decoded_bytes = value;
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
        hex::encode(zstar::Digest::sha256(bytes).as_bytes()),
        case["archive_sha256"].as_str().unwrap()
    );
}
#[test]
fn entry_manifest_operations_match_independent_corpus() {
    for case in cases() {
        let bytes = artifact(case["archive"].as_str().unwrap());
        check_hash(&case, &bytes);
        let result = (|| {
            let mut reader = BlockingArchive::open(SliceReader::new(&bytes), options(&case))?;
            let operation = case["operation"].as_str().unwrap();
            let mut objects = Objects::from_case(&case);
            if operation == "metadata" {
                return reader.manifest().validate_metadata();
            }
            if matches!(operation, "inline-range" | "external-range") {
                let (path, offset, length) = request(&case);
                let resolver = (operation == "external-range")
                    .then_some(&mut objects as &mut dyn EncodedObjectResolver);
                let actual = reader.read(
                    path,
                    offset,
                    length,
                    resolver,
                    case["verify_external"].as_bool().unwrap_or(true),
                )?;
                assert_eq!(
                    actual,
                    artifact(case["range_result"].as_str().unwrap()),
                    "{}",
                    case["name"]
                );
                return Ok(());
            }
            reader.validate()?;
            if operation == "external-closure" {
                for entry in reader.manifest().collect_entries()? {
                    if entry.is_external() {
                        reader
                            .open_file(&entry.path, Some(&mut objects), true)?
                            .copy_to(&mut std::io::sink())?;
                    }
                }
            }
            for (path, expected) in case["members"]
                .as_object()
                .into_iter()
                .flat_map(|m| m.iter())
            {
                let entry = reader.entry(path)?.unwrap();
                let actual = reader.read(path, 0, entry.size()?, Some(&mut objects), true)?;
                assert_eq!(
                    actual,
                    artifact(expected.as_str().unwrap()),
                    "{}: {path}",
                    case["name"]
                );
            }
            for (path, target) in case["links"].as_object().into_iter().flat_map(|m| m.iter()) {
                assert_eq!(reader.read_link(path)?, target.as_str().unwrap());
            }
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
    use zstar::{Archive, ObjectResolver, ReaderOptions, VerificationScope};
    struct Source(Bytes);
    #[async_trait::async_trait]
    impl zstar::AsyncRangeReader for Source {
        async fn size(&self) -> Result<u64> {
            Ok(self.0.len() as u64)
        }
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
            let operation = case["operation"].as_str().unwrap();
            let bytes = artifact(case["archive"].as_str().unwrap());
            check_hash(&case, &bytes);
            let result = async {
                let reader = Archive::open_with_options(
                    Arc::new(Source(bytes.into())),
                    ReaderOptions {
                        manifest: options(&case),
                        resolver: Some(Arc::new(Objects::from_case(&case))),
                        verify_external: case["verify_external"].as_bool().unwrap_or(true),
                        ..Default::default()
                    },
                )
                .await?;
                if operation == "metadata" {
                    return reader.validate_metadata().await;
                }
                if !matches!(operation, "inline-range" | "external-range") {
                    let scope = if operation == "external-closure" {
                        VerificationScope::ExternalClosure
                    } else {
                        VerificationScope::Archive
                    };
                    reader.verify(scope).await?;
                    return Ok(());
                }
                let (path, offset, length) = request(&case);
                let (actual, report) = reader
                    .read_range_with_report(
                        path,
                        offset..offset.checked_add(length).ok_or(Error::Range)?,
                    )
                    .await?;
                assert!(report.metadata_authenticated);
                assert!(!report.full_archive_validated);
                assert_eq!(
                    actual.as_ref(),
                    artifact(case["range_result"].as_str().unwrap())
                );
                if operation == "external-range" {
                    if case["verify_external"].as_bool() == Some(false) {
                        assert_ne!(report.external_objects_unchecked, [] as [[u8; 32]; 0]);
                        assert_eq!(report.external_objects_sha256_checked.len(), 0);
                    } else {
                        assert_ne!(report.external_objects_sha256_checked, [] as [[u8; 32]; 0]);
                        assert_eq!(report.external_objects_unchecked.len(), 0);
                    }
                }
                Ok(())
            }
            .await;
            compare_outcome(&case, result);
        }
    }
}
