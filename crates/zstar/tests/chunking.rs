#![allow(clippy::unused_async_trait_impl)] // In-memory fixtures implement the native async storage contract.
use sha2::{Digest, Sha256};
use std::io::Read;
use zstar::chunking::{Chunker, Chunking};

fn input(recipe: &serde_json::Value) -> Vec<u8> {
    let length = recipe["length"].as_u64().unwrap() as usize;
    let mut data = if recipe["kind"] == "zeros" {
        vec![0; length]
    } else {
        (0..length.div_ceil(32))
            .flat_map(|i| Sha256::digest((i as u64).to_le_bytes()).to_vec())
            .take(length)
            .collect()
    };
    if let Some(at) = recipe["insert"].as_u64() {
        data.splice(at as usize..at as usize, b"zstar-edit".iter().copied());
    }
    if let Some(at) = recipe["delete"].as_u64() {
        data.drain(at as usize..at as usize + 13);
    }
    data
}
#[test]
fn shared_vectors_match_and_short_reads_do_not_change_boundaries() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("../../../testdata/chunking/vectors.json")).unwrap();
    for case in vectors["cases"].as_array().unwrap() {
        let data = input(&case["recipe"]);
        let settings = Chunking::FastCdc {
            min: case["min"].as_u64().unwrap() as usize,
            target: case["target"].as_u64().unwrap() as usize,
            max: case["max"].as_u64().unwrap() as usize,
        };
        for step in [1, 7, 4096, 1 << 20] {
            struct Short<'a>(&'a [u8], usize);
            impl Read for Short<'_> {
                fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                    let size = bytes.len().min(self.1);
                    self.0.read(&mut bytes[..size])
                }
            }
            let mut source = Short(&data, step);
            let mut chunker = Chunker::new(settings).unwrap();
            let mut actual = Vec::new();
            let mut position = 0;
            while let Some(chunk) = chunker.next(&mut source).unwrap() {
                actual.push(serde_json::json!([
                    position,
                    chunk.len(),
                    hex::encode(Sha256::digest(&chunk))
                ]));
                assert_eq!(chunk, data[position..position + chunk.len()]);
                position += chunk.len();
            }
            assert_eq!(
                serde_json::Value::Array(actual),
                case["chunks"],
                "{} step {step}",
                case["name"]
            );
        }
    }
}
#[test]
fn invalid_settings_are_rejected_before_reading() {
    for strategy in [
        Chunking::Fixed { size: 0 },
        Chunking::Fixed { size: 17 << 20 },
        Chunking::FastCdc {
            min: 63,
            target: 256,
            max: 1024,
        },
        Chunking::FastCdc {
            min: 64,
            target: 250,
            max: 1024,
        },
    ] {
        assert!(Chunker::new(strategy).is_err());
    }
}
#[test]
fn input_errors_are_not_treated_as_eof() {
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("input failed"))
        }
    }
    assert!(
        Chunker::new(Chunking::default())
            .unwrap()
            .next(&mut Broken)
            .is_err()
    );
}
#[cfg(feature = "async")]
#[tokio::test]
async fn async_streaming_reconstructs_and_empty_files_stay_inline() {
    use zstar::chunking::{ObjectWriter, external_file};
    struct Writer(Vec<Vec<u8>>);
    impl ObjectWriter for Writer {
        async fn put_chunk(&mut self, raw: bytes::Bytes) -> zstar::Result<zstar::Extent> {
            let encoded = zstar::encode_frame(&raw, 3)?;
            let digest = *zstar::Digest::sha256(&raw).as_bytes();
            self.0.push(raw.to_vec());
            Ok(zstar::Extent {
                size: raw.len() as u64,
                source: zstar::Source::Object(digest),
                encoded_size: encoded.len() as u64,
                offset: 0,
            })
        }
    }
    let data = input(&serde_json::json!({"kind":"varied", "length":40000}));
    let mut writer = Writer(Vec::new());
    let extents = external_file(
        data.as_slice(),
        data.len() as u64,
        Chunking::FastCdc {
            min: 64,
            target: 256,
            max: 1024,
        },
        &mut writer,
    )
    .await
    .unwrap();
    assert_eq!(writer.0.concat(), data);
    assert_eq!(extents.len(), writer.0.len());
    assert_eq!(
        external_file(&b""[..], 0, Chunking::default(), &mut writer)
            .await
            .unwrap(),
        [] as [zstar::Extent; 0]
    );
    assert!(
        external_file(&b"short"[..], 6, Chunking::default(), &mut writer)
            .await
            .is_err()
    );
    assert!(
        external_file(&b"long"[..], 3, Chunking::default(), &mut writer)
            .await
            .is_err()
    );
}
