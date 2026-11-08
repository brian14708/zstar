use super::Builder as BlockingBuilder;
use super::*;
use crate::blocking::engine::BlockingArchive;
#[test]
fn root_extensions_validate_before_capture_and_roundtrip() {
    use ciborium::Value;
    for extensions in [
        BTreeMap::from([("unknown".into(), Value::Bool(true))]),
        BTreeMap::from([("x-null".into(), Value::Null)]),
        BTreeMap::from([(
            "x-duplicate".into(),
            Value::Map(vec![
                (Value::Text("a".into()), Value::Bool(true)),
                (Value::Text("a".into()), Value::Bool(false)),
            ]),
        )]),
    ] {
        assert!(matches!(
            BlockingBuilder::try_with_options(
                Vec::new(),
                WriterOptions {
                    root_extensions: extensions,
                    ..Default::default()
                }
            ),
            Err(Error::MalformedToc(_))
        ));
    }
    let extensions = BTreeMap::from([(
        "x-data".into(),
        Value::Array(vec![Value::Bytes(vec![9; 1024]), Value::Bool(true)]),
    )]);
    let writer = BlockingBuilder::try_with_options(
        Vec::new(),
        WriterOptions {
            root_extensions: extensions.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    let bytes = writer.finish().unwrap();
    let reader = BlockingArchive::open(
        crate::blocking::MemorySource::new(&bytes),
        ManifestOptions::default(),
    )
    .unwrap();
    assert_eq!(reader.manifest().root().extensions, extensions);
}
