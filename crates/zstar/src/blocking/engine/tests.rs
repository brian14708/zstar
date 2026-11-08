use super::*;
use crate::blocking::Builder as BlockingBuilder;
use crate::blocking::{Archive, MemorySource, ReaderOptions};
use crate::manifest::encode_pointer;
use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;

/// Shared resolver state so tests can swap payloads and count fetches.
#[derive(Clone, Default)]
struct Objects(Rc<RefCell<(Vec<u8>, usize)>>);
impl Objects {
    fn new(encoded: Vec<u8>) -> Self {
        Self(Rc::new(RefCell::new((encoded, 0))))
    }
    fn calls(&self) -> usize {
        self.0.borrow().1
    }
}
impl EncodedObjectResolver for Objects {
    fn get_encoded(&mut self, _: [u8; 32], _: u64, _: u64) -> Result<Vec<u8>> {
        let mut state = self.0.borrow_mut();
        state.1 += 1;
        Ok(state.0.clone())
    }
}
fn open<B: AsRef<[u8]>>(
    bytes: B,
    resolver: Option<Objects>,
    verify_external: bool,
) -> Archive<MemorySource<B>> {
    Archive::open_with_options(
        MemorySource::new(bytes),
        ReaderOptions {
            verify_external,
            resolver: resolver.map(|r| Box::new(r) as Box<dyn EncodedObjectResolver>),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn shared_and_split_frames_without_a_frame_table() {
    let mut writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: 2048,
            compression_level: 3,
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .add_file(Entry::file("a"), 5, &mut b"hello".as_slice())
        .unwrap();
    writer
        .add_file(Entry::file("b"), 5, &mut b"world".as_slice())
        .unwrap();
    let large = vec![7; 10_000];
    writer
        .add_file(Entry::file("c"), large.len() as u64, &mut large.as_slice())
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = open(&bytes, None, true);
    assert_eq!(reader.read_range("a", 1..4).unwrap(), b"ell");
    assert_eq!(reader.read_range("b", 0..5).unwrap(), b"world");
    assert_eq!(reader.read_range("c", 0..10_000).unwrap(), large);
    let a = reader.entry_layout("a").unwrap().unwrap();
    let b = reader.entry_layout("b").unwrap().unwrap();
    assert_eq!(a.extents[0].source, b.extents[0].source);
    let mut decoded = Vec::new();
    reader
        .engine
        .visit_frames(|_, _, bytes| {
            decoded.extend_from_slice(bytes);
            Ok(())
        })
        .unwrap();
    assert_eq!(&decoded[decoded.len() - 1024..], &[0; 1024]);
    assert!(reader.read_range("c", 10_001..10_001).is_err());

    let mut streaming = Vec::new();
    reader
        .open_file("c")
        .unwrap()
        .read_to_end(&mut streaming)
        .unwrap();
    assert_eq!(streaming, large);
    assert!(matches!(
        reader.open_file("missing"),
        Err(Error::MissingEntry(path)) if path == "missing"
    ));
    assert!(matches!(
        reader.read_range("missing", 0..1),
        Err(Error::MissingEntry(_))
    ));
    assert!(matches!(
        reader.read_link("missing"),
        Err(Error::MissingEntry(_))
    ));
}

#[test]
fn file_stream_reuses_adjacent_objects_and_failure_cannot_skip_an_extent() {
    let bytes = include_bytes!("../../../../../testdata/entry-manifest/thin.zstar");
    let objects = Objects::new(crate::encode_frame(b"hello\nworld\n", 3).unwrap());
    let mut reader = open(bytes, Some(objects.clone()), true);
    let mut output = Vec::new();
    let copied = reader
        .open_file("repeat")
        .unwrap()
        .copy_to(&mut output)
        .unwrap();
    assert_eq!(copied, 12);
    assert_eq!(output, b"hello\nhello\n");
    assert_eq!(objects.calls(), 1);

    objects.0.borrow_mut().0 = b"bad codec".to_vec();
    let mut reader = open(bytes, Some(objects.clone()), false);
    let mut file = reader.open_file("repeat").unwrap();
    assert_eq!(file.read(&mut []).unwrap(), 0);
    let mut buffer = [0; 1];
    assert!(file.read(&mut buffer).is_err());
    assert!(file.read(&mut buffer).is_err());
    drop(file);
    assert_eq!(objects.calls(), 2);
}

#[test]
fn sequential_reads_decode_a_large_object_once() {
    let raw: Vec<u8> = (0..3 << 20).map(|i: u32| (i % 251) as u8).collect();
    let hash = *crate::Digest::sha256(&raw).as_bytes();
    let encoded = crate::encode_frame(&raw, 1).unwrap();
    let mut entry = Entry::file("large");
    entry.extents = vec![Extent {
        size: raw.len() as u64,
        source: Source::Object(hash),
        encoded_size: encoded.len() as u64,
        offset: 0,
    }];
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
    writer.add_metadata(entry).unwrap();
    let bytes = writer.finish().unwrap();
    let objects = Objects::new(encoded);
    let mut reader = open(&bytes, Some(objects.clone()), true);
    let mut output = Vec::new();
    let mut file = reader.open_file("large").unwrap();
    let mut buffer = [0; 8 << 10];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        output.extend_from_slice(&buffer[..count]);
    }
    assert_eq!(output, raw);
    assert_eq!(objects.calls(), 1);
}

/// Asserts that the stale encoded-size hint is passed through unchanged.
struct Stale(Vec<u8>);
impl EncodedObjectResolver for Stale {
    fn get_encoded(&mut self, _: [u8; 32], hint: u64, _: u64) -> Result<Vec<u8>> {
        assert_eq!(hint, 1);
        Ok(self.0.clone())
    }
}
#[test]
fn external_hint_can_be_stale_and_only_objects_are_hashed() {
    let raw = b"some shared object";
    let encoded = crate::encode_frame(raw, 3).unwrap();
    let hash = *crate::Digest::sha256(raw).as_bytes();
    let mut entry = Entry::file("external");
    entry.extents = vec![Extent {
        size: 6,
        source: Source::Object(hash),
        encoded_size: 1,
        offset: 5,
    }];
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
    writer.add_metadata(entry.clone()).unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = Archive::open_with_options(
        MemorySource::new(&bytes),
        ReaderOptions {
            verify_external: true,
            resolver: Some(Box::new(Stale(encoded))),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(reader.read_range("external", 0..6).unwrap(), &raw[5..11]);
    assert_eq!(
        crate::manifest::decode_pointer(&encode_pointer(&entry.extents).unwrap()).unwrap(),
        entry.extents
    );
}
