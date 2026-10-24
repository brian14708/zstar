#![cfg(feature = "compression")]

use zstar::backend::BlockingArchive;

#[test]
fn content_cache_reuses_objects_across_files_and_enforces_verification_policy() {
    use zstar::backend::EncodedObjectResolver;
    struct Objects {
        bytes: Vec<u8>,
        calls: usize,
    }
    impl EncodedObjectResolver for Objects {
        fn get_encoded(&mut self, _: [u8; 32], _: u64, _: u64) -> zstar::Result<Vec<u8>> {
            self.calls += 1;
            Ok(self.bytes.clone())
        }
    }
    let bytes = include_bytes!("../../../testdata/entry-manifest/thin.zstar");
    let mut reader =
        BlockingArchive::open(zstar::SliceReader::new(bytes), Default::default()).unwrap();
    reader.set_content_cache_bytes(1024);
    let mut objects = Objects {
        bytes: zstar::encode_frame(b"hello\nworld\n", 3).unwrap(),
        calls: 0,
    };
    assert_eq!(
        reader
            .read("hello.txt", 0, 6, Some(&mut objects), false)
            .unwrap(),
        b"hello\n"
    );
    // Upgrade unchecked retained bytes and ignore recompression-dependent hints.
    assert_eq!(
        reader
            .read("world.txt", 0, 6, Some(&mut objects), true)
            .unwrap(),
        b"world\n"
    );
    let mut output = Vec::new();
    reader
        .open_file("repeat", None, true)
        .unwrap()
        .copy_to(&mut output)
        .unwrap();
    assert_eq!(output, b"hello\nhello\n");
    assert_eq!(objects.calls, 1);
    assert!(reader.content_cache_bytes() <= 1024);
    reader.set_content_cache_bytes(1024);
    objects.bytes = zstar::encode_frame(b"wrong\nbytes\n", 3).unwrap();
    assert_eq!(
        reader
            .read("hello.txt", 0, 6, Some(&mut objects), false)
            .unwrap(),
        b"wrong\n"
    );
    for _ in 0..2 {
        assert!(
            reader
                .read("world.txt", 0, 6, Some(&mut objects), true)
                .is_err()
        );
    }
    assert_eq!(objects.calls, 2);
    // A failed verification never promotes content. Unchecked bytes cannot be
    // reused by a subsequent call without a resolver namespace.
    assert!(reader.read("hello.txt", 0, 6, None, false).is_err());
    objects.bytes = zstar::encode_frame(b"other\nstore\n", 3).unwrap();
    assert_eq!(
        reader
            .read("hello.txt", 0, 6, Some(&mut objects), false)
            .unwrap(),
        b"other\n"
    );
    assert_eq!(objects.calls, 3);
    assert!(reader.content_cache_bytes() <= 1024);
    reader.set_content_cache_bytes(0);
    assert_eq!(reader.content_cache_bytes(), 0);
    assert!(reader.read("hello.txt", 0, 6, None, false).is_err());
}

#[test]
fn content_cache_reuses_archive_frames_and_evicts_with_a_byte_budget() {
    use zstar::{RangeReader, SliceReader};
    struct Counted<'a> {
        source: SliceReader<'a>,
        calls: usize,
    }
    impl RangeReader for Counted<'_> {
        fn len(&self) -> u64 {
            self.source.len()
        }
        fn read_range(&mut self, range: std::ops::Range<u64>) -> zstar::Result<Vec<u8>> {
            self.calls += 1;
            self.source.read_range(range)
        }
    }
    let bytes = include_bytes!("../../../testdata/entry-manifest/inline.zstar");
    let mut reader = BlockingArchive::open(
        Counted {
            source: SliceReader::new(bytes),
            calls: 0,
        },
        Default::default(),
    )
    .unwrap();
    reader.set_content_cache_bytes(1 << 20);
    let expected = reader.read("a-b", 0, 5, None, true).unwrap();
    for _ in 0..4 {
        assert_eq!(reader.read("a-b", 0, 5, None, true).unwrap(), expected);
    }
    let reads = reader.into_source().calls;
    let mut reader = BlockingArchive::open(
        Counted {
            source: SliceReader::new(bytes),
            calls: 0,
        },
        Default::default(),
    )
    .unwrap();
    reader.set_content_cache_bytes(1); // No complete frame fits.
    for _ in 0..5 {
        reader.read("a-b", 0, 5, None, true).unwrap();
    }
    assert_eq!(reader.content_cache_bytes(), 0);
    assert_eq!(reader.into_source().calls, reads + 4);
}

#[test]
fn content_cache_evicts_least_recently_used_complete_frames() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use zstar::{
        RangeReader, SliceReader,
        backend::BlockingBuilder,
        manifest::{Entry, EntryKind},
    };
    let mut writer =
        BlockingBuilder::with_options(Vec::new(), 1024, 3, Default::default()).unwrap();
    for name in ["a", "b"] {
        writer
            .add_file(
                Entry {
                    path: name.to_owned(),
                    kind: EntryKind::File,
                    mode: 420,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: Default::default(),
                },
                128,
                &mut [7; 128].as_slice(),
            )
            .unwrap();
    }
    let bytes = writer.finish().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    struct Counted<'a> {
        bytes: SliceReader<'a>,
        calls: Arc<AtomicUsize>,
    }
    impl RangeReader for Counted<'_> {
        fn len(&self) -> u64 {
            self.bytes.len()
        }
        fn read_range(&mut self, range: std::ops::Range<u64>) -> zstar::Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.bytes.read_range(range)
        }
    }
    let mut reader = BlockingArchive::open(
        Counted {
            bytes: SliceReader::new(&bytes),
            calls: calls.clone(),
        },
        Default::default(),
    )
    .unwrap();
    reader.set_content_cache_bytes(1280); // One 1 KiB frame plus bookkeeping.
    let before = calls.load(Ordering::Relaxed);
    for name in ["a", "b", "b", "a"] {
        assert_eq!(reader.read(name, 0, 1, None, true).unwrap(), [7]);
        assert!(reader.content_cache_bytes() <= 1280);
    }
    assert_eq!(calls.load(Ordering::Relaxed) - before, 3);
}
