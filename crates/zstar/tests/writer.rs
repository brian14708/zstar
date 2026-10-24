#![cfg(feature = "compression")]
use std::io::{self, Read, Write};
use zstar::backend::{BlockingArchive, BlockingBuilder};
use zstar::{Entry, EntryKind, Error, SliceReader};
fn build(frame: usize, workers: usize) -> Vec<u8> {
    let mut writer = BlockingBuilder::with_options(Vec::new(), frame, 3, Default::default())
        .unwrap()
        .with_workers(workers)
        .unwrap();
    for (name, bytes) in [("a", vec![]), ("b", vec![42; 8193]), ("雪", vec![7; 16385])] {
        writer
            .add_file(Entry::file(name), bytes.len() as u64, &mut bytes.as_slice())
            .unwrap();
    }
    writer.finish().unwrap()
}
#[test]
fn streaming_frames_and_payloads_cover_empty_split_and_unicode_members() {
    for frame in [512, 777, 1024, 4096] {
        let bytes = build(frame, 0);
        let mut reader =
            BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
        reader
            .visit_frames(|_, _, raw| {
                assert!(raw.len() <= frame);
                Ok(())
            })
            .unwrap();
        let mut payloads = std::collections::BTreeMap::<String, Vec<u8>>::new();
        reader
            .visit_payloads(|e, raw| {
                payloads.entry(e.path.clone()).or_default().extend(raw);
                Ok(())
            })
            .unwrap();
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads["a"], [] as [u8; 0]);
        assert_eq!(payloads["b"], vec![42; 8193]);
        assert_eq!(payloads["雪"], vec![7; 16385]);
        for (offset, size) in [(0, 512), (512, 512), (1000, 2048), (8192, 1), (0, 8193)] {
            assert_eq!(
                reader.read("b", offset, size, None, true).unwrap(),
                vec![42; size as usize]
            );
        }
        assert!(reader.entry("absent").unwrap().is_none());
        assert!(matches!(
            reader.read("b", 8193, 1, None, true),
            Err(Error::Range)
        ));
    }
}
#[test]
fn long_paths_exact_link_text_and_wide_metadata_survive_physical_validation() {
    let path = "雪".repeat(70);
    let target = format!(".././{}/missing", "target".repeat(50));
    let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
    let mut link = Entry::file("link");
    link.kind = EntryKind::Symlink;
    link.linkname = Some(target.clone());
    writer.add_metadata(link).unwrap();
    let mut file = Entry::file(path.clone());
    file.uid = u64::MAX;
    file.gid = u64::MAX;
    file.mtime_ns = i64::MIN;
    writer
        .add_file(file, 7, &mut b"unicode".as_slice())
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    reader.validate().unwrap();
    assert_eq!(reader.read_link("link").unwrap(), target);
    let e = reader.entry(&path).unwrap().unwrap();
    assert_eq!((e.uid, e.gid, e.mtime_ns), (u64::MAX, u64::MAX, i64::MIN));
}
#[test]
fn changed_lengths_duplicate_paths_and_output_failure_poison_the_writer() {
    for mut payload in [b"ab".as_slice(), b"abcd".as_slice()] {
        let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
        assert!(writer.add_file(Entry::file("a"), 3, &mut payload).is_err());
        assert!(writer.finish().is_err());
    }
    let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
    writer
        .add_file(Entry::file("a"), 0, &mut [].as_slice())
        .unwrap();
    writer
        .add_file(Entry::file("a"), 0, &mut [].as_slice())
        .unwrap();
    assert!(writer.finish().is_err());
    struct Broken;
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("broken output"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BlockingBuilder::with_options(Broken, 1024, 3, Default::default())
        .unwrap()
        .with_workers(2)
        .unwrap();
    assert!(
        writer
            .add_file(Entry::file("a"), 32768, &mut vec![0; 32768].as_slice())
            .is_err()
    );
    assert!(writer.finish().is_err());
}
#[test]
fn parallel_compression_preserves_serial_bytes_and_bounds_worker_count() {
    let serial = build(1024, 0);
    for workers in [1, 2, 4] {
        assert_eq!(build(1024, workers), serial);
    }
    assert!(
        BlockingBuilder::new(Vec::new())
            .unwrap()
            .with_workers(17)
            .is_err()
    );
}
#[test]
fn interrupted_short_payload_reads_work_and_callbacks_can_stop() {
    struct Short<'a> {
        bytes: &'a [u8],
        interrupt: bool,
    }
    impl Read for Short<'_> {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let count = out.len().min(7);
            self.bytes.read(&mut out[..count])
        }
    }
    let payload = vec![42; 4097];
    let mut writer =
        BlockingBuilder::with_options(Vec::new(), 1024, 3, Default::default()).unwrap();
    writer
        .add_file(
            Entry::file("file"),
            4097,
            &mut Short {
                bytes: &payload,
                interrupt: false,
            },
        )
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    assert_eq!(reader.read("file", 0, 4097, None, true).unwrap(), payload);
    let mut calls = 0;
    assert!(matches!(
        reader.visit_frames(|_, _, _| {
            calls += 1;
            Err(Error::Unsupported("stopped"))
        }),
        Err(Error::Unsupported("stopped"))
    ));
    assert_eq!(calls, 1);
}
#[test]
fn pax_headers_split_at_unaligned_frame_boundaries_remain_exact() {
    for frame in [513, 523, 777] {
        let mut writer =
            BlockingBuilder::with_options(Vec::new(), frame, 3, Default::default()).unwrap();
        let mut link = Entry::file("link");
        link.kind = EntryKind::Symlink;
        link.linkname = Some("雪".repeat(70));
        writer.add_metadata(link).unwrap();
        let mut file = Entry::file("雪".repeat(70));
        file.uid = u64::MAX;
        file.mtime_ns = i64::MIN;
        writer
            .add_file(file, 4097, &mut vec![42; 4097].as_slice())
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader =
            BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
        reader.validate().unwrap();
    }
}

#[test]
fn late_parents_and_reverse_insertion_keep_tar_order_and_sort_manifest() {
    let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
    writer
        .add_file(Entry::file("z/child"), 1, &mut b"z".as_slice())
        .unwrap();
    let mut parent = Entry::file("z");
    parent.kind = EntryKind::Directory;
    writer.add_metadata(parent).unwrap();
    writer
        .add_file(Entry::file("a"), 1, &mut b"a".as_slice())
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    reader.validate().unwrap();
    let mut physical = Vec::new();
    reader
        .visit_payloads(|e, _| {
            physical.push(e.path.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(physical, ["z/child", "a"]);
    assert_eq!(reader.read("z/child", 0, 1, None, true).unwrap(), b"z");
}
