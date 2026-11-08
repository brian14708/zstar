use super::Builder as BlockingBuilder;
use crate::blocking::MemorySource;
use crate::blocking::engine::BlockingArchive;

fn read<R: crate::RangeReader>(
    reader: &mut BlockingArchive<R>,
    path: &str,
    offset: u64,
    length: u64,
) -> crate::Result<Vec<u8>> {
    let plan = reader.plan(&[crate::ReadRequest {
        path,
        offset,
        length,
    }])?;
    let (mut outputs, _) = reader.execute_plan(plan, None, false)?;
    Ok(outputs.remove(0))
}
use crate::{Entry, EntryKind, Error};
use std::io::{self, Read, Write};
fn build(frame: usize, workers: usize) -> Vec<u8> {
    let mut writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: frame,
            compression_level: 3,
            workers,
            ..Default::default()
        },
    )
    .unwrap();
    for (name, bytes) in [("a", vec![]), ("b", vec![42; 8193]), ("雪", vec![7; 16385])] {
        writer
            .add_file(Entry::file(name), bytes.len() as u64, &mut bytes.as_slice())
            .unwrap();
    }
    writer.finish().unwrap()
}
#[test]
fn empty_symlink_targets_are_rejected_before_writing() {
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
    assert!(matches!(
        writer.add_metadata(Entry::symlink("link", "")),
        Err(Error::MalformedToc(_))
    ));
    assert!(writer.finish().is_err());
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
    writer
        .add_metadata(Entry::symlink("link", "../target"))
        .unwrap();
    let bytes = writer.finish().unwrap();
    BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
}
#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
#[test]
fn streaming_frames_and_payloads_cover_empty_split_and_unicode_members() {
    for frame in [512, 777, 1024, 4096] {
        let bytes = build(frame, 0);
        let mut reader = BlockingArchive::open(
            MemorySource::new(&bytes),
            crate::manifest_io::ManifestOptions::default(),
        )
        .unwrap();
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
                read(&mut reader, "b", offset, size).unwrap(),
                vec![42; size as usize]
            );
        }
        assert!(reader.manifest().entry("absent").is_none());
        assert!(matches!(read(&mut reader, "b", 8193, 1), Err(Error::Range)));
    }
}
#[test]
fn long_paths_exact_link_text_and_wide_metadata_survive_physical_validation() {
    let path = "雪".repeat(70);
    let target = format!(".././{}/missing", "target".repeat(50));
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
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
    let reader = BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();

    assert_eq!(
        reader.manifest().entry("link").unwrap().linkname,
        Some(target)
    );
    let e = reader.manifest().entry(&path).unwrap();
    assert_eq!((e.uid, e.gid, e.mtime_ns), (u64::MAX, u64::MAX, i64::MIN));
}
#[expect(
    clippy::items_after_statements,
    reason = "Keep local helpers next to the operation that uses them."
)]
#[test]
fn changed_lengths_duplicate_paths_and_output_failure_poison_the_writer() {
    for mut payload in [b"ab".as_slice(), b"abcd".as_slice()] {
        let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
        assert!(writer.add_file(Entry::file("a"), 3, &mut payload).is_err());
        assert!(
            writer
                .add_file(Entry::file("b"), 2, &mut b"ok".as_slice())
                .is_err()
        );
        assert!(writer.finish().is_err());
    }
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
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
    let mut writer = BlockingBuilder::try_with_options(
        Broken,
        crate::WriterOptions {
            frame_size: 1024,
            compression_level: 3,
            workers: 2,
            ..Default::default()
        },
    )
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
        BlockingBuilder::try_with_options(
            Vec::new(),
            crate::WriterOptions {
                workers: 17,
                ..Default::default()
            }
        )
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
    let mut writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: 1024,
            compression_level: 3,
            ..Default::default()
        },
    )
    .unwrap();
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
    let mut reader = BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
    assert_eq!(read(&mut reader, "file", 0, 4097).unwrap(), payload);
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
fn late_parents_and_reverse_insertion_keep_tar_order_and_sort_manifest() {
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
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
    let mut reader = BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();

    let mut physical = Vec::new();
    reader
        .visit_payloads(|e, _| {
            physical.push(e.path.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(physical, ["z/child", "a"]);
    assert_eq!(read(&mut reader, "z/child", 0, 1).unwrap(), b"z");
}

#[test]
fn writer_accepts_more_than_65536_inline_extents() {
    let size = 512 * 65_537;
    let mut writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: 512,
            compression_level: 1,
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .add_file(Entry::file("large"), size, &mut io::repeat(7).take(size))
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
    let entry = reader.manifest().entry("large").unwrap();
    assert_eq!(entry.extents.len(), 65_537);
    assert_eq!(entry.size().unwrap(), size);
    assert_eq!(read(&mut reader, "large", size - 1, 1).unwrap(), [7]);
}

#[test]
fn large_external_entries_and_standalone_references_round_trip() {
    let input = include_bytes!("../../../../testdata/entry-manifest/limits/many-extents.zstar");
    let reader = BlockingArchive::open(
        MemorySource::new(input),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
    let entry = reader.manifest().entry("file").unwrap();
    assert_eq!(entry.extents.len(), 65_537);
    let reference = crate::manifest::encode_external_reference(&entry.extents).unwrap();
    assert_eq!(
        crate::manifest::decode_external_reference(&reference).unwrap(),
        entry.extents
    );
    let mut writer = BlockingBuilder::try_new(Vec::new()).unwrap();
    writer.add_metadata(entry).unwrap();
    let bytes = writer.finish().unwrap();
    BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
}

#[test]
fn impossible_inline_metadata_is_rejected_before_reading_or_writing() {
    struct Unreadable;
    impl Read for Unreadable {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("preflight must not consume the body");
        }
    }
    let mut output = Vec::new();
    let mut writer = BlockingBuilder::try_with_options(
        &mut output,
        crate::WriterOptions {
            frame_size: 1 << 20,
            compression_level: 3,
            limits: crate::Limits {
                max_entry_bytes: 8 << 20,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        writer.add_file(Entry::file("huge"), 1 << 50, &mut Unreadable),
        Err(Error::Budget(_))
    ));
    drop(writer);
    assert_eq!(output, [] as [u8; 0]);
}

#[test]
fn frame_sizes_are_caller_policy() {
    let size = 17 << 20;
    let mut writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: 32 << 20,
            compression_level: 1,
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .add_file(Entry::file("large"), size, &mut io::repeat(9).take(size))
        .unwrap();
    let bytes = writer.finish().unwrap();
    let mut reader = BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
    assert_eq!(read(&mut reader, "large", size - 1, 1).unwrap(), [9]);

    let options = crate::manifest_io::ManifestOptions {
        limits: crate::Limits {
            max_frame_decoded_bytes: 16 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(matches!(
        BlockingArchive::open(MemorySource::new(&bytes), options),
        Err(Error::Budget(_))
    ));
    let options = crate::manifest_io::ManifestOptions {
        limits: crate::Limits {
            max_frame_decoded_bytes: 16 << 20,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(matches!(
        BlockingBuilder::try_with_options(
            Vec::new(),
            crate::WriterOptions {
                frame_size: 32 << 20,
                compression_level: 1,
                limits: options.limits,
                ..Default::default()
            }
        ),
        Err(Error::Budget(_))
    ));
}

#[test]
fn optional_extension_pages_round_trip() {
    let mut options = crate::MetadataWriterOptions::default();
    options.extension_pages.insert(
        "x-optional".into(),
        vec![
            ciborium::Value::Text("first".into()),
            ciborium::Value::Array(vec![ciborium::Value::Integer(7.into())]),
        ],
    );
    let writer = BlockingBuilder::try_with_options(
        Vec::new(),
        crate::WriterOptions {
            frame_size: 4096,
            compression_level: 3,
            metadata: options,
            ..Default::default()
        },
    )
    .unwrap();
    let bytes = writer.finish().unwrap();
    BlockingArchive::open(
        MemorySource::new(&bytes),
        crate::manifest_io::ManifestOptions::default(),
    )
    .unwrap();
}
