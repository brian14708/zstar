//! Consumer contracts: imports and operations work with no default features.
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::rc::Rc;
use zstar::blocking::{
    Archive, Builder, MemorySource, ObjectResolver, RangeReader, ReaderOptions, SeekSource,
};
use zstar::{Error, Header, ReadRequest, Result, WriterOptions};

fn contents() -> Vec<u8> {
    (0_u8..=255).cycle().take(11_111).collect()
}
fn encode(options: WriterOptions) -> Vec<u8> {
    let mut builder = Builder::try_with_options(Vec::new(), options).unwrap();
    builder.append_dir("資料").unwrap();
    builder.append_bytes("資料/empty", b"").unwrap();
    builder.append_bytes("資料/small", b"hello").unwrap();
    builder.append_bytes("資料/split", &contents()).unwrap();
    builder.append_symlink("link", "資料/split").unwrap();
    builder.finish().unwrap()
}

#[test]
fn supported_reads_listing_and_seek_cross_extents() {
    let bytes = encode(WriterOptions {
        frame_size: 2048,
        ..Default::default()
    });
    let mut archive = Archive::from_bytes(bytes).unwrap();
    let paths: Vec<_> = archive
        .entries()
        .map(|e| e.unwrap().path().to_owned())
        .collect();
    assert_eq!(
        paths,
        ["link", "資料", "資料/empty", "資料/small", "資料/split"]
    );
    assert_eq!(archive.entries_with_prefix("資料/s").count(), 2);
    assert_eq!(archive.read("資料/empty").unwrap(), b"");
    assert_eq!(archive.read("資料/small").unwrap(), b"hello");
    assert_eq!(archive.read_link("link").unwrap(), "資料/split");
    for path in ["link", "資料"] {
        assert!(matches!(
            archive.read_range(path, 0..0),
            Err(Error::Unsupported(_))
        ));
    }
    assert!(matches!(
        archive.read_range("missing", 0..0),
        Err(Error::MissingEntry(_))
    ));
    assert!(matches!(
        archive.read_range("資料/empty", 1..1),
        Err(Error::Range)
    ));
    let expected = contents();
    let mut file = archive.open_file("資料/split").unwrap();
    assert_eq!(file.size(), expected.len() as u64);
    file.seek(SeekFrom::Start(1300)).unwrap();
    let mut buffer = vec![0; 5000];
    file.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, expected[1300..6300]);
    file.seek(SeekFrom::End(-17)).unwrap();
    assert!(file.seek(SeekFrom::Current(i64::MIN)).is_err());
    assert_eq!(file.stream_position().unwrap(), expected.len() as u64 - 17);
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).unwrap();
    assert_eq!(tail, expected[expected.len() - 17..]);
    file.seek(SeekFrom::End(8)).unwrap();
    assert_eq!(file.read(&mut [0; 1]).unwrap(), 0);
    file.seek(SeekFrom::Start(u64::MAX)).unwrap();
    assert!(file.seek(SeekFrom::Current(1)).is_err());
    assert_eq!(file.stream_position().unwrap(), u64::MAX);
    drop(file);
    assert_eq!(archive.read("資料/split").unwrap(), expected);
}

struct Counted {
    memory: MemorySource<Vec<u8>>,
    calls: Rc<Cell<usize>>,
    short: Rc<Cell<bool>>,
}
impl RangeReader for Counted {
    fn len(&self) -> u64 {
        self.memory.len()
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        self.calls.set(self.calls.get() + 1);
        let mut bytes = self.memory.read_range(range)?;
        if self.short.get() {
            bytes.pop();
        }
        Ok(bytes)
    }
}
#[test]
fn non_send_sources_validate_batches_before_content_and_share_frames() {
    let calls = Rc::new(Cell::new(0));
    let short = Rc::new(Cell::new(false));
    let source = Counted {
        memory: MemorySource::new(encode(WriterOptions::default())),
        calls: calls.clone(),
        short: short.clone(),
    };
    let mut archive = Archive::open_with_options(
        source,
        ReaderOptions {
            max_read_bytes: 8,
            ..Default::default()
        },
    )
    .unwrap();
    calls.set(0);
    let requests = [
        ReadRequest {
            path: "資料/small",
            offset: 1,
            length: 2,
        },
        ReadRequest {
            path: "資料/small",
            offset: 0,
            length: 3,
        },
    ];
    assert_eq!(
        archive.read_many(&requests).unwrap(),
        [b"el".to_vec(), b"hel".to_vec()]
    );
    assert_eq!(calls.get(), 1);
    calls.set(0);
    let invalid = [
        requests[0],
        ReadRequest {
            path: "missing",
            offset: 0,
            length: 1,
        },
    ];
    assert!(matches!(
        archive.read_many(&invalid),
        Err(Error::MissingEntry(_))
    ));
    assert!(matches!(archive.read("資料/split"), Err(Error::Budget(_))));
    assert_eq!(calls.get(), 0);
    let (_, report) = archive.read_range_with_report("資料/small", 0..1).unwrap();
    assert!(report.metadata_validated);
    assert_eq!(report.data_frames_checked.len(), 1);
    assert_eq!(report.external_objects_unchecked, [] as [[u8; 32]; 0]);
    short.set(true);
    assert!(matches!(
        archive.read_range("資料/small", 0..1),
        Err(Error::MalformedFrame(_))
    ));
}

#[test]
fn stream_failure_after_progress_cannot_skip_the_failed_extent() {
    let short = Rc::new(Cell::new(false));
    let source = Counted {
        memory: MemorySource::new(encode(WriterOptions {
            frame_size: 2048,
            ..Default::default()
        })),
        calls: Rc::new(Cell::new(0)),
        short: short.clone(),
    };
    let mut archive = Archive::open_with_options(source, ReaderOptions::default()).unwrap();
    let mut file = archive.open_file("資料/split").unwrap();
    let mut buffer = [0; 4096];
    let count = file.read(&mut buffer).unwrap();
    assert!(count > 0 && count < buffer.len());
    short.set(true);
    assert!(file.read(&mut buffer).is_err());
    assert_eq!(file.stream_position().unwrap(), count as u64);
    short.set(false);
    file.rewind().unwrap();
    assert!(file.read(&mut buffer).is_err());
}

struct Interrupted {
    data: io::Cursor<Vec<u8>>,
    interrupt: bool,
}
impl Read for Interrupted {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.interrupt = !self.interrupt;
        if self.interrupt {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let count = out.len().min(3);
        self.data.read(&mut out[..count])
    }
}
impl Seek for Interrupted {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.data.seek(from)
    }
}
struct SharedCursor(Rc<RefCell<io::Cursor<Vec<u8>>>>);
impl Read for SharedCursor {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.0.borrow_mut().read(output)
    }
}
impl Seek for SharedCursor {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.0.borrow_mut().seek(from)
    }
}
#[test]
fn seek_adapter_handles_interrupted_short_reads_and_detects_truncation() {
    let bytes = encode(WriterOptions::default());
    let mut cursor = io::Cursor::new(bytes.clone());
    cursor.set_position(7);
    let source = SeekSource::new(Interrupted {
        data: cursor,
        interrupt: false,
    })
    .unwrap();
    let mut archive = Archive::open_with_options(source, ReaderOptions::default()).unwrap();
    assert_eq!(archive.read("資料/small").unwrap(), b"hello");
    let mut memory = MemorySource::new(bytes);
    assert!(memory.read_range(Range { start: 3, end: 2 }).is_err());
    assert!(memory.read_range(memory.len()..memory.len() + 1).is_err());
    let cursor = Rc::new(RefCell::new(io::Cursor::new(vec![1, 2, 3])));
    let mut source = SeekSource::new(SharedCursor(cursor.clone())).unwrap();
    assert_eq!(source.read_range(1..3).unwrap(), [2, 3]);
    cursor.borrow_mut().get_mut().truncate(2);
    assert!(
        matches!(source.read_range(0..3), Err(Error::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof)
    );
}

#[test]
fn member_cache_retention_is_bounded() {
    for size in [1024, 3 << 20] {
        let mut writer = Builder::try_with_options(
            Vec::new(),
            WriterOptions {
                frame_size: 4 << 20,
                ..Default::default()
            },
        )
        .unwrap();
        writer.append_bytes("file", &vec![42; size]).unwrap();
        let calls = Rc::new(Cell::new(0));
        let source = Counted {
            memory: MemorySource::new(writer.finish().unwrap()),
            calls: calls.clone(),
            short: Rc::new(Cell::new(false)),
        };
        let mut archive = Archive::open(source).unwrap();
        calls.set(0);
        let mut file = archive.open_file("file").unwrap();
        file.read_exact(&mut [0; 1]).unwrap();
        file.read_exact(&mut [0; 1]).unwrap();
        // Sequential reads keep the current source regardless of size.
        assert_eq!(calls.get(), 1);
        // Only sources within the budget survive a seek.
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut [0; 1]).unwrap();
        assert_eq!(calls.get(), if size < 2 << 20 { 1 } else { 2 });
    }
}

struct FlushSink {
    bytes: Vec<u8>,
    flushed: Rc<Cell<bool>>,
}
impl Write for FlushSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flushed.set(true);
        Ok(())
    }
}
#[test]
fn writer_flushes_and_failed_bodies_cannot_finish() {
    let flushed = Rc::new(Cell::new(false));
    let writer = Builder::try_new(FlushSink {
        bytes: Vec::new(),
        flushed: flushed.clone(),
    })
    .unwrap();
    writer.finish().unwrap();
    assert!(flushed.get());
    for body in [b"short".as_slice(), b"oversized body".as_slice()] {
        let mut writer = Builder::try_new(Vec::new()).unwrap();
        assert!(writer.append_data(Header::file("a", 8), body).is_err());
        assert!(writer.append_bytes("b", b"ok").is_err());
        assert!(writer.finish().is_err());
    }
}

#[test]
fn append_contract_violations_poison_the_writer() {
    type Misuse = fn(&mut Builder<Vec<u8>>) -> Result<()>;
    let misuses: [Misuse; 3] = [
        |w| w.append_data(Header::directory("d"), b"".as_slice()),
        |w| w.append_header(Header::file("f", 1)),
        |w| w.append_external(Header::directory("d"), Vec::new()),
    ];
    for misuse in misuses {
        let mut writer = Builder::try_new(Vec::new()).unwrap();
        assert!(matches!(misuse(&mut writer), Err(Error::Unsupported(_))));
        assert!(writer.append_bytes("b", b"ok").is_err());
        assert!(writer.finish().is_err());
    }
}

#[test]
fn pointer_budget_failure_poisons_the_writer() {
    let mut writer = Builder::try_with_options(
        Vec::new(),
        WriterOptions {
            limits: zstar::Limits {
                max_pointer_bytes: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let extents = vec![zstar::layout::Extent {
        size: 1,
        source: zstar::layout::Source::Object([0; 32]),
        encoded_size: 1,
        offset: 0,
    }];
    assert!(matches!(
        writer.append_external(Header::file("f", 1), extents),
        Err(Error::Budget(_))
    ));
    assert!(writer.append_bytes("next", b"data").is_err());
    assert!(writer.finish().is_err());
}

#[test]
fn externalized_empty_file_is_an_empty_regular_file() {
    let mut writer = Builder::try_new(Vec::new()).unwrap();
    writer
        .append_external(Header::file("empty", 0), Vec::new())
        .unwrap();
    let mut archive = Archive::from_bytes(writer.finish().unwrap()).unwrap();
    assert_eq!(archive.read("empty").unwrap(), b"");
}

struct Missing;
impl ObjectResolver for Missing {
    fn get_encoded(&mut self, address: [u8; 32], _: u64, _: u64) -> Result<Vec<u8>> {
        Err(Error::MissingObject(address))
    }
}
#[test]
fn external_policies_validate_whole_declared_selections_and_missing_objects() {
    let bytes = include_bytes!("../../../testdata/entry-manifest/thin.zstar");
    let mut archive = Archive::open_with_options(
        MemorySource::new(bytes),
        ReaderOptions {
            resolver: Some(Box::new(Missing)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        archive.read_range("repeat", 0..1),
        Err(Error::MissingObject(_))
    ));
    let empty: Vec<zstar::EntryMetadata> = archive
        .entries_with_prefix("nonexistent")
        .collect::<Result<_>>()
        .unwrap();
    assert_eq!(empty, Vec::<zstar::EntryMetadata>::new());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn both_facades_emit_identical_bytes_and_read_reports() {
    use std::sync::Arc;
    use zstar::{Archive as AsyncArchive, Builder as AsyncBuilder};
    for workers in [0, 2] {
        let options = WriterOptions {
            workers,
            frame_size: 2048,
            metadata: zstar::MetadataWriterOptions {
                block_target: 512,
                extension_pages: std::collections::BTreeMap::from([(
                    "x-notes".into(),
                    vec![ciborium::Value::Text("shared".into())],
                )]),
                ..Default::default()
            },
            root_extensions: std::collections::BTreeMap::from([(
                "x-version".into(),
                ciborium::Value::Integer(1.into()),
            )]),
            ..Default::default()
        };
        let expected = encode(options.clone());
        let mut builder = AsyncBuilder::try_with_options(Vec::new(), options)
            .await
            .unwrap();
        builder.append_dir("資料").await.unwrap();
        builder.append_bytes("資料/empty", b"").await.unwrap();
        builder.append_bytes("資料/small", b"hello").await.unwrap();
        builder
            .append_bytes("資料/split", &contents())
            .await
            .unwrap();
        builder.append_symlink("link", "資料/split").await.unwrap();
        assert_eq!(builder.finish().await.unwrap(), expected);
        let mut blocking = Archive::from_bytes(expected.clone()).unwrap();
        let asynchronous = AsyncArchive::open(Arc::new(bytes::Bytes::from(expected)))
            .await
            .unwrap();
        for (path, offset, length) in [
            ("資料/empty", 0, 0),
            ("資料/small", 1, 3),
            ("資料/split", 1700, 5000),
        ] {
            let (a, mut ar) = blocking
                .read_range_with_report(path, offset..offset + length)
                .unwrap();
            let (b, mut br) = asynchronous
                .read_range_with_report(path, offset..offset + length)
                .await
                .unwrap();
            assert_eq!(a, b);
            ar.data_frames_checked.sort_unstable();
            br.data_frames_checked.sort_unstable();
            assert_eq!(ar.data_frames_checked, br.data_frames_checked);
            assert_eq!(ar.metadata_validated, br.metadata_validated);
        }
    }
}
