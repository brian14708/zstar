use std::io::{Cursor, Read, Seek, SeekFrom};
use zstar::blocking::{Archive, Builder, ReaderOptions};
use zstar::{BudgetResource, Error, Header, Limits, WriterOptions};

fn fixture() -> Vec<u8> {
    let mut writer =
        Builder::try_with_options(Vec::new(), WriterOptions::bounded().with_frame_size(512))
            .unwrap();
    writer.append_dir("data").unwrap();
    let header = Header::file("data/file", 2048)
        .with_mode(0o640)
        .with_uid(42)
        .with_gid(13)
        .with_mtime_ns(-123);
    writer
        .append_data(header, vec![7; 2048].as_slice())
        .unwrap();
    writer.append_symlink("link", "data/file").unwrap();
    writer.finish().unwrap()
}

#[test]
fn metadata_and_positional_reads_work_with_a_seekable_reader() {
    let mut archive =
        Archive::from_reader_with_options(Cursor::new(fixture()), ReaderOptions::bounded())
            .unwrap();
    let metadata = archive.entry("data/file").unwrap().unwrap();
    assert_eq!(metadata.size(), 2048);
    assert_eq!(metadata.path(), "data/file");
    assert_eq!(
        (
            metadata.mode(),
            metadata.uid(),
            metadata.gid(),
            metadata.mtime_ns()
        ),
        (0o640, 42, 13, -123)
    );
    assert_eq!(
        archive
            .entries_with_prefix("data/")
            .next()
            .unwrap()
            .unwrap(),
        metadata
    );
    assert_eq!(
        archive.entry("link").unwrap().unwrap().linkname(),
        Some("data/file")
    );
    archive.validate_all_metadata().unwrap();
    let mut file = archive.open_file("data/file").unwrap();
    assert_eq!(file.metadata(), &metadata);
    file.seek(SeekFrom::Start(1200)).unwrap();
    assert_eq!(file.read_range(500..1100).unwrap(), vec![7; 600]);
    assert_eq!(file.stream_position().unwrap(), 1200);
    assert!(matches!(file.read_range(2048..2049), Err(Error::Range)));
    assert_eq!(file.stream_position().unwrap(), 1200);
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).unwrap();
    assert_eq!(tail, vec![7; 848]);
}

#[test]
fn options_are_validated_before_source_io_and_budget_errors_are_typed() {
    struct NoIo;
    impl zstar::blocking::RangeReader for NoIo {
        fn len(&self) -> u64 {
            panic!("invalid options must fail before source I/O")
        }
        fn read_range(&mut self, _: std::ops::Range<u64>) -> zstar::Result<Vec<u8>> {
            panic!("invalid options must fail before source I/O")
        }
    }
    assert!(matches!(
        Archive::open_with_options(NoIo, ReaderOptions::default().with_max_read_bytes(0)),
        Err(Error::InvalidOptions {
            option: "max_read_bytes",
            ..
        })
    ));
    for options in [
        WriterOptions::default().with_workers(17),
        WriterOptions::default().with_frame_size(1),
        WriterOptions::default().with_compression_level(i32::MAX),
    ] {
        assert!(matches!(
            options.validate(),
            Err(Error::InvalidOptions { .. })
        ));
    }
    let error = WriterOptions::bounded()
        .with_limits(Limits::bounded().with_max_frame_decoded_bytes(512))
        .validate()
        .unwrap_err();
    let Error::Budget(details) = error else {
        panic!("expected a resource limit")
    };
    assert_eq!(details.resource, BudgetResource::DecodedFrameBytes);
    assert_eq!(details.limit, Some(512));
    assert_eq!(details.requested, Some(1 << 20));
}

#[test]
fn external_append_uses_headers_and_checks_declared_size() {
    let mut writer = Builder::try_new(Vec::new()).unwrap();
    writer
        .append_external(Header::file("empty", 0).with_mode(0o600), Vec::new())
        .unwrap();
    let archive = Archive::from_bytes(writer.finish().unwrap()).unwrap();
    assert_eq!(archive.entry("empty").unwrap().unwrap().mode(), 0o600);
    for header in [
        Header::file("wrong-size", 1),
        Header::directory("wrong-kind"),
    ] {
        let mut writer = Builder::try_new(Vec::new()).unwrap();
        assert!(writer.append_external(header, Vec::new()).is_err());
        assert!(writer.finish().is_err());
    }
}

#[test]
fn positional_reads_reuse_the_blocking_member_cache() {
    use std::{cell::Cell, rc::Rc};
    struct Source {
        inner: zstar::blocking::MemorySource<Vec<u8>>,
        calls: Rc<Cell<usize>>,
    }
    impl zstar::blocking::RangeReader for Source {
        fn len(&self) -> u64 {
            self.inner.len()
        }
        fn read_range(&mut self, range: std::ops::Range<u64>) -> zstar::Result<Vec<u8>> {
            self.calls.set(self.calls.get() + 1);
            self.inner.read_range(range)
        }
    }
    let calls = Rc::new(Cell::new(0));
    let source = Source {
        inner: zstar::blocking::MemorySource::new(fixture()),
        calls: calls.clone(),
    };
    let mut archive = Archive::open(source).unwrap();
    let mut file = archive.open_file("data/file").unwrap();
    assert_eq!(file.read_range(0..1).unwrap(), [7]);
    let count = calls.get();
    assert_eq!(file.read_range(1..2).unwrap(), [7]);
    let mut output = [0];
    file.read_exact(&mut output).unwrap();
    assert_eq!(output, [7]);
    assert_eq!(calls.get(), count);
    assert_eq!(file.stream_position().unwrap(), 1);
}

#[test]
fn external_headers_roundtrip_nonempty_content_and_metadata() {
    struct Objects(Vec<u8>);
    impl zstar::blocking::ObjectResolver for Objects {
        fn get_encoded(&mut self, _: [u8; 32], _: u64, _: u64) -> zstar::Result<Vec<u8>> {
            Ok(self.0.clone())
        }
    }
    let (address, encoded) = zstar::backend::encode_object(b"hello", 3, Limits::bounded()).unwrap();
    let extents = vec![zstar::layout::Extent {
        size: 5,
        source: zstar::layout::Source::Object(address),
        encoded_size: encoded.len() as u64,
        offset: 0,
    }];
    let mut writer = Builder::try_new(Vec::new()).unwrap();
    writer
        .append_external(Header::file("external", 5).with_uid(42), extents)
        .unwrap();
    let mut archive = Archive::open_with_options(
        zstar::blocking::MemorySource::new(writer.finish().unwrap()),
        ReaderOptions::bounded()
            .with_resolver(Box::new(Objects(encoded)))
            .with_verify_external(true),
    )
    .unwrap();
    let metadata = archive.entry("external").unwrap().unwrap();
    assert!(metadata.is_external());
    assert_eq!((metadata.size(), metadata.uid()), (5, 42));
    let (bytes, report) = archive.read_with_report("external").unwrap();
    assert_eq!(bytes, b"hello");
    assert_eq!(report.external_objects_sha256_checked, vec![address]);
}

#[cfg(feature = "fs")]
#[test]
fn local_path_constructor_and_filesystem_errors_preserve_io_details() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.zstar");
    std::fs::write(&path, fixture()).unwrap();
    let archive = Archive::open_path(&path).unwrap();
    assert_eq!(archive.entry_count(), 3);
    let error = zstar::fs::pack(&dir.path().join("missing")).unwrap_err();
    let Error::Io(source) = error else {
        panic!("filesystem error lost its type")
    };
    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn owned_file_positional_reads_survive_archive_drop_and_preserve_position() {
    use futures_util::TryStreamExt;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let bytes = fixture();
    let blocking = Archive::from_bytes(&bytes).unwrap();
    let archive = zstar::Archive::from_bytes(bytes.clone()).await.unwrap();
    let entries: Vec<_> = archive.entries().try_collect().await.unwrap();
    assert_eq!(
        entries,
        blocking
            .entries()
            .collect::<zstar::Result<Vec<_>>>()
            .unwrap()
    );
    archive.validate_all_metadata().await.unwrap();
    let entries_after_scan: Vec<_> = archive
        .entries_with_prefix("data/")
        .try_collect()
        .await
        .unwrap();
    assert_eq!(entries_after_scan, vec![entries[1].clone()]);
    let mut file = archive.open_file("data/file").await.unwrap();
    drop(archive);
    file.seek(SeekFrom::Start(1200)).await.unwrap();
    assert_eq!(
        tokio::spawn(file.read_range(0..2)).await.unwrap().unwrap(),
        [7; 2].as_slice()
    );
    let (first, second) = tokio::join!(file.read_range(0..2), file.read_range(500..1100));
    assert_eq!(first.unwrap(), [7; 2].as_slice());
    assert_eq!(second.unwrap(), vec![7; 600]);
    assert!(matches!(
        file.read_range(2048..2049).await,
        Err(Error::Range)
    ));
    assert_eq!(file.stream_position().await.unwrap(), 1200);
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, vec![7; 848]);
}

#[cfg(all(feature = "fs", feature = "async"))]
#[tokio::test]
async fn async_local_path_constructor_applies_reader_options() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.zstar");
    std::fs::write(&path, fixture()).unwrap();
    let archive = zstar::Archive::open_path_with_options(
        &path,
        zstar::ReaderOptions::bounded().with_max_read_bytes(1),
    )
    .await
    .unwrap();
    assert_eq!(
        archive.read_range("data/file", 0..1).await.unwrap(),
        [7].as_slice()
    );
    assert!(matches!(
        archive.read("data/file").await,
        Err(Error::Budget(_))
    ));
}

#[cfg(feature = "async")]
#[tokio::test]
async fn metadata_streams_own_the_archive_and_prefix_and_can_move_between_tasks() {
    use futures_util::TryStreamExt;
    let archive = zstar::Archive::from_bytes(fixture()).await.unwrap();
    let all = archive.entries();
    let selected = {
        let prefix = String::from("data/");
        archive.entries_with_prefix(&prefix)
    };
    drop(archive);
    let entries = tokio::spawn(async move { all.try_collect::<Vec<_>>().await })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entries.len(), 3);
    let selected = tokio::spawn(async move { selected.try_collect::<Vec<_>>().await })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected, vec![entries[1].clone()]);
}
