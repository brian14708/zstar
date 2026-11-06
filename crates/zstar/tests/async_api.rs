#![cfg(feature = "async")]

use bytes::Bytes;
use futures_util::TryStreamExt;
use std::io::{self, SeekFrom};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use zstar::{
    Archive, AsyncRangeReader, Builder, Error, Header, ObjectResolver, ReadRequest, ReaderOptions,
    Result, VerificationScope, WriterOptions,
};

fn payload() -> Vec<u8> {
    let mut state = 7_u32;
    (0..8193)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect()
}

async fn fixture() -> Bytes {
    let mut builder = Builder::with_options(
        Vec::new(),
        WriterOptions {
            frame_size: 1024,
            workers: 2,
            ..Default::default()
        },
    );
    let mut header = Header::file("dir/file", payload().len() as u64);
    header.set_mode(0o640);
    header.set_uid(42);
    header.set_gid(13);
    header.set_mtime_ns(-123);
    header.set_attributes(std::collections::BTreeMap::from([(
        "x-note".into(),
        ciborium::Value::Text("retained".into()),
    )]));
    // Members may precede their explicit ancestors; the index is sorted at finish.
    builder
        .append_data(header, payload().as_slice())
        .await
        .unwrap();
    let mut directory = Header::directory("dir");
    directory.set_mode(0o750);
    builder.append_header(directory).await.unwrap();
    builder.append_symlink("link", "../dir/file").await.unwrap();
    builder.append_bytes("empty", b"").await.unwrap();
    builder.finish().await.unwrap().into()
}

#[tokio::test]
async fn tar_style_roundtrip_ranges_metadata_and_verification() {
    let archive = Archive::from_bytes(fixture().await).await.unwrap();
    assert_eq!(archive.entry_count(), 4);
    assert_eq!(archive.read("dir/file").await.unwrap(), payload());
    assert_eq!(
        archive.read_range("dir/file", 1000..1100).await.unwrap(),
        payload()[1000..1100]
    );
    assert!(archive.read("empty").await.unwrap().is_empty());
    assert!(matches!(
        archive.read("missing").await,
        Err(Error::MissingEntry(_))
    ));
    assert!(matches!(
        archive.read_range("dir/file", 0..8194).await,
        Err(Error::Range)
    ));
    assert!(matches!(
        archive
            .read_range("dir/file", Range { start: 2, end: 1 })
            .await,
        Err(Error::Range)
    ));
    assert!(archive.open_file("dir").await.is_err());
    assert_eq!(archive.read_link("link").await.unwrap(), "../dir/file");
    let file = archive.open_file("dir/file").await.unwrap();
    assert_eq!(file.metadata().mode, 0o640);
    assert_eq!(file.metadata().uid, 42);
    assert_eq!(file.metadata().gid, 13);
    assert_eq!(file.metadata().mtime_ns, -123);
    assert_eq!(
        file.metadata().attributes["x-note"],
        ciborium::Value::Text("retained".into())
    );
    assert_eq!(archive.entry("dir").await.unwrap().unwrap().mode, 0o750);
    let (_, report) = archive
        .read_range_with_report("dir/file", 1000..1100)
        .await
        .unwrap();
    assert!(report.metadata_authenticated);
    assert!(!report.full_archive_validated);
    assert_ne!(report.data_frames_checked, [] as [u64; 0]);
    let report = archive.verify(VerificationScope::Archive).await.unwrap();
    assert_eq!(report.scope, VerificationScope::Archive);
    assert_eq!(report.tar_members, 4);
    assert_eq!(report.pointer_members, 0);
    assert!(report.data_frames > 1);
}

#[tokio::test]
async fn handles_seek_across_frames_and_survive_archive_drop() {
    let archive = Archive::from_bytes(fixture().await).await.unwrap();
    let mut file = archive.open_file("dir/file").await.unwrap();
    drop(archive);
    assert_eq!(file.seek(SeekFrom::Start(1000)).await.unwrap(), 1000);
    let mut buffer = [0; 1200];
    file.read_exact(&mut buffer).await.unwrap();
    assert_eq!(buffer, payload()[1000..2200]);
    assert_eq!(file.seek(SeekFrom::Current(-1200)).await.unwrap(), 1000);
    assert_eq!(file.seek(SeekFrom::End(-3)).await.unwrap(), 8190);
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, payload()[8190..]);
    assert!(file.seek(SeekFrom::End(-8194)).await.is_err());
    assert_eq!(file.stream_position().await.unwrap(), 8193);
    file.seek(SeekFrom::Start(u64::MAX)).await.unwrap();
    assert_eq!(file.read(&mut buffer).await.unwrap(), 0);
    file.seek(SeekFrom::Start(0)).await.unwrap();
    tail.clear();
    file.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, payload());
}

#[tokio::test]
async fn standalone_cursors_read_retained_entries_across_extents() {
    let archive = Archive::from_bytes(fixture().await).await.unwrap();
    let mut entry = archive.entry("dir/file").await.unwrap().unwrap();
    // Reads must use the retained entry even when its path is not indexed.
    entry.path = "retained-entry".into();
    let cursor = zstar::FileCursor::new(entry).unwrap();
    let expected = payload();
    for (offset, length) in [(0, 1), (500, 2048), (1000, 6000), (8190, 3), (8193, 0)] {
        assert_eq!(
            cursor
                .read(&archive, offset as u64, length as u64)
                .await
                .unwrap(),
            expected[offset..offset + length]
        );
    }
    assert!(matches!(
        cursor.read(&archive, 8193, 1).await,
        Err(Error::Range)
    ));
}

#[tokio::test]
async fn archive_cursors_reject_other_archives() {
    let bytes = fixture().await;
    let archive = Archive::from_bytes(bytes.clone()).await.unwrap();
    let other = Archive::from_bytes(bytes).await.unwrap();
    let entry = archive.entry("dir/file").await.unwrap().unwrap();
    for cursor in [
        archive.open_cursor("dir/file").await.unwrap(),
        archive.cursor_from_entry(entry).await.unwrap(),
    ] {
        assert!(matches!(
            cursor.read(&other, 0, 1).await,
            Err(Error::Unsupported(_))
        ));
        let (first, second) = tokio::join!(
            cursor.read(&archive, 0, 1000),
            cursor.read(&archive, 500, 2048),
        );
        assert_eq!(first.unwrap(), payload()[..1000]);
        assert_eq!(second.unwrap(), payload()[500..2548]);
    }
}

#[tokio::test]
async fn owned_stream_combinators_and_large_seek_indexes() {
    let bytes = payload().repeat(16);
    let mut builder = Builder::with_options(
        Vec::new(),
        WriterOptions {
            frame_size: 1024,
            ..Default::default()
        },
    );
    builder.append_bytes("file", &bytes).await.unwrap();
    let archive = Archive::from_bytes(builder.finish().await.unwrap())
        .await
        .unwrap();
    let stream = archive.entries();
    drop(archive);
    let mut files: Vec<_> = stream.try_collect().await.unwrap();
    let file = &mut files[0];
    assert!(file.metadata().extents.len() > 64);
    file.seek(SeekFrom::End(-100)).await.unwrap();
    let mut tail = Vec::new();
    file.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, bytes[bytes.len() - 100..]);
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
#[tokio::test]
async fn verify_finds_corruption_outside_a_selected_read() {
    let mut builder = Builder::with_options(
        Vec::new(),
        WriterOptions {
            frame_size: 1024,
            ..Default::default()
        },
    );
    builder.append_bytes("first", b"hello").await.unwrap();
    builder.append_bytes("last", &payload()).await.unwrap();
    let mut bytes = builder.finish().await.unwrap();
    let footer = zstar::parse_footer(&bytes[bytes.len() - 80..]).unwrap();
    bytes[footer.toc_offset as usize - 1] ^= 1;
    let archive = Archive::from_bytes(bytes).await.unwrap();
    assert_eq!(archive.read("first").await.unwrap(), b"hello".as_slice());
    assert!(archive.verify(VerificationScope::Archive).await.is_err());
}

struct Objects {
    encoded: Bytes,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ObjectResolver for Objects {
    async fn get_encoded(&self, _: [u8; 32], _: u64, _: u64) -> Result<Bytes> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.encoded.clone())
    }
}

#[tokio::test]
async fn configured_resolver_applies_to_reads_handles_batches_and_closure() {
    let objects = Arc::new(Objects {
        encoded: zstar::encode_frame(b"hello\nworld\n", 3).unwrap().into(),
        calls: AtomicUsize::new(0),
    });
    let source = Arc::new(Bytes::from_static(include_bytes!(
        "../../../testdata/entry-manifest/thin.zstar"
    )));
    let archive = Archive::open_with_options(
        source.clone(),
        ReaderOptions {
            resolver: Some(objects.clone()),
            verify_external: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let physical = archive.verify(VerificationScope::Archive).await.unwrap();
    assert!(physical.pointer_members > 0);
    assert_eq!(objects.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        archive.read("repeat").await.unwrap(),
        b"hello\nhello\n".as_slice()
    );
    let mut file = archive.open_file("world.txt").await.unwrap();
    let mut text = String::new();
    file.read_to_string(&mut text).await.unwrap();
    assert_eq!(text, "world\n");
    assert_eq!(
        archive
            .read_many(&[ReadRequest {
                path: "hello.txt",
                offset: 1,
                length: 3
            }])
            .await
            .unwrap()[0],
        b"ell".as_slice()
    );
    let closure = archive
        .verify(VerificationScope::ExternalClosure)
        .await
        .unwrap();
    assert_eq!(closure.external_objects_sha256_checked.len(), 1);
    assert_eq!(closure.external_objects_unchecked, [] as [[u8; 32]; 0]);
    assert!(objects.calls.load(Ordering::SeqCst) > 1);
    let unresolved = Archive::open(source).await.unwrap();
    assert!(
        unresolved
            .verify(VerificationScope::ExternalClosure)
            .await
            .is_err()
    );
}

struct StalledInput;

#[test]
fn idle_builders_do_not_occupy_blocking_worker_slots() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let _idle = Builder::new(Vec::new());
        let mut active = Builder::new(Vec::new());
        timeout(Duration::from_secs(2), async {
            active.append_bytes("file", b"data").await.unwrap();
            active.finish().await.unwrap();
        })
        .await
        .unwrap();
    });
}

#[tokio::test]
async fn panicking_codec_callbacks_do_not_strand_output_waits() {
    let mut builder = Builder::with_options(
        Vec::new(),
        WriterOptions {
            manifest: zstar::ManifestOptions {
                memory_bytes: 1,
                create_spill_store: Some(zstar::SpillFactory::new(|| {
                    panic!("spill callback failed")
                })),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    assert!(
        timeout(
            Duration::from_secs(2),
            builder.append_bytes("file", b"data")
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(builder.finish().await.is_err());
}

impl AsyncRead for StalledInput {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn cancelled_and_failed_appends_poison_the_builder() {
    let mut builder = Builder::new(Vec::new());
    assert!(
        timeout(
            Duration::from_millis(30),
            builder.append_data(Header::file("file", 1), StalledInput)
        )
        .await
        .is_err()
    );
    assert!(builder.append_bytes("next", b"ok").await.is_err());
    assert!(
        timeout(Duration::from_secs(2), builder.finish())
            .await
            .unwrap()
            .is_err()
    );
    for (size, body) in [(1, b"xx".as_slice()), (3, b"xx".as_slice())] {
        let mut builder = Builder::new(Vec::new());
        assert!(
            builder
                .append_data(Header::file("file", size), body)
                .await
                .is_err()
        );
        assert!(builder.append_dir("dir").await.is_err());
        assert!(builder.finish().await.is_err());
    }
    let builder = Builder::with_options(
        Vec::new(),
        WriterOptions {
            frame_size: 1,
            ..Default::default()
        },
    );
    assert!(builder.finish().await.is_err());
}

#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    flushed: bool,
    shut_down: bool,
    fail: bool,
}
impl AsyncWrite for Sink {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.fail {
            return Poll::Ready(Err(io::Error::other("sink failed")));
        }
        this.bytes.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().flushed = true;
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().shut_down = true;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn finish_flushes_and_returns_sink_without_shutdown_and_propagates_sink_errors() {
    let mut builder = Builder::new(Sink::default());
    builder.append_bytes("file", b"data").await.unwrap();
    let sink = builder.finish().await.unwrap();
    assert!(sink.flushed);
    assert!(!sink.shut_down);
    assert_eq!(
        Archive::from_bytes(sink.bytes)
            .await
            .unwrap()
            .read("file")
            .await
            .unwrap(),
        b"data".as_slice()
    );
    let mut builder = Builder::with_options(
        Sink {
            fail: true,
            ..Default::default()
        },
        WriterOptions {
            frame_size: 1024,
            ..Default::default()
        },
    );
    assert!(builder.append_bytes("file", &payload()).await.is_err());
    assert!(builder.finish().await.is_err());
}

#[tokio::test]
async fn bounded_duplex_output_makes_progress_with_backpressure() {
    let (sink, mut source) = tokio::io::duplex(32);
    let write = async {
        let mut builder = Builder::with_options(
            sink,
            WriterOptions {
                frame_size: 1024,
                ..Default::default()
            },
        );
        builder.append_bytes("file", &payload()).await.unwrap();
        let mut sink = builder.finish().await.unwrap();
        sink.shutdown().await.unwrap();
    };
    let read = async {
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes).await.unwrap();
        bytes
    };
    let ((), bytes) = timeout(Duration::from_secs(3), async { tokio::join!(write, read) })
        .await
        .unwrap();
    assert_eq!(
        Archive::from_bytes(bytes)
            .await
            .unwrap()
            .read("file")
            .await
            .unwrap(),
        payload()
    );
}

struct StalledSource {
    bytes: Bytes,
    stall: AtomicBool,
    started: Notify,
    dropped: Arc<Notify>,
}
struct ReadDrop(Arc<Notify>);
impl Drop for ReadDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait::async_trait]
impl AsyncRangeReader for StalledSource {
    async fn size(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if self.stall.load(Ordering::SeqCst) {
            let _drop = ReadDrop(self.dropped.clone());
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        self.bytes.read_range(range).await
    }
}

#[tokio::test]
async fn cancelled_cursor_reads_and_stream_seeks_release_pending_io() {
    let source = Arc::new(StalledSource {
        bytes: fixture().await,
        stall: AtomicBool::new(false),
        started: Notify::new(),
        dropped: Arc::new(Notify::new()),
    });
    let archive = Archive::open_with_options(
        source.clone(),
        ReaderOptions {
            concurrent_chunks: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cursor = archive.open_cursor("dir/file").await.unwrap();
    let mut file = archive.open_file("dir/file").await.unwrap();
    source.stall.store(true, Ordering::SeqCst);
    {
        let read = cursor.read(&archive, 500, 2048);
        tokio::pin!(read);
        timeout(Duration::from_secs(2), async {
            tokio::select! {
                result = &mut read => panic!("stalled cursor completed: {result:?}"),
                () = source.started.notified() => {}
            }
        })
        .await
        .unwrap();
    }
    timeout(Duration::from_secs(2), source.dropped.notified())
        .await
        .unwrap();
    let mut output = [0; 100];
    timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = file.read(&mut output) => panic!("stalled stream completed: {result:?}"),
            () = source.started.notified() => {}
        }
    })
    .await
    .unwrap();
    // Seeking drops pending I/O before reading from the new position.
    file.seek(SeekFrom::Start(1000)).await.unwrap();
    timeout(Duration::from_secs(2), source.dropped.notified())
        .await
        .unwrap();
    source.stall.store(false, Ordering::SeqCst);
    timeout(Duration::from_secs(2), file.read_exact(&mut output))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, payload()[1000..1100]);
    assert_eq!(
        cursor.read(&archive, 500, 2048).await.unwrap(),
        payload()[500..2548]
    );
}

#[tokio::test]
async fn cancelling_physical_verification_drops_pending_source_reads() {
    let source = Arc::new(StalledSource {
        bytes: fixture().await,
        stall: AtomicBool::new(false),
        started: Notify::new(),
        dropped: Arc::new(Notify::new()),
    });
    let archive = Arc::new(
        Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                concurrent_chunks: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    source.stall.store(true, Ordering::SeqCst);
    let task = {
        let archive = archive.clone();
        tokio::spawn(async move { archive.verify(VerificationScope::Archive).await })
    };
    timeout(Duration::from_secs(2), source.started.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(2), source.dropped.notified())
        .await
        .unwrap();
    source.stall.store(false, Ordering::SeqCst);
    // The single worker permit is released after the cancelled validator exits.
    assert_eq!(
        timeout(Duration::from_secs(2), archive.read_range("dir/file", 0..1))
            .await
            .unwrap()
            .unwrap(),
        payload()[..1]
    );
}

#[tokio::test]
async fn modified_entries_cannot_become_authenticated_cursors() {
    let archive = Archive::from_bytes(fixture().await).await.unwrap();
    let mut entry = archive.entry("dir/file").await.unwrap().unwrap();
    entry.extents[0].offset += 1;
    assert!(matches!(
        archive.cursor_from_entry(entry).await,
        Err(Error::Unsupported(_))
    ));
}

struct CountedSource {
    bytes: Bytes,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AsyncRangeReader for CountedSource {
    async fn size(&self) -> Result<u64> {
        Ok(self.bytes.len() as u64)
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.bytes.read_range(range).await
    }
}

#[tokio::test]
async fn stream_cache_reuses_validated_sources_and_obeys_its_budget() {
    let mut builder = Builder::new(Vec::new());
    builder
        .append_bytes("file", &vec![7; 16_384])
        .await
        .unwrap();
    let source = Arc::new(CountedSource {
        bytes: builder.finish().await.unwrap().into(),
        calls: AtomicUsize::new(0),
    });
    let archive = Archive::open(source.clone()).await.unwrap();
    for (budget, expected) in [(2 << 20, 1), (0, 128), (1024, 128)] {
        let mut file = archive
            .open_file("file")
            .await
            .unwrap()
            .with_cache_bytes(budget);
        source.calls.store(0, Ordering::SeqCst);
        for _ in 0..128 {
            let mut byte = [0];
            file.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [7]);
        }
        assert_eq!(source.calls.load(Ordering::SeqCst), expected);
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, [7]);
    }
}

#[tokio::test]
async fn standalone_cursor_cache_never_mixes_archive_sessions() {
    let mut first = Builder::new(Vec::new());
    first.append_bytes("file", b"aaaaaa").await.unwrap();
    let first = Archive::from_bytes(first.finish().await.unwrap())
        .await
        .unwrap();
    let mut second = Builder::new(Vec::new());
    second.append_bytes("file", b"bbbbbb").await.unwrap();
    let second = Archive::from_bytes(second.finish().await.unwrap())
        .await
        .unwrap();
    let cursor = zstar::FileCursor::new(first.entry("file").await.unwrap().unwrap()).unwrap();
    assert_eq!(
        cursor.read(&first, 0, 6).await.unwrap(),
        b"aaaaaa".as_slice()
    );
    assert_eq!(
        cursor.read(&second, 0, 6).await.unwrap(),
        b"bbbbbb".as_slice()
    );
    assert_eq!(
        cursor.read(&first, 0, 6).await.unwrap(),
        b"aaaaaa".as_slice()
    );
}

#[test]
fn writer_construction_is_runtime_independent() {
    let builder = Builder::new(Vec::new());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let bytes = builder.finish().await.unwrap();
        Archive::from_bytes(bytes)
            .await
            .unwrap()
            .verify(VerificationScope::Archive)
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn fallible_writer_reports_invalid_options_before_returning() {
    let result = Builder::try_with_options(
        Vec::new(),
        WriterOptions {
            frame_size: 1,
            ..Default::default()
        },
    )
    .await;
    assert!(result.is_err());
    let writer = Builder::try_new(Vec::new()).await.unwrap();
    writer.finish().await.unwrap();
}

#[tokio::test]
async fn external_chunks_accept_boxed_writers_and_reject_bad_sizes() {
    struct Store;
    #[async_trait::async_trait]
    impl zstar::chunking::ObjectWriter for Store {
        async fn put_chunk(&mut self, raw: Bytes) -> Result<zstar::Extent> {
            tokio::task::yield_now().await;
            Ok(zstar::Extent {
                size: raw.len() as u64,
                source: zstar::Source::Object(*zstar::Digest::sha256(&raw).as_bytes()),
                encoded_size: zstar::encode_frame(&raw, 3)?.len() as u64,
                offset: 0,
            })
        }
    }
    // Object writer futures remain Send.
    let extents = tokio::spawn(async {
        let mut store = Store;
        let chunks = futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))]);
        zstar::chunking::external_chunks(chunks, 3, &mut store).await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(extents.len(), 1);

    let mut store: Box<dyn zstar::chunking::ObjectWriter> = Box::new(Store);
    let writer = store.as_mut();
    let chunks = futures_util::stream::iter([
        Ok(Bytes::from_static(b"abc")),
        Ok(Bytes::from_static(b"def")),
    ]);
    let extents = zstar::chunking::external_chunks(chunks, 6, writer)
        .await
        .unwrap();
    assert_eq!(extents.len(), 2);
    let chunks = futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))]);
    assert!(
        zstar::chunking::external_chunks(chunks, 2, writer)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn focused_metadata_options_enforce_index_limits() {
    let options = WriterOptions::default().with_metadata(zstar::MetadataWriterOptions {
        block_target: 512,
        ..Default::default()
    });
    let mut builder = Builder::try_with_options(Vec::new(), options)
        .await
        .unwrap();
    builder.append_bytes("a", b"a").await.unwrap();
    builder.append_bytes("b", b"b").await.unwrap();
    let bytes = builder.finish().await.unwrap();
    let options = ReaderOptions::default().with_metadata(zstar::MetadataReaderOptions {
        max_entries: 1,
        ..Default::default()
    });
    let result = Archive::open_with_options(Arc::new(Bytes::from(bytes)), options).await;
    assert!(matches!(result, Err(Error::Budget(_))));
}
