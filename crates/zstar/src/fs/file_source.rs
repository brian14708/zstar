//! Positional local ranges without a shared seek cursor or memory mapping.
use crate::{Error, RangeReader, Result};
use std::{fs::File, io, ops::Range, path::Path};

const DEFAULT_MAX_RANGE: usize = 64 << 20;

/// Open-handle local archive source. Replacing the path does not retarget it.
/// Callers keep file contents immutable for the lifetime of an indexed reader
/// and its cached values. Content digests are checked by the reader, not here.
pub struct LocalFileSource {
    file: File,
    size: u64,
    max_range_bytes: usize,
}
impl LocalFileSource {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_file(File::open(path)?)
    }
    pub fn from_file(file: File) -> Result<Self> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(Error::Unsupported("local source requires a regular file"));
        }
        Ok(Self {
            file,
            size: metadata.len(),
            max_range_bytes: DEFAULT_MAX_RANGE,
        })
    }
    /// Set a per-range allocation limit before placing the source in a reader.
    /// The default is 64 MiB; larger permitted TOCs may need a larger limit.
    pub fn with_max_range_bytes(mut self, limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(Error::Budget("local range limit must be positive"));
        }
        self.max_range_bytes = limit;
        Ok(self)
    }
    #[must_use]
    pub fn len(&self) -> u64 {
        self.size
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    fn range_len(&self, range: &Range<u64>) -> Result<usize> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, length, self.size)?;
        let count = usize::try_from(length).map_err(|_| Error::Budget("local range size"))?;
        if count > self.max_range_bytes {
            return Err(Error::Budget("local range size"));
        }
        Ok(count)
    }
    /// Exact positional read. Truncation/short reads fail instead of returning
    /// partial data. No cursor lock is needed for concurrent distinct ranges.
    pub fn read_range(&self, range: Range<u64>) -> Result<Vec<u8>> {
        let count = self.range_len(&range)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(count)
            .map_err(|_| Error::Budget("local range allocation"))?;
        bytes.resize(count, 0);
        let mut remaining = bytes.as_mut_slice();
        let mut offset = range.start;
        while !remaining.is_empty() {
            match read_at(&self.file, remaining, offset) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
                Ok(count) => {
                    offset += count as u64;
                    remaining = &mut remaining[count..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(bytes)
    }
}
impl RangeReader for LocalFileSource {
    fn len(&self) -> u64 {
        self.size
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        Self::read_range(self, range)
    }
}
#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, bytes, offset)
}
#[cfg(windows)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
    // Every operation specifies its own offset. No seek/read pair or shared
    // cursor position is used, even though Windows may update that cursor.
    std::os::windows::fs::FileExt::seek_read(file, bytes, offset)
}
#[cfg(not(any(unix, windows)))]
fn read_at(_: &File, _: &mut [u8], _: u64) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "positional local files require Unix or Windows",
    ))
}

mod asynchronous {
    use super::LocalFileSource;
    use crate::{AsyncRangeReader, Error, Result};
    use bytes::Bytes;
    use std::{io, ops::Range, path::Path, sync::Arc};
    use tokio::sync::Semaphore;

    /// Bounded blocking positional reads, safe for cold files on async runtimes.
    /// Cancellation while waiting for a slot starts no I/O. Once submitted,
    /// blocking work retains its permit and buffers until completion, even if
    /// the awaiting future is dropped. Clones share the concurrency bound.
    #[derive(Clone)]
    pub struct AsyncLocalFileSource {
        source: Arc<LocalFileSource>,
        slots: Arc<Semaphore>,
    }
    impl AsyncLocalFileSource {
        /// Open file/metadata off the runtime thread, with eight read slots.
        pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
            let path = path.as_ref().to_path_buf();
            let source = tokio::task::spawn_blocking(move || LocalFileSource::open(path))
                .await
                .map_err(join_error)??;
            Self::new(source, 8)
        }
        /// Configure an existing source; concurrency must be in 1..=1024.
        pub fn new(source: LocalFileSource, concurrency: usize) -> Result<Self> {
            if !(1..=1024).contains(&concurrency) {
                return Err(Error::Unsupported(
                    "local read concurrency must be 1..=1024",
                ));
            }
            Ok(Self {
                source: Arc::new(source),
                slots: Arc::new(Semaphore::new(concurrency)),
            })
        }
        async fn run<T: Send + 'static>(
            &self,
            operation: impl FnOnce(Arc<LocalFileSource>) -> Result<T> + Send + 'static,
        ) -> Result<T> {
            let permit = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| io::Error::other("local source closed"))?;
            let source = self.source.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                operation(source)
            })
            .await
            .map_err(join_error)?
        }
    }
    fn join_error(_: tokio::task::JoinError) -> Error {
        io::Error::other("local file worker failed").into()
    }
    #[async_trait::async_trait]
    impl AsyncRangeReader for AsyncLocalFileSource {
        async fn size(&self) -> Result<u64> {
            Ok(self.source.len())
        }
        async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
            if self.source.range_len(&range)? == 0 {
                return Ok(Bytes::new());
            }
            self.run(move |source| source.read_range(range).map(Bytes::from))
                .await
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::backend::BlockingBuilder;
        use crate::{Archive, Entry, ReaderOptions};
        use tokio::sync::Notify;

        #[tokio::test]
        async fn indexed_reads_are_exact_concurrent_and_verified() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("archive");
            let mut writer =
                BlockingBuilder::with_options(Vec::new(), 1024, 3, Default::default()).unwrap();
            writer
                .add_file(Entry::file("a"), 16_384, &mut vec![7; 16_384].as_slice())
                .unwrap();
            std::fs::write(&path, writer.finish().unwrap()).unwrap();
            let source = Arc::new(AsyncLocalFileSource::open(&path).await.unwrap());
            let reader = Archive::open_with_options(source.clone(), ReaderOptions::default())
                .await
                .unwrap();
            let (a, b, c) = tokio::join!(
                reader.read_range("a", 0..16_384),
                reader.read_range("a", 1023..5119),
                reader.read_range("a", 16_384..16_384)
            );
            assert_eq!(a.unwrap().as_ref(), &[7; 16_384]);
            assert_eq!(b.unwrap().as_ref(), &[7; 4096]);
            assert_eq!(c.unwrap().len(), 0);
            let (_, report) = reader
                .read_range_with_report_and_resolver("a", 0, 16_384, None)
                .await
                .unwrap();
            assert!(report.metadata_authenticated);
            assert_ne!(report.data_frames_checked, [] as [u64; 0]);
            assert!(!report.full_archive_validated);
            assert!(matches!(
                source.read_range(Range { start: 1, end: 0 }).await,
                Err(Error::Range)
            ));
            assert!(source.read_range(0..u64::MAX).await.is_err());
        }
        #[tokio::test]
        async fn cancellation_cannot_release_a_running_workers_permit() {
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), b"xyz").unwrap();
            let local = LocalFileSource::open(file.path())
                .unwrap()
                .with_max_range_bytes(1)
                .unwrap();
            let source = AsyncLocalFileSource::new(local, 1).unwrap();
            let entered = Arc::new(Notify::new());
            let (release, wait) = std::sync::mpsc::channel();
            let worker = {
                let source = source.clone();
                let entered = entered.clone();
                tokio::spawn(async move {
                    source
                        .run(move |_| {
                            entered.notify_one();
                            wait.recv().unwrap();
                            Ok(())
                        })
                        .await
                })
            };
            entered.notified().await;
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
            assert_eq!(source.slots.available_permits(), 0);
            // Empty ranges and invalid/budgeted ranges never wait for a worker.
            assert_eq!(source.read_range(1..1).await.unwrap().len(), 0);
            assert!(matches!(
                source.read_range(0..2).await,
                Err(Error::Budget(_))
            ));
            assert!(matches!(source.read_range(0..4).await, Err(Error::Range)));
            let queued = {
                let source = source.clone();
                tokio::spawn(async move { source.read_range(0..1).await })
            };
            tokio::task::yield_now().await;
            assert!(!queued.is_finished());
            queued.abort();
            assert!(queued.await.unwrap_err().is_cancelled());
            release.send(()).unwrap();
            assert_eq!(source.read_range(0..1).await.unwrap().as_ref(), b"x");
            assert_eq!(source.slots.available_permits(), 1);
        }
    }
}
pub use asynchronous::AsyncLocalFileSource;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_ranges_budgets_truncation_and_independent_offsets() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let bytes: Vec<_> = (0..131_072).map(|n| (n % 251) as u8).collect();
        std::fs::write(file.path(), &bytes).unwrap();
        let source = LocalFileSource::open(file.path())
            .unwrap()
            .with_max_range_bytes(4096)
            .unwrap();
        assert_eq!(source.len(), bytes.len() as u64);
        assert!(!source.is_empty());
        assert_eq!(source.read_range(91..301).unwrap(), bytes[91..301]);
        assert_eq!(source.read_range(0..32).unwrap(), bytes[..32]);
        assert_eq!(
            source.read_range(source.len()..source.len()).unwrap().len(),
            0
        );
        assert!(matches!(
            source.read_range(Range { start: 1, end: 0 }),
            Err(Error::Range)
        ));
        assert!(matches!(source.read_range(0..4097), Err(Error::Budget(_))));
        assert!(matches!(
            source.read_range(source.len()..source.len() + 1),
            Err(Error::Range)
        ));
        std::thread::scope(|scope| {
            for n in 0..16 {
                let source = &source;
                let bytes = &bytes;
                scope.spawn(move || {
                    for _ in 0..32 {
                        let at = n * 4096;
                        assert_eq!(
                            source.read_range(at as u64..at as u64 + 4096).unwrap(),
                            bytes[at..at + 4096]
                        );
                    }
                });
            }
        });
        file.as_file().set_len(1).unwrap();
        assert!(
            matches!(source.read_range(0..32), Err(Error::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof)
        );
        assert!(
            LocalFileSource::open(file.path())
                .unwrap()
                .with_max_range_bytes(0)
                .is_err()
        );
    }
    #[test]
    fn replacing_a_path_does_not_retarget_the_open_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("archive");
        std::fs::write(&path, b"original").unwrap();
        let source = LocalFileSource::open(&path).unwrap();
        std::fs::rename(&path, directory.path().join("old")).unwrap();
        std::fs::write(&path, b"replaced").unwrap();
        assert_eq!(source.read_range(0..8).unwrap(), b"original");
    }
    #[test]
    fn rejects_nonregular_files_and_supports_empty_files() {
        let directory = tempfile::tempdir().unwrap();
        assert!(LocalFileSource::open(directory.path()).is_err());
        let file = tempfile::NamedTempFile::new().unwrap();
        let source = LocalFileSource::from_file(file.reopen().unwrap()).unwrap();
        assert!(source.is_empty());
        assert_eq!(source.read_range(0..0).unwrap().len(), 0);
    }
}
