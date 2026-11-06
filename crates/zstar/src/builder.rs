//! Async tar-style writing over bounded channels to the compression worker.
use crate::backend::BlockingBuilder;
use crate::{Entry, EntryKind, Error, Extensions, ManifestOptions, Result};
use bytes::Bytes;
use std::io::{self, Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

/// Metadata for an entry to append with [`Builder`].
///
/// The encoder assigns extents; this header describes the input file and its
/// declared size. Directory and symlink headers have size zero.
#[derive(Clone, Debug)]
pub struct Header {
    entry: Entry,
    size: u64,
}
impl Header {
    /// Creates a regular-file header with a declared byte length.
    pub fn file(path: impl Into<String>, size: u64) -> Self {
        Self {
            entry: Entry::file(path),
            size,
        }
    }
    /// Creates a directory header with mode `0o755`.
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            entry: Entry::directory(path),
            size: 0,
        }
    }
    /// Creates a symlink header with mode `0o777` and an exact target string.
    pub fn symlink(path: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            entry: Entry::symlink(path, target),
            size: 0,
        }
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.entry.path
    }
    /// Returns the declared payload length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
    /// Sets the Unix permission bits.
    pub const fn set_mode(&mut self, mode: u64) {
        self.entry.mode = mode;
    }
    /// Sets the user ID recorded in the archive.
    pub const fn set_uid(&mut self, uid: u64) {
        self.entry.uid = uid;
    }
    /// Sets the group ID recorded in the archive.
    pub const fn set_gid(&mut self, gid: u64) {
        self.entry.gid = gid;
    }
    /// Sets modification time as signed nanoseconds since the Unix epoch.
    pub const fn set_mtime_ns(&mut self, timestamp: i64) {
        self.entry.mtime_ns = timestamp;
    }
    /// Replaces the entry's `x-*` CBOR extension values.
    pub fn set_attributes(&mut self, attributes: Extensions) {
        self.entry.attributes = attributes;
    }
}

/// Compression, metadata, and storage options for a [`Builder`].
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Decoded data-frame target: at least 512 bytes, subject to manifest limits.
    pub frame_size: usize,
    /// Zstandard compression level; defaults to 3.
    pub compression_level: i32,
    /// Compression worker count, from 0 (serial) to 16.
    pub workers: usize,
    /// Metadata encoding and storage limits.
    pub manifest: ManifestOptions,
    /// Archive-level `x-*` CBOR values.
    pub root_extensions: Extensions,
}
impl WriterOptions {
    /// Sets writer metadata/spool policy without exposing reader-only options.
    #[must_use]
    pub fn with_metadata(mut self, options: crate::MetadataWriterOptions) -> Self {
        options.apply(&mut self.manifest);
        self
    }
}
impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            frame_size: 1 << 20,
            compression_level: 3,
            workers: 0,
            manifest: ManifestOptions::default(),
            root_extensions: Extensions::default(),
        }
    }
}

enum Output {
    Bytes(Bytes),
    Done(Result<()>),
}
struct Sink(mpsc::Sender<Output>);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .blocking_send(Output::Bytes(Bytes::copy_from_slice(bytes)))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "archive sink closed"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Body {
    input: mpsc::Receiver<Bytes>,
    pending: Bytes,
}
impl Read for Body {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.pending.is_empty() {
            let Some(bytes) = self.input.blocking_recv() else {
                return Ok(0);
            };
            self.pending = bytes;
        }
        let count = output.len().min(self.pending.len());
        output[..count].copy_from_slice(&self.pending[..count]);
        self.pending = self.pending.slice(count..);
        Ok(count)
    }
}

/// Writes an indexed tar archive to an [`AsyncWrite`] sink.
///
/// Entries may be appended in any order; include all ancestor directories.
/// Appending in UTF-8 path order avoids an additional manifest sorting spool.
/// [`Self::finish`] sorts the manifest if needed, writes the footer, and flushes the sink.
/// It returns the sink without calling [`AsyncWriteExt::shutdown`].
///
/// # Errors and cancellation
///
/// An append error or cancelled append makes the builder unusable. Dropping it
/// cancels channel waits and releases encoder storage after any running job
/// finishes. Discard partial output. Borrow a remote upload sink with `&mut` if
/// it must remain available for aborting the upload after an error.
pub struct Builder<W> {
    initial: Option<WriterOptions>,
    sink: Option<W>,
    output: Option<mpsc::Receiver<Output>>,
    writer: Option<BlockingBuilder<Sink>>,
    worker: Option<tokio::task::JoinHandle<Option<BlockingBuilder<Sink>>>>,
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
// Completion sends the error once through the output stream. No worker waits
// for the next append: codec state returns to the async owner between jobs.
fn complete(
    send: mpsc::Sender<Output>,
    result: Result<Option<BlockingBuilder<Sink>>>,
) -> Option<BlockingBuilder<Sink>> {
    match result {
        Ok(writer) => {
            if send.blocking_send(Output::Done(Ok(()))).is_ok() {
                writer
            } else {
                None
            }
        }
        Err(error) => {
            let _ = send.blocking_send(Output::Done(Err(error)));
            None
        }
    }
}
impl<W> Builder<W> {
    fn stop(&mut self) {
        self.initial.take();
        self.output.take();
        self.writer.take();
        self.worker.take();
    }
}
impl<W: AsyncWrite + Unpin> Builder<W> {
    /// Creates a lazy builder with default [`WriterOptions`].
    /// No runtime is needed until an async operation is polled.
    /// Use [`Self::try_with_options`] to report initialization errors immediately.
    pub fn new(output: W) -> Self {
        Self::with_options(output, WriterOptions::default())
    }
    /// Creates a lazy builder. Initialization errors are returned by the first
    /// append or finish operation. Construction does not start background work.
    pub const fn with_options(output: W, options: WriterOptions) -> Self {
        Self {
            initial: Some(options),
            sink: Some(output),
            output: None,
            writer: None,
            worker: None,
        }
    }
    /// Initializes a writer and reports invalid options before returning it.
    ///
    /// # Errors
    /// Returns configuration, storage initialization, or missing-runtime errors.
    pub async fn try_with_options(output: W, options: WriterOptions) -> Result<Self> {
        let mut builder = Self::with_options(output, options);
        builder.ready().await?;
        Ok(builder)
    }
    /// Initializes a writer with default options.
    ///
    /// ```no_run
    /// # async fn example() -> zstar::Result<()> {
    /// let mut writer = zstar::Builder::try_new(Vec::new()).await?;
    /// writer.append_bytes("hello", b"world").await?;
    /// let bytes = writer.finish().await?;
    /// let archive = zstar::Archive::from_bytes(bytes).await?;
    /// assert_eq!(&archive.read("hello").await?[..], b"world");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Returns the initialization errors described by [`Self::try_with_options`].
    pub async fn try_new(output: W) -> Result<Self> {
        Self::try_with_options(output, WriterOptions::default()).await
    }
    fn initialize(&mut self) -> Result<()> {
        if let Some(options) = self.initial.take() {
            let handle = tokio::runtime::Handle::try_current()
                .map_err(|_| Error::Unsupported("writer requires a Tokio runtime"))?;
            let (send, output_rx) = mpsc::channel(2);
            self.output = Some(output_rx);
            self.worker = Some(handle.spawn_blocking(move || {
                let writer = BlockingBuilder::with_options(
                    Sink(send.clone()),
                    options.frame_size,
                    options.compression_level,
                    options.manifest,
                )
                .and_then(|writer| writer.with_workers(options.workers))
                .and_then(|writer| writer.with_root_extensions(options.root_extensions));
                complete(send, writer.map(Some))
            }));
        }
        Ok(())
    }
    fn start(
        &mut self,
        operation: impl FnOnce(BlockingBuilder<Sink>) -> Result<Option<BlockingBuilder<Sink>>>
        + Send
        + 'static,
    ) {
        let writer = self.writer.take().unwrap();
        let send = writer.output().0.clone();
        self.worker = Some(tokio::task::spawn_blocking(move || {
            complete(send, operation(writer))
        }));
    }
    async fn join(&mut self) -> Result<()> {
        self.writer = self
            .worker
            .take()
            .unwrap()
            .await
            .map_err(|error| Error::Io(io::Error::other(error)))?;
        Ok(())
    }
    /// Turn the generic "worker vanished" symptoms into the worker's actual
    /// failure (for example a panic payload), when it can be recovered.
    async fn explain(&mut self, error: Error) -> Error {
        if !matches!(
            error,
            Error::Unsupported("archive writer stopped" | "archive body closed")
        ) {
            return error;
        }
        let Some(worker) = self.worker.take() else {
            return error;
        };
        match worker.await {
            Err(join) => {
                let detail = match join.try_into_panic() {
                    Ok(payload) => payload
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic".into()),
                    Err(join) => join.to_string(),
                };
                Error::Io(io::Error::other(format!(
                    "archive writer worker failed: {detail}"
                )))
            }
            Ok(_) => error,
        }
    }
    async fn drain_self(&mut self) -> Result<()> {
        match Self::drain(self.sink.as_mut().unwrap(), self.output.as_mut().unwrap()).await {
            Err(error) => Err(self.explain(error).await),
            ok => ok,
        }
    }
    async fn drain(sink: &mut W, output: &mut mpsc::Receiver<Output>) -> Result<()> {
        while let Some(message) = output.recv().await {
            match message {
                Output::Bytes(bytes) => sink.write_all(&bytes).await?,
                Output::Done(result) => return result,
            }
        }
        Err(Error::Unsupported("archive writer stopped"))
    }
    async fn ready(&mut self) -> Result<()> {
        self.initialize()?;
        if self.output.is_none() {
            return Err(Error::Unsupported("archive writer failed"));
        }
        if self.worker.is_some() {
            self.drain_self().await?;
            self.join().await?;
        }
        Ok(())
    }
    async fn append(&mut self, entry: Entry) -> Result<()> {
        let mut operation = Operation {
            builder: self,
            complete: false,
        };
        let this = &mut *operation.builder;
        this.ready().await?;
        this.start(move |mut writer| {
            writer.add_metadata(entry)?;
            Ok(Some(writer))
        });
        this.drain_self().await?;
        this.join().await?;
        operation.complete = true;
        Ok(())
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends a regular file from a byte slice with default metadata.
    pub async fn append_bytes(&mut self, path: impl Into<String>, bytes: &[u8]) -> Result<()> {
        self.append_data(Header::file(path, bytes.len() as u64), bytes)
            .await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    #[expect(
        clippy::missing_panics_doc,
        reason = "Unwraps enforce internal invariants established before this operation."
    )]
    /// Appends a regular file using its header and an exact-size input.
    ///
    /// Reads through EOF to detect inputs shorter or longer than [`Header::size`].
    /// A directory or symlink header is rejected; use [`Self::append_header`].
    /// Input and output channels each retain at most two chunks; input chunks
    /// are at most 64 KiB.
    pub async fn append_data(
        &mut self,
        header: Header,
        mut input: impl AsyncRead + Unpin,
    ) -> Result<()> {
        let mut operation = Operation {
            builder: self,
            complete: false,
        };
        let this = &mut *operation.builder;
        this.ready().await?;
        if header.entry.kind != EntryKind::File {
            this.stop();
            return Err(Error::Unsupported("append_data requires a file header"));
        }
        let (sender, body) = mpsc::channel(2);
        let result = async {
            this.start(move |mut writer| {
                writer.add_file(
                    header.entry,
                    header.size,
                    &mut Body {
                        input: body,
                        pending: Bytes::new(),
                    },
                )?;
                Ok(Some(writer))
            });
            let feed = async move {
                let mut remaining = header.size;
                // One buffer is reused: `split` hands out the filled prefix and
                // `reserve` reclaims the allocation once chunks are consumed.
                let mut buffer = bytes::BytesMut::new();
                loop {
                    let capacity = remaining.saturating_add(1).min(64 << 10);
                    buffer.reserve(capacity as usize);
                    let count = (&mut input).take(capacity).read_buf(&mut buffer).await?;
                    if count == 0 {
                        break;
                    }
                    if count as u64 > remaining {
                        return Err(Error::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "payload grew",
                        )));
                    }
                    remaining -= count as u64;
                    sender
                        .send(buffer.split().freeze())
                        .await
                        .map_err(|_| Error::Unsupported("archive body closed"))?;
                }
                if remaining != 0 {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "payload shrank",
                    )));
                }
                drop(sender);
                Ok(())
            };
            let joined = tokio::try_join!(
                feed,
                Self::drain(this.sink.as_mut().unwrap(), this.output.as_mut().unwrap())
            );
            if let Err(error) = joined {
                return Err(this.explain(error).await);
            }
            this.join().await?;
            Ok(())
        }
        .await;
        operation.complete = result.is_ok();
        result
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends metadata for a directory, symlink or empty regular file.
    pub async fn append_header(&mut self, header: Header) -> Result<()> {
        if header.size != 0 {
            self.stop();
            return Err(Error::Unsupported("nonempty file requires append_data"));
        }
        self.append(header.entry).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends a directory with default metadata.
    pub async fn append_dir(&mut self, path: impl Into<String>) -> Result<()> {
        self.append(Header::directory(path).entry).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends a symlink with default metadata and an exact target string.
    pub async fn append_symlink(
        &mut self,
        path: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<()> {
        self.append(Header::symlink(path, target).entry).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends file metadata that references already encoded external objects.
    pub async fn append_external(&mut self, entry: Entry) -> Result<()> {
        if !entry.is_external() {
            self.stop();
            return Err(Error::Unsupported("external entry required"));
        }
        self.append(entry).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    #[expect(
        clippy::missing_panics_doc,
        reason = "Unwraps enforce internal invariants established before this operation."
    )]
    /// Writes the manifest and footer, flushes, and returns the output sink.
    ///
    /// Rejects duplicate paths and missing ancestor directories. Does not shut
    /// down the sink; remote uploads require their own completion operation.
    pub async fn finish(mut self) -> Result<W> {
        self.ready().await?;
        self.start(|writer| writer.finish().map(|_| None));
        self.drain_self().await?;
        self.join().await?;
        self.stop();
        let mut sink = self.sink.take().unwrap();
        sink.flush().await?;
        Ok(sink)
    }
}
impl<W> Drop for Builder<W> {
    fn drop(&mut self) {
        self.stop();
    }
}

// A cancelled append cannot leave stale worker output for the next operation.
struct Operation<'a, W> {
    builder: &'a mut Builder<W>,
    complete: bool,
}
impl<W> Drop for Operation<'_, W> {
    fn drop(&mut self) {
        if !self.complete {
            self.builder.stop();
        }
    }
}
