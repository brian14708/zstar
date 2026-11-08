//! Async tar-style writing over bounded channels to the compression worker.
use crate::blocking::Builder as BlockingBuilder;
use crate::{Error, Header, Result, WriterOptions};
use bytes::Bytes;
use std::io::{self, Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

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
/// Appending in UTF-8 path order avoids an additional manifest sorting buffer.
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
        self.output.take();
        self.writer.take();
        self.worker.take();
    }
}
impl<W: AsyncWrite + Unpin> Builder<W> {
    /// Initializes a writer and reports invalid options before returning it.
    ///
    /// # Errors
    /// Returns configuration, storage initialization, or missing-runtime errors.
    pub async fn try_with_options(output: W, options: WriterOptions) -> Result<Self> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Unsupported("writer requires a Tokio runtime"))?;
        let (send, output_rx) = mpsc::channel(2);
        let worker = handle.spawn_blocking(move || {
            let writer = BlockingBuilder::try_with_options(Sink(send.clone()), options);
            complete(send, writer.map(Some))
        });
        let mut builder = Self {
            sink: Some(output),
            output: Some(output_rx),
            writer: None,
            worker: Some(worker),
        };
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
        // Early worker rejection closes the input channel before the feed task
        // sees its result. Drain output so scheduling cannot hide that rejection
        // behind a generic body-channel error, or strand the worker's sender.
        let error = if matches!(error, Error::Unsupported("archive body closed")) {
            match Self::drain(self.sink.as_mut().unwrap(), self.output.as_mut().unwrap()).await {
                Err(cause) => cause,
                Ok(()) => error,
            }
        } else {
            error
        };
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
        if self.output.is_none() {
            return Err(Error::Unsupported("archive writer failed"));
        }
        if self.worker.is_some() {
            self.drain_self().await?;
            self.join().await?;
        }
        Ok(())
    }
    async fn append(
        &mut self,
        append: impl FnOnce(&mut BlockingBuilder<Sink>) -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let mut operation = Operation {
            builder: self,
            complete: false,
        };
        let this = &mut *operation.builder;
        this.ready().await?;
        this.start(move |mut writer| {
            append(&mut writer)?;
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
        let (sender, body) = mpsc::channel(2);
        let size = header.size;
        let result = async {
            this.start(move |mut writer| {
                writer.append_data(
                    header,
                    &mut Body {
                        input: body,
                        pending: Bytes::new(),
                    },
                )?;
                Ok(Some(writer))
            });
            let feed = async move {
                let mut remaining = size;
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
        self.append(move |writer| writer.append_header(header))
            .await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends a directory with default metadata.
    pub async fn append_dir(&mut self, path: impl Into<String>) -> Result<()> {
        self.append_header(Header::directory(path)).await
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
        self.append_header(Header::symlink(path, target)).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Appends file metadata that references already encoded external objects.
    /// An empty file with no extents, as produced by externalizing zero bytes, is accepted.
    pub async fn append_external(
        &mut self,
        header: Header,
        extents: Vec<crate::Extent>,
    ) -> Result<()> {
        self.append(move |writer| writer.append_external(header, extents))
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
