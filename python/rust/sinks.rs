//! Native file and multipart S3 sinks, with publication owned by the adapter.
use crate::{
    dispatch::Scope,
    storage::Location,
    support::{block_on, core_error, lock_state},
};
use bytes::Bytes;
use cap_fs_ext::{FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use pyo3::prelude::*;
use std::{
    future::Future,
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio::{io::AsyncWrite, sync::Mutex as AsyncMutex};
use zstar::s3::{ArchiveUpload, Path, UploadMode};

struct LocalOutput {
    file: Arc<Mutex<std::fs::File>>,
    parent: Arc<cap_std::fs::Dir>,
    name: PathBuf,
    identity: (u64, u64),
    destination: Option<(PathBuf, bool)>,
    keep: AtomicBool,
}
async fn file_work<T: Send + 'static>(
    file: Arc<Mutex<std::fs::File>>,
    work: impl FnOnce(&mut std::fs::File) -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    static CAPACITY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
    crate::support::blocking(&CAPACITY, move || work(&mut lock_state(&file))).await
}
/// Remove the output only while its name still refers to the created inode.
fn remove_if_same(
    parent: &cap_std::fs::Dir,
    name: &std::path::Path,
    identity: (u64, u64),
) -> io::Result<()> {
    if let Ok(metadata) = parent.symlink_metadata(name)
        && (metadata.dev(), metadata.ino()) == identity
    {
        parent.remove_file(name)?;
    }
    Ok(())
}
impl LocalOutput {
    fn open(path: &std::path::Path, exclusive: bool, atomic: bool) -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let destination: PathBuf = path
            .file_name()
            .ok_or_else(|| io::Error::other("output path requires a filename"))?
            .into();
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let parent = Arc::new(cap_std::fs::Dir::open_ambient_dir(
            parent,
            cap_std::ambient_authority(),
        )?);
        let mut options = cap_std::fs::OpenOptions::new();
        options.write(true).follow(FollowSymlinks::No);
        if exclusive || atomic {
            options.create_new(true);
        } else {
            options.create(true).truncate(true);
        }
        let (name, file) = if atomic {
            check_destination(&parent, &destination, exclusive)?;
            let mut output = None;
            for _ in 0..128 {
                let id = NEXT.fetch_add(1, Ordering::Relaxed);
                let name = PathBuf::from(format!(".zstar-{}-{id}.tmp", std::process::id()));
                if name == destination {
                    continue;
                }
                match parent.open_with(&name, &options) {
                    Ok(file) => {
                        output = Some((name, file));
                        break;
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
                    Err(error) => return Err(error),
                }
            }
            output.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "cannot create archive staging file",
                )
            })?
        } else {
            let file = parent.open_with(&destination, &options)?;
            (destination.clone(), file)
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::other("file sink requires a regular file"));
        }
        Ok(Self {
            file: Arc::new(Mutex::new(file.into_std())),
            parent,
            name,
            identity: (metadata.dev(), metadata.ino()),
            destination: atomic.then_some((destination, exclusive)),
            keep: AtomicBool::new(false),
        })
    }
    async fn finish(&self) -> io::Result<()> {
        let (parent, name, identity, destination) = (
            self.parent.clone(),
            self.name.clone(),
            self.identity,
            self.destination.clone(),
        );
        file_work(self.file.clone(), move |file| {
            file.flush()?;
            if let Some((destination, exclusive)) = destination {
                file.sync_all()?;
                let metadata = parent.symlink_metadata(&name)?;
                if (metadata.dev(), metadata.ino()) != identity {
                    return Err(io::Error::other("archive staging file was replaced"));
                }
                check_destination(&parent, &destination, exclusive)?;
                if exclusive {
                    // Linking publishes without overwriting a concurrent creator.
                    parent.hard_link(&name, &parent, &destination)?;
                    parent.remove_file(&name)?;
                } else {
                    parent.rename(&name, &parent, &destination)?;
                }
            }
            Ok(())
        })
        .await?;
        self.keep.store(true, Ordering::Release);
        Ok(())
    }
    async fn discard(&self) -> io::Result<()> {
        let (parent, name, identity) = (self.parent.clone(), self.name.clone(), self.identity);
        // Holding the file lock orders cleanup after any in-flight write.
        file_work(self.file.clone(), move |_| {
            remove_if_same(&parent, &name, identity)
        })
        .await?;
        self.keep.store(true, Ordering::Release);
        Ok(())
    }
}
fn check_destination(
    parent: &cap_std::fs::Dir,
    name: &std::path::Path,
    exclusive: bool,
) -> io::Result<()> {
    match parent.symlink_metadata(name) {
        Ok(_) if exclusive => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "archive destination already exists",
        )),
        Ok(metadata) if !metadata.is_file() => {
            Err(io::Error::other("file sink requires a regular file"))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
impl Drop for LocalOutput {
    fn drop(&mut self) {
        if self.keep.load(Ordering::Acquire) {
            return;
        }
        let parent = self.parent.clone();
        let name = self.name.clone();
        let identity = self.identity;
        let file = self.file.clone();
        pyo3_async_runtimes::tokio::get_runtime().spawn_blocking(move || {
            // Hold the original inode until the identity check completes.
            let _file = file;
            let _ = remove_if_same(&parent, &name, identity);
        });
    }
}
enum Output {
    Local(LocalOutput),
    Remote(ArchiveUpload),
}
impl Output {
    /// Writes every byte, so one builder write costs one blocking job.
    async fn write(&mut self, bytes: Bytes) -> io::Result<usize> {
        let count = bytes.len();
        match self {
            Self::Local(local) => {
                file_work(local.file.clone(), move |file| file.write_all(&bytes)).await?;
            }
            Self::Remote(remote) => remote.write(&bytes).await?,
        }
        Ok(count)
    }
    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Local(local) => file_work(local.file.clone(), Write::flush).await,
            Self::Remote(_) => Ok(()),
        }
    }
    async fn finish(&mut self) -> io::Result<()> {
        match self {
            Self::Local(local) => local.finish().await,
            Self::Remote(remote) => Ok(remote.finish().await?),
        }
    }
    async fn abort(&mut self) -> io::Result<()> {
        match self {
            Self::Local(local) => local.discard().await,
            Self::Remote(remote) => Ok(remote.abort().await?),
        }
    }
}
pub struct SinkState {
    output: AsyncMutex<Option<Output>>,
    scope: Mutex<Option<Arc<Scope>>>,
    closed: AtomicBool,
    failed: AtomicBool,
    cleanup_started: AtomicBool,
}
impl SinkState {
    fn new(output: Output, scope: Arc<Scope>) -> Arc<Self> {
        Arc::new(Self {
            output: AsyncMutex::new(Some(output)),
            scope: Mutex::new(Some(scope)),
            closed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            cleanup_started: AtomicBool::new(false),
        })
    }
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        let scope = lock_state(&self.scope).clone();
        if let Some(scope) = scope {
            scope.reject(py)?;
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(pyo3::exceptions::PyValueError::new_err("sink closed"));
        }
        Ok(())
    }
    pub fn mark_failed(&self) {
        self.failed.store(true, Ordering::Release);
    }
    async fn write(&self, bytes: Bytes) -> io::Result<usize> {
        let mut output = self.output.lock().await;
        if self.closed.load(Ordering::Acquire) || self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("sink closed or failed"));
        }
        let mut mutation = OutputMutation {
            state: self,
            complete: false,
        };
        let result = output
            .as_mut()
            .ok_or_else(|| io::Error::other("sink closed"))?
            .write(bytes)
            .await;
        mutation.complete = result.is_ok();
        // Publish failure before another operation can acquire the output.
        drop(mutation);
        drop(output);
        result
    }
    async fn flush(&self) -> io::Result<()> {
        let mut output = self.output.lock().await;
        if self.closed.load(Ordering::Acquire) || self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("sink closed or failed"));
        }
        let mut mutation = OutputMutation {
            state: self,
            complete: false,
        };
        let result = output
            .as_mut()
            .ok_or_else(|| io::Error::other("sink closed"))?
            .flush()
            .await;
        mutation.complete = result.is_ok();
        // Publish failure before another operation can acquire the output.
        drop(mutation);
        drop(output);
        result
    }
    pub(crate) fn close_work(
        self: &Arc<Self>,
        abort: bool,
    ) -> impl Future<Output = PyResult<()>> + Send + 'static {
        self.closed.store(true, Ordering::Release);
        let ticket = Closing {
            state: self.clone(),
            complete: false,
        };
        self.clone().finish(abort, ticket)
    }
    async fn finish(self: Arc<Self>, abort: bool, mut ticket: Closing) -> PyResult<()> {
        let mut guard = self.output.lock().await;
        let Some(output) = guard.as_mut() else {
            ticket.complete = true;
            return Ok(());
        };
        let mut errors = Vec::new();
        if !abort
            && !self.failed.load(Ordering::Acquire)
            && let Err(error) = output.finish().await
        {
            self.mark_failed();
            errors.push(core_error(error.into()));
        }
        if (abort || self.failed.load(Ordering::Acquire))
            && let Err(error) = output.abort().await
        {
            errors.push(core_error(error.into()));
        }
        let output = guard.take();
        drop(guard);
        drop(output);
        let scope = lock_state(&self.scope).take();
        drop(scope);
        ticket.complete = true;
        match errors.len() {
            0 => Ok(()),
            1 => Err(errors.pop().unwrap()),
            _ => Err(crate::support::exception_group(
                "output publication and cleanup failed",
                errors,
            )),
        }
    }
    fn cleanup_background(self: &Arc<Self>) {
        if self.cleanup_started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closed.store(true, Ordering::Release);
        self.mark_failed();
        let state = self.clone();
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let ticket = Closing {
                state: state.clone(),
                complete: false,
            };
            let _ = state.finish(true, ticket).await;
        });
    }
    fn traverse(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(scope) = lock_state(&self.scope).as_ref() {
            scope.traverse(visit)?;
        }
        Ok(())
    }
}
struct OutputMutation<'a> {
    state: &'a SinkState,
    complete: bool,
}
impl Drop for OutputMutation<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.state.mark_failed();
        }
    }
}
struct Closing {
    state: Arc<SinkState>,
    complete: bool,
}
impl Drop for Closing {
    fn drop(&mut self) {
        if !self.complete {
            self.state.cleanup_background();
        }
    }
}

/// Borrowed native handle. Dropping it never publishes or closes the sink.
pub struct BorrowedSink {
    state: Arc<SinkState>,
    write: Option<IoJob<usize>>,
    flush: Option<IoJob<()>>,
}
type IoJob<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send>>;
impl AsyncWrite for BorrowedSink {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.write.is_none() {
            let bytes = Bytes::copy_from_slice(bytes);
            let state = this.state.clone();
            this.write = Some(Box::pin(async move { state.write(bytes).await }));
        }
        match this.write.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.write = None;
                Poll::Ready(result)
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.flush.is_none() {
            let state = this.state.clone();
            this.flush = Some(Box::pin(async move { state.flush().await }));
        }
        match this.flush.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.flush = None;
                Poll::Ready(result)
            }
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct Sink {
    pub state: Arc<SinkState>,
}
impl Drop for Sink {
    fn drop(&mut self) {
        if !self.state.closed.load(Ordering::Acquire) {
            self.state.cleanup_background();
        }
    }
}
#[pymethods]
impl Sink {
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<usize> {
        self.state.check(py)?;
        let data = crate::support::shared_bytes(data)?;
        let state = self.state.clone();
        block_on(py, async move {
            state
                .write(data)
                .await
                .map_err(|error| core_error(error.into()))
        })
    }
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.state.check(py)?;
        let state = self.state.clone();
        block_on(py, async move {
            state
                .flush()
                .await
                .map_err(|error| core_error(error.into()))
        })
    }
    #[getter]
    fn closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if !self.closed() {
            self.state.check(py)?;
        }
        block_on(py, self.state.close_work(false))
    }
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        if !self.closed() {
            self.state.check(py)?;
        }
        block_on(py, self.state.close_work(true))
    }
    fn __enter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        slf.state.check(py)?;
        Ok(slf)
    }
    fn __exit__(
        &self,
        py: Python<'_>,
        kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if kind.is_none() {
            self.close(py)
        } else {
            self.abort(py)
        }
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.state.traverse(&visit)
    }
    fn __clear__(&self) {
        self.state.cleanup_background();
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct AsyncSink {
    pub state: Arc<SinkState>,
}
impl Drop for AsyncSink {
    fn drop(&mut self) {
        if !self.state.closed.load(Ordering::Acquire) {
            self.state.cleanup_background();
        }
    }
}
#[pymethods]
impl AsyncSink {
    fn write<'py>(&self, py: Python<'py>, data: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.state.check(py)?;
        let data = crate::support::shared_bytes(data)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state
                .write(data)
                .await
                .map_err(|error| core_error(error.into()))
        })
    }
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.state.check(py)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state
                .flush()
                .await
                .map_err(|error| core_error(error.into()))
        })
    }
    #[getter]
    fn closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.closed() {
            self.state.check(py)?;
        }
        pyo3_async_runtimes::tokio::future_into_py(py, self.state.close_work(false))
    }
    fn aabort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.closed() {
            self.state.check(py)?;
        }
        pyo3_async_runtimes::tokio::future_into_py(py, self.state.close_work(true))
    }
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        slf.state.check(py)?;
        let owner: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(owner) })
    }
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if kind.is_none() {
            self.aclose(py)
        } else {
            self.aabort(py)
        }
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.state.traverse(&visit)
    }
    fn __clear__(&self) {
        self.state.cleanup_background();
    }
}
fn open_work(
    py: Python<'_>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    asynchronous: bool,
    local_path: bool,
    mode: &str,
    atomic: bool,
) -> PyResult<impl Future<Output = PyResult<Arc<SinkState>>> + Send + 'static> {
    if mode != "w" && mode != "x" {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "sink mode must be w or x",
        ));
    }
    let exclusive = mode == "x";
    let location = if local_path {
        Location::File(location.into())
    } else {
        crate::storage::location(location)?
    };
    let s3_options = crate::config::SinkOptions::parse(options)?;
    if matches!(&location, Location::Http(_)) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "HTTP locations cannot be sinks",
        ));
    }
    let s3 = if let Location::S3 { bucket, .. } = &location {
        if atomic {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "atomic replacement is supported for local files",
            ));
        }
        if exclusive {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "exclusive creation is supported for local files",
            ));
        }
        Some(
            s3_options
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "S3 locations require explicit S3Options",
                    )
                })?
                .builder(bucket.clone())?,
        )
    } else {
        if s3_options.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "S3 settings require an S3 location",
            ));
        }
        None
    };
    let scope = Scope::new(py, asynchronous)?;
    Ok(async move {
        let output = match location {
            Location::File(path) => Output::Local(
                tokio::task::spawn_blocking(move || LocalOutput::open(&path, exclusive, atomic))
                    .await
                    .map_err(|_| {
                        pyo3::exceptions::PyOSError::new_err("file open worker failed")
                    })??,
            ),
            Location::S3 { key, .. } => {
                let store = s3.unwrap().build().map_err(|_| {
                    core_error(zstar::Error::Unsupported("S3 configuration failed"))
                })?;
                Output::Remote(ArchiveUpload::new(
                    Arc::new(store),
                    Path::from(key),
                    UploadMode::Overwrite,
                ))
            }
            Location::Http(_) => unreachable!(),
        };
        Ok(SinkState::new(output, scope))
    })
}
#[pyfunction]
#[pyo3(signature = (location, options = None, *, local_path = false, mode = "w", atomic = false))]
fn open_sink(
    py: Python<'_>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    local_path: bool,
    mode: &str,
    atomic: bool,
) -> PyResult<Sink> {
    let work = open_work(py, location, options, false, local_path, mode, atomic)?;
    block_on(py, async move { Ok(Sink { state: work.await? }) })
}
#[pyfunction]
#[pyo3(signature = (location, options = None, *, local_path = false, mode = "w", atomic = false))]
fn aopen_sink<'py>(
    py: Python<'py>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    local_path: bool,
    mode: &str,
    atomic: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let work = open_work(py, location, options, true, local_path, mode, atomic)?;
    pyo3_async_runtimes::tokio::future_into_py(
        py,
        async move { Ok(AsyncSink { state: work.await? }) },
    )
}

pub fn native_sink(
    py: Python<'_>,
    sink: &Py<PyAny>,
    asynchronous: bool,
) -> PyResult<Option<Arc<SinkState>>> {
    let blocking = sink
        .bind(py)
        .extract::<PyRef<'_, crate::adapters::BlockingAdapter>>()
        .ok()
        .map(|adapter| adapter.original(py))
        .transpose()?;
    let object = blocking.as_ref().unwrap_or(sink).bind(py);
    if let Ok(sink) = object.extract::<PyRef<'_, Sink>>() {
        if asynchronous && blocking.is_none() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "async writers require an AsyncBinaryWriter",
            ));
        }
        sink.state.check(py)?;
        return Ok(Some(sink.state.clone()));
    }
    if let Ok(sink) = object.extract::<PyRef<'_, AsyncSink>>() {
        if !asynchronous {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "async sinks require AsyncArchiveWriter",
            ));
        }
        sink.state.check(py)?;
        return Ok(Some(sink.state.clone()));
    }
    Ok(None)
}
pub fn borrow(state: Arc<SinkState>) -> BorrowedSink {
    BorrowedSink {
        state,
        write: None,
        flush: None,
    }
}
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Sink>()?;
    module.add_class::<AsyncSink>()?;
    module.add_function(wrap_pyfunction!(open_sink, module)?)?;
    module.add_function(wrap_pyfunction!(aopen_sink, module)?)?;
    Ok(())
}
