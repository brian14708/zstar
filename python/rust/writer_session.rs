//! One async Rust builder with thin Python waiting and awaitable facades.
use crate::{
    dispatch::{Callback, Scope},
    protocol_io::{Body, Sink},
    support::{block_on, core_error, lock_state},
    values::HeaderValue,
};
use pyo3::{
    prelude::*,
    pybacked::PyBackedBytes,
    types::{PyBytes, PyString, PyTuple},
};
use std::collections::BTreeSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, Ordering},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, watch};

const OPEN: u8 = 0;
const FINISHING: u8 = 1;
const FINISHED: u8 = 2;
const ABORTED: u8 = 3;
const FAILED: u8 = 4;
/// Encodes chunks on the bounded codec pool and stores them through the
/// Python `put_encoded` protocol.
struct ProtocolObjectWriter<'a, F> {
    store: &'a Callback,
    level: i32,
    limits: zstar::Limits,
    stored: u64,
    fail: &'a F,
}
#[async_trait::async_trait]
impl<F: Fn(PyErr) -> zstar::Error + Sync> zstar::chunking::ObjectWriter
    for ProtocolObjectWriter<'_, F>
{
    async fn put_chunk(&mut self, raw: bytes::Bytes) -> zstar::Result<zstar::layout::Extent> {
        // Reject before uploading objects the archive could never reference.
        if self.limits.max_extents != 0 && self.stored >= self.limits.max_extents {
            return Err(zstar::Error::budget(
                zstar::BudgetResource::Extents,
                "external extent count",
            ));
        }
        let size = raw.len() as u64;
        let (digest, encoded) = crate::external::encode_chunk(raw, self.level, self.limits).await?;
        let args = Python::attach(|py| {
            (
                zstar::backend::manifest::object_address(&digest),
                PyBytes::new(py, &encoded),
                size,
            )
                .into_pyobject(py)
                .map(Bound::unbind)
        })
        .map_err(self.fail)?;
        let stored = self.store.call(args).await.map_err(self.fail)?;
        let encoded_size =
            Python::attach(|py| stored.bind(py).extract::<crate::support::Unsigned>())
                .map_err(self.fail)?
                .0;
        self.stored += 1;
        Ok(crate::external::object_extent(size, digest, encoded_size))
    }
}
type Engine = zstar::Builder<Box<dyn tokio::io::AsyncWrite + Unpin + Send>>;
enum ExternalInput {
    Chunks(Callback),
    Body(Body),
}
struct Shared {
    builder: Arc<AsyncMutex<Option<Engine>>>,
    status: AtomicU8,
    scope: Mutex<Option<Arc<Scope>>>,
    stop: watch::Sender<bool>,
    limits: zstar::Limits,
    level: i32,
    native_sink: Option<Arc<crate::sinks::SinkState>>,
    native_owner: Mutex<Option<Py<PyAny>>>,
    owned: bool,
    publication: Mutex<Option<(Callback, Callback)>>,
    terminal: crate::completion::Completion,
    failure: Mutex<Option<Py<PyAny>>>,
    directories: Mutex<BTreeSet<String>>,
}
impl Shared {
    fn scope(&self, py: Python<'_>) -> PyResult<Arc<Scope>> {
        let scope = lock_state(&self.scope)
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("writer closed"))?;
        scope.reject(py)?;
        Ok(scope)
    }
    fn check_open(&self) -> PyResult<()> {
        match self.status.load(Ordering::Acquire) {
            OPEN => Ok(()),
            FAILED => Err(pyo3::exceptions::PyValueError::new_err("writer failed")),
            _ => Err(pyo3::exceptions::PyValueError::new_err("writer closed")),
        }
    }
    fn fail_native(&self) {
        if let Some(sink) = &self.native_sink {
            sink.mark_failed();
        }
    }
    async fn admit(self: &Arc<Self>) -> PyResult<Mutation> {
        let guard = self.builder.clone().lock_owned().await;
        self.check_open()?;
        Ok(Mutation {
            owner: self.clone(),
            guard: Some(guard),
            complete: false,
        })
    }

    async fn append(
        self: Arc<Self>,
        header: zstar::Header,
        body: Option<Body>,
        directory: Option<String>,
    ) -> PyResult<()> {
        let mut mutation = self.admit().await?;
        let mut stop = self.stop.subscribe();
        let engine = mutation.guard.as_mut().unwrap().as_mut().unwrap();
        let result = tokio::select! {
            biased;
            () = wait_stop(&mut stop) => Err(core_error(zstar::Error::Unsupported("writer aborted"))),
            result = async {
                match body {
                    Some(body) => engine.append_data(header, body).await,
                    None => engine.append_header(header).await,
                }.map_err(core_error)
            } => result,
        };
        if result.is_ok()
            && let Some(path) = directory
        {
            lock_state(&self.directories).insert(path);
        }
        mutation.complete(result).await
    }
    async fn append_bytes(self: Arc<Self>, header: zstar::Header, data: Vec<u8>) -> PyResult<()> {
        let mut mutation = self.admit().await?;
        let mut stop = self.stop.subscribe();
        let engine = mutation.guard.as_mut().unwrap().as_mut().unwrap();
        let result = tokio::select! {
            biased;
            () = wait_stop(&mut stop) => Err(core_error(zstar::Error::Unsupported("writer aborted"))),
            result = engine.append_data(header, data.as_slice()) => result.map_err(core_error),
        };
        mutation.complete(result).await
    }
    async fn ensure_directory(self: Arc<Self>, path: String) -> PyResult<()> {
        let mut mutation = self.admit().await?;
        let mut stop = self.stop.subscribe();
        let engine = mutation.guard.as_mut().unwrap().as_mut().unwrap();
        let result = tokio::select! {
            biased;
            () = wait_stop(&mut stop) => Err(core_error(zstar::Error::Unsupported("writer aborted"))),
            result = async {
                for end in path.match_indices('/').map(|(index, _)| index).chain(std::iter::once(path.len())) {
                    let directory = &path[..end];
                    if lock_state(&self.directories).contains(directory) {
                        continue;
                    }
                    engine.append_dir(directory).await.map_err(core_error)?;
                    lock_state(&self.directories).insert(directory.to_owned());
                }
                Ok(())
            } => result,
        };
        mutation.complete(result).await
    }
    async fn append_external(
        self: Arc<Self>,
        mut entry: zstar::layout::Entry,
        size: u64,
        input: ExternalInput,
        store: Callback,
    ) -> PyResult<()> {
        let mut mutation = self.admit().await?;
        let mut stop = self.stop.subscribe();
        let result = tokio::select! {
            biased;
            () = wait_stop(&mut stop) => Err(core_error(zstar::Error::Unsupported("writer aborted"))),
            result = async {
                // Python callback failures propagate unchanged rather than as core errors.
                let failure = Mutex::new(None);
                let fail = |error: PyErr| {
                    *lock_state(&failure) = Some(error);
                    zstar::Error::Unsupported("Python callback failed")
                };
                let mut writer = ProtocolObjectWriter {
                    store: &store,
                    level: self.level,
                    limits: self.limits,
                    stored: 0,
                    fail: &fail,
                };
                entry.extents = match input {
                    ExternalInput::Chunks(chunks) => {
                        let source = futures_util::stream::try_unfold((), |()| async {
                            let args = Python::attach(|py| PyTuple::empty(py).unbind());
                            let chunk = chunks.call(args).await.map_err(fail)?;
                            let raw = Python::attach(|py| -> PyResult<Option<bytes::Bytes>> {
                                if chunk.is_none(py) { return Ok(None); }
                                let raw: PyBackedBytes = chunk.bind(py).extract()?;
                                Ok(Some(bytes::Bytes::from_owner(raw)))
                            }).map_err(fail)?;
                            Ok(raw.map(|raw| (raw, ())))
                        });
                        zstar::chunking::external_chunks(Box::pin(source), size, &mut writer).await
                    }
                    ExternalInput::Body(mut body) => zstar::chunking::external_file(
                        &mut body, size, zstar::chunking::Chunking::FastCdc {
                            min: 2 << 20, target: 8 << 20, max: 16 << 20,
                        }, &mut writer,
                    ).await,
                }.map_err(|error| lock_state(&failure).take().unwrap_or_else(|| core_error(error)))?;
                let engine = mutation.guard.as_mut().unwrap().as_mut().unwrap();
                let extents = std::mem::take(&mut entry.extents);
                let header = HeaderValue { entry, size }.native_header();
                engine.append_external(header, extents).await.map_err(core_error)
            } => result,
        };
        mutation.complete(result).await
    }
    async fn append_path(
        self: Arc<Self>,
        path: std::path::PathBuf,
        name: String,
        preserve: bool,
        recursive: bool,
    ) -> PyResult<()> {
        static CAPACITY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
        let mut mutation = self.admit().await?;
        let mut stop = self.stop.subscribe();
        let (send, mut receive) = tokio::sync::mpsc::channel(1);
        let permit = CAPACITY
            .acquire()
            .await
            .map_err(|_| core_error(zstar::Error::Unsupported("capture closed")))?;
        let job = lock_state(&self.scope)
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("writer closed"))?
            .job();
        let mut capture = CaptureTask(tokio::task::spawn_blocking(move || {
            let (_permit, _job) = (permit, job);
            zstar::fs::visit_path(
                &path,
                &name,
                &mut |entry, size, body| {
                    send.blocking_send((entry, size, body))
                        .map_err(|_| zstar::Error::Unsupported("capture cancelled"))?;
                    Ok(())
                },
                preserve,
                recursive,
            )
            .map_err(core_error)
        }));
        let result = tokio::select! {
            biased;
            () = wait_stop(&mut stop) => Err(core_error(zstar::Error::Unsupported("writer aborted"))),
            result = async {
                while let Some((entry, size, body)) = receive.recv().await {
                    let directory = (entry.kind == zstar::EntryKind::Directory).then(|| entry.path.clone());
                    let header = HeaderValue { entry, size }.native_header();
                    let engine = mutation.guard.as_mut().unwrap().as_mut().unwrap();
                    if let Some(body) = body {
                        engine.append_data(header, tokio::fs::File::from_std(body)).await.map_err(core_error)?;
                    } else { engine.append_header(header).await.map_err(core_error)?; }
                    if let Some(path) = directory {
                        lock_state(&self.directories).insert(path);
                    }
                }
                (&mut capture.0).await.map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("capture worker failed"))?
            } => result,
        };
        mutation.complete(result).await
    }
    fn record_failure(&self, error: &PyErr) {
        let value = Python::attach(|py| error.value(py).clone().into_any().unbind());
        let mut failure = lock_state(&self.failure);
        if failure.is_none() {
            *failure = Some(value);
        }
    }
    fn start_terminal(self: &Arc<Self>, py: Python<'_>, abort: bool) -> PyResult<()> {
        let owner = self.clone();
        self.terminal.start(py, move || {
            if abort {
                if owner.status.load(Ordering::Acquire) != FAILED {
                    owner.status.store(ABORTED, Ordering::Release);
                }
                owner.stop.send_replace(true);
                owner.fail_native();
            } else {
                owner.status.store(FINISHING, Ordering::Release);
            }
            owner.terminate(abort)
        })
    }
    async fn terminate(self: Arc<Self>, abort: bool) -> PyResult<()> {
        let engine = self.builder.lock().await.take();
        let failed = self.status.load(Ordering::Acquire) == FAILED;
        let abort = abort || failed;
        let mut errors = Vec::new();
        if let Some(engine) = engine {
            if abort {
                drop(engine);
            } else if let Err(error) = async { engine.finish().await.map_err(core_error) }.await {
                self.fail_native();
                errors.push(error);
            }
        }
        // Encoding may leave codec workers or borrowed callbacks draining.
        let scope = lock_state(&self.scope).clone();
        if let Some(scope) = &scope {
            scope.drain().await;
        }
        if self.owned {
            let discard = abort || !errors.is_empty();
            let publication = lock_state(&self.publication).take();
            let result = if let Some(sink) = &self.native_sink {
                sink.close_work(discard).await
            } else if let Some((publish, cleanup)) = publication {
                let args = Python::attach(|py| PyTuple::empty(py).unbind());
                if discard {
                    cleanup.call(args).await.map(|_| ())
                } else {
                    match publish.call(args).await {
                        Ok(_) => Ok(()),
                        Err(primary) => {
                            let args = Python::attach(|py| PyTuple::empty(py).unbind());
                            match cleanup.call(args).await {
                                Ok(_) => Err(primary),
                                Err(secondary) => Err(crate::support::exception_group(
                                    "publication and cleanup failed",
                                    vec![primary, secondary],
                                )),
                            }
                        }
                    }
                }
            } else {
                Ok(())
            };
            if let Err(error) = result {
                errors.push(error);
            }
        }
        self.status.store(
            if errors.is_empty() && !failed {
                if abort { ABORTED } else { FINISHED }
            } else {
                FAILED
            },
            Ordering::Release,
        );
        let scope = lock_state(&self.scope).take();
        drop(scope);
        let owner = lock_state(&self.native_owner).take();
        drop(owner);
        lock_state(&self.directories).clear();
        match errors.len() {
            0 => Ok(()),
            1 => Err(errors.pop().unwrap()),
            _ => Err(crate::support::exception_group(
                "writer cleanup failed",
                errors,
            )),
        }
    }
    fn terminal_work(
        self: &Arc<Self>,
        py: Python<'_>,
        abort: bool,
    ) -> PyResult<impl std::future::Future<Output = PyResult<()>> + Send + 'static> {
        if lock_state(&self.scope).is_some() {
            self.scope(py)?;
        }
        self.start_terminal(py, abort || self.status.load(Ordering::Acquire) == FAILED)?;
        let owner = self.clone();
        let completion = self.terminal.wait();
        Ok(async move {
            completion.await?;
            if !abort {
                let failure = Python::attach(|py| {
                    lock_state(&owner.failure)
                        .as_ref()
                        .map(|error| PyErr::from_value(error.bind(py).clone()))
                });
                if let Some(error) = failure {
                    return Err(error);
                }
                if owner.status.load(Ordering::Acquire) == FAILED {
                    return Err(pyo3::exceptions::PyValueError::new_err("writer failed"));
                }
                if owner.status.load(Ordering::Acquire) == ABORTED {
                    return Err(pyo3::exceptions::PyValueError::new_err("writer aborted"));
                }
            }
            Ok(())
        })
    }
    fn clear(self: &Arc<Self>) {
        let _ = Python::attach(|py| self.start_terminal(py, true));
        drop(lock_state(&self.failure).take());
        self.terminal.clear();
    }
}
async fn wait_stop(stop: &mut watch::Receiver<bool>) {
    if !*stop.borrow_and_update() {
        let _ = stop.changed().await;
    }
}
// Cancellation stops a queued capture; a running worker observes receiver closure.
// Its scope ticket keeps abort/drain responsible for eventual file cleanup.
struct CaptureTask(tokio::task::JoinHandle<PyResult<()>>);
impl Drop for CaptureTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Mutation {
    owner: Arc<Shared>,
    guard: Option<OwnedMutexGuard<Option<Engine>>>,
    complete: bool,
}
impl Mutation {
    async fn complete(mut self, result: PyResult<()>) -> PyResult<()> {
        self.complete = result.is_ok();
        if let Err(error) = &result {
            self.owner.record_failure(error);
        }
        let owner = self.owner.clone();
        drop(self);
        if let Err(primary) = result {
            if owner.owned
                && let Err(cleanup) = owner.terminal.wait().await
            {
                return Err(crate::support::exception_group(
                    "operation and cleanup failed",
                    vec![primary, cleanup],
                ));
            }
            return Err(primary);
        }
        Ok(())
    }
}
impl Drop for Mutation {
    fn drop(&mut self) {
        if !self.complete {
            self.owner.fail_native();
            if self.owner.status.load(Ordering::Acquire) != ABORTED {
                self.owner.status.store(FAILED, Ordering::Release);
            }
            let engine = self.guard.as_mut().and_then(|guard| guard.take());
            drop(self.guard.take());
            drop(engine);
            if self.owner.owned {
                let _ = Python::attach(|py| self.owner.start_terminal(py, true));
            }
        }
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct WriterSession {
    inner: Arc<Shared>,
}
impl WriterSession {
    fn create(
        py: Python<'_>,
        sink: Py<PyAny>,
        options: &crate::config::WriterConfig,
        asynchronous: bool,
        owned: bool,
    ) -> PyResult<impl std::future::Future<Output = PyResult<Self>> + Send + 'static> {
        let scope = Scope::new(py, asynchronous)?;
        let options = options.policy();
        let native_sink = crate::sinks::native_sink(py, &sink, asynchronous)?;
        let native_owner = (owned || native_sink.is_some()).then(|| sink.clone_ref(py));
        let publication = if owned && native_sink.is_none() {
            let finish = if asynchronous { "aclose" } else { "close" };
            let abort = if asynchronous { "aabort" } else { "abort" };
            let abort = if sink.bind(py).hasattr(abort)? {
                abort
            } else {
                finish
            };
            Some((
                scope.protocol_callback(py, sink.clone_ref(py), finish)?,
                scope.protocol_callback(py, sink.clone_ref(py), abort)?,
            ))
        } else {
            None
        };
        let output: Box<dyn tokio::io::AsyncWrite + Unpin + Send> = match &native_sink {
            Some(state) => Box::new(crate::sinks::borrow(state.clone())),
            None => Box::new(Sink::new(
                scope.protocol_callback(py, sink.clone_ref(py), "write")?,
                scope.protocol_callback(py, sink, "flush")?,
            )),
        };
        Ok(async move {
            let level = options.compression_level;
            let limits = options.limits;
            let builder = match zstar::Builder::try_with_options(output, options).await {
                Ok(builder) => builder,
                Err(error) => {
                    let primary = core_error(error);
                    scope.drain().await;
                    let cleanup = if !owned {
                        Ok(())
                    } else if let Some(sink) = &native_sink {
                        sink.close_work(true).await
                    } else if let Some((_, abort)) = &publication {
                        let args = Python::attach(|py| PyTuple::empty(py).unbind());
                        abort.call(args).await.map(|_| ())
                    } else {
                        Ok(())
                    };
                    if let Err(cleanup) = cleanup {
                        return Err(crate::support::exception_group(
                            "open and cleanup failed",
                            vec![primary, cleanup],
                        ));
                    }
                    return Err(primary);
                }
            };
            Ok(Self {
                inner: Arc::new(Shared {
                    builder: Arc::new(AsyncMutex::new(Some(builder))),
                    status: AtomicU8::new(OPEN),
                    scope: Mutex::new(Some(scope)),
                    stop: watch::channel(false).0,
                    limits,
                    level,
                    native_sink,
                    native_owner: Mutex::new(native_owner),
                    owned,
                    publication: Mutex::new(publication),
                    terminal: crate::completion::Completion::default(),
                    failure: Mutex::new(None),
                    directories: Mutex::new(BTreeSet::new()),
                }),
            })
        })
    }
    fn append_work(
        &self,
        py: Python<'_>,
        header: &HeaderValue,
        body: Option<Py<PyAny>>,
    ) -> PyResult<impl std::future::Future<Output = PyResult<()>> + Send + 'static> {
        let scope = self.inner.scope(py)?;
        self.inner.check_open()?;
        if body.is_some() && header.entry.kind != zstar::EntryKind::File {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "body requires a file header",
            ));
        }
        if body.is_none() && header.size != 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "nonempty files require a body",
            ));
        }
        let body = body
            .map(|object| scope.protocol_callback(py, object, "read").map(Body::new))
            .transpose()?;
        let directory =
            (header.entry.kind == zstar::EntryKind::Directory).then(|| header.entry.path.clone());
        Ok(self
            .inner
            .clone()
            .append(header.native_header(), body, directory))
    }
    fn bytes_work(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<impl std::future::Future<Output = PyResult<()>> + Send + 'static> {
        self.inner.scope(py)?;
        self.inner.check_open()?;
        let data = crate::support::buffer_vec(py, data)?;
        let header = if path.is_instance_of::<PyString>() {
            let path: String = path.extract()?;
            zstar::layout::Entry::file(&path)
                .validate(u64::MAX)
                .map_err(core_error)?;
            zstar::Header::file(path, data.len() as u64)
        } else {
            let header = path.extract::<PyRef<'_, HeaderValue>>()?;
            if header.entry.kind != zstar::EntryKind::File {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "bytes require a file header",
                ));
            }
            if header.size != data.len() as u64 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "header size does not match bytes",
                ));
            }
            header.native_header()
        };
        Ok(self.inner.clone().append_bytes(header, data))
    }
    fn external_work(
        &self,
        py: Python<'_>,
        header: &HeaderValue,
        chunks: Py<PyAny>,
        store: Py<PyAny>,
    ) -> PyResult<impl std::future::Future<Output = PyResult<()>> + Send + 'static> {
        let scope = self.inner.scope(py)?;
        self.inner.check_open()?;
        if header.entry.kind != zstar::EntryKind::File {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "external content requires a file header",
            ));
        }
        let chunks = scope.protocol_callback(py, chunks, "next_chunk")?;
        let store = scope.protocol_callback(py, store, "put_encoded")?;
        Ok(self.inner.clone().append_external(
            header.entry.clone(),
            header.size,
            ExternalInput::Chunks(chunks),
            store,
        ))
    }
    fn path_work(
        &self,
        py: Python<'_>,
        path: std::path::PathBuf,
        name: String,
        preserve: bool,
        recursive: bool,
    ) -> PyResult<impl std::future::Future<Output = PyResult<()>> + Send + 'static> {
        self.inner.scope(py)?;
        self.inner.check_open()?;
        zstar::layout::Entry::file(&name)
            .validate(u64::MAX)
            .map_err(core_error)?;
        Ok(self
            .inner
            .clone()
            .append_path(path, name, preserve, recursive))
    }
}
#[pymethods]
impl WriterSession {
    #[staticmethod]
    #[pyo3(signature = (sink, options, *, owned=false))]
    pub(crate) fn to_sink(
        py: Python<'_>,
        sink: Py<PyAny>,
        options: &crate::config::WriterConfig,
        owned: bool,
    ) -> PyResult<Self> {
        block_on(py, Self::create(py, sink, options, false, owned)?)
    }
    #[staticmethod]
    #[pyo3(signature = (sink, options, *, owned=false))]
    fn ato_sink<'py>(
        py: Python<'py>,
        sink: Py<PyAny>,
        options: &crate::config::WriterConfig,
        owned: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            Self::create(py, sink, options, true, owned)?,
        )
    }
    #[pyo3(signature = (header, body = None))]
    fn append(
        &self,
        py: Python<'_>,
        header: &HeaderValue,
        body: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        block_on(py, self.append_work(py, header, body)?)
    }
    #[pyo3(signature = (header, body = None))]
    fn aappend<'py>(
        &self,
        py: Python<'py>,
        header: &HeaderValue,
        body: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, self.append_work(py, header, body)?)
    }
    fn append_bytes(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        block_on(py, self.bytes_work(py, path, data)?)
    }
    fn aappend_bytes<'py>(
        &self,
        py: Python<'py>,
        path: &Bound<'_, PyAny>,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, self.bytes_work(py, path, data)?)
    }
    fn ensure_directory(&self, py: Python<'_>, path: String) -> PyResult<()> {
        self.inner.scope(py)?;
        self.inner.check_open()?;
        zstar::layout::Entry::directory(&path)
            .validate(u64::MAX)
            .map_err(core_error)?;
        block_on(py, self.inner.clone().ensure_directory(path))
    }
    fn aensure_directory<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        self.inner.scope(py)?;
        self.inner.check_open()?;
        zstar::layout::Entry::directory(&path)
            .validate(u64::MAX)
            .map_err(core_error)?;
        pyo3_async_runtimes::tokio::future_into_py(py, self.inner.clone().ensure_directory(path))
    }
    fn append_external(
        &self,
        py: Python<'_>,
        header: &HeaderValue,
        chunks: Py<PyAny>,
        store: Py<PyAny>,
    ) -> PyResult<()> {
        block_on(py, self.external_work(py, header, chunks, store)?)
    }
    fn aappend_external<'py>(
        &self,
        py: Python<'py>,
        header: &HeaderValue,
        chunks: Py<PyAny>,
        store: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            self.external_work(py, header, chunks, store)?,
        )
    }
    fn aappend_external_body<'py>(
        &self,
        py: Python<'py>,
        header: &HeaderValue,
        body: Py<PyAny>,
        store: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = self.inner.scope(py)?;
        self.inner.check_open()?;
        if header.entry.kind != zstar::EntryKind::File {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "external content requires a file header",
            ));
        }
        let body = Body::new(scope.protocol_callback(py, body, "read")?);
        let store = scope.protocol_callback(py, store, "put_encoded")?;
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            self.inner.clone().append_external(
                header.entry.clone(),
                header.size,
                ExternalInput::Body(body),
                store,
            ),
        )
    }
    fn append_path(
        &self,
        py: Python<'_>,
        path: std::path::PathBuf,
        name: String,
        preserve: bool,
        recursive: bool,
    ) -> PyResult<()> {
        block_on(py, self.path_work(py, path, name, preserve, recursive)?)
    }
    fn aappend_path<'py>(
        &self,
        py: Python<'py>,
        path: std::path::PathBuf,
        name: String,
        preserve: bool,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            self.path_work(py, path, name, preserve, recursive)?,
        )
    }
    pub(crate) fn close(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, self.inner.terminal_work(py, false)?)
    }
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, self.inner.terminal_work(py, false)?)
    }
    pub(crate) fn abort(&self, py: Python<'_>) -> PyResult<()> {
        if lock_state(&self.inner.scope).is_some() {
            self.inner.scope(py)?;
        }
        block_on(py, self.inner.terminal_work(py, true)?)
    }
    fn aabort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if lock_state(&self.inner.scope).is_some() {
            self.inner.scope(py)?;
        }
        pyo3_async_runtimes::tokio::future_into_py(py, self.inner.terminal_work(py, true)?)
    }
    fn _check_access(&self, py: Python<'_>) -> PyResult<()> {
        if lock_state(&self.inner.scope).is_some() {
            self.inner.scope(py)?;
        }
        Ok(())
    }
    #[getter]
    fn failed(&self) -> bool {
        self.inner.status.load(Ordering::Acquire) == FAILED
    }
    #[getter]
    fn closed(&self) -> bool {
        self.inner.status.load(Ordering::Acquire) != OPEN
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(scope) = lock_state(&self.inner.scope).as_ref() {
            scope.traverse(&visit)?;
        }
        visit.call(&*lock_state(&self.inner.native_owner))?;
        visit.call(&*lock_state(&self.inner.failure))?;
        self.inner.terminal.traverse(&visit)?;
        Ok(())
    }
    fn __clear__(&self) {
        self.inner.clear();
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        if self.status.load(Ordering::Acquire) != FINISHED {
            self.fail_native();
        }
    }
}
impl Drop for WriterSession {
    fn drop(&mut self) {
        let _ = Python::attach(|py| self.inner.start_terminal(py, true));
    }
}
