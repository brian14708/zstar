//! Reader session shared by synchronous, asynchronous, and filesystem adapters.
use pyo3::{
    buffer::PyBuffer,
    prelude::*,
    types::{PyBytes, PyMemoryView, PySlice},
};
use std::{
    future::Future,
    ops::Deref,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

use crate::support::{core_error, py_io};
use zstar::ReaderOptions;

pub type Reader = zstar::Archive<dyn zstar::AsyncRangeReader>;

/// Owned metadata and stream position. No archive/source ownership survives a
/// call: every read must acquire a live session lease from its adapter.
#[derive(Clone)]
#[pyclass(module = "zstar._native", frozen)]
pub struct MemberCursor {
    inner: Arc<MemberState>,
    size: u64,
}
struct MemberState {
    cursor: zstar::backend::FileCursor,
    position: Mutex<u64>,
}
impl MemberCursor {
    pub(crate) async fn open(reader: &Reader, path: &str) -> PyResult<Self> {
        let entry = reader
            .entry(path)
            .await
            .map_err(crate::support::core_error)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(path.to_owned()))?;
        match entry.kind() {
            zstar::EntryKind::Directory => {
                return Err(pyo3::exceptions::PyIsADirectoryError::new_err(
                    path.to_owned(),
                ));
            }
            zstar::EntryKind::Symlink => {
                return Err(core_error(zstar::Error::Unsupported(
                    "cannot open a symlink as a file",
                )));
            }
            zstar::EntryKind::File => (),
        }
        let cursor = zstar::backend::open_cursor(reader, path)
            .await
            .map_err(crate::support::core_error)?;
        Ok(Self {
            size: cursor.size(),
            inner: Arc::new(MemberState {
                cursor,
                position: Mutex::new(0),
            }),
        })
    }
    pub async fn read(&self, reader: &Reader, length: Option<u64>) -> PyResult<Py<PyBytes>> {
        let position = *crate::support::lock_state(&self.inner.position);
        let available = self.size.saturating_sub(position);
        let count = length.unwrap_or(available).min(available);
        let bytes = if count == 0 {
            bytes::Bytes::new()
        } else {
            self.inner
                .cursor
                .read(reader, position, count)
                .await
                .map_err(crate::support::core_error)?
        };
        let output = Python::attach(|py| PyBytes::new(py, &bytes).unbind());
        Ok(output)
    }
    pub async fn read_into(&self, reader: &Reader, buffer: &WritableBuffer) -> PyResult<usize> {
        let start = *crate::support::lock_state(&self.inner.position);
        self.read_into_at(reader, start, buffer).await
    }
    pub async fn read_into_at(
        &self,
        reader: &Reader,
        start: u64,
        buffer: &WritableBuffer,
    ) -> PyResult<usize> {
        let count = self.size.saturating_sub(start).min(buffer.length as u64);
        let step = reader.max_read_bytes().clamp(1, 1 << 20);
        let mut done = 0;
        while done < count {
            let bytes = self
                .inner
                .cursor
                .read(reader, start + done, (count - done).min(step))
                .await
                .map_err(crate::support::core_error)?;
            if bytes.is_empty() {
                break;
            }
            buffer.copy(done, &bytes)?;
            done += bytes.len() as u64;
        }
        // The adapter advances only after Python receives the result. This
        // also covers cancellation after native completion but before delivery.
        Ok(usize::try_from(done).unwrap())
    }
}
#[pymethods]
impl MemberCursor {
    #[getter]
    const fn size(&self) -> u64 {
        self.size
    }
    fn advance(&self, count: u64) -> PyResult<()> {
        let advanced = {
            let mut position = crate::support::lock_state(&self.inner.position);
            if count > self.size.saturating_sub(*position) {
                false
            } else {
                *position += count;
                true
            }
        };
        if advanced {
            Ok(())
        } else {
            Err(crate::support::core_error(zstar::Error::Range))
        }
    }
    fn tell(&self) -> u64 {
        *crate::support::lock_state(&self.inner.position)
    }
    fn seek(&self, offset: i128, whence: i32) -> PyResult<u64> {
        // Do arithmetic under the lock; construct Python exceptions only after
        // releasing it, since exception construction may re-enter Python.
        let result = {
            let mut current = crate::support::lock_state(&self.inner.position);
            seek_position(*current, self.size, offset, whence).inspect(|position| {
                *current = *position;
            })
        };
        result.map_err(|error| match error {
            SeekError::Whence => pyo3::exceptions::PyValueError::new_err("invalid whence"),
            SeekError::Negative => {
                pyo3::exceptions::PyValueError::new_err("negative seek position")
            }
            SeekError::Overflow => pyo3::exceptions::PyOverflowError::new_err(
                "seek position exceeds the archive range",
            ),
        })
    }
}
enum SeekError {
    Whence,
    Negative,
    Overflow,
}
fn seek_position(current: u64, size: u64, offset: i128, whence: i32) -> Result<u64, SeekError> {
    let origin = match whence {
        0 => 0,
        1 => current,
        2 => size,
        _ => return Err(SeekError::Whence),
    };
    let position = i128::from(origin)
        .checked_add(offset)
        .ok_or(SeekError::Overflow)?;
    if position < 0 {
        return Err(SeekError::Negative);
    }
    u64::try_from(position).map_err(|_| SeekError::Overflow)
}
/// Own a contiguous writable byte view across native work. Slices are copied
/// through Python's buffer protocol, without retaining raw pointers or Rust
/// references to memory that a free-threaded Python caller could mutate.
pub struct WritableBuffer {
    view: Py<PyAny>,
    length: usize,
}
impl WritableBuffer {
    pub fn new(buffer: &Bound<'_, PyAny>) -> PyResult<Self> {
        let view = PyMemoryView::from(buffer)?.call_method1("cast", ("B",))?;
        let buffer = PyBuffer::<u8>::get(&view)?;
        if buffer.readonly() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "readinto requires a writable buffer",
            ));
        }
        Ok(Self {
            length: buffer.item_count(),
            view: view.unbind(),
        })
    }
    fn copy(&self, offset: u64, bytes: &[u8]) -> PyResult<()> {
        Python::attach(|py| {
            let start = isize::try_from(offset)
                .map_err(|_| crate::support::core_error(zstar::Error::Range))?;
            let length = isize::try_from(bytes.len())
                .map_err(|_| crate::support::core_error(zstar::Error::Range))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| crate::support::core_error(zstar::Error::Range))?;
            let slice = self
                .view
                .bind(py)
                .get_item(PySlice::new(py, start, end, 1))?;
            PyBuffer::<u8>::get(&slice)?.copy_from_slice(py, bytes)
        })
    }
}

/// Python file reads accept negative sizes as "to EOF" and arbitrarily large
/// positive integers; clamp before extracting to the wire's u64 address space.
pub fn member_read_length(size: Option<&Bound<'_, PyAny>>) -> PyResult<Option<u64>> {
    let Some(size) = size.filter(|size| !size.is_none()) else {
        return Ok(None);
    };
    let size = size.call_method0("__index__")?;
    if size.lt(0)? {
        Ok(None)
    } else if size.gt(u64::MAX)? {
        Ok(Some(u64::MAX))
    } else {
        size.extract().map(Some)
    }
}

/// Owns a reader snapshot. Adapters admit operations through short-lived leases;
/// closing stops admission without holding a lock while calling Python.
pub struct Session {
    owner: Arc<Mutex<Owner>>,
    scope: Arc<Mutex<Option<Arc<crate::dispatch::Scope>>>>,
    native_token: Option<u64>,
    terminal: crate::completion::Completion,
}
struct Owner {
    open: Option<Arc<State>>,
    closing: Option<Arc<State>>,
}
pub struct State {
    pub reader: Reader,
    closed: watch::Sender<bool>,
    jobs: crate::dispatch::JobCounter,
    native_owner: Option<Py<PyAny>>,
    native_source: Option<Arc<crate::sources::SourceState>>,
    owned: bool,
}
pub struct Lease(Arc<State>);
impl Deref for Lease {
    type Target = State;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.jobs.exit();
    }
}
impl Session {
    pub async fn open_scoped(
        source: Arc<dyn zstar::AsyncRangeReader>,
        options: ReaderOptions,
        scope: Arc<crate::dispatch::Scope>,
        native_owner: Option<Py<PyAny>>,
        native_token: Option<u64>,
        native_source: Option<Arc<crate::sources::SourceState>>,
        owned: bool,
    ) -> PyResult<Self> {
        let reader = match Reader::open_with_options(source, options).await {
            Ok(reader) => reader,
            Err(error) => {
                scope.drain().await;
                if owned && let Some(source) = native_source {
                    source.close().await;
                }
                return Err(core_error(error));
            }
        };
        Ok(Self {
            owner: Arc::new(Mutex::new(Owner {
                open: Some(Arc::new(State {
                    reader,
                    closed: watch::channel(false).0,
                    jobs: crate::dispatch::JobCounter::default(),
                    native_owner,
                    native_source,
                    owned,
                })),
                closing: None,
            })),
            scope: Arc::new(Mutex::new(Some(scope))),
            native_token,
            terminal: crate::completion::Completion::default(),
        })
    }
    pub fn lease(&self) -> PyResult<Lease> {
        Python::attach(|py| self.check_access(py))?;
        let admitted = {
            let owner = crate::support::lock_state(&self.owner);
            owner.open.as_ref().map(|state| {
                state.jobs.enter();
                state.clone()
            })
        };
        admitted
            .map(Lease)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("archive closed"))
    }
    pub fn check_access(&self, py: Python<'_>) -> PyResult<()> {
        let scope = crate::support::lock_state(&self.scope).clone();
        if let Some(scope) = scope {
            scope.reject(py)?;
        }
        if let Some(token) = self.native_token {
            crate::dispatch::callbacks(py)?.reject.call1(py, (token,))?;
        }
        Ok(())
    }
    pub fn closed(&self) -> bool {
        crate::support::lock_state(&self.owner).open.is_none()
    }
    /// Stop admission and cancel async work immediately, even if the adapter's
    /// close awaitable is dropped. Repeated closers join the same active state.
    fn begin_close(&self) -> Closing {
        let state = {
            let mut owner = crate::support::lock_state(&self.owner);
            if let Some(state) = owner.open.take() {
                owner.closing = Some(state.clone());
                Some(state)
            } else {
                owner.closing.clone()
            }
        };
        if let Some(state) = &state {
            state.closed.send_replace(true);
        }
        Closing {
            state,
            scope: self.scope.clone(),
            owner: self.owner.clone(),
        }
    }
    pub fn close(
        &self,
        py: Python<'_>,
    ) -> PyResult<impl Future<Output = PyResult<()>> + Send + 'static> {
        self.check_access(py)?;
        self.start_close(py)?;
        Ok(self.terminal.wait())
    }
    fn start_close(&self, py: Python<'_>) -> PyResult<()> {
        self.terminal.start(py, || {
            let closing = self.begin_close();
            async move {
                closing.drain().await;
                Ok(())
            }
        })
    }
    pub fn clear(&self) {
        let _ = Python::attach(|py| self.start_close(py));
        self.terminal.clear();
    }
    pub fn traverse(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(scope) = crate::support::lock_state(&self.scope).as_ref() {
            scope.traverse(visit)?;
        }
        let owner = crate::support::lock_state(&self.owner);
        if let Some(state) = owner.open.as_ref().or(owner.closing.as_ref()) {
            visit.call(&state.native_owner)?;
        }
        drop(owner);
        self.terminal.traverse(visit)?;
        Ok(())
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        let _ = Python::attach(|py| self.start_close(py));
    }
}
/// Keeps the closing state alive while an async adapter drains admitted work.
pub struct Closing {
    state: Option<Arc<State>>,
    owner: Arc<Mutex<Owner>>,
    scope: Arc<Mutex<Option<Arc<crate::dispatch::Scope>>>>,
}
impl Closing {
    pub async fn drain(self) {
        if let Some(state) = self.state {
            state.jobs.drain().await;
            if let Some(source) = &state.native_source {
                source.drain_callbacks().await;
                if state.owned {
                    source.clone().close().await;
                }
            }
        }
        let scope = crate::support::lock_state(&self.scope).clone();
        if let Some(scope) = scope {
            scope.drain().await;
        }
        let scope = crate::support::lock_state(&self.scope).take();
        drop(scope);
        let state = crate::support::lock_state(&self.owner).closing.take();
        drop(state);
    }
}
impl Lease {
    /// Async adapters select against close; synchronous adapters finish admitted
    /// calls directly on the native runtime without a Python event-loop hop.
    pub async fn run<T>(&self, work: impl Future<Output = PyResult<T>>) -> PyResult<T> {
        let mut closed = self.closed.subscribe();
        tokio::select! {
            biased;
            () = async {
                if !*closed.borrow_and_update() {
                    let _ = closed.changed().await;
                }
            } => Err(crate::support::core_error(zstar::Error::Unsupported("reader closed"))),
            result = work => result,
        }
    }
}
impl State {
    async fn check_file(&self, path: &str) -> PyResult<()> {
        let entry = self
            .reader
            .entry(path)
            .await
            .map_err(core_error)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(path.to_owned()))?;
        match entry.kind() {
            zstar::EntryKind::File => Ok(()),
            zstar::EntryKind::Directory => Err(pyo3::exceptions::PyIsADirectoryError::new_err(
                path.to_owned(),
            )),
            zstar::EntryKind::Symlink => Err(core_error(zstar::Error::Unsupported(
                "cannot read a symlink as a file",
            ))),
        }
    }

    /// Map a failed read, giving missing/kind errors the precedence they had when
    /// every path was checked up front. Only failures pay for the extra lookup.
    async fn read_error(
        &self,
        paths: impl IntoIterator<Item = &str>,
        error: zstar::Error,
    ) -> PyErr {
        for path in paths {
            if let Err(error) = self.check_file(path).await {
                return error;
            }
        }
        core_error(error)
    }

    async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
    ) -> zstar::Result<std::ops::Range<u64>> {
        let end = match length {
            Some(length) => offset.checked_add(length).ok_or(zstar::Error::Range)?,
            None => self
                .reader
                .entry(path)
                .await?
                .ok_or_else(|| zstar::Error::MissingEntry(path.into()))?
                .size(),
        };
        if end < offset {
            return Err(zstar::Error::Range);
        }
        Ok(offset..end)
    }
    pub async fn entry_value(&self, path: &str) -> PyResult<Option<crate::values::EntryValue>> {
        self.reader
            .entry_layout(path)
            .await
            .map_err(core_error)?
            .map(crate::values::EntryValue::new)
            .transpose()
    }
    pub async fn read(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
    ) -> PyResult<Py<PyBytes>> {
        let result = async {
            let range = self.read_range(path, offset, length).await?;
            self.reader.read_range(path, range).await
        }
        .await;
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => return Err(self.read_error([path], error).await),
        };
        Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
    }
    pub async fn read_many(
        &self,
        requests: &[(String, u64, Option<u64>)],
    ) -> PyResult<Vec<Py<PyBytes>>> {
        let mut batch = Vec::with_capacity(requests.len());
        for (path, offset, length) in requests {
            let length = match length {
                Some(length) => *length,
                None => match self.read_range(path, *offset, None).await {
                    Ok(range) => range.end - range.start,
                    Err(error) => return Err(self.read_error([path.as_str()], error).await),
                },
            };
            batch.push(zstar::ReadRequest {
                path,
                offset: *offset,
                length,
            });
        }
        let bytes = match self.reader.read_many(&batch).await {
            Ok(bytes) => bytes,
            Err(error) => {
                // Collected first: a borrowing iterator here breaks the `Send` bound.
                let paths: Vec<&str> = batch.iter().map(|request| request.path).collect();
                return Err(self.read_error(paths, error).await);
            }
        };
        Ok(Python::attach(|py| {
            bytes
                .iter()
                .map(|bytes| PyBytes::new(py, bytes).unbind())
                .collect()
        }))
    }
}

pub fn request_values(requests: &Bound<'_, PyAny>) -> PyResult<Vec<(String, u64, Option<u64>)>> {
    let count = requests.len()?;
    if count > zstar::MAX_BATCH_REQUESTS {
        return Err(core_error(zstar::Error::budget(
            zstar::BudgetResource::BatchRequests,
            "batch request count",
        )));
    }
    (0..count)
        .map(|index| {
            let request = requests.get_item(index)?;
            Ok((
                request.getattr("path")?.extract()?,
                request
                    .getattr("offset")?
                    .extract::<crate::support::Unsigned>()?
                    .0,
                request
                    .getattr("length")?
                    .extract::<Option<crate::support::Unsigned>>()?
                    .map(|length| length.0),
            ))
        })
        .collect()
}

/// Shared transactional metadata cursor. Each adapter leases the parent anew;
/// pending and buffered metadata never keep the archive source alive.
pub struct EntryIterator {
    prefix: Option<String>,
    checkpoint: Option<zstar::backend::EntryCursor>,
    exhausted: bool,
}

impl EntryIterator {
    pub const fn new(prefix: Option<String>) -> Self {
        Self {
            prefix,
            checkpoint: None,
            exhausted: false,
        }
    }
    pub async fn next_batch(&mut self, reader: &Reader) -> PyResult<Vec<Py<PyAny>>> {
        if !self.exhausted {
            let checkpoint = match self.checkpoint.clone() {
                Some(checkpoint) => checkpoint,
                None => match &self.prefix {
                    Some(prefix) => reader.cursor_prefix(prefix).await,
                    None => reader.cursor().await,
                }
                .map_err(core_error)?,
            };
            let (checkpoint, entries) = reader
                .next_entries(checkpoint, 64)
                .await
                .map_err(core_error)?;
            let output = Python::attach(|py| {
                entries
                    .into_iter()
                    .map(
                        |entry| Ok(Py::new(py, crate::values::EntryValue::new(entry)?)?.into_any()),
                    )
                    .collect::<PyResult<Vec<_>>>()
            })?;
            self.exhausted = output.is_empty();
            self.checkpoint = Some(checkpoint);
            return Ok(output);
        }
        Ok(Vec::new())
    }
}

/// Exact-range source using the explicit mode selected by the owning facade.
struct ProtocolSource {
    size: u64,
    read: crate::dispatch::Callback,
}
#[async_trait::async_trait]
impl zstar::AsyncRangeReader for ProtocolSource {
    async fn size(&self) -> zstar::Result<u64> {
        Ok(self.size)
    }
    async fn read_range(&self, range: std::ops::Range<u64>) -> zstar::Result<bytes::Bytes> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or(zstar::Error::Range)?;
        zstar::backend::checked_range(range.start, length, self.size)?;
        let args = Python::attach(|py| (range.start, length).into_pyobject(py).map(Bound::unbind))
            .map_err(|error| zstar::Error::Io(py_io(error)))?;
        let value = self
            .read
            .call(args)
            .await
            .map_err(|error| zstar::Error::Io(py_io(error)))?;
        Python::attach(|py| {
            let value = value.bind(py).cast::<PyBytes>()?;
            if value.as_bytes().len() as u64 != length {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "range source must return exactly the requested bytes",
                ));
            }
            // Share the immutable Python bytes instead of copying them.
            Ok(bytes::Bytes::from_owner(
                pyo3::pybacked::PyBackedBytes::from(value.clone()),
            ))
        })
        .map_err(|error| zstar::Error::Io(py_io(error)))
    }
}

pub fn protocol_open(
    py: Python<'_>,
    source: Py<PyAny>,
    options: &crate::config::ReaderConfig,
    asynchronous: bool,
    owned: bool,
) -> PyResult<impl Future<Output = PyResult<Session>> + Send + 'static> {
    // Complete policy conversion and callable validation before any source I/O.
    let scope = crate::dispatch::Scope::new(py, asynchronous)?;
    let policy = options.bind(py, &scope)?;
    let native = crate::sources::native_source(py, &source, asynchronous)?;
    let callbacks = if native.is_none() {
        Some((
            scope.protocol_callback(py, source.clone_ref(py), "size")?,
            scope.protocol_callback(py, source.clone_ref(py), "read_at")?,
        ))
    } else {
        None
    };
    Ok(async move {
        let native_source = native.clone();
        let (source, native_owner, native_token): (Arc<dyn zstar::AsyncRangeReader>, _, _) =
            if let Some(native) = native {
                let token = native.token;
                (native, Some(source), Some(token))
            } else {
                let (size, read) = callbacks.unwrap();
                let args = Python::attach(|py| pyo3::types::PyTuple::empty(py).unbind());
                let value = size
                    .call(args)
                    .await
                    .map_err(|error| core_error(zstar::Error::Io(py_io(error))))?;
                let size =
                    Python::attach(|py| value.bind(py).extract::<crate::support::Unsigned>())?.0;
                (Arc::new(ProtocolSource { size, read }), None, None)
            };
        Session::open_scoped(
            source,
            policy,
            scope,
            native_owner,
            native_token,
            native_source,
            owned,
        )
        .await
    })
}
