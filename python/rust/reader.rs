//! Reader session shared by synchronous, asynchronous, and filesystem adapters.
use pyo3::{
    buffer::PyBuffer,
    prelude::*,
    types::{PyBytes, PyMemoryView, PySlice},
};
use std::{
    future::Future,
    ops::Deref,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, watch};

use super::request::FailureSlot;
use crate::support::{PyCause, core_error, py_io};
use zstar::{ObjectResolver, ReaderOptions};

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
    cursor: zstar::FileCursor,
    position: Mutex<u64>,
}
impl MemberCursor {
    pub(crate) async fn open(reader: &Reader, path: &str, cache_bytes: usize) -> PyResult<Self> {
        let entry = reader
            .entry(path)
            .await
            .map_err(crate::support::core_error)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(path.to_owned()))?;
        match entry.kind {
            zstar::EntryKind::Directory => {
                return Err(pyo3::exceptions::PyIsADirectoryError::new_err(
                    path.to_owned(),
                ));
            }
            zstar::EntryKind::Symlink => {
                return Python::attach(|py| {
                    let error = py
                        .import("tarfile")?
                        .getattr("ExtractError")?
                        .call1(("cannot open a symlink as a file",))?;
                    Err(PyErr::from_value(error))
                });
            }
            zstar::EntryKind::File => (),
        }
        let cursor = reader
            .open_cursor(path)
            .await
            .map_err(crate::support::core_error)?
            .with_cache_bytes(cache_bytes);
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
        let step = reader.max_read_bytes().max(1);
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
    owner: Mutex<Owner>,
}
struct Owner {
    open: Option<Arc<State>>,
    closing: Weak<State>,
}
pub struct State {
    pub reader: Reader,
    pub resolver: Option<Arc<dyn ObjectResolver>>,
    closed: watch::Sender<bool>,
    active: AtomicUsize,
    drained: Notify,
    failures: Option<Arc<FailureSlot>>,
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
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}
impl Session {
    pub async fn open(
        source: Arc<dyn zstar::AsyncRangeReader>,
        options: ReaderOptions,
        failures: Option<Arc<FailureSlot>>,
    ) -> PyResult<Self> {
        let resolver = options.resolver.clone();
        let reader = Reader::open_with_options(source, options)
            .await
            .map_err(crate::support::core_error)
            .map_err(|error| match &failures {
                Some(failures) => failures.annotate(error),
                None => error,
            })?;
        Ok(Self {
            owner: Mutex::new(Owner {
                open: Some(Arc::new(State {
                    reader,
                    resolver,
                    closed: watch::channel(false).0,
                    active: AtomicUsize::new(0),
                    drained: Notify::new(),
                    failures,
                })),
                closing: Weak::new(),
            }),
        })
    }
    pub async fn from_bytes(data: Vec<u8>, options: ReaderOptions) -> PyResult<Self> {
        Self::open(Arc::new(bytes::Bytes::from(data)), options, None).await
    }
    pub async fn from_file(path: std::path::PathBuf, options: ReaderOptions) -> PyResult<Self> {
        let source = zstar::fs::AsyncLocalFileSource::open(path)
            .await
            .map_err(crate::support::local_source_error)?;
        Self::open(Arc::new(source), options, None).await
    }
    pub async fn from_python(
        source: Py<PyAny>,
        size: u64,
        options: ReaderOptions,
    ) -> PyResult<Self> {
        Self::open(Arc::new(PythonSource::new(source, size)), options, None).await
    }
    pub async fn from_fileobj(file: Py<PyAny>, options: ReaderOptions) -> PyResult<Self> {
        static CAPACITY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
        let (source, size) = crate::support::blocking(&CAPACITY, move || {
            Python::attach(|py| {
                let source = py
                    .import("zstar._source")?
                    .getattr("_FileSource")?
                    .call1((file,))?;
                let size = source.getattr("size")?.extract()?;
                Ok((source.unbind(), size))
            })
        })
        .await?;
        Self::from_python(source, size, options).await
    }
    pub async fn from_s3(
        builder: zstar::s3::AmazonS3Builder,
        key: String,
        prefix: Option<String>,
        mut options: ReaderOptions,
    ) -> PyResult<Self> {
        let source = super::s3::source(builder, key, prefix, &mut options).map_err(core_error)?;
        Self::open(source, options, None).await
    }
    pub fn lease(&self) -> PyResult<Lease> {
        let admitted = {
            let owner = crate::support::lock_state(&self.owner);
            owner.open.as_ref().map(|state| {
                state.active.fetch_add(1, Ordering::Relaxed);
                state.clone()
            })
        };
        admitted
            .map(Lease)
            .ok_or_else(|| crate::support::core_error(zstar::Error::Unsupported("reader closed")))
    }
    pub fn closed(&self) -> bool {
        crate::support::lock_state(&self.owner).open.is_none()
    }
    /// Stop admission and cancel async work immediately, even if the adapter's
    /// close awaitable is dropped. Repeated closers join the same active state.
    pub fn close(&self) -> Closing {
        let state = {
            let mut owner = crate::support::lock_state(&self.owner);
            if let Some(state) = owner.open.take() {
                owner.closing = Arc::downgrade(&state);
                Some(state)
            } else {
                owner.closing.upgrade()
            }
        };
        if let Some(state) = &state {
            state.closed.send_replace(true);
        }
        Closing(state)
    }
}
/// Keeps the closing state alive while an async adapter drains admitted work.
pub struct Closing(Option<Arc<State>>);
impl Closing {
    pub async fn drain(self) {
        if let Some(state) = self.0 {
            loop {
                let drained = state.drained.notified();
                tokio::pin!(drained);
                drained.as_mut().enable();
                if state.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                drained.await;
            }
        }
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
            result = work => result.map_err(|error| match &self.failures {
                Some(failures) => failures.annotate(error),
                None => error,
            }),
        }
    }
}
impl State {
    pub async fn member_info(&self, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let entry = self.reader.entry(path).await.map_err(core_error)?;
        Python::attach(|py| {
            entry
                .as_ref()
                .map(|entry| member_metadata(py, entry))
                .transpose()
        })
    }

    fn object_resolver<'a>(
        &'a self,
        resolver: Option<&'a dyn ObjectResolver>,
    ) -> Option<&'a dyn ObjectResolver> {
        resolver.or(self.resolver.as_deref())
    }
    async fn read_length(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
    ) -> zstar::Result<u64> {
        match length {
            Some(length) => Ok(length),
            None => self
                .reader
                .entry(path)
                .await?
                .ok_or_else(|| zstar::Error::MissingEntry(path.into()))?
                .size()?
                .checked_sub(offset)
                .ok_or(zstar::Error::Range),
        }
    }
    async fn read_bytes(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<&dyn ObjectResolver>,
    ) -> zstar::Result<bytes::Bytes> {
        let length = self.read_length(path, offset, length).await?;
        self.reader
            .read_range_with_resolver(path, offset, length, self.object_resolver(resolver))
            .await
    }
    async fn read_bytes_with_report(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<&dyn ObjectResolver>,
    ) -> zstar::Result<(bytes::Bytes, zstar::ReadReport)> {
        let length = self.read_length(path, offset, length).await?;
        self.reader
            .read_range_with_report_and_resolver(
                path,
                offset,
                length,
                self.object_resolver(resolver),
            )
            .await
    }
    async fn read_many_bytes(
        &self,
        requests: &[(String, u64, u64)],
        resolver: Option<&dyn ObjectResolver>,
    ) -> zstar::Result<Vec<bytes::Bytes>> {
        let requests: Vec<_> = requests
            .iter()
            .map(|(path, offset, length)| zstar::ReadRequest {
                path,
                offset: *offset,
                length: *length,
            })
            .collect();
        self.reader
            .read_many_with_resolver(&requests, self.object_resolver(resolver))
            .await
    }
}

/// One keyword parser and set of defaults for every native reader factory.
#[pyclass(module = "zstar._native", frozen)]
pub struct ReaderConfig {
    inner: ReaderOptions,
}
#[pymethods]
impl ReaderConfig {
    #[new]
    #[pyo3(signature = (*,
            limits = None,
            concurrent_chunks = 8,
            verify_external = false,
            memory_bytes = 8 << 20,
            max_encoded_bytes = 4 << 30,
            max_decoded_bytes = 4 << 30,
            max_read_bytes = 64 << 20,
            max_retained_entry_bytes = 1 << 30,
            max_entries = 0,
            max_spool_bytes = 4 << 30,
            create_spill_store = None,
            resolver = None,
    ))]
    #[expect(
        clippy::too_many_arguments,
        reason = "Shared Python constructor keyword options."
    )]
    fn new(
        py: Python<'_>,
        limits: Option<&Bound<'_, pyo3::types::PyDict>>,
        concurrent_chunks: usize,
        verify_external: bool,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let options = ReaderOptions {
            concurrent_chunks,
            verify_external,
            resolver: resolver
                .map(|r| super::external::binding(py, r))
                .transpose()?,
            limits: self::limits(limits)?,
            max_read_bytes,
            metadata: zstar::MetadataReaderOptions {
                spool: zstar::SpoolOptions {
                    memory_bytes,
                    budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
                    create_store: super::manifest::spill_factory(py, create_spill_store)?,
                    ..Default::default()
                },
                max_encoded_bytes,
                max_decoded_bytes,
                max_retained_entry_bytes,
                max_entries,
            },
        };
        Ok(Self { inner: options })
    }
}

pub fn reader_options(
    py: Python<'_>,
    options: Option<&Bound<'_, pyo3::types::PyDict>>,
) -> PyResult<ReaderOptions> {
    if let Some(resolver) = options
        .map(|o| o.get_item("resolver"))
        .transpose()?
        .flatten()
        && !resolver.is_none()
        && crate::callback::AsyncCallback::is_async_method(py, &resolver.unbind(), "get_encoded")?
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "use AsyncArchive for async resolvers",
        ));
    }
    async_reader_options(py, options)
}

pub fn async_reader_options(
    py: Python<'_>,
    options: Option<&Bound<'_, pyo3::types::PyDict>>,
) -> PyResult<ReaderOptions> {
    let config = py.get_type::<ReaderConfig>().call((), options)?;
    Ok(config.extract::<PyRef<'_, ReaderConfig>>()?.inner.clone())
}

pub fn batch_requests(requests: &Bound<'_, PyAny>) -> PyResult<Vec<(String, u64, u64)>> {
    let count = requests.len()?;
    if count > zstar::MAX_BATCH_REQUESTS {
        return Err(core_error(zstar::Error::Budget("batch request count")));
    }
    (0..count)
        .map(|index| requests.get_item(index)?.extract())
        .collect()
}

pub fn report_dict(py: Python<'_>, report: zstar::ReadReport) -> PyResult<Py<pyo3::types::PyDict>> {
    let result = pyo3::types::PyDict::new(py);
    result.set_item("metadata_validated", report.metadata_validated)?;
    result.set_item("full_archive_validated", report.full_archive_validated)?;
    result.set_item("data_frames_checked", report.data_frames_checked)?;
    result.set_item(
        "external_objects_sha256_checked",
        report
            .external_objects_sha256_checked
            .iter()
            .map(zstar::backend::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    result.set_item(
        "external_objects_unchecked",
        report
            .external_objects_unchecked
            .iter()
            .map(zstar::backend::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    Ok(result.unbind())
}

struct PythonSource {
    object: Arc<Py<PyAny>>,
    size: u64,
    permits: Arc<tokio::sync::Semaphore>,
    callback: Option<crate::callback::AsyncCallback>,
}
impl PythonSource {
    fn new(source: Py<PyAny>, size: u64) -> Self {
        Self {
            object: Arc::new(source),
            size,
            permits: Arc::new(tokio::sync::Semaphore::new(8)),
            callback: None,
        }
    }
}

#[async_trait::async_trait]
impl zstar::AsyncRangeReader for PythonSource {
    async fn size(&self) -> zstar::Result<u64> {
        Ok(self.size)
    }
    async fn read_range(&self, range: std::ops::Range<u64>) -> zstar::Result<bytes::Bytes> {
        zstar::backend::checked_range(
            range.start,
            range
                .end
                .checked_sub(range.start)
                .ok_or(zstar::Error::Range)?,
            self.size,
        )?;
        if let Some(callback) = &self.callback {
            let args = Python::attach(|py| {
                (range.start, range.end - range.start)
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .map_err(|error| zstar::Error::Io(py_io(error)))?;
            let value = callback
                .call(args)
                .await
                .map_err(|error| zstar::Error::Io(py_io(error)))?;
            let bytes = Python::attach(|py| -> PyResult<Vec<u8>> {
                if value.bind(py).len()? as u64 != range.end - range.start {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "short range response",
                    ));
                }
                value.extract(py)
            })
            .map_err(|error| zstar::Error::Io(py_io(error)))?;
            return Ok(bytes::Bytes::from(bytes));
        }
        let object = self.object.clone();
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| zstar::Error::Unsupported("source closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let bytes: Vec<u8> = Python::attach(|py| -> PyResult<Vec<u8>> {
                let value = object.call_method1(
                    py,
                    "read_range",
                    (range.start, range.end - range.start),
                )?;
                if value.bind(py).len()? as u64 != range.end - range.start {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "short range response",
                    ));
                }

                value.extract(py)
            })
            .map_err(|error| zstar::Error::Io(PyCause::io(error.to_string(), error)))?;
            if bytes.len() as u64 != range.end - range.start {
                return Err(zstar::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short range response",
                )));
            }
            Ok(bytes::Bytes::from(bytes))
        })
        .await
        .map_err(|_| zstar::Error::Io(std::io::Error::other("Python source worker failed")))?
    }
}

impl State {
    pub async fn entry(&self, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let entry = self.reader.entry(path).await.map_err(core_error)?;
        Python::attach(|py| {
            entry
                .map(|entry| super::manifest::to_py(py, &entry.to_value()))
                .transpose()
        })
    }
    pub async fn read(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<&dyn ObjectResolver>,
    ) -> PyResult<Py<PyBytes>> {
        let bytes = self
            .read_bytes(path, offset, length, resolver)
            .await
            .map_err(core_error)?;
        Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
    }
    pub async fn read_with_report(
        &self,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<&dyn ObjectResolver>,
    ) -> PyResult<(Py<PyBytes>, Py<pyo3::types::PyDict>)> {
        let (bytes, report) = self
            .read_bytes_with_report(path, offset, length, resolver)
            .await
            .map_err(core_error)?;
        Python::attach(|py| Ok((PyBytes::new(py, &bytes).unbind(), report_dict(py, report)?)))
    }
    pub async fn read_many(
        &self,
        requests: &[(String, u64, u64)],
        resolver: Option<&dyn ObjectResolver>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        let bytes = self
            .read_many_bytes(requests, resolver)
            .await
            .map_err(core_error)?;
        Ok(Python::attach(|py| {
            bytes
                .iter()
                .map(|bytes| PyBytes::new(py, bytes).unbind())
                .collect()
        }))
    }
    pub async fn verify(&self, external: bool) -> PyResult<()> {
        let scope = if external {
            zstar::VerificationScope::ExternalClosure
        } else {
            zstar::VerificationScope::Archive
        };
        self.reader.verify(scope).await.map_err(core_error)
    }
    pub async fn read_link(&self, path: &str) -> PyResult<String> {
        self.reader.read_link(path).await.map_err(core_error)
    }
}

/// Shared transactional metadata cursor. Each adapter leases the parent anew;
/// pending and buffered metadata never keep the archive source alive.
pub struct EntryIterator {
    prefix: Option<String>,
    checkpoint: Option<zstar::backend::EntryCursor>,
    exhausted: bool,
    buffer: std::collections::VecDeque<Py<PyAny>>,
    projection: MetadataProjection,
}

#[derive(Clone, Copy)]
pub enum MetadataProjection {
    Wire,
    Member,
    Name,
}

fn member_metadata(py: Python<'_>, entry: &zstar::Entry) -> PyResult<Py<PyAny>> {
    Ok((
        &entry.path,
        entry.kind.code(),
        entry.mode,
        entry.uid,
        entry.gid,
        entry.mtime_ns,
        entry.size().map_err(core_error)?,
        entry.linkname.as_deref().unwrap_or(""),
        !entry.attributes.is_empty(),
    )
        .into_pyobject(py)?
        .into_any()
        .unbind())
}

impl EntryIterator {
    pub const fn new(prefix: Option<String>) -> Self {
        Self::projected(prefix, MetadataProjection::Wire)
    }

    pub const fn projected(prefix: Option<String>, projection: MetadataProjection) -> Self {
        Self {
            prefix,
            checkpoint: None,
            exhausted: false,
            buffer: std::collections::VecDeque::new(),
            projection,
        }
    }
    pub async fn next_batch(&mut self, reader: &Reader, limit: usize) -> PyResult<Vec<Py<PyAny>>> {
        if self.buffer.is_empty() && !self.exhausted {
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
                    .iter()
                    .map(|entry| match self.projection {
                        MetadataProjection::Wire => super::manifest::to_py(py, &entry.to_value()),
                        MetadataProjection::Member => member_metadata(py, entry),
                        MetadataProjection::Name => {
                            Ok(entry.path.as_str().into_pyobject(py)?.into_any().unbind())
                        }
                    })
                    .collect::<PyResult<std::collections::VecDeque<_>>>()
            })?;
            self.exhausted = output.is_empty();
            self.checkpoint = Some(checkpoint);
            self.buffer = output;
        }
        Ok(self.buffer.drain(..limit.min(self.buffer.len())).collect())
    }
}

/// Parse caller policy without introducing wire-format maxima.
pub fn limits(options: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<zstar::Limits> {
    let mut result = zstar::Limits::default();
    if let Some(options) = options {
        for (key, value) in options {
            let key: String = key.extract()?;
            let field = match key.as_str() {
                "max_frame_encoded_bytes" => &mut result.max_frame_encoded_bytes,
                "max_frame_decoded_bytes" => &mut result.max_frame_decoded_bytes,
                "max_window_bytes" => &mut result.max_window_bytes,
                "max_entry_bytes" => &mut result.max_entry_bytes,
                "max_extents" => &mut result.max_extents,
                "max_pointer_bytes" => &mut result.max_pointer_bytes,
                _ => {
                    return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                        "unknown limit: {key}"
                    )));
                }
            };
            *field = value.extract()?;
        }
    }
    Ok(result)
}

pub fn python_source(
    py: Python<'_>,
    source: Py<PyAny>,
    size: u64,
) -> PyResult<Arc<dyn zstar::AsyncRangeReader>> {
    let callback = crate::callback::AsyncCallback::detect(py, &source, "read_range")?;
    let mut reader = PythonSource::new(source, size);
    reader.callback = callback;
    Ok(Arc::new(reader))
}
