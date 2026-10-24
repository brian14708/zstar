use crate::core_error;
use ciborium::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyList, PyString, PyTuple};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use zstar::backend::{BlockingArchive, BlockingBuilder, EncodedObjectResolver};
use zstar::{Entry, EntryCursor, EntryKind, Error, ManifestOptions, RangeReader};

fn py_io(error: PyErr) -> std::io::Error {
    crate::PyCause::io(error.to_string(), error)
}
/// Copy a bytes-like object returned by a Python callback into `out`,
/// rejecting anything longer than the requested bound.
fn copy_bytes_like(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    out: &mut [u8],
    what: &'static str,
) -> PyResult<usize> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        let bytes = bytes.as_bytes();
        if bytes.len() > out.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(what));
        }
        out[..bytes.len()].copy_from_slice(bytes);
        return Ok(bytes.len());
    }
    let buffer = pyo3::buffer::PyBuffer::<u8>::get(value)
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("expected a bytes-like object"))?;
    if buffer.item_count() > out.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(what));
    }
    let length = buffer.item_count();
    buffer.copy_to_slice(py, &mut out[..length])?;
    Ok(length)
}
fn from_py(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > 64 {
        return Err(core_error(Error::Budget("CBOR nesting limit")));
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(value.extract()?));
    }
    if value.is_instance_of::<PyInt>() {
        let integer: i128 = value.extract()?;
        let integer = ciborium::value::Integer::try_from(integer).map_err(|_| {
            pyo3::exceptions::PyOverflowError::new_err("integer is outside CBOR range")
        })?;
        return Ok(Value::Integer(integer));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(Value::Text(value.extract()?));
    }
    if value.is_instance_of::<PyBytes>() {
        return Ok(Value::Bytes(value.extract()?));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let mut map = Vec::new();
        for (key, val) in dict.iter() {
            map.push((Value::Text(key.extract()?), from_py(&val, depth + 1)?));
        }
        return Ok(Value::Map(map));
    }
    if value.is_instance_of::<PyTuple>() || value.is_instance_of::<PyList>() {
        let mut values = Vec::new();
        for item in value.try_iter()? {
            values.push(from_py(&item?, depth + 1)?);
        }
        return Ok(Value::Array(values));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected restricted CBOR value",
    ))
}
struct PythonSpill(Py<PyAny>);
impl Read for PythonSpill {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        Python::attach(|py| -> PyResult<usize> {
            let value = self.0.call_method1(py, "read", (out.len(),))?;
            copy_bytes_like(py, value.bind(py), out, "oversized spill-store read")
        })
        .map_err(py_io)
    }
}
impl Write for PythonSpill {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        Python::attach(|py| -> PyResult<usize> {
            let result = self
                .0
                .call_method1(py, "write", (PyBytes::new(py, bytes),))?;
            let size: usize = if result.is_none(py) {
                bytes.len()
            } else {
                result.extract(py)?
            };
            if size > bytes.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "oversized spill-store write",
                ));
            }
            Ok(size)
        })
        .map_err(py_io)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Python::attach(|py| self.0.call_method0(py, "flush").map(|_| ())).map_err(py_io)
    }
}
impl Seek for PythonSpill {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let (offset, whence): (i128, u8) = match position {
            SeekFrom::Start(at) => (at.into(), 0),
            SeekFrom::Current(at) => (at.into(), 1),
            SeekFrom::End(at) => (at.into(), 2),
        };
        Python::attach(|py| {
            self.0
                .call_method1(py, "seek", (offset, whence))?
                .extract(py)
        })
        .map_err(py_io)
    }
}
pub(crate) fn spill_factory(
    py: Python<'_>,
    callback: Option<Py<PyAny>>,
) -> PyResult<Option<zstar::SpillFactory>> {
    callback
        .map(|callback| {
            if !callback.bind(py).is_callable() {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "create_spill_store must be callable",
                ));
            }
            Ok(zstar::SpillFactory::new(move || {
                Python::attach(|py| callback.call0(py))
                    .map(|store| Box::new(PythonSpill(store)) as Box<dyn zstar::SpillStore>)
                    .map_err(|error| Error::Io(py_io(error)))
            }))
        })
        .transpose()
}
pub(crate) fn to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Integer(n) => i128::from(*n).into_pyobject(py)?.into_any().unbind(),
        Value::Bool(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Text(s) => s.into_pyobject(py)?.into_any().unbind(),
        Value::Bytes(b) => PyBytes::new(py, b).into_any().unbind(),
        Value::Array(values) => PyTuple::new(
            py,
            values
                .iter()
                .map(|v| to_py(py, v))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any()
        .unbind(),
        Value::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(to_py(py, key)?, to_py(py, value)?)?;
            }
            dict.into_any().unbind()
        }
        _ => {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "unsupported CBOR value",
            ));
        }
    })
}

struct Source {
    object: Py<PyAny>,
    size: u64,
}
impl RangeReader for Source {
    fn len(&self) -> u64 {
        self.size
    }
    fn read_range(&mut self, range: std::ops::Range<u64>) -> zstar::Result<Vec<u8>> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        zstar::checked_range(range.start, length, self.size)?;
        Python::attach(|py| {
            let value = self
                .object
                .call_method1(py, "read_range", (range.start, length))?;
            if value.bind(py).len()? as u64 != length {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "short range response",
                ));
            }
            value.extract::<Vec<u8>>(py)
        })
        .map_err(|e| Error::Io(py_io(e)))
    }
}
struct Resolver(Py<PyAny>);
impl EncodedObjectResolver for Resolver {
    fn get_encoded(&mut self, digest: [u8; 32], hint: u64, limit: u64) -> zstar::Result<Vec<u8>> {
        Python::attach(|py| {
            let value = self.0.call_method1(
                py,
                "get_encoded",
                (zstar::manifest::object_address(&digest), hint, limit),
            )?;
            if value.bind(py).len()? as u64 > limit {
                return Err(core_error(Error::Budget("encoded resolver output size")));
            }
            value.extract(py)
        })
        .map_err(|error: PyErr| {
            Python::attach(|py| {
                if error.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
                    Error::MissingObject(digest)
                } else if error.is_instance_of::<crate::ZstarError>(py)
                    && error
                        .value(py)
                        .getattr("category")
                        .and_then(|c| c.extract::<String>())
                        .ok()
                        .as_deref()
                        == Some("resource-limit")
                {
                    Error::Budget("encoded resolver output size")
                } else {
                    Error::Io(std::io::Error::other(
                        "Python encoded-object resolver failed",
                    ))
                }
            })
        })
    }
}

thread_local! {
    static THREAD_TOKEN: u64 = {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.fetch_add(1, Ordering::Relaxed)
    };
}
fn thread_token() -> u64 {
    THREAD_TOKEN.with(|token| *token)
}

struct ClearOwner<'a>(&'a AtomicU64);
impl Drop for ClearOwner<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// Serializes operations on a mutable native object that calls back into
/// Python (sources, resolvers, sinks, spill stores).
///
/// * Other threads wait for the running operation *detached from Python*, so a
///   waiter never blocks the thread the operation's callbacks need.
/// * A callback that re-enters the same object on the operating thread is
///   rejected with `ZstarError` (category `unsupported-operation`) instead of
///   deadlocking or aliasing the mutable reader.
/// * `closed` is an atomic readable at any time without waiting.
struct Gate<T> {
    value: Mutex<Option<T>>,
    owner: AtomicU64,
    closed: AtomicBool,
}
impl<T: Send> Gate<T> {
    fn new(value: T) -> Self {
        Self {
            value: Mutex::new(Some(value)),
            owner: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    fn reject_reentry(&self) -> Result<(), Error> {
        if self.owner.load(Ordering::Acquire) == thread_token() {
            return Err(Error::Unsupported(
                "re-entrant use of an archive from its own callback",
            ));
        }
        Ok(())
    }
    /// Run `work` with exclusive access, detached from the interpreter.
    fn run<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut T) -> Result<R, Error> + Send,
    ) -> PyResult<R> {
        self.reject_reentry().map_err(core_error)?;
        py.detach(|| {
            let mut guard = crate::lock_work(&self.value)?;
            let value = guard.as_mut().ok_or(Error::Unsupported("reader closed"))?;
            self.owner.store(thread_token(), Ordering::Release);
            let _clear = ClearOwner(&self.owner);
            work(value)
        })
        .map_err(core_error)
    }
    /// Mark closed and take the value. Waits (detached) for a running
    /// operation. The caller drops or finishes the value after the lock is
    /// released, so destructors and final callbacks run lock-free.
    fn take(&self, py: Python<'_>) -> PyResult<Option<T>> {
        self.reject_reentry().map_err(core_error)?;
        self.closed.store(true, Ordering::Release);
        Ok(py.detach(|| crate::lock_state(&self.value).take()))
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let value = self.take(py)?;
        py.detach(|| drop(value));
        Ok(())
    }
}

/// Paged manifest reader. The source implements synchronous `read_range(offset,length)`.
///
/// Thread safety: operations serialize; waiting threads release the GIL. A
/// callback must not call back into the same archive (it raises `ZstarError`).
#[pyclass(module = "zstar.format", frozen)]
pub struct ManifestArchive {
    inner: Arc<Gate<BlockingArchive<Source>>>,
    entry_count: u64,
}
#[pymethods]
impl ManifestArchive {
    #[new]
    #[pyo3(signature=(source,size,*,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,cache_bytes=16<<20,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None))]
    #[allow(clippy::too_many_arguments)] // Public Python keyword options.
    fn new(
        py: Python<'_>,
        source: Py<PyAny>,
        size: u64,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        cache_bytes: usize,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        let shared_cache = crate::decoded_cache_binding(py, shared_cache, shared_cache_identity)?;
        let options = ManifestOptions {
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            spool_budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
            create_spill_store: spill_factory(py, create_spill_store)?,
            ..Default::default()
        };
        let mut reader = py
            .detach(|| {
                BlockingArchive::open(
                    Source {
                        object: source,
                        size,
                    },
                    options,
                )
            })
            .map_err(core_error)?;
        reader.set_content_cache_bytes(cache_bytes);
        reader.set_shared_decoded_cache(shared_cache);
        let entry_count = reader.manifest().root().entry_count;
        Ok(Self {
            inner: Arc::new(Gate::new(reader)),
            entry_count,
        })
    }
    fn entry(&self, py: Python<'_>, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let entry = self.inner.run(py, |reader| reader.entry(path))?;
        entry.map(|e| to_py(py, &e.to_value())).transpose()
    }
    /// Immutable after open; readable without waiting for running operations.
    #[getter]
    fn entry_count(&self) -> PyResult<u64> {
        if self.inner.is_closed() {
            return Err(core_error(Error::Unsupported("reader closed")));
        }
        Ok(self.entry_count)
    }
    #[getter]
    fn spilled(&self, py: Python<'_>) -> PyResult<bool> {
        self.inner.run(py, |r| Ok(r.manifest().spilled()))
    }
    #[getter]
    fn content_cache_bytes(&self, py: Python<'_>) -> PyResult<usize> {
        self.inner.run(py, |r| Ok(r.content_cache_bytes()))
    }
    /// A bounded cursor. Each result owns its metadata; closing the archive
    /// invalidates all cursors. The archive validates entry count and required features at open.
    fn iter_entries(&self, py: Python<'_>) -> PyResult<ManifestEntries> {
        let cursor = self
            .inner
            .run(py, |reader| Ok(reader.manifest().cursor()))?;
        Ok(ManifestEntries {
            inner: self.inner.clone(),
            cursor: Mutex::new(cursor),
        })
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        self.inner.close(py)
    }
    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.closed() {
            return Err(core_error(Error::Unsupported("reader closed")));
        }
        Ok(slf)
    }
    fn __exit__(
        &self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }
    #[pyo3(signature=(path,offset=0,length=None,*,resolver=None,verify_external=true))]
    fn read(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
        verify_external: bool,
    ) -> PyResult<Py<PyBytes>> {
        let bytes = self.inner.run(py, |reader| {
            let entry = reader
                .entry(path)?
                .ok_or_else(|| Error::MissingEntry(path.into()))?;
            let size = entry.size()?;
            let length = length.unwrap_or(size.checked_sub(offset).ok_or(Error::Range)?);
            let mut resolver = resolver.map(Resolver);
            reader.read(
                path,
                offset,
                length,
                resolver
                    .as_mut()
                    .map(|r| r as &mut dyn EncodedObjectResolver),
                verify_external,
            )
        })?;
        Ok(PyBytes::new(py, &bytes).unbind())
    }
    fn validate_metadata(&self, py: Python<'_>) -> PyResult<()> {
        self.inner
            .run(py, |reader| reader.manifest().validate_metadata())
    }
    fn validate(&self, py: Python<'_>) -> PyResult<()> {
        self.inner.run(py, BlockingArchive::validate)
    }
    /// Explicit eager enumeration; `entry()` keeps lookup bounded for large archives.
    fn entries(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let entries = self
            .inner
            .run(py, |reader| reader.manifest().collect_entries())?;
        entries.iter().map(|e| to_py(py, &e.to_value())).collect()
    }
}

/// Entry cursor over a [`ManifestArchive`]. Concurrent `next()` calls serialize.
#[pyclass(module = "zstar.format", frozen)]
pub struct ManifestEntries {
    inner: Arc<Gate<BlockingArchive<Source>>>,
    cursor: Mutex<EntryCursor>,
}
#[pymethods]
impl ManifestEntries {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let entry = self.inner.run(py, |reader| {
            let mut cursor = crate::lock_work(&self.cursor)?;
            reader.manifest().next_entry(&mut cursor)
        })?;
        entry.map(|e| to_py(py, &e.to_value())).transpose()
    }
}

struct Sink(Py<PyAny>);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        Python::attach(|py| -> PyResult<usize> {
            let result = self
                .0
                .call_method1(py, "write", (PyBytes::new(py, bytes),))?;
            // Raw-less sinks (for example `BufferedWriter` wrappers or plain
            // objects) may return None, meaning everything was accepted.
            let size = if result.is_none(py) {
                bytes.len()
            } else {
                result.extract(py)?
            };
            if size > bytes.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "oversized sink write",
                ));
            }
            Ok(size)
        })
        .map_err(py_io)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Python::attach(|py| self.0.call_method0(py, "flush").map(|_| ())).map_err(py_io)
    }
}
struct Body(Py<PyAny>);
impl Read for Body {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        Python::attach(|py| -> PyResult<usize> {
            let value = self.0.call_method1(py, "read", (buffer.len(),))?;
            copy_bytes_like(py, value.bind(py), buffer, "oversized body read")
        })
        .map_err(py_io)
    }
}
/// Streaming entry writer.
///
/// Thread safety: `add` and `close` serialize across threads (waiters release
/// the GIL). The first failure poisons the writer: later calls raise
/// `ValueError("writer failed")`. A sink, body, or spill-store callback must not
/// call back into the same writer (it raises `ZstarError`). Calls racing a
/// `close` observe a closed writer.
#[pyclass(module = "zstar.format", frozen)]
pub struct EntryArchiveWriter {
    inner: Gate<BlockingBuilder<Sink>>,
    failed: AtomicBool,
}
impl EntryArchiveWriter {
    fn check_failed(&self) -> PyResult<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(pyo3::exceptions::PyValueError::new_err("writer failed"));
        }
        Ok(())
    }
}
#[pymethods]
impl EntryArchiveWriter {
    #[getter]
    fn closed(&self) -> bool {
        self.inner.is_closed()
    }
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        if slf.closed() {
            return Err(core_error(Error::Unsupported("writer closed")));
        }
        slf.check_failed()?;
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
            self.failed.store(true, Ordering::Release);
            self.inner.close(py)
        }
    }
    #[new]
    #[pyo3(signature=(output,*,frame_size=1<<20,compression_level=3,memory_bytes=8<<20,block_target=64<<10,max_spool_bytes=4<<30,create_spill_store=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        output: Py<PyAny>,
        frame_size: usize,
        compression_level: i32,
        memory_bytes: u64,
        block_target: usize,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let writer = BlockingBuilder::with_options(
            Sink(output),
            frame_size,
            compression_level,
            ManifestOptions {
                memory_bytes,
                block_target,
                spool_budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
                create_spill_store: spill_factory(py, create_spill_store)?,
                ..Default::default()
            },
        )
        .map_err(core_error)?;
        Ok(Self {
            inner: Gate::new(writer),
            failed: AtomicBool::new(false),
        })
    }
    #[pyo3(signature=(entry,body=None,size=0))]
    fn add(
        &self,
        py: Python<'_>,
        entry: &Bound<'_, PyAny>,
        body: Option<Py<PyAny>>,
        size: u64,
    ) -> PyResult<()> {
        // A rejected re-entrant call must not poison the in-flight operation.
        self.inner.reject_reentry().map_err(core_error)?;
        self.check_failed()?;
        let entry = Entry::from_value(&from_py(entry, 0)?, u64::MAX).map_err(core_error)?;
        let result = self.inner.run(py, |writer| {
            if let Some(body) = body {
                if entry.kind != EntryKind::File || entry.is_external() {
                    return Err(Error::Unsupported("body requires an inline file"));
                }
                writer.add_file(entry, size, &mut Body(body))
            } else {
                if size != 0 {
                    return Err(Error::Unsupported("size requires a body"));
                }
                writer.add_metadata(entry)
            }
        });
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let writer = self.inner.take(py)?;
        self.check_failed()?;
        if let Some(writer) = writer {
            py.detach(|| writer.finish().map(|_| ()))
                .map_err(core_error)?;
        }
        Ok(())
    }
}
#[pyfunction]
fn decode_manifest_block(py: Python<'_>, raw: &[u8]) -> PyResult<Py<PyAny>> {
    to_py(py, &zstar::manifest::decode_block(raw).map_err(core_error)?)
}
#[pyfunction]
fn encode_manifest_block<'py>(
    py: Python<'py>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = zstar::manifest::encode_block(&from_py(value, 0)?).map_err(core_error)?;
    Ok(PyBytes::new(py, &bytes))
}
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ManifestArchive>()?;
    module.add_class::<ManifestEntries>()?;
    module.add_class::<EntryArchiveWriter>()?;
    module.add_function(wrap_pyfunction!(decode_manifest_block, module)?)?;
    module.add_function(wrap_pyfunction!(encode_manifest_block, module)?)?;
    Ok(())
}
