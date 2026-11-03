use crate::support::{Gate, copy_bytes_like, core_error, py_io, write_python};
use ciborium::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyList, PyString, PyTuple};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use zstar::backend::{BlockingArchive, EncodedObjectResolver};
use zstar::{EntryCursor, Error, ManifestOptions, RangeReader};

pub fn from_py(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
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
        write_python(&self.0, bytes, "oversized spill-store write")
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
pub fn spill_factory(
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
pub fn to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
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

pub fn extensions_to_py(py: Python<'_>, extensions: &zstar::Extensions) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in extensions {
        dict.set_item(key, to_py(py, value)?)?;
    }
    Ok(dict.into_any().unbind())
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
        crate::external::resolve_encoded(&self.0, digest, hint, limit)
    }
}

/// Paged manifest reader. The source implements synchronous `read_range(offset,length)`.
///
/// Thread safety: operations serialize; waiting threads release the GIL. A
/// callback must not call back into the same archive (it raises `ZstarError`).
#[pyclass(module = "zstar._native", frozen)]
pub struct ManifestArchive {
    inner: Arc<Gate<BlockingArchive<Source>>>,
    entry_count: u64,
}
#[pymethods]
impl ManifestArchive {
    #[new]
    #[pyo3(
        signature = (
            source,
            size,
            *,
            limits = None,
            memory_bytes = 8 << 20,
            max_encoded_bytes = 4 << 30,
            max_decoded_bytes = 4 << 30,
            max_read_bytes = 64 << 20,
            max_retained_entry_bytes = 1 << 30,
            max_entries = 0,
            max_spool_bytes = 4 << 30,
            create_spill_store = None,
        )
    )]
    #[expect(clippy::too_many_arguments, reason = "Public Python keyword options.")]
    fn new(
        py: Python<'_>,
        source: Py<PyAny>,
        size: u64,
        limits: Option<&Bound<'_, PyDict>>,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let options = ManifestOptions {
            limits: super::reader::limits(limits)?,
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
        let reader = py
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
    #[pyo3(
        signature = (
            path,
            offset = 0,
            length = None,
            *,
            resolver = None,
            verify_external = false,
        )
    )]
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
#[pyclass(module = "zstar._native", frozen)]
pub struct ManifestEntries {
    inner: Arc<Gate<BlockingArchive<Source>>>,
    cursor: Mutex<EntryCursor>,
}
#[pymethods]
impl ManifestEntries {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let entry = self.inner.run(py, |reader| {
            let mut cursor = crate::support::lock_work(&self.cursor)?;
            reader.manifest().next_entry(&mut cursor)
        })?;
        entry.map(|e| to_py(py, &e.to_value())).transpose()
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
    module.add_function(wrap_pyfunction!(decode_manifest_block, module)?)?;
    module.add_function(wrap_pyfunction!(encode_manifest_block, module)?)?;
    Ok(())
}
