//! Streaming entry writer over Python-owned sinks and bodies.
use crate::{
    manifest::{from_py, spill_factory},
    support::{Gate, copy_bytes_like, core_error, py_io, write_python},
};
use pyo3::{prelude::*, types::PyDict};
use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use zstar::{Entry, EntryKind, Error, ManifestOptions, backend::BlockingBuilder};
struct Sink(Py<PyAny>);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        write_python(&self.0, bytes, "oversized sink write")
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
#[pyclass(module = "zstar._native", frozen)]
pub struct EntryArchiveWriter {
    inner: Gate<BlockingBuilder<Sink>>,
    failed: AtomicBool,
}
impl EntryArchiveWriter {
    pub(crate) fn add_entry(
        &self,
        py: Python<'_>,
        entry: Entry,
        body: Option<Py<PyAny>>,
        size: u64,
    ) -> PyResult<()> {
        // A rejected re-entrant call must not poison the in-flight operation.
        self.inner.reject_reentry().map_err(core_error)?;
        self.check_failed()?;
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
    pub(crate) fn abort(&self, py: Python<'_>) -> PyResult<()> {
        self.failed.store(true, Ordering::Release);
        self.inner.close(py)
    }
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
    #[pyo3(
        signature = (
            output,
            *,
            frame_size = 1 << 20,
            compression_level = 3,
            limits = None,
            memory_bytes = 8 << 20,
            block_target = 64 << 10,
            max_spool_bytes = 4 << 30,
            create_spill_store = None,
            workers = 0,
            root_extensions = None,
        )
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "Independent wire fields or Python keyword options form this API."
    )]
    fn new(
        py: Python<'_>,
        output: Py<PyAny>,
        frame_size: usize,
        compression_level: i32,
        limits: Option<&Bound<'_, PyDict>>,
        memory_bytes: u64,
        block_target: usize,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        workers: usize,
        root_extensions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let writer = BlockingBuilder::with_options(
            Sink(output),
            frame_size,
            compression_level,
            ManifestOptions {
                limits: super::reader::limits(limits)?,
                memory_bytes,
                block_target,
                spool_budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
                create_spill_store: spill_factory(py, create_spill_store)?,
                ..Default::default()
            },
        )
        .map_err(core_error)?;
        let extensions = root_extensions
            .into_iter()
            .flat_map(PyDictMethods::iter)
            .map(|(key, value)| Ok((key.extract()?, from_py(&value, 1)?)))
            .collect::<PyResult<zstar::Extensions>>()?;
        let writer = writer
            .with_workers(workers)
            .map_err(core_error)?
            .with_root_extensions(extensions)
            .map_err(core_error)?;
        Ok(Self {
            inner: Gate::new(writer),
            failed: AtomicBool::new(false),
        })
    }
    #[pyo3(
        signature = (
            entry,
            body = None,
            size = 0,
        )
    )]
    fn add(
        &self,
        py: Python<'_>,
        entry: &Bound<'_, PyAny>,
        body: Option<Py<PyAny>>,
        size: u64,
    ) -> PyResult<()> {
        let entry = Entry::from_value(&from_py(entry, 0)?, u64::MAX).map_err(core_error)?;
        self.add_entry(py, entry, body, size)
    }
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Ownership matches the public API or asynchronous boundary."
    )]
    #[pyo3(
        signature = (
            path,
            archive_name,
            *,
            preserve_metadata = false,
            recursive = true,
        )
    )]
    fn add_path(
        &self,
        py: Python<'_>,
        path: std::path::PathBuf,
        archive_name: String,
        preserve_metadata: bool,
        recursive: bool,
    ) -> PyResult<()> {
        self.inner.reject_reentry().map_err(core_error)?;
        self.check_failed()?;
        let result = self.inner.run(py, |writer| {
            zstar::fs::append_path(&path, &archive_name, writer, preserve_metadata, recursive)
                .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))
        });
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
    pub(crate) fn close(&self, py: Python<'_>) -> PyResult<()> {
        let writer = self.inner.take(py)?;
        self.check_failed()?;
        if let Some(writer) = writer {
            py.detach(|| {
                let mut sink = writer.finish()?;
                sink.flush().map_err(Error::Io)
            })
            .map_err(core_error)?;
        }
        Ok(())
    }
}

/// Run synchronous writer work away from the Python event loop. Eight jobs can
/// be admitted at once across writers; cancellation retains running capacity.
#[pyfunction]
pub fn _writer_call(py: Python<'_>, operation: Py<PyAny>) -> PyResult<Bound<'_, PyAny>> {
    static CAPACITY: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        crate::support::blocking(&CAPACITY, move || Python::attach(|py| operation.call0(py))).await
    })
}
