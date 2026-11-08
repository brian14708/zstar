//! Synchronous Python adapter for the shared native reader session.
use pyo3::prelude::*;
use std::sync::Arc;

use crate::{manifest, reader, support::block_on};

/// Concurrent indexed reads over a synchronous Python source. Content requests
/// use the native async entry reader; callbacks run on bounded blocking workers.
#[pyclass(module = "zstar._native", frozen)]
pub struct SyncArchive {
    session: reader::Session,
}
impl SyncArchive {
    fn reader(&self) -> PyResult<reader::Lease> {
        self.session.lease()
    }
}
#[pymethods]
impl SyncArchive {
    fn _check_access(&self, py: Python<'_>) -> PyResult<()> {
        self.session.check_access(py)
    }
    fn _read_many_values(
        &self,
        py: Python<'_>,
        requests: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Py<pyo3::types::PyBytes>>> {
        let requests = reader::request_values(requests)?;
        let lease = self.reader()?;
        block_on(py, async move { lease.read_many(&requests).await })
    }
    #[staticmethod]
    #[pyo3(signature = (source, options, *, owned=false))]
    fn _from_source(
        py: Python<'_>,
        source: Py<PyAny>,
        options: &crate::config::ReaderConfig,
        owned: bool,
    ) -> PyResult<Self> {
        let work = reader::protocol_open(py, source, options, false, owned)?;
        block_on(py, work).map(|session| Self { session })
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.session.traverse(&visit)
    }
    fn __clear__(&self) {
        self.session.clear();
    }
    fn _entry_value(
        &self,
        py: Python<'_>,
        path: String,
    ) -> PyResult<Option<crate::values::EntryValue>> {
        let lease = self.reader()?;
        block_on(py, async move { lease.entry_value(&path).await })
    }
    #[pyo3(signature = (prefix = None))]
    fn _iter_values(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<SyncEntries> {
        slf.reader()?;
        Ok(SyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(reader::EntryIterator::new(prefix))),
        })
    }
    fn _open_member(&self, py: Python<'_>, path: String) -> PyResult<reader::MemberCursor> {
        let lease = self.reader()?;
        block_on(py, async move {
            reader::MemberCursor::open(&lease.reader, &path).await
        })
    }
    fn _read_member(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        size: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let lease = self.reader()?;
        let length = reader::member_read_length(size)?;
        let cursor = cursor.clone();
        block_on(py, async move { cursor.read(&lease.reader, length).await })
    }
    fn _read_member_into(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let lease = self.reader()?;
        let buffer = reader::WritableBuffer::new(buffer)?;
        let cursor = cursor.clone();
        block_on(
            py,
            async move { cursor.read_into(&lease.reader, &buffer).await },
        )
    }
    #[getter]
    fn root_extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reader = self.reader()?;
        manifest::extensions_to_py(py, reader.reader.root_extensions())
    }
    #[getter]
    fn max_read_bytes(&self) -> PyResult<u64> {
        Ok(self.session.lease()?.reader.max_read_bytes())
    }
    #[getter]
    fn closed(&self) -> bool {
        self.session.closed()
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, self.session.close(py)?)
    }

    #[pyo3(
        signature = (
            path,
            offset = crate::support::Unsigned(0),
            length = None,
        )
    )]
    fn read(
        &self,
        py: Python<'_>,
        path: String,
        offset: crate::support::Unsigned,
        length: Option<crate::support::Unsigned>,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let lease = self.reader()?;
        block_on(py, async move {
            lease
                .read(&path, offset.0, length.map(|length| length.0))
                .await
        })
    }
}
/// Synchronous facade over the shared transactional metadata cursor.
#[pyclass(module = "zstar._native", frozen)]
pub struct SyncEntries {
    archive: Py<SyncArchive>,
    cursor: Arc<tokio::sync::Mutex<reader::EntryIterator>>,
}
#[pymethods]
impl SyncEntries {
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        visit.call(&self.archive)
    }
    fn _next_batch(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let lease = self.archive.get().reader()?;
        let cursor = self.cursor.clone();
        block_on(py, async move {
            let mut cursor = cursor.lock().await;
            cursor.next_batch(&lease.reader).await
        })
    }
}
