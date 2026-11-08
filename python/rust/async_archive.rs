use pyo3::prelude::*;
use std::sync::Arc;

use super::reader::{Lease, Session};

/// Async indexed archive over bytes, local sources, HTTP, or S3.
/// Open validates metadata; reads validate selected content.
#[pyclass(module = "zstar._native", frozen)]
pub struct AsyncArchive {
    session: Session,
}
impl AsyncArchive {
    fn lease(&self) -> PyResult<Lease> {
        self.session.lease()
    }
}
#[pymethods]
impl AsyncArchive {
    fn _check_access(&self, py: Python<'_>) -> PyResult<()> {
        self.session.check_access(py)
    }
    fn _read_many_values<'py>(
        &self,
        py: Python<'py>,
        requests: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests = super::reader::request_values(requests)?;
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.read_many(&requests)).await
        })
    }
    #[staticmethod]
    #[pyo3(signature = (source, options, *, owned=false))]
    fn _from_source<'py>(
        py: Python<'py>,
        source: Py<PyAny>,
        options: &crate::config::ReaderConfig,
        owned: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let work = super::reader::protocol_open(py, source, options, true, owned)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Self {
                session: work.await?,
            })
        })
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.session.traverse(&visit)
    }
    fn __clear__(&self) {
        self.session.clear();
    }
    fn _entry_value<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.entry_value(&path)).await
        })
    }
    #[pyo3(signature = (prefix = None))]
    fn _iter_values(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<AsyncEntries> {
        slf.lease()?;
        Ok(AsyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(super::reader::EntryIterator::new(
                prefix,
            ))),
        })
    }
    fn _open_member<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(super::reader::MemberCursor::open(&lease.reader, &path))
                .await
        })
    }
    fn _read_member<'py>(
        &self,
        py: Python<'py>,
        cursor: &super::reader::MemberCursor,
        size: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let cursor = cursor.clone();
        let length = super::reader::member_read_length(size)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(cursor.read(&lease.reader, length)).await
        })
    }
    fn _read_member_into<'py>(
        &self,
        py: Python<'py>,
        cursor: &super::reader::MemberCursor,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let cursor = cursor.clone();
        let buffer = super::reader::WritableBuffer::new(buffer)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(cursor.read_into(&lease.reader, &buffer)).await
        })
    }
    fn _read_member_into_at<'py>(
        &self,
        py: Python<'py>,
        cursor: &super::reader::MemberCursor,
        offset: u64,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let cursor = cursor.clone();
        let buffer = super::reader::WritableBuffer::new(buffer)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(cursor.read_into_at(&lease.reader, offset, &buffer))
                .await
        })
    }
    #[getter]
    fn root_extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let lease = self.lease()?;
        super::manifest::extensions_to_py(py, lease.reader.root_extensions())
    }
    #[getter]
    fn max_read_bytes(&self) -> PyResult<u64> {
        Ok(self.session.lease()?.reader.max_read_bytes())
    }
    #[getter]
    fn closed(&self) -> bool {
        self.session.closed()
    }
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, self.session.close(py)?)
    }

    #[pyo3(
        signature = (
            path,
            offset = crate::support::Unsigned(0),
            length = None,
        )
    )]
    fn read<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: crate::support::Unsigned,
        length: Option<crate::support::Unsigned>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(lease.read(&path, offset.0, length.map(|length| length.0)))
                .await
        })
    }
}

/// An async cursor over owned entry metadata. Closing its archive invalidates it.
#[pyclass(module = "zstar._native", frozen)]
pub struct AsyncEntries {
    archive: Py<AsyncArchive>,
    cursor: Arc<tokio::sync::Mutex<super::reader::EntryIterator>>,
}
#[pymethods]
impl AsyncEntries {
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        visit.call(&self.archive)
    }
    fn _next_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.archive.get().lease()?;
        let cursor = self.cursor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, Self::next_batch(lease, cursor))
    }
}
impl AsyncEntries {
    async fn next_batch(
        lease: Lease,
        cursor: Arc<tokio::sync::Mutex<super::reader::EntryIterator>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        lease
            .run(async {
                let mut cursor = cursor.lock().await;
                cursor.next_batch(&lease.reader).await
            })
            .await
    }
}
