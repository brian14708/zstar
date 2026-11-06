use pyo3::prelude::*;
use std::{collections::BTreeMap, sync::Arc};

use super::reader::{Lease, Session, async_reader_options as reader_options};
use super::request::FailureSlot;

/// Async indexed archive over bytes, local sources, HTTP, or S3.
/// Open authenticates metadata; use `verify` to check physical content.
#[pyclass(module = "zstar._native", frozen)]
pub struct AsyncArchive {
    session: Session,
}
impl AsyncArchive {
    fn projected_entries(
        slf: PyRef<'_, Self>,
        prefix: Option<String>,
        projection: super::reader::MetadataProjection,
    ) -> PyResult<AsyncEntries> {
        slf.lease()?;
        Ok(AsyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(
                super::reader::EntryIterator::projected(prefix, projection),
            )),
        })
    }
    fn open(
        py: Python<'_>,
        work: impl std::future::Future<Output = PyResult<Session>> + Send + 'static,
    ) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Self {
                session: work.await?,
            })
        })
    }
    fn lease(&self) -> PyResult<Lease> {
        self.session.lease()
    }
}
#[pymethods]
impl AsyncArchive {
    #[getter]
    fn _has_root_extensions(&self) -> PyResult<bool> {
        Ok(!self.lease()?.reader.root_extensions().is_empty())
    }
    fn _member_info<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.member_info(&path)).await
        })
    }

    #[pyo3(signature = (prefix = None))]
    fn _iter_members(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<AsyncEntries> {
        Self::projected_entries(slf, prefix, super::reader::MetadataProjection::Member)
    }

    #[pyo3(signature = (prefix = None))]
    fn _iter_names(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<AsyncEntries> {
        Self::projected_entries(slf, prefix, super::reader::MetadataProjection::Name)
    }
    fn _open_member<'py>(
        &self,
        py: Python<'py>,
        path: String,
        cache_bytes: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(super::reader::MemberCursor::open(
                    &lease.reader,
                    &path,
                    cache_bytes,
                ))
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
    #[pyo3(
        signature = (
            *,
            external = false,
        )
    )]
    /// Verify physical frames and tar members, optionally including external objects.
    fn verify<'py>(&self, py: Python<'py>, external: bool) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.verify(external)).await
        })
    }
    /// Awaitable: owned entry metadata, or None if the path is absent.
    /// Never blocks the calling thread (or event loop) on the runtime.
    fn entry<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.entry(&path)).await
        })
    }
    #[getter]
    fn root_extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let lease = self.lease()?;
        super::manifest::extensions_to_py(py, lease.reader.root_extensions())
    }
    #[getter]
    fn entry_count(&self) -> PyResult<u64> {
        Ok(self.lease()?.reader.entry_count())
    }
    /// Lazily yield owned entry tuples in path order with async iteration.
    #[pyo3(
        signature = (
            prefix = None,
        )
    )]
    fn iter_entries(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<AsyncEntries> {
        slf.lease()?;
        Ok(AsyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(super::reader::EntryIterator::new(
                prefix,
            ))),
        })
    }
    /// Open an immutable encoded byte snapshot.
    #[staticmethod]
    #[pyo3(
        signature = (
            data,
            *,
            **options,
        )
    )]
    fn from_bytes<'py>(
        py: Python<'py>,
        data: &Bound<'py, PyAny>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        Self::open(
            py,
            Session::from_bytes(crate::support::buffer_vec(py, data)?, options),
        )
    }
    /// Open an immutable local archive using bounded positional file reads.
    #[staticmethod]
    #[pyo3(
        signature = (
            path,
            *,
            **options,
        )
    )]
    fn from_file<'py>(
        py: Python<'py>,
        path: std::path::PathBuf,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        Self::open(py, Session::from_file(path, options))
    }
    /// Borrow a seekable Python file object; sizing and reads run off-loop.
    #[staticmethod]
    #[pyo3(
        signature = (
            fileobj,
            *,
            **options,
        )
    )]
    fn from_fileobj<'py>(
        py: Python<'py>,
        fileobj: Py<PyAny>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        Self::open(py, Session::from_fileobj(fileobj, options))
    }
    /// Open an immutable synchronous `read_range(offset, length)` source.
    /// Callbacks run on bounded workers.
    #[staticmethod]
    #[pyo3(
        signature = (
            source,
            size,
            *,
            **options,
        )
    )]
    fn from_source<'py>(
        py: Python<'py>,
        source: Py<PyAny>,
        size: u64,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        let source = super::reader::python_source(py, source, size)?;
        Self::open(py, Session::open(source, options, None))
    }

    #[staticmethod]
    #[pyo3(
        signature = (
            url = None,
            *,
            headers = None,
            provider = None,
            **options,
        )
    )]
    fn from_url<'py>(
        py: Python<'py>,
        url: Option<String>,
        headers: Option<BTreeMap<String, String>>,
        provider: Option<Py<PyAny>>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        let failures = Arc::new(FailureSlot::default());
        let source = super::request::source(py, "GET", url, headers, provider, failures.clone())?;
        Self::open(py, Session::open(Arc::new(source), options, Some(failures)))
    }
    #[staticmethod]
    #[pyo3(
        signature = (
            bucket,
            key,
            *,
            endpoint = None,
            region = None,
            object_prefix = None,
            anon = None,
            access_key_id = None,
            secret_access_key = None,
            session_token = None,
            **options,
        )
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "Independent wire fields or Python keyword options form this API."
    )]
    fn from_s3<'py>(
        py: Python<'py>,
        bucket: String,
        key: String,
        endpoint: Option<String>,
        region: Option<String>,
        object_prefix: Option<String>,
        anon: Option<bool>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let options = reader_options(py, options)?;
        let builder = super::s3::builder(
            bucket,
            endpoint,
            region,
            anon,
            access_key_id,
            secret_access_key,
            session_token,
        )?;
        Self::open(py, Session::from_s3(builder, key, object_prefix, options))
    }
    /// Batch logical ranges with chunk sharing and cancellation propagation.
    #[pyo3(
        signature = (
            requests,
            *,
            resolver = None,
        )
    )]
    fn read_many<'py>(
        &self,
        py: Python<'py>,
        requests: &Bound<'_, PyAny>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests = super::reader::batch_requests(requests)?;
        let lease = self.lease()?;
        let resolver = resolver
            .map(|resolver| super::external::binding(py, resolver))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(lease.read_many(&requests, resolver.as_deref()))
                .await
        })
    }
    #[pyo3(
        signature = (
            path,
            offset,
            length,
            *,
            resolver = None,
        )
    )]
    fn read_with_report<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let resolver = resolver
            .map(|resolver| super::external::binding(py, resolver))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(lease.read_with_report(&path, offset, length, resolver.as_deref()))
                .await
        })
    }
    fn read_link<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease.run(lease.read_link(&path)).await
        })
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
        let closing = self.session.close();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            closing.drain().await;
            Ok(())
        })
    }
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        slf.lease()?;
        let owner: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(owner) })
    }
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.close(py)
    }
    #[pyo3(
        signature = (
            path,
            offset = 0,
            length = None,
            *,
            resolver = None,
        )
    )]
    fn read<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let resolver = resolver
            .map(|resolver| super::external::binding(py, resolver))
            .transpose()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(lease.read(&path, offset, length, resolver.as_deref()))
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
    const fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.archive.get().lease()?;
        let cursor = self.cursor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::next_batch(lease, cursor, 1)
                .await?
                .pop()
                .ok_or_else(|| pyo3::exceptions::PyStopAsyncIteration::new_err(()))
        })
    }
    fn _next_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.archive.get().lease()?;
        let cursor = self.cursor.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, Self::next_batch(lease, cursor, 64))
    }
}
impl AsyncEntries {
    async fn next_batch(
        lease: Lease,
        cursor: Arc<tokio::sync::Mutex<super::reader::EntryIterator>>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyAny>>> {
        lease
            .run(async {
                let mut cursor = cursor.lock().await;
                cursor.next_batch(&lease.reader, limit).await
            })
            .await
    }
}
