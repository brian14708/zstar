//! Synchronous Python adapter for the shared native reader session.
use pyo3::prelude::*;
use std::sync::Arc;

use crate::{
    external, manifest, reader,
    reader::Session,
    request::FailureSlot,
    s3,
    support::{block_on, buffer_vec},
};

/// Concurrent indexed reads over a synchronous Python source. Content requests
/// use the native async entry reader; callbacks run on bounded blocking workers.
#[pyclass(module = "zstar._native", frozen)]
pub struct SyncArchive {
    session: reader::Session,
}
impl SyncArchive {
    fn projected_entries(
        slf: PyRef<'_, Self>,
        prefix: Option<String>,
        projection: reader::MetadataProjection,
    ) -> PyResult<SyncEntries> {
        slf.reader()?;
        Ok(SyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(reader::EntryIterator::projected(
                prefix, projection,
            ))),
        })
    }
    fn open(
        py: Python<'_>,
        work: impl std::future::Future<Output = PyResult<Session>> + Send,
    ) -> PyResult<Self> {
        block_on(py, work).map(|session| Self { session })
    }
    fn reader(&self) -> PyResult<reader::Lease> {
        self.session.lease()
    }
}
#[pymethods]
impl SyncArchive {
    #[getter]
    fn _has_root_extensions(&self) -> PyResult<bool> {
        Ok(!self.reader()?.reader.root_extensions().is_empty())
    }
    fn _member_info(&self, py: Python<'_>, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let lease = self.reader()?;
        block_on(py, lease.member_info(path))
    }

    #[pyo3(signature = (prefix = None))]
    fn _iter_members(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<SyncEntries> {
        Self::projected_entries(slf, prefix, reader::MetadataProjection::Member)
    }

    #[pyo3(signature = (prefix = None))]
    fn _iter_names(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<SyncEntries> {
        Self::projected_entries(slf, prefix, reader::MetadataProjection::Name)
    }
    fn _open_member(
        &self,
        py: Python<'_>,
        path: &str,
        cache_bytes: usize,
    ) -> PyResult<reader::MemberCursor> {
        let lease = self.reader()?;
        block_on(
            py,
            reader::MemberCursor::open(&lease.reader, path, cache_bytes),
        )
    }
    fn _read_member(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        size: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let lease = self.reader()?;
        let length = reader::member_read_length(size)?;
        block_on(py, cursor.read(&lease.reader, length))
    }
    fn _read_member_into(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let lease = self.reader()?;
        let buffer = reader::WritableBuffer::new(buffer)?;
        block_on(py, cursor.read_into(&lease.reader, &buffer))
    }
    fn entry(&self, py: Python<'_>, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let lease = self.reader()?;
        block_on(py, lease.entry(path))
    }
    #[getter]
    fn root_extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reader = self.reader()?;
        manifest::extensions_to_py(py, reader.reader.root_extensions())
    }
    fn read_link(&self, py: Python<'_>, path: &str) -> PyResult<String> {
        let lease = self.reader()?;
        block_on(py, lease.read_link(path))
    }
    #[getter]
    fn entry_count(&self) -> PyResult<u64> {
        Ok(self.reader()?.reader.entry_count())
    }
    #[pyo3(
        signature = (
            prefix = None,
        )
    )]
    fn iter_entries(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<SyncEntries> {
        slf.reader()?;
        Ok(SyncEntries {
            archive: slf.into(),
            cursor: Arc::new(tokio::sync::Mutex::new(reader::EntryIterator::new(prefix))),
        })
    }
    #[pyo3(
        signature = (
            *,
            external = false,
        )
    )]
    fn verify(&self, py: Python<'_>, external: bool) -> PyResult<Py<pyo3::types::PyDict>> {
        let lease = self.reader()?;
        block_on(py, lease.verify(external))
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
    fn from_bytes(
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        Self::open(py, Session::from_bytes(buffer_vec(py, data)?, options))
    }

    /// Open a strong-ETag-pinned HTTP archive.
    #[staticmethod]
    #[pyo3(
        signature = (
            url,
            *,
            headers = None,
            **options,
        )
    )]
    fn from_url(
        py: Python<'_>,
        url: String,
        headers: Option<std::collections::BTreeMap<String, String>>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        let failures = Arc::new(FailureSlot::default());
        let source = crate::request::source(py, "GET", Some(url), headers, None, failures.clone())?;
        Self::open(py, Session::open(Arc::new(source), options, Some(failures)))
    }

    /// Open an immutable native S3 archive.
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
    fn from_s3(
        py: Python<'_>,
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
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        let builder = s3::builder(
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

    /// Open a local archive using native positional reads.
    #[staticmethod]
    #[pyo3(
        signature = (
            path,
            *,
            **options,
        )
    )]
    fn from_file(
        py: Python<'_>,
        path: std::path::PathBuf,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        Self::open(py, Session::from_file(path, options))
    }

    #[staticmethod]
    #[pyo3(signature = (fileobj, *, **options))]
    fn from_fileobj(
        py: Python<'_>,
        fileobj: Py<PyAny>,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        Self::open(py, Session::from_fileobj(fileobj, options))
    }

    #[staticmethod]
    #[pyo3(
        signature = (
            source,
            size,
            *,
            **options,
        )
    )]
    fn from_source(
        py: Python<'_>,
        source: Py<PyAny>,
        size: u64,
        options: Option<&Bound<'_, pyo3::types::PyDict>>,
    ) -> PyResult<Self> {
        let options = reader::reader_options(py, options)?;
        Self::open(py, Session::from_python(source, size, options))
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
    fn read_with_report(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<(Py<pyo3::types::PyBytes>, Py<pyo3::types::PyDict>)> {
        let lease = self.reader()?;
        let resolver = resolver.map(|resolver| external::binding(py, resolver));
        block_on(
            py,
            lease.read_with_report(path, offset, length, resolver.as_deref()),
        )
    }
    #[pyo3(
        signature = (
            requests,
            *,
            resolver = None,
        )
    )]
    fn read_many(
        &self,
        py: Python<'_>,
        requests: &Bound<'_, PyAny>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Vec<Py<pyo3::types::PyBytes>>> {
        let requests = reader::batch_requests(requests)?;
        let lease = self.reader()?;
        let resolver = resolver.map(|resolver| external::binding(py, resolver));
        block_on(py, lease.read_many(&requests, resolver.as_deref()))
    }
    #[getter]
    fn max_read_bytes(&self) -> PyResult<u64> {
        Ok(self.session.lease()?.reader.max_read_bytes())
    }
    #[getter]
    fn closed(&self) -> bool {
        self.session.closed()
    }
    fn close(&self, py: Python<'_>) {
        let closing = self.session.close();
        // Existing calls retain their reader lease until bounded work finishes.
        // Drop the owner's cache/spool/source references without holding the GIL.
        py.detach(|| drop(closing));
    }
    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<PyRef<'_, Self>> {
        slf.reader()?;
        Ok(slf)
    }
    fn __exit__(
        &self,
        py: Python<'_>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close(py);
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
    fn read(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let lease = self.reader()?;
        let resolver = resolver.map(|resolver| external::binding(py, resolver));
        block_on(py, lease.read(path, offset, length, resolver.as_deref()))
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
    fn _next_batch(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let lease = self.archive.get().reader()?;
        block_on(py, async {
            let mut cursor = self.cursor.lock().await;
            cursor.next_batch(&lease.reader, 64).await
        })
    }
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let lease = self.archive.get().reader()?;
        block_on(py, async {
            let mut cursor = self.cursor.lock().await;
            Ok(cursor.next_batch(&lease.reader, 1).await?.pop())
        })
    }
}
