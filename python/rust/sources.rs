//! Closeable native sources; archive cores borrow these exact Rust handles.
use crate::{
    dispatch::{JobCounter, Scope},
    storage::Location,
    support::{Unsigned, block_on, core_error, lock_state},
};
use pyo3::{prelude::*, types::PyBytes};
use std::sync::{Arc, Mutex};
use zstar::AsyncRangeReader;

pub struct SourceState {
    source: Mutex<Option<Arc<dyn AsyncRangeReader>>>,
    scope: Mutex<Option<Arc<Scope>>>,
    size: u64,
    pub token: u64,
    jobs: Arc<JobCounter>,
}
struct Lease {
    source: Arc<dyn AsyncRangeReader>,
    jobs: Arc<JobCounter>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.jobs.exit();
    }
}
impl SourceState {
    fn new(source: Arc<dyn AsyncRangeReader>, scope: Arc<Scope>, size: u64) -> Arc<Self> {
        Arc::new(Self {
            source: Mutex::new(Some(source)),
            token: scope.id,
            scope: Mutex::new(Some(scope)),
            size,
            jobs: Arc::default(),
        })
    }
    fn check(&self, py: Python<'_>) -> PyResult<()> {
        let scope = lock_state(&self.scope).clone();
        if let Some(scope) = scope {
            scope.reject(py)?;
        }
        if self.closed() {
            return Err(pyo3::exceptions::PyValueError::new_err("source closed"));
        }
        Ok(())
    }
    fn lease(&self) -> zstar::Result<Lease> {
        let guard = lock_state(&self.source);
        let source = guard
            .as_ref()
            .ok_or(zstar::Error::Unsupported("source closed"))?
            .clone();
        self.jobs.enter();
        drop(guard);
        Ok(Lease {
            source,
            jobs: self.jobs.clone(),
        })
    }
    fn closed(&self) -> bool {
        lock_state(&self.source).is_none()
    }
    fn clear(&self) {
        let source = lock_state(&self.source).take();
        let scope = lock_state(&self.scope).take();
        drop(source);
        drop(scope);
    }
    pub(crate) async fn close(self: Arc<Self>) {
        let source = lock_state(&self.source).take();
        let scope = lock_state(&self.scope).clone();
        self.jobs.drain().await;
        if let Some(scope) = scope {
            scope.drain().await;
        }
        let scope = lock_state(&self.scope).take();
        drop(scope);
        drop(source);
    }
    pub async fn drain_callbacks(&self) {
        let scope = lock_state(&self.scope).clone();
        if let Some(scope) = scope {
            scope.drain().await;
        }
    }
    fn traverse(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(scope) = lock_state(&self.scope).as_ref() {
            scope.traverse(visit)?;
        }
        Ok(())
    }
    async fn read(&self, offset: u64, length: u64) -> PyResult<Py<PyBytes>> {
        let range = zstar::backend::checked_range(offset, length, self.size).map_err(core_error)?;
        let bytes = self.read_range(range).await.map_err(core_error)?;
        Ok(Python::attach(|py| PyBytes::new(py, &bytes).unbind()))
    }
}
#[async_trait::async_trait]
impl AsyncRangeReader for SourceState {
    async fn size(&self) -> zstar::Result<u64> {
        let _lease = self.lease()?;
        Ok(self.size)
    }
    async fn read_range(&self, range: std::ops::Range<u64>) -> zstar::Result<bytes::Bytes> {
        let lease = self.lease()?;
        lease.source.read_range(range).await
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct Source {
    pub state: Arc<SourceState>,
}
#[pymethods]
impl Source {
    #[staticmethod]
    fn memory(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Self> {
        let data = crate::support::shared_bytes(data)?;
        let size = data.len() as u64;
        Ok(Self {
            state: SourceState::new(Arc::new(data), Scope::new(py, false)?, size),
        })
    }
    fn size(&self, py: Python<'_>) -> PyResult<u64> {
        self.state.check(py)?;
        Ok(self.state.size)
    }
    fn read_at(&self, py: Python<'_>, offset: Unsigned, length: Unsigned) -> PyResult<Py<PyBytes>> {
        self.state.check(py)?;
        let state = self.state.clone();
        block_on(py, async move { state.read(offset.0, length.0).await })
    }
    #[getter]
    fn closed(&self) -> bool {
        self.state.closed()
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if !self.state.closed() {
            self.state.check(py)?;
        }
        let state = self.state.clone();
        block_on(py, async move {
            state.close().await;
            Ok(())
        })
    }
    fn __enter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        slf.state.check(py)?;
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
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.state.traverse(&visit)
    }
    fn __clear__(&self) {
        self.state.clear();
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct AsyncSource {
    pub state: Arc<SourceState>,
}
#[pymethods]
impl AsyncSource {
    #[staticmethod]
    fn memory<'py>(py: Python<'py>, data: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let data = crate::support::shared_bytes(data)?;
        let size = data.len() as u64;
        let state = SourceState::new(Arc::new(data), Scope::new(py, true)?, size);
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(Self { state }) })
    }
    fn size<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.state.check(py)?;
        let size = self.state.size;
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(size) })
    }
    fn read_at<'py>(
        &self,
        py: Python<'py>,
        offset: Unsigned,
        length: Unsigned,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.state.check(py)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state.read(offset.0, length.0).await
        })
    }
    #[getter]
    fn closed(&self) -> bool {
        self.state.closed()
    }
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if !self.state.closed() {
            self.state.check(py)?;
        }
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state.close().await;
            Ok(())
        })
    }
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        slf.state.check(py)?;
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
        self.aclose(py)
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        self.state.traverse(&visit)
    }
    fn __clear__(&self) {
        self.state.clear();
    }
}
fn open_work(
    py: Python<'_>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    asynchronous: bool,
    local_path: bool,
) -> PyResult<impl std::future::Future<Output = PyResult<Arc<SourceState>>> + Send + 'static> {
    let location = if local_path {
        Location::File(location.into())
    } else {
        crate::storage::location(location)?
    };
    let mut options = crate::config::SourceOptions::parse(options)?;
    let range_cache = options.range_cache()?;
    let (headers, s3_options, provider) = options.into_parts();
    let scope = Scope::new(py, asynchronous)?;
    let s3 = if let Location::S3 { bucket, .. } = &location {
        Some(
            s3_options
                .ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err(
                        "S3 locations require explicit S3Options",
                    )
                })?
                .builder(bucket.clone())?,
        )
    } else {
        if s3_options.is_some() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "S3 settings require an S3 location",
            ));
        }
        None
    };
    if !matches!(&location, Location::Http(_)) && (!headers.is_empty() || provider.is_some()) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "HTTP settings require an HTTP location",
        ));
    }
    let http = if let Location::Http(url) = &location {
        Some(crate::request::protocol_source(
            py,
            url.clone(),
            headers,
            provider,
            &scope,
        )?)
    } else {
        None
    };
    Ok(async move {
        let source: Arc<dyn AsyncRangeReader> = match location {
            Location::File(path) => Arc::new(
                zstar::fs::AsyncLocalFileSource::open(path)
                    .await
                    .map_err(crate::support::local_source_error)?,
            ),
            Location::Http(_) => Arc::new(http.unwrap()),
            Location::S3 { key, .. } => {
                let store = s3.unwrap().build().map_err(|_| {
                    core_error(zstar::Error::Unsupported("S3 configuration failed"))
                })?;
                Arc::new(zstar::s3::S3RangeSource::new(
                    Arc::new(store),
                    zstar::s3::Path::from(key),
                ))
            }
        };
        let source: Arc<dyn AsyncRangeReader> = if let Some(options) = range_cache {
            Arc::new(zstar::CachedRangeReader::new(source, options).map_err(core_error)?)
        } else {
            source
        };
        let size = source.size().await.map_err(core_error)?;
        Ok(SourceState::new(source, scope, size))
    })
}
#[pyfunction]
#[pyo3(signature = (location, options = None, *, local_path = false))]
fn open_source(
    py: Python<'_>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    local_path: bool,
) -> PyResult<Source> {
    let work = open_work(py, location, options, false, local_path)?;
    block_on(py, async move { Ok(Source { state: work.await? }) })
}
#[pyfunction]
#[pyo3(signature = (location, options = None, *, local_path = false))]
fn aopen_source<'py>(
    py: Python<'py>,
    location: String,
    options: Option<&Bound<'_, PyAny>>,
    local_path: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let work = open_work(py, location, options, true, local_path)?;
    pyo3_async_runtimes::tokio::future_into_py(
        py,
        async move { Ok(AsyncSource { state: work.await? }) },
    )
}

pub fn native_source(
    py: Python<'_>,
    source: &Py<PyAny>,
    asynchronous: bool,
) -> PyResult<Option<Arc<SourceState>>> {
    let blocking = source
        .bind(py)
        .extract::<PyRef<'_, crate::adapters::BlockingAdapter>>()
        .ok()
        .map(|adapter| adapter.original(py))
        .transpose()?;
    let object = blocking.as_ref().unwrap_or(source).bind(py);
    if let Ok(source) = object.extract::<PyRef<'_, Source>>() {
        if asynchronous && blocking.is_none() {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "async readers require an AsyncRangeSource; use from_fileobj for a file",
            ));
        }
        source.state.check(py)?;
        return Ok(Some(source.state.clone()));
    }
    if let Ok(source) = object.extract::<PyRef<'_, AsyncSource>>() {
        if !asynchronous {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "async sources require AsyncArchive",
            ));
        }
        source.state.check(py)?;
        return Ok(Some(source.state.clone()));
    }
    Ok(None)
}
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Source>()?;
    module.add_class::<AsyncSource>()?;
    module.add_function(wrap_pyfunction!(open_source, module)?)?;
    module.add_function(wrap_pyfunction!(aopen_source, module)?)?;
    Ok(())
}
