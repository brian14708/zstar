//! Native chunking and immutable packed storage; no per-byte Python callbacks.
use super::core_error;
use bytes::Bytes;
use pyo3::{prelude::*, types::PyBytes};
use std::{
    future::Future,
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};
use zstar::{
    chunking::{Chunking, external_file},
    manifest_io::{ManifestOptions, Spool},
    pack::{
        Catalog, CatalogLimits, Generation, LocalPackStore, PackWriter, PackWriterOptions,
        PackedObjectResolver, ResolverOptions, S3PackStore, Snapshot, publish_generation,
    },
};

enum Store {
    Local(LocalPackStore),
    S3(S3PackStore),
}
impl zstar::pack::PackStore for Store {
    fn cache_allowed(&self, snapshot: &Snapshot) -> bool {
        match self {
            Self::Local(store) => store.cache_allowed(snapshot),
            Self::S3(store) => store.cache_allowed(snapshot),
        }
    }
    async fn put(&self, key: String, data: Spool) -> zstar::Result<Snapshot> {
        match self {
            Self::Local(s) => s.put(key, data).await,
            Self::S3(s) => s.put(key, data).await,
        }
    }
    async fn get_range(&self, s: &Snapshot, r: Range<u64>) -> zstar::Result<Bytes> {
        match self {
            Self::Local(store) => store.get_range(s, r).await,
            Self::S3(store) => store.get_range(s, r).await,
        }
    }
}
#[pyclass(name = "PackStore", module = "zstar", frozen)]
struct PyStore {
    inner: Arc<Store>,
}
#[pymethods]
impl PyStore {
    #[staticmethod]
    fn local(py: Python<'_>, root: std::path::PathBuf) -> PyResult<Self> {
        py.detach(|| LocalPackStore::open(root))
            .map(|s| Self {
                inner: Arc::new(Store::Local(s)),
            })
            .map_err(core_error)
    }
    #[staticmethod]
    #[pyo3(signature=(bucket,*,prefix=String::new(),endpoint=None,region=None,anon=None,access_key_id=None,secret_access_key=None,session_token=None,part_size=8<<20,concurrency=4))]
    #[allow(clippy::too_many_arguments)] // Standard S3 configuration keyword API.
    fn s3(
        py: Python<'_>,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: Option<String>,
        anon: Option<bool>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        part_size: usize,
        concurrency: usize,
    ) -> PyResult<Self> {
        let builder = super::s3::builder(
            bucket,
            endpoint,
            region,
            anon,
            access_key_id,
            secret_access_key,
            session_token,
        );
        py.detach(|| {
            let store = builder
                .build()
                .map_err(|_| zstar::Error::Unsupported("S3 configuration failed"))?;
            S3PackStore::new(
                Arc::new(store),
                zstar::s3::Path::from(prefix),
                part_size,
                concurrency,
            )
        })
        .map(|s| Self {
            inner: Arc::new(Store::S3(s)),
        })
        .map_err(core_error)
    }
}
#[pyclass(name = "PackedCatalog", module = "zstar", frozen)]
struct PyCatalog {
    inner: Catalog,
}
#[pymethods]
impl PyCatalog {
    #[new]
    #[pyo3(signature=(data=None,*,max_encoded_bytes=16<<20,max_memory_bytes=512<<20,max_packs=65536,max_chunks=262144))]
    fn new(
        py: Python<'_>,
        data: Option<Vec<u8>>,
        max_encoded_bytes: usize,
        max_memory_bytes: usize,
        max_packs: usize,
        max_chunks: usize,
    ) -> PyResult<Self> {
        let limits = CatalogLimits {
            max_encoded_bytes,
            max_memory_bytes,
            max_packs,
            max_chunks,
        };
        py.detach(|| match data {
            Some(data) => Catalog::decode(&data, limits),
            None => Ok(Catalog::default()),
        })
        .map(|inner| Self { inner })
        .map_err(core_error)
    }
    fn encode<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let raw = py
            .detach(|| self.inner.encode(CatalogLimits::default()))
            .map_err(core_error)?;
        Ok(PyBytes::new(py, &raw))
    }
    #[getter]
    fn chunk_count(&self) -> usize {
        self.inner.chunks().len()
    }
    #[getter]
    fn pack_count(&self) -> usize {
        self.inner.packs().len()
    }
}
#[pyclass(name = "Chunking", module = "zstar", frozen)]
struct PyChunking {
    inner: Chunking,
}
#[pymethods]
impl PyChunking {
    #[new]
    #[pyo3(signature=(kind="fixed",*,target_bytes=None,min_bytes=64<<10,max_bytes=1<<20))]
    fn new(
        kind: &str,
        target_bytes: Option<usize>,
        min_bytes: usize,
        max_bytes: usize,
    ) -> PyResult<Self> {
        let target_bytes =
            target_bytes.unwrap_or(if kind == "fixed" { 1 << 20 } else { 256 << 10 });
        let inner = match kind {
            "fixed" => Chunking::Fixed { size: target_bytes },
            "fastcdc" => Chunking::FastCdc {
                min: min_bytes,
                target: target_bytes,
                max: max_bytes,
            },
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "kind must be fixed or fastcdc",
                ));
            }
        };
        inner.validate().map_err(core_error)?;
        Ok(Self { inner })
    }
}
#[pyclass(name = "ExternalFile", module = "zstar", frozen)]
struct PyExternal {
    extents: Vec<zstar::Extent>,
    size: u64,
}
#[pymethods]
impl PyExternal {
    #[getter]
    fn size(&self) -> u64 {
        self.size
    }
    #[getter]
    fn chunk_count(&self) -> usize {
        self.extents.len()
    }
    fn _entry(&self, py: Python<'_>, entry: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut entry =
            zstar::Entry::from_value(&super::manifest_api::from_py(entry, 0)?, u64::MAX)
                .map_err(core_error)?;
        if entry.kind != zstar::EntryKind::File {
            return Err(core_error(zstar::Error::Unsupported(
                "external body requires file",
            )));
        }
        entry.extents.clone_from(&self.extents);
        super::manifest_api::to_py(py, &entry.to_value())
    }
}
type ReadJob = Pin<Box<dyn Future<Output = std::io::Result<Bytes>> + Send>>;
struct Body {
    object: Arc<Py<PyAny>>,
    pending: Option<ReadJob>,
    buffer: Bytes,
}
impl AsyncRead for Body {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.buffer.is_empty() {
            if this.pending.is_none() {
                let object = this.object.clone();
                this.pending = Some(Box::pin(async move {
                    static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
                        std::sync::OnceLock::new();
                    let permit = PERMITS
                        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| std::io::Error::other("body reader closed"))?;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        Python::attach(|py| {
                            let value = object.call_method1(py, "read", (64 << 10,))?;
                            let raw = super::buffer_vec(py, value.bind(py))?;
                            if raw.len() > 64 << 10 {
                                return Err(pyo3::exceptions::PyValueError::new_err(
                                    "oversized body read",
                                ));
                            }
                            Ok(Bytes::from(raw))
                        })
                        .map_err(|cause: PyErr| super::PyCause::io("body read failed", cause))
                    })
                    .await
                    .map_err(|_| std::io::Error::other("body worker failed"))?
                }));
            }
            match this.pending.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    this.pending = None;
                    this.buffer = result?;
                }
            }
        }
        let n = this.buffer.len().min(out.remaining());
        out.put_slice(&this.buffer.split_to(n));
        Poll::Ready(Ok(()))
    }
}
#[pyclass(name = "PackWriter", module = "zstar", frozen)]
struct PyWriter {
    inner: Arc<tokio::sync::Mutex<PackWriter<Store>>>,
}
#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature=(store,*,base=None,prefix="objects".to_string(),target_bytes=64<<20,compression_level=3,memory_bytes=8<<20,max_spool_bytes=4<<30,max_chunks=262144,max_packs=65536,max_catalog_bytes=16<<20,max_index_bytes=512<<20))]
    #[allow(clippy::too_many_arguments)] // Independently configurable resource budgets.
    fn new(
        store: PyRef<'_, PyStore>,
        base: Option<PyRef<'_, PyCatalog>>,
        prefix: String,
        target_bytes: u64,
        compression_level: i32,
        memory_bytes: u64,
        max_spool_bytes: u64,
        max_chunks: usize,
        max_packs: usize,
        max_catalog_bytes: usize,
        max_index_bytes: usize,
    ) -> PyResult<Self> {
        let options = PackWriterOptions {
            prefix,
            target_bytes,
            compression_level,
            spool: ManifestOptions {
                memory_bytes,
                spool_budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
                ..Default::default()
            },
            catalog: CatalogLimits {
                max_encoded_bytes: max_catalog_bytes,
                max_memory_bytes: max_index_bytes,
                max_chunks,
                max_packs,
            },
        };
        PackWriter::new(
            store.inner.clone(),
            base.map_or_else(Catalog::default, |c| c.inner.clone()),
            options,
        )
        .map(|w| Self {
            inner: Arc::new(tokio::sync::Mutex::new(w)),
        })
        .map_err(core_error)
    }
    #[pyo3(signature=(fileobj,size,*,chunking=None))]
    fn externalize(
        &self,
        py: Python<'_>,
        fileobj: Py<PyAny>,
        size: u64,
        chunking: Option<PyRef<'_, PyChunking>>,
    ) -> PyResult<PyExternal> {
        let inner = self.inner.clone();
        let strategy = chunking.map_or_else(Chunking::default, |c| c.inner);
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(externalize(inner, fileobj, size, strategy))
        })
        .map_err(core_error)
    }
    #[pyo3(signature=(fileobj,size,*,chunking=None))]
    fn externalize_async<'py>(
        &self,
        py: Python<'py>,
        fileobj: Py<PyAny>,
        size: u64,
        chunking: Option<PyRef<'_, PyChunking>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let strategy = chunking.map_or_else(Chunking::default, |c| c.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            externalize(inner, fileobj, size, strategy)
                .await
                .map_err(core_error)
        })
    }
    fn finish(&self, py: Python<'_>) -> PyResult<PyCatalog> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                self.inner
                    .try_lock()
                    .map_err(|_| zstar::Error::Unsupported("pack writer busy or reentrant"))?
                    .finish()
                    .await
            })
        })
        .map(|inner| PyCatalog { inner })
        .map_err(core_error)
    }
    fn finish_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .try_lock()
                .map_err(|_| {
                    core_error(zstar::Error::Unsupported("pack writer busy or reentrant"))
                })?
                .finish()
                .await
                .map(|inner| PyCatalog { inner })
                .map_err(core_error)
        })
    }
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
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
    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(slf) })
    }
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _kind: &Bound<'_, PyAny>,
        _value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .try_lock()
                .map_err(|_| {
                    core_error(zstar::Error::Unsupported("pack writer busy or reentrant"))
                })?
                .abort();
            Ok(())
        })
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.inner
                .try_lock()
                .map_err(|_| {
                    core_error(zstar::Error::Unsupported("pack writer busy or reentrant"))
                })?
                .abort();
            Ok(())
        })
    }
    fn completed_packs(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            let writer = self.inner.try_lock().map_err(|_| {
                core_error(zstar::Error::Unsupported("pack writer busy or reentrant"))
            })?;
            Ok(writer
                .completed_packs()
                .iter()
                .map(|s| s.key.clone())
                .collect())
        })
    }
}
async fn externalize(
    inner: Arc<tokio::sync::Mutex<PackWriter<Store>>>,
    object: Py<PyAny>,
    size: u64,
    strategy: Chunking,
) -> zstar::Result<PyExternal> {
    let mut writer = inner
        .try_lock()
        .map_err(|_| zstar::Error::Unsupported("pack writer busy or reentrant"))?;
    let mut body = Body {
        object: Arc::new(object),
        pending: None,
        buffer: Bytes::new(),
    };
    let extents = external_file(&mut body, size, strategy, &mut *writer).await?;
    Ok(PyExternal { extents, size })
}
#[pyclass(name = "PackedObjectResolver", module = "zstar", frozen)]
pub(crate) struct PyResolver {
    pub(crate) inner: Arc<dyn zstar::ObjectResolver>,
}
#[pymethods]
impl PyResolver {
    #[new]
    #[pyo3(signature=(store,catalog,*,max_range_bytes=8<<20,max_overfetch_bytes=0,max_batch_bytes=64<<20,cache_bytes=0,concurrency=8))]
    #[allow(clippy::too_many_arguments)] // Independent range and retention budgets.
    fn new(
        store: PyRef<'_, PyStore>,
        catalog: PyRef<'_, PyCatalog>,
        max_range_bytes: u64,
        max_overfetch_bytes: u64,
        max_batch_bytes: u64,
        cache_bytes: usize,
        concurrency: usize,
    ) -> PyResult<Self> {
        PackedObjectResolver::new(
            store.inner.clone(),
            catalog.inner.clone(),
            ResolverOptions {
                max_range_bytes,
                max_overfetch_bytes,
                max_batch_bytes,
                cache_bytes,
                concurrent_requests: concurrency,
            },
        )
        .map(|r| Self { inner: Arc::new(r) })
        .map_err(core_error)
    }
    fn get_encoded<'py>(
        &self,
        py: Python<'py>,
        address: &str,
        hint: u64,
        max_bytes: u64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let digest = zstar::manifest::parse_object_address(address).map_err(core_error)?;
        let data = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(self.inner.get_encoded(digest, hint, max_bytes))
            })
            .map_err(core_error)?;
        Ok(PyBytes::new(py, &data))
    }
}
pub(crate) fn binding(py: Python<'_>, object: Py<PyAny>) -> Arc<dyn zstar::ObjectResolver> {
    if let Ok(native) = object.bind(py).extract::<PyRef<'_, PyResolver>>() {
        native.inner.clone()
    } else {
        Arc::new(super::PythonResolver(Arc::new(object)))
    }
}
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStore>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyWriter>()?;
    m.add_class::<PyResolver>()?;
    m.add_class::<PyExternal>()?;
    m.add_class::<PyChunking>()?;
    m.add_class::<PyGeneration>()?;
    m.add_function(wrap_pyfunction!(publish_packed_generation, m)?)?;
    m.add_function(wrap_pyfunction!(publish_packed_generation_async, m)?)?;
    Ok(())
}
#[pyclass(name = "PackedGeneration", module = "zstar", frozen)]
struct PyGeneration {
    inner: Generation,
}
#[pymethods]
impl PyGeneration {
    #[new]
    fn new(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        py.detach(|| Generation::decode(&data))
            .map(|inner| Self { inner })
            .map_err(core_error)
    }
    fn encode<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.encode().map_err(core_error)?;
        Ok(PyBytes::new(py, &bytes))
    }
    #[getter]
    fn archive_key(&self) -> &str {
        &self.inner.archive.key
    }
    #[getter]
    fn catalog_key(&self) -> &str {
        &self.inner.catalog.key
    }
}
async fn publish(
    store: Arc<Store>,
    object: Py<PyAny>,
    prefix: String,
    catalog: Catalog,
    strategy: Chunking,
) -> PyResult<(String, PyGeneration)> {
    use tokio::io::AsyncReadExt;
    let mut body = Body {
        object: Arc::new(object),
        pending: None,
        buffer: Bytes::new(),
    };
    let mut spool = Spool::new(ManifestOptions::default());
    let mut buffer = vec![0; 64 << 10];
    loop {
        let n = body
            .read(&mut buffer)
            .await
            .map_err(|e| core_error(zstar::Error::Io(e)))?;
        if n == 0 {
            break;
        }
        let data = buffer[..n].to_vec();
        spool = tokio::task::spawn_blocking(move || {
            spool.append(&data)?;
            Ok::<_, zstar::Error>(spool)
        })
        .await
        .map_err(|_| core_error(zstar::Error::Unsupported("archive spool worker failed")))?
        .map_err(core_error)?;
    }
    publish_generation(
        store.as_ref(),
        &prefix,
        spool,
        &catalog,
        strategy,
        CatalogLimits::default(),
    )
    .await
    .map(|(s, g)| (s.key, PyGeneration { inner: g }))
    .map_err(|failure| {
        let error = core_error(failure.error);
        Python::attach(|py| {
            let _ = error.value(py).setattr(
                "completed_artifacts",
                failure
                    .completed
                    .into_iter()
                    .map(|s| s.key)
                    .collect::<Vec<_>>(),
            );
        });
        error
    })
}
#[pyfunction]
#[pyo3(signature=(store,archive,catalog,*,prefix="objects".to_string(),chunking=None))]
fn publish_packed_generation(
    py: Python<'_>,
    store: PyRef<'_, PyStore>,
    archive: Py<PyAny>,
    catalog: PyRef<'_, PyCatalog>,
    prefix: String,
    chunking: Option<PyRef<'_, PyChunking>>,
) -> PyResult<(String, PyGeneration)> {
    let store = store.inner.clone();
    let catalog = catalog.inner.clone();
    let strategy = chunking.map_or_else(Chunking::default, |c| c.inner);
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(publish(store, archive, prefix, catalog, strategy))
    })
}
#[pyfunction]
#[pyo3(signature=(store,archive,catalog,*,prefix="objects".to_string(),chunking=None))]
fn publish_packed_generation_async<'py>(
    py: Python<'py>,
    store: PyRef<'_, PyStore>,
    archive: Py<PyAny>,
    catalog: PyRef<'_, PyCatalog>,
    prefix: String,
    chunking: Option<PyRef<'_, PyChunking>>,
) -> PyResult<Bound<'py, PyAny>> {
    let store = store.inner.clone();
    let catalog = catalog.inner.clone();
    let strategy = chunking.map_or_else(Chunking::default, |c| c.inner);
    pyo3_async_runtimes::tokio::future_into_py(
        py,
        publish(store, archive, prefix, catalog, strategy),
    )
}
