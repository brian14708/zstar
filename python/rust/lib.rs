use pyo3::{buffer::PyBuffer, prelude::*};
use std::sync::Arc;
mod async_archive;
mod manifest_api;
mod pack;
mod reader;
mod request;
mod s3;

pyo3::create_exception!(zstar, ZstarError, pyo3::exceptions::PyValueError);

/// Lock a state mutex whose protected value stays consistent across a panic
/// (short critical sections that only swap or clone an `Option`/`Arc`).
/// Poisoning is therefore ignored instead of cascading panics into every
/// Python thread. Never hold such a guard while calling into Python.
pub(crate) fn lock_state<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Lock a mutex guarding long-running work whose partial effects are unknown
/// after a panic. A poisoned guard is reported as an error, never as a panic.
pub(crate) fn lock_work<T>(
    mutex: &std::sync::Mutex<T>,
) -> zstar::Result<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| zstar::Error::Unsupported("object poisoned by an earlier panic"))
}

/// A Python exception raised by a user callback, carried through the Rust
/// error chain so it can be restored as `__cause__`. The display text is fixed
/// because callback exceptions can contain credentials or signed URLs.
#[derive(Debug)]
pub(crate) struct PyCause {
    message: String,
    cause: PyErr,
}
impl PyCause {
    pub(crate) fn io(message: impl Into<String>, cause: PyErr) -> std::io::Error {
        std::io::Error::other(Self {
            message: message.into(),
            cause,
        })
    }
}
impl std::fmt::Display for PyCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for PyCause {}

fn python_cause(error: &zstar::Error) -> Option<&PyCause> {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    for _ in 0..64 {
        if let Some(found) = cause.downcast_ref::<PyCause>() {
            return Some(found);
        }
        let io = match cause.downcast_ref::<zstar::Error>() {
            Some(zstar::Error::Io(io)) => Some(io),
            _ => cause.downcast_ref::<std::io::Error>(),
        };
        if let Some(io) = io
            && let Some(inner) = io.get_ref()
        {
            cause = inner;
            continue;
        }
        cause = cause.source()?;
    }
    None
}

/// Bounded decoded-chunk memory shared by explicitly bound readers.
#[pyclass(module = "zstar", frozen)]
pub(crate) struct DecodedMemoryCache {
    inner: Arc<zstar::MemoryDecodedCache>,
}
#[pymethods]
impl DecodedMemoryCache {
    #[new]
    fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(zstar::MemoryDecodedCache::new(capacity)),
        }
    }
    #[getter]
    fn used_bytes(&self) -> usize {
        self.inner.used()
    }
    fn clear(&self) {
        self.inner.clear();
    }
}

/// Immutable validated content; only readers can construct tokens.
#[pyclass(frozen, module = "zstar")]
pub(crate) struct DecodedChunk {
    inner: zstar::CachedChunk,
}
fn decoded_cache_key(key: &zstar::DecodedCacheKey) -> String {
    fn hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for &byte in bytes {
            output.push(char::from(DIGITS[usize::from(byte >> 4)]));
            output.push(char::from(DIGITS[usize::from(byte & 15)]));
        }
        output
    }
    match key {
        zstar::DecodedCacheKey::Archive {
            identity,
            metadata,
            offset,
            encoded_size,
        } => format!(
            "a:{}:{}:{offset}:{encoded_size}",
            hex(identity),
            hex(metadata)
        ),
        zstar::DecodedCacheKey::Object(digest) => format!("o:sha256:{}", hex(digest)),
    }
}
#[pymethods]
impl DecodedChunk {
    #[getter]
    fn key(&self) -> String {
        decoded_cache_key(self.inner.key())
    }
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new(py, self.inner.as_bytes())
    }
    #[getter]
    fn weight(&self) -> usize {
        self.inner.weight()
    }
}
struct PythonDecodedCache {
    object: Py<PyAny>,
}
impl zstar::DecodedCache for PythonDecodedCache {
    fn get(&self, key: &zstar::DecodedCacheKey) -> std::io::Result<Option<zstar::CachedChunk>> {
        Python::attach(|py| {
            let result = self
                .object
                .bind(py)
                .call_method1("get", (decoded_cache_key(key),))?;
            if result.is_none() {
                return Ok(None);
            }
            let token = result.extract::<PyRef<'_, DecodedChunk>>()?;
            Ok(Some(token.inner.clone()))
        })
        .map_err(|e: PyErr| std::io::Error::other(e.to_string()))
    }
    fn put(&self, value: zstar::CachedChunk) -> std::io::Result<()> {
        Python::attach(|py| {
            let token = Py::new(py, DecodedChunk { inner: value })?;
            self.object.bind(py).call_method1("put", (token,))?;
            Ok(())
        })
        .map_err(|e: PyErr| std::io::Error::other(e.to_string()))
    }
}

pub(crate) fn decoded_cache_binding(
    py: Python<'_>,
    cache: Option<Py<PyAny>>,
    identity: Option<Vec<u8>>,
) -> PyResult<Option<zstar::SharedDecodedCache>> {
    match (cache, identity) {
        (None, None) => Ok(None),
        (Some(cache), Some(identity)) => {
            let backend: Arc<dyn zstar::DecodedCache> =
                if let Ok(memory) = cache.bind(py).extract::<PyRef<'_, DecodedMemoryCache>>() {
                    memory.inner.clone()
                } else {
                    for method in ["get", "put"] {
                        if !cache.bind(py).getattr(method)?.is_callable() {
                            return Err(pyo3::exceptions::PyTypeError::new_err(
                                "shared_cache must provide callable get and put methods",
                            ));
                        }
                    }
                    Arc::new(PythonDecodedCache { object: cache })
                };
            zstar::SharedDecodedCache::new(&identity, backend)
                .map(Some)
                .map_err(core_error)
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            "shared_cache and shared_cache_identity must be supplied together",
        )),
    }
}

/// Copy any contiguous or strided bytes-like object exactly once.
pub(crate) fn buffer_vec(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let buffer = PyBuffer::<u8>::get(data)?;
    buffer.to_vec(py)
}

fn local_source_error(error: zstar::Error) -> PyErr {
    match error {
        zstar::Error::Io(error) => error.into(),
        other => core_error(other),
    }
}

fn core_error(error: zstar::Error) -> PyErr {
    if let zstar::Error::MissingEntry(path) = &error {
        return pyo3::exceptions::PyKeyError::new_err(path.clone());
    }
    let exception = ZstarError::new_err(error.to_string());
    let _ = Python::attach(|py| {
        // The category is independent of human-readable diagnostic text.
        exception.value(py).setattr("category", error.category())?;
        if let Some(found) = python_cause(&error) {
            exception.set_cause(py, Some(found.cause.clone_ref(py)));
        }
        Ok::<_, PyErr>(())
    });
    exception
}

async fn read_length(
    reader: &zstar::Archive<dyn zstar::AsyncRangeReader>,
    path: &str,
    offset: u64,
    length: Option<u64>,
) -> zstar::Result<u64> {
    match length {
        Some(length) => Ok(length),
        None => reader
            .entry(path)
            .await?
            .ok_or_else(|| zstar::Error::MissingEntry(path.into()))?
            .size()?
            .checked_sub(offset)
            .ok_or(zstar::Error::Range),
    }
}

fn filesystem_error(error: Box<dyn std::error::Error + Send + Sync>) -> PyErr {
    // Streaming body readers preserve format errors inside std::io::Error.
    // Inspect those payloads as well as ordinary source chains before falling
    // back to an I/O category. Bound traversal for third-party cyclic causes.
    let mut cause: &(dyn std::error::Error + 'static) = error.as_ref();
    let mut category = "io";
    for _ in 0..64 {
        if let Some(core) = cause.downcast_ref::<zstar::Error>() {
            category = core.category();
            if let zstar::Error::Io(io) = core
                && let Some(inner) = io.get_ref()
            {
                cause = inner;
                continue;
            }
            break;
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>()
            && let Some(inner) = io.get_ref()
        {
            cause = inner;
            continue;
        }
        let Some(inner) = cause.source() else { break };
        cause = inner;
    }
    let exception = ZstarError::new_err(error.to_string());
    let _ = Python::attach(|py| exception.value(py).setattr("category", category));
    exception
}

fn batch_requests(requests: &Bound<'_, PyAny>) -> PyResult<Vec<(String, u64, u64)>> {
    let count = requests.len()?;
    if count > zstar::MAX_BATCH_REQUESTS {
        return Err(core_error(zstar::Error::Budget("batch request count")));
    }
    (0..count)
        .map(|index| requests.get_item(index)?.extract())
        .collect()
}

fn report_dict(py: Python<'_>, report: zstar::ReadReport) -> PyResult<Py<pyo3::types::PyDict>> {
    let result = pyo3::types::PyDict::new(py);
    result.set_item("metadata_authenticated", report.metadata_authenticated)?;
    result.set_item("full_archive_validated", report.full_archive_validated)?;
    result.set_item("data_frames_checked", report.data_frames_checked)?;
    result.set_item(
        "external_objects_sha256_checked",
        report
            .external_objects_sha256_checked
            .iter()
            .map(zstar::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    result.set_item(
        "external_objects_unchecked",
        report
            .external_objects_unchecked
            .iter()
            .map(zstar::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    Ok(result.unbind())
}

fn verification_dict(
    py: Python<'_>,
    report: zstar::VerificationReport,
) -> PyResult<Py<pyo3::types::PyDict>> {
    let result = pyo3::types::PyDict::new(py);
    for field in [
        "metadata_validated",
        "full_archive_verified",
        "tar_members_verified",
        "pointer_text_verified",
        "data_checksums_checked",
    ] {
        result.set_item(field, true)?;
    }
    result.set_item(
        "external_closure_validated",
        report.scope == zstar::VerificationScope::ExternalClosure,
    )?;
    result.set_item("tar_members", report.tar_members)?;
    result.set_item("pointer_members", report.pointer_members)?;
    result.set_item("data_frames", report.data_frames)?;
    result.set_item(
        "external_objects_sha256_checked",
        report
            .external_objects_sha256_checked
            .iter()
            .map(zstar::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    result.set_item(
        "external_objects_unchecked",
        report
            .external_objects_unchecked
            .iter()
            .map(zstar::manifest::object_address)
            .collect::<Vec<_>>(),
    )?;
    Ok(result.unbind())
}

struct PythonSource {
    cache_policy: Arc<std::sync::Mutex<zstar::http::CachePolicy>>,
    object: Arc<Py<PyAny>>,
    size: u64,
    permits: Arc<tokio::sync::Semaphore>,
}
#[async_trait::async_trait]
impl zstar::AsyncRangeReader for PythonSource {
    fn cache_allowed(&self) -> bool {
        self.cache_policy.lock().unwrap().allows_reuse()
    }
    async fn size(&self) -> zstar::Result<u64> {
        Ok(self.size)
    }
    async fn read_range(&self, range: std::ops::Range<u64>) -> zstar::Result<bytes::Bytes> {
        zstar::checked_range(
            range.start,
            range
                .end
                .checked_sub(range.start)
                .ok_or(zstar::Error::Range)?,
            self.size,
        )?;
        let object = self.object.clone();
        let cache_policy = self.cache_policy.clone();
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
                if let Ok(control) = object.bind(py).getattr("_zstar_cache_control") {
                    let control: String = control.extract()?;
                    cache_policy
                        .lock()
                        .unwrap()
                        .observe(zstar::http::CachePolicy::from_cache_control(&control));
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
/// Shared callback validation for blocking manifest and async indexed readers.
fn resolve_encoded(
    object: &Py<PyAny>,
    digest: [u8; 32],
    hint: u64,
    max_bytes: u64,
) -> zstar::Result<Vec<u8>> {
    Python::attach(|py| -> PyResult<Vec<u8>> {
        let value = object.call_method1(
            py,
            "get_encoded",
            (zstar::manifest::object_address(&digest), hint, max_bytes),
        )?;
        if value.bind(py).len()? as u64 > max_bytes {
            return Err(core_error(zstar::Error::Budget(
                "encoded resolver output size",
            )));
        }
        value.extract(py)
    })
    .map_err(|error| {
        Python::attach(|py| {
            if error.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
                zstar::Error::MissingObject(digest)
            } else if error.is_instance_of::<ZstarError>(py)
                && error
                    .value(py)
                    .getattr("category")
                    .and_then(|c| c.extract::<String>())
                    .ok()
                    .as_deref()
                    == Some("resource-limit")
            {
                zstar::Error::Budget("encoded resolver output size")
            } else {
                zstar::Error::Io(PyCause::io("Python encoded-object resolver failed", error))
            }
        })
    })
}

struct PythonResolver(Arc<Py<PyAny>>);
#[async_trait::async_trait]
impl zstar::ObjectResolver for PythonResolver {
    async fn get_encoded(
        &self,
        digest: [u8; 32],
        hint: u64,
        max_bytes: u64,
    ) -> zstar::Result<bytes::Bytes> {
        let object = self.0.clone();
        // A cancelled await cannot interrupt Python code already running.
        // Retain a global callback permit until that code actually completes.
        static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        let permit = PERMITS
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| zstar::Error::Unsupported("resolver closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            resolve_encoded(&object, digest, hint, max_bytes).map(bytes::Bytes::from)
        })
        .await
        .map_err(|_| zstar::Error::Io(std::io::Error::other("Python resolver worker failed")))?
    }
}

/// Concurrent indexed reads over a synchronous Python source. Content requests
/// use the native async entry reader; callbacks run on bounded blocking workers.
#[pyclass(module = "zstar.format", frozen)]
struct IndexedArchive {
    inner: std::sync::Mutex<Option<Arc<IndexedState>>>,
}
struct IndexedState {
    reader: zstar::Archive<dyn zstar::AsyncRangeReader>,
    resolver: Option<Arc<dyn zstar::ObjectResolver>>,
}
impl IndexedArchive {
    async fn open_source(
        source: Arc<dyn zstar::AsyncRangeReader>,
        options: zstar::ReaderOptions,
        shared_cache: Option<zstar::SharedDecodedCache>,
    ) -> PyResult<Self> {
        let resolver = options.resolver.clone();
        let reader = reader::open_reader(source, options, shared_cache).await?;
        Ok(Self {
            inner: std::sync::Mutex::new(Some(Arc::new(IndexedState { reader, resolver }))),
        })
    }
    fn reader(&self) -> PyResult<Arc<IndexedState>> {
        // Clone under the mutex, release it, and only then build a Python error
        // (which attaches to the interpreter and may run arbitrary code).
        let state = lock_state(&self.inner).clone();
        state.ok_or_else(|| core_error(zstar::Error::Unsupported("reader closed")))
    }
}
#[pymethods]
impl IndexedArchive {
    fn _open_member(&self, py: Python<'_>, path: &str) -> PyResult<reader::MemberCursor> {
        let inner = self.reader()?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(reader::MemberCursor::open(&inner.reader, path))
        })
    }
    fn _read_member(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        offset: u64,
        length: u64,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let inner = self.reader()?;
        let bytes = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    cursor
                        .inner
                        .lock()
                        .await
                        .read(&inner.reader, offset, length)
                        .await
                })
            })
            .map_err(core_error)?;
        Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
    }
    fn _read_member_into(
        &self,
        py: Python<'_>,
        cursor: &reader::MemberCursor,
        offset: u64,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let inner = self.reader()?;
        let buffer = PyBuffer::<u8>::get(buffer)?;
        let bytes = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    cursor
                        .inner
                        .lock()
                        .await
                        .read(&inner.reader, offset, buffer.item_count() as u64)
                        .await
                })
            })
            .map_err(core_error)?;
        buffer.copy_from_slice(py, &bytes)?;
        Ok(bytes.len())
    }
    fn entry(&self, py: Python<'_>, path: &str) -> PyResult<Option<Py<PyAny>>> {
        let reader = self.reader()?;
        let entry = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(reader.reader.entry(path))
            })
            .map_err(core_error)?;
        entry
            .map(|entry| manifest_api::to_py(py, &entry.to_value()))
            .transpose()
    }
    #[getter]
    fn entry_count(&self) -> PyResult<u64> {
        Ok(self.reader()?.reader.entry_count())
    }
    #[pyo3(signature=(prefix=None))]
    fn iter_entries(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<IndexedEntries> {
        slf.reader()?;
        Ok(IndexedEntries {
            archive: slf.into(),
            state: std::sync::Mutex::new(EntriesState {
                cursor: None,
                prefix,
                exhausted: false,
                buffer: std::collections::VecDeque::new(),
            }),
        })
    }
    #[pyo3(signature=(*,external=false))]
    fn verify(&self, py: Python<'_>, external: bool) -> PyResult<Py<pyo3::types::PyDict>> {
        let reader = self.reader()?;
        let scope = if external {
            zstar::VerificationScope::ExternalClosure
        } else {
            zstar::VerificationScope::Archive
        };
        let report = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(reader.reader.verify(scope))
            })
            .map_err(core_error)?;
        verification_dict(py, report)
    }

    /// Open an immutable encoded byte snapshot.
    #[staticmethod]
    #[pyo3(signature=(data,*,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_bytes(
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        cache_bytes: usize,
        verify_external: bool,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let (options, shared_cache) = reader::reader_options(
            py,
            cache_bytes,
            verify_external,
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            max_spool_bytes,
            create_spill_store,
            shared_cache,
            shared_cache_identity,
            resolver,
        )?;
        let source: Arc<dyn zstar::AsyncRangeReader> =
            Arc::new(bytes::Bytes::from(buffer_vec(py, data)?));
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(Self::open_source(
                source,
                options,
                shared_cache,
            ))
        })
    }

    /// Open a strong-ETag-pinned HTTP archive.
    #[staticmethod]
    #[pyo3(signature=(url,*,headers=None,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_url(
        py: Python<'_>,
        url: String,
        headers: Option<std::collections::BTreeMap<String, String>>,
        cache_bytes: usize,
        verify_external: bool,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let (options, shared_cache) = reader::reader_options(
            py,
            cache_bytes,
            verify_external,
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            max_spool_bytes,
            create_spill_store,
            shared_cache,
            shared_cache_identity,
            resolver,
        )?;
        let mut request = zstar::http::PresignedRequest::get(url);
        request.headers = headers.unwrap_or_default().into_iter().collect();
        let transport =
            zstar::http::ReqwestTransport::new().map_err(|error| core_error(error.into()))?;
        let source: Arc<dyn zstar::AsyncRangeReader> = Arc::new(zstar::http::HttpRangeSource::new(
            Arc::new(transport),
            request,
        ));
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(Self::open_source(
                source,
                options,
                shared_cache,
            ))
        })
    }

    /// Open an immutable native S3 archive.
    #[staticmethod]
    #[pyo3(signature=(bucket,key,*,endpoint=None,region=None,object_prefix=None,anon=None,access_key_id=None,secret_access_key=None,session_token=None,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
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
        cache_bytes: usize,
        verify_external: bool,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let (options, shared_cache) = reader::reader_options(
            py,
            cache_bytes,
            verify_external,
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            max_spool_bytes,
            create_spill_store,
            shared_cache,
            shared_cache_identity,
            resolver,
        )?;
        let builder = s3::builder(
            bucket,
            endpoint,
            region,
            anon,
            access_key_id,
            secret_access_key,
            session_token,
        );
        py.detach(|| {
            let mut options = options;
            let source =
                s3::source(builder, key, object_prefix, &mut options).map_err(core_error)?;
            pyo3_async_runtimes::tokio::get_runtime().block_on(Self::open_source(
                source,
                options,
                shared_cache,
            ))
        })
    }

    /// Open a local archive using native positional reads.
    #[staticmethod]
    #[pyo3(signature=(path,*,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_file(
        py: Python<'_>,
        path: std::path::PathBuf,
        cache_bytes: usize,
        verify_external: bool,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let (options, shared_cache) = reader::reader_options(
            py,
            cache_bytes,
            verify_external,
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            max_spool_bytes,
            create_spill_store,
            shared_cache,
            shared_cache_identity,
            resolver,
        )?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let source: Arc<dyn zstar::AsyncRangeReader> = Arc::new(
                    zstar::fs::AsyncLocalFileSource::open(path)
                        .await
                        .map_err(local_source_error)?,
                );
                Self::open_source(source, options, shared_cache).await
            })
        })
    }

    #[new]
    #[pyo3(signature=(source,size,*,cache_bytes=16<<20,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,verify_external=true,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        source: Py<PyAny>,
        size: u64,
        cache_bytes: usize,
        memory_bytes: u64,
        max_encoded_bytes: u64,
        max_decoded_bytes: u64,
        max_read_bytes: u64,
        max_retained_entry_bytes: u64,
        max_entries: u64,
        verify_external: bool,
        max_spool_bytes: u64,
        create_spill_store: Option<Py<PyAny>>,
        shared_cache: Option<Py<PyAny>>,
        shared_cache_identity: Option<Vec<u8>>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let (options, shared_cache) = reader::reader_options(
            py,
            cache_bytes,
            verify_external,
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            max_spool_bytes,
            create_spill_store,
            shared_cache,
            shared_cache_identity,
            resolver,
        )?;
        let source: Arc<dyn zstar::AsyncRangeReader> = Arc::new(PythonSource {
            cache_policy: Arc::new(std::sync::Mutex::new(zstar::http::CachePolicy::default())),
            object: Arc::new(source),
            size,
            permits: Arc::new(tokio::sync::Semaphore::new(8)),
        });
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(Self::open_source(
                source,
                options,
                shared_cache,
            ))
        })
    }
    fn paths(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let inner = self.reader()?;
        let entries = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(inner.reader.collect_entries())
            })
            .map_err(core_error)?;
        Ok(entries.into_iter().map(|e| e.path).collect())
    }
    fn validate_metadata(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.reader()?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(inner.reader.validate_metadata())
        })
        .map_err(core_error)
    }
    #[pyo3(signature=(path,offset,length,*,resolver=None))]
    fn read_with_report(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<(Py<pyo3::types::PyBytes>, Py<pyo3::types::PyDict>)> {
        let inner = self.reader()?;
        let resolver = resolver.map(|r| pack::binding(py, r));
        let configured = inner.resolver.clone();
        let (bytes, report) = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    let length = read_length(&inner.reader, path, offset, length).await?;
                    inner
                        .reader
                        .read_range_with_report_and_resolver(
                            path,
                            offset,
                            length,
                            resolver
                                .as_ref()
                                .map(|r| r.as_ref() as &dyn zstar::ObjectResolver)
                                .or(configured.as_deref()),
                        )
                        .await
                })
            })
            .map_err(core_error)?;
        Ok((
            pyo3::types::PyBytes::new(py, &bytes).unbind(),
            report_dict(py, report)?,
        ))
    }
    #[pyo3(signature=(requests,*,resolver=None))]
    fn read_many(
        &self,
        py: Python<'_>,
        requests: &Bound<'_, PyAny>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Vec<Py<pyo3::types::PyBytes>>> {
        let inner = self.reader()?;
        let requests = batch_requests(requests)?;
        let resolver = resolver.map(|r| pack::binding(py, r));
        let configured = inner.resolver.clone();
        let outputs = py
            .detach(|| {
                let requests: Vec<_> = requests
                    .iter()
                    .map(|(path, offset, length)| zstar::ReadRequest {
                        path,
                        offset: *offset,
                        length: *length,
                    })
                    .collect();
                pyo3_async_runtimes::tokio::get_runtime().block_on(
                    inner.reader.read_many_with_resolver(
                        &requests,
                        resolver
                            .as_ref()
                            .map(|r| r.as_ref() as &dyn zstar::ObjectResolver)
                            .or(configured.as_deref()),
                    ),
                )
            })
            .map_err(core_error)?;
        Ok(outputs
            .iter()
            .map(|bytes| pyo3::types::PyBytes::new(py, bytes).unbind())
            .collect())
    }
    #[getter]
    fn cache_bytes(&self) -> usize {
        lock_state(&self.inner)
            .as_ref()
            .map_or(0, |inner| inner.reader.cache_len_bytes())
    }
    #[getter]
    fn closed(&self) -> bool {
        lock_state(&self.inner).is_none()
    }
    fn close(&self, py: Python<'_>) {
        let inner = lock_state(&self.inner).take();
        // Existing calls retain their reader lease until bounded work finishes.
        // Drop the owner's cache/spool/source references without holding the GIL.
        py.detach(|| drop(inner));
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
    fn _entry(&self, py: Python<'_>, path: &str) -> PyResult<Py<pyo3::types::PyDict>> {
        let inner = self.reader()?;
        let entry = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(inner.reader.entry(path)))
            .map_err(core_error)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(path.to_owned()))?;
        entry_info(py, &entry)
    }
    #[pyo3(signature=(path,offset=0,length=None,*,resolver=None))]
    fn read(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Py<pyo3::types::PyBytes>> {
        let inner = self.reader()?;
        let resolver = resolver.map(|r| pack::binding(py, r));
        let configured = inner.resolver.clone();
        let data = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                    let length = read_length(&inner.reader, path, offset, length).await?;
                    inner
                        .reader
                        .read_range_with_resolver(
                            path,
                            offset,
                            length,
                            resolver
                                .as_ref()
                                .map(|r| r.as_ref() as &dyn zstar::ObjectResolver)
                                .or(configured.as_deref()),
                        )
                        .await
                })
            })
            .map_err(core_error)?;
        Ok(pyo3::types::PyBytes::new(py, &data).unbind())
    }
    fn _readinto(
        &self,
        py: Python<'_>,
        path: &str,
        offset: u64,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let inner = self.reader()?;
        let buffer = PyBuffer::<u8>::get(buffer)?;
        let length = buffer.item_count() as u64;
        let data = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(
                    inner.reader.read_range_with_resolver(
                        path,
                        offset,
                        length,
                        inner.resolver.as_deref(),
                    ),
                )
            })
            .map_err(core_error)?;
        buffer.copy_from_slice(py, &data)?;
        Ok(data.len())
    }
}
/// Cursor state of a synchronous entry iterator; guarded by a mutex so that
/// concurrent `next()` calls from several threads serialize instead of raising
/// the interpreter's "already borrowed" error.
struct EntriesState {
    cursor: Option<zstar::EntryCursor>,
    prefix: Option<String>,
    exhausted: bool,
    buffer: std::collections::VecDeque<zstar::Entry>,
}
#[pyclass(module = "zstar.format", frozen)]
struct IndexedEntries {
    archive: Py<IndexedArchive>,
    state: std::sync::Mutex<EntriesState>,
}
#[pymethods]
impl IndexedEntries {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let reader = self.archive.get().reader()?;
        // Wait for the cursor and do all blocking work detached from Python;
        // convert to Python objects only after the lock is released.
        let entry = py
            .detach(|| {
                let mut state = lock_work(&self.state)?;
                if state.exhausted {
                    return Ok(None);
                }
                if state.buffer.is_empty() {
                    let checkpoint = state.cursor.clone();
                    let (cursor, entries) =
                        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                            let cursor = match checkpoint {
                                Some(cursor) => cursor,
                                None => match &state.prefix {
                                    Some(prefix) => reader.reader.cursor_prefix(prefix).await?,
                                    None => reader.reader.cursor().await?,
                                },
                            };
                            reader.reader.next_entries(cursor, 64).await
                        })?;
                    state.cursor = Some(cursor);
                    state.buffer = entries.into();
                }
                let entry = state.buffer.pop_front();
                if entry.is_none() {
                    state.exhausted = true;
                }
                Ok::<_, zstar::Error>(entry)
            })
            .map_err(core_error)?;
        entry
            .map(|entry| manifest_api::to_py(py, &entry.to_value()))
            .transpose()
    }
}

fn entry_info(py: Python<'_>, entry: &zstar::manifest::Entry) -> PyResult<Py<pyo3::types::PyDict>> {
    let info = pyo3::types::PyDict::new(py);
    info.set_item("path", &entry.path)?;
    info.set_item(
        "kind",
        match entry.kind {
            zstar::manifest::EntryKind::File => "file",
            zstar::manifest::EntryKind::Directory => "directory",
            zstar::manifest::EntryKind::Symlink => "symlink",
        },
    )?;
    info.set_item("size", entry.size().map_err(core_error)?)?;
    info.set_item("mode", entry.mode)?;
    info.set_item("uid", entry.uid)?;
    info.set_item("gid", entry.gid)?;
    info.set_item("mtime_ns", entry.mtime_ns)?;
    Ok(info.unbind())
}

/// Decode one independent external Zstandard object under the format limit.
#[pyfunction]
fn decode_object(
    py: Python<'_>,
    encoded: Vec<u8>,
    size: usize,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    if size == 0 || size > 16 << 20 || encoded.len() > 17 << 20 {
        return Err(core_error(zstar::Error::MalformedExternalContent(
            "object size outside format bounds".into(),
        )));
    }
    let raw = py
        .detach(|| zstar::decode_frame(&encoded, size))
        .map_err(|error| core_error(zstar::Error::MalformedExternalContent(error.to_string())))?;
    Ok(pyo3::types::PyBytes::new(py, &raw).unbind())
}

#[pyfunction]
fn pack_file(py: Python<'_>, path: &str, data: Vec<u8>) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| {
            let mut writer = zstar::backend::BlockingBuilder::new(Vec::new())?;
            writer.add_file(
                zstar::manifest::Entry {
                    path: path.to_owned(),
                    kind: zstar::manifest::EntryKind::File,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: Default::default(),
                },
                data.len() as u64,
                &mut data.as_slice(),
            )?;
            writer.finish()
        })
        .map_err(core_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}

#[pyfunction]
#[pyo3(signature=(path,*,preserve_metadata=false))]
fn pack_directory(
    py: Python<'_>,
    path: std::path::PathBuf,
    preserve_metadata: bool,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| {
            let mut bytes = Vec::new();
            zstar::fs::pack_to_with_options(
                &path,
                &mut bytes,
                zstar::fs::PackOptions {
                    preserve_metadata,
                    ..Default::default()
                },
            )?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(bytes)
        })
        .map_err(filesystem_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}
#[pyfunction]
#[pyo3(signature=(data,destination,*,objects=None,allow_special_bits=false,verify_external=true))]
fn extract(
    py: Python<'_>,
    data: Vec<u8>,
    destination: std::path::PathBuf,
    objects: Option<std::path::PathBuf>,
    allow_special_bits: bool,
    verify_external: bool,
) -> PyResult<()> {
    py.detach(|| -> zstar::fs::AnyResult<()> {
        let mut archive = zstar::backend::BlockingArchive::open(
            zstar::SliceReader::new(&data),
            Default::default(),
        )?;
        let objects = objects
            .as_deref()
            .map(zstar::fs::LocalObjects::open)
            .transpose()?;
        zstar::fs::extract_with_options(
            &mut archive,
            &destination,
            objects,
            zstar::fs::ExtractOptions {
                allow_special_bits,
                verify_external,
                ..Default::default()
            },
        )
    })
    .map_err(filesystem_error)
}
#[pyfunction]
#[pyo3(signature=(data,*,objects=None,verify_external=true))]
fn materialize(
    py: Python<'_>,
    data: Vec<u8>,
    objects: Option<std::path::PathBuf>,
    verify_external: bool,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| -> zstar::fs::AnyResult<Vec<u8>> {
            let mut archive = zstar::backend::BlockingArchive::open(
                zstar::SliceReader::new(&data),
                Default::default(),
            )?;
            let objects = objects
                .as_deref()
                .map(zstar::fs::LocalObjects::open)
                .transpose()?;
            let mut output = Vec::new();
            zstar::fs::materialize_to(
                &mut archive,
                &mut output,
                objects,
                verify_external,
                Default::default(),
            )?;
            Ok(output)
        })
        .map_err(filesystem_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}

/// The extension keeps no GIL-protected global state: every pyclass is `Sync`
/// (frozen or internally locked) and no lock is held across a Python callback,
/// so it declares support for free-threaded `CPython`.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    manifest_api::register(m)?;
    pack::register(m)?;
    m.add("ZstarError", m.py().get_type::<ZstarError>())?;
    m.add_function(wrap_pyfunction!(decode_object, m)?)?;
    m.add_function(wrap_pyfunction!(request::put_presigned, m)?)?;
    m.add_function(wrap_pyfunction!(pack_file, m)?)?;
    m.add_function(wrap_pyfunction!(pack_directory, m)?)?;
    m.add_function(wrap_pyfunction!(extract, m)?)?;
    m.add_function(wrap_pyfunction!(materialize, m)?)?;
    m.add_function(wrap_pyfunction!(_s3_archive_metadata, m)?)?;
    m.add_class::<IndexedArchive>()?;
    m.add_class::<reader::MemberCursor>()?;
    m.add_class::<IndexedEntries>()?;
    m.add_class::<DecodedMemoryCache>()?;
    m.add_class::<DecodedChunk>()?;
    m.add_class::<async_archive::AsyncArchive>()?;
    m.add_class::<async_archive::AsyncEntries>()?;
    Ok(())
}

#[pyfunction]
fn _s3_archive_metadata(
    footer: &[u8],
    source_size: u64,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    let footer = zstar::parse_footer(footer).map_err(core_error)?;
    let attributes = zstar::s3::archive_attributes(&footer, source_size).map_err(core_error)?;
    Ok(attributes
        .iter()
        .filter_map(|(key, value)| {
            if let zstar::s3::object_store::Attribute::Metadata(key) = key {
                Some((key.to_string(), value.as_ref().to_owned()))
            } else {
                None
            }
        })
        .collect())
}
