use pyo3::{buffer::PyBuffer, prelude::*, types::PyBytes};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, watch};
use zstar::AsyncRangeReader;
use zstar::{ObjectResolver, ReaderOptions};

use super::reader::{Reader, open_reader, reader_options};
use super::request::FailureSlot;

/// Async indexed archive over bytes, local sources, HTTP, or S3.
/// Open authenticates metadata; use `verify` to check physical content.
#[pyclass(module = "zstar", frozen)]
pub struct AsyncArchive {
    state: Mutex<Owner>,
}
struct Owner {
    open: Option<Arc<State>>,
    closing: Weak<State>,
}
struct State {
    reader: Reader,
    resolver: Option<Arc<dyn ObjectResolver>>,
    closed: watch::Sender<bool>,
    active: AtomicUsize,
    drained: Notify,
    failures: Option<Arc<FailureSlot>>,
}
struct Lease(Arc<State>);
impl Drop for Lease {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}
impl Lease {
    async fn run<T>(&self, work: impl Future<Output = PyResult<T>>) -> PyResult<T> {
        let mut closed = self.0.closed.subscribe();
        tokio::select! {
            biased;
            () = async {
                if !*closed.borrow_and_update() {
                    let _ = closed.changed().await;
                }
            } => Err(super::core_error(zstar::Error::Unsupported("reader closed"))),
            result = work => result.map_err(|error| match &self.0.failures {
                Some(failures) => failures.annotate(error),
                None => error,
            }),
        }
    }
}
impl AsyncArchive {
    async fn open_source(
        source: Arc<dyn AsyncRangeReader>,
        options: ReaderOptions,
        shared_cache: Option<zstar::SharedDecodedCache>,
        failures: Option<Arc<FailureSlot>>,
    ) -> PyResult<Self> {
        let resolver = options.resolver.clone();
        let reader = open_reader(source, options, shared_cache)
            .await
            .map_err(|error| match &failures {
                Some(failures) => failures.annotate(error),
                None => error,
            })?;
        Ok(Self::owned(reader, resolver, failures))
    }
    fn owned(
        reader: Reader,
        resolver: Option<Arc<dyn ObjectResolver>>,
        failures: Option<Arc<FailureSlot>>,
    ) -> Self {
        Self {
            state: Mutex::new(Owner {
                open: Some(Arc::new(State {
                    reader,
                    resolver,
                    closed: watch::channel(false).0,
                    active: AtomicUsize::new(0),
                    drained: Notify::new(),
                    failures,
                })),
                closing: Weak::new(),
            }),
        }
    }
    fn lease(&self) -> PyResult<Lease> {
        // Admit under the mutex, release it, and only then build a Python error.
        let admitted = {
            let owner = super::lock_state(&self.state);
            owner.open.as_ref().map(|state| {
                state.active.fetch_add(1, Ordering::Relaxed);
                state.clone()
            })
        };
        admitted
            .map(Lease)
            .ok_or_else(|| super::core_error(zstar::Error::Unsupported("reader closed")))
    }
}
#[pymethods]
impl AsyncArchive {
    fn _open_member<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(super::reader::MemberCursor::open(&lease.0.reader, &path))
                .await
        })
    }
    fn _read_member<'py>(
        &self,
        py: Python<'py>,
        cursor: &super::reader::MemberCursor,
        offset: u64,
        length: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let cursor = cursor.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = lease
                .run(async {
                    cursor
                        .lock()
                        .await
                        .read(&lease.0.reader, offset, length)
                        .await
                        .map_err(super::core_error)
                })
                .await?;
            Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
        })
    }
    fn _read_member_into<'py>(
        &self,
        py: Python<'py>,
        cursor: &super::reader::MemberCursor,
        offset: u64,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let cursor = cursor.inner.clone();
        let buffer = PyBuffer::<u8>::get(buffer)?;
        let length = buffer.item_count() as u64;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes = lease
                .run(async {
                    cursor
                        .lock()
                        .await
                        .read(&lease.0.reader, offset, length)
                        .await
                        .map_err(super::core_error)
                })
                .await?;
            Python::attach(|py| {
                buffer.copy_from_slice(py, &bytes)?;
                Ok(bytes.len())
            })
        })
    }
    /// Verify every physical frame and tar member, optionally including external objects.
    #[pyo3(signature=(*,external=false))]
    fn verify<'py>(&self, py: Python<'py>, external: bool) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let scope = if external {
            zstar::VerificationScope::ExternalClosure
        } else {
            zstar::VerificationScope::Archive
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    let report = lease
                        .0
                        .reader
                        .verify(scope)
                        .await
                        .map_err(super::core_error)?;
                    Python::attach(|py| super::verification_dict(py, report))
                })
                .await
        })
    }
    /// Awaitable: owned entry metadata, or None if the path is absent.
    /// Never blocks the calling thread (or event loop) on the runtime.
    fn entry<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let entry = lease
                .run(async { lease.0.reader.entry(&path).await.map_err(super::core_error) })
                .await?;
            Python::attach(|py| {
                entry
                    .map(|entry| super::manifest_api::to_py(py, &entry.to_value()))
                    .transpose()
            })
        })
    }
    #[getter]
    fn entry_count(&self) -> PyResult<u64> {
        Ok(self.lease()?.0.reader.entry_count())
    }
    /// Lazily yield owned entry tuples in path order with async iteration.
    #[pyo3(signature=(prefix=None))]
    fn iter_entries(slf: PyRef<'_, Self>, prefix: Option<String>) -> PyResult<AsyncEntries> {
        slf.lease()?;
        Ok(AsyncEntries {
            archive: slf.into(),
            prefix,
            cursor: Arc::new(tokio::sync::Mutex::new((None, false))),
        })
    }
    /// Open an immutable encoded byte snapshot.
    #[staticmethod]
    #[pyo3(signature=(data,*,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_bytes<'py>(
        py: Python<'py>,
        data: &Bound<'py, PyAny>,
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        let source: Arc<dyn AsyncRangeReader> =
            Arc::new(bytes::Bytes::from(super::buffer_vec(py, data)?));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::open_source(source, options, shared_cache, None).await
        })
    }
    /// Open an immutable local archive using bounded positional file reads.
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
    ) -> PyResult<Bound<'_, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let source: Arc<dyn AsyncRangeReader> = Arc::new(
                zstar::fs::AsyncLocalFileSource::open(path)
                    .await
                    .map_err(super::local_source_error)?,
            );
            Self::open_source(source, options, shared_cache, None).await
        })
    }
    /// Borrow a seekable Python file object; sizing and reads run off-loop.
    #[staticmethod]
    #[pyo3(signature=(fileobj,*,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_fileobj(
        py: Python<'_>,
        fileobj: Py<PyAny>,
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
    ) -> PyResult<Bound<'_, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (object, size) = tokio::task::spawn_blocking(move || {
                Python::attach(|py| {
                    let source = py
                        .import("zstar._source")?
                        .getattr("_FileSource")?
                        .call1((fileobj,))?;
                    let size: u64 = source.getattr("size")?.extract()?;
                    Ok::<_, PyErr>((source.unbind(), size))
                })
            })
            .await
            .map_err(|_| {
                super::core_error(zstar::Error::Io(std::io::Error::other(
                    "file object worker failed",
                )))
            })??;
            let source: Arc<dyn AsyncRangeReader> = Arc::new(super::PythonSource {
                cache_policy: Arc::new(std::sync::Mutex::new(zstar::http::CachePolicy::default())),
                object: Arc::new(object),
                size,
                permits: Arc::new(tokio::sync::Semaphore::new(8)),
            });
            Self::open_source(source, options, shared_cache, None).await
        })
    }
    /// Open an immutable synchronous `read_range(offset, length)` source.
    /// Callbacks run on bounded workers.
    #[staticmethod]
    #[pyo3(signature=(source,size,*,cache_bytes=16<<20,verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_source(
        py: Python<'_>,
        source: Py<PyAny>,
        size: u64,
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
    ) -> PyResult<Bound<'_, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        let source: Arc<dyn AsyncRangeReader> = Arc::new(super::PythonSource {
            cache_policy: Arc::new(std::sync::Mutex::new(zstar::http::CachePolicy::default())),
            object: Arc::new(source),
            size,
            permits: Arc::new(tokio::sync::Semaphore::new(8)),
        });
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::open_source(source, options, shared_cache, None).await
        })
    }

    #[staticmethod]
    #[pyo3(signature=(url=None, *, headers=None, provider=None, cache_bytes=16_777_216, verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
    #[allow(clippy::too_many_arguments)]
    fn from_url(
        py: Python<'_>,
        url: Option<String>,
        headers: Option<BTreeMap<String, String>>,
        provider: Option<Py<PyAny>>,
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
    ) -> PyResult<Bound<'_, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        let failures = Arc::new(FailureSlot::default());
        let source: Arc<dyn AsyncRangeReader> = Arc::new(super::request::source(
            py,
            "GET",
            url,
            headers,
            provider,
            failures.clone(),
        )?);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::open_source(source, options, shared_cache, Some(failures)).await
        })
    }
    #[staticmethod]
    #[pyo3(signature=(bucket, key, *, endpoint=None, region=None, object_prefix=None, anon=None,access_key_id=None,secret_access_key=None,session_token=None, cache_bytes=16_777_216, verify_external=true,memory_bytes=8<<20,max_encoded_bytes=4<<30,max_decoded_bytes=4<<30,max_read_bytes=64<<20,max_retained_entry_bytes=1<<30,max_entries=0,max_spool_bytes=4<<30,create_spill_store=None,shared_cache=None,shared_cache_identity=None,resolver=None))]
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
    ) -> PyResult<Bound<'_, PyAny>> {
        let (options, shared_cache) = reader_options(
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
        let builder = super::s3::builder(
            bucket,
            endpoint,
            region,
            anon,
            access_key_id,
            secret_access_key,
            session_token,
        );
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut options = options;
            let source = super::s3::source(builder, key, object_prefix, &mut options)
                .map_err(super::core_error)?;
            Self::open_source(source, options, shared_cache, None).await
        })
    }
    /// Batch logical ranges with chunk sharing and cancellation propagation.
    #[pyo3(signature=(requests,*,resolver=None))]
    fn read_many<'py>(
        &self,
        py: Python<'py>,
        requests: &Bound<'_, PyAny>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let requests = super::batch_requests(requests)?;
        let lease = self.lease()?;
        let resolver = resolver.map(|r| super::pack::binding(py, r));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ranges: Vec<_> = requests
                .iter()
                .map(|(path, offset, length)| zstar::ReadRequest {
                    path,
                    offset: *offset,
                    length: *length,
                })
                .collect();
            lease
                .run(async {
                    let object_resolver = resolver
                        .as_ref()
                        .map(|r| r.as_ref() as &dyn ObjectResolver)
                        .or(lease.0.resolver.as_deref());
                    let outputs = lease
                        .0
                        .reader
                        .read_many_with_resolver(&ranges, object_resolver)
                        .await
                        .map_err(super::core_error)?;
                    Python::attach(|py| {
                        Ok(outputs
                            .iter()
                            .map(|bytes| PyBytes::new(py, bytes).unbind())
                            .collect::<Vec<_>>())
                    })
                })
                .await
        })
    }
    fn validate_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    lease
                        .0
                        .reader
                        .validate_metadata()
                        .await
                        .map_err(super::core_error)
                })
                .await
        })
    }
    #[pyo3(signature=(path,offset,length,*,resolver=None))]
    fn read_with_report<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let resolver = resolver.map(|r| super::pack::binding(py, r));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    let length = super::read_length(&lease.0.reader, &path, offset, length)
                        .await
                        .map_err(super::core_error)?;
                    let object_resolver = resolver
                        .as_ref()
                        .map(|r| r.as_ref() as &dyn ObjectResolver)
                        .or(lease.0.resolver.as_deref());
                    let (raw, report) = lease
                        .0
                        .reader
                        .read_range_with_report_and_resolver(&path, offset, length, object_resolver)
                        .await
                        .map_err(super::core_error)?;
                    Python::attach(|py| {
                        Ok((
                            PyBytes::new(py, &raw).unbind(),
                            super::report_dict(py, report)?,
                        ))
                    })
                })
                .await
        })
    }
    fn paths<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let entries = lease
                .run(async {
                    lease
                        .0
                        .reader
                        .collect_entries()
                        .await
                        .map_err(super::core_error)
                })
                .await?;
            Ok(entries.into_iter().map(|e| e.path).collect::<Vec<_>>())
        })
    }
    fn _entry<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let entry = lease
                .run(async { lease.0.reader.entry(&path).await.map_err(super::core_error) })
                .await?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(path.clone()))?;
            Python::attach(|py| super::entry_info(py, &entry))
        })
    }
    fn read_link<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    lease
                        .0
                        .reader
                        .read_link(&path)
                        .await
                        .map_err(super::core_error)
                })
                .await
        })
    }
    #[getter]
    fn cache_bytes(&self) -> usize {
        super::lock_state(&self.state)
            .open
            .as_ref()
            .map_or(0, |state| state.reader.cache_len_bytes())
    }
    #[getter]
    fn closed(&self) -> bool {
        super::lock_state(&self.state).open.is_none()
    }
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = {
            let mut owner = super::lock_state(&self.state);
            if let Some(state) = owner.open.take() {
                owner.closing = Arc::downgrade(&state);
                Some(state)
            } else {
                owner.closing.upgrade()
            }
        };
        if let Some(state) = &state {
            state.closed.send_replace(true);
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(state) = state {
                loop {
                    let drained = state.drained.notified();
                    tokio::pin!(drained);
                    drained.as_mut().enable();
                    if state.active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    drained.await;
                }
            }
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
    #[pyo3(signature=(path,offset=0,length=None,*,resolver=None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        length: Option<u64>,
        resolver: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let resolver = resolver.map(|r| super::pack::binding(py, r));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    let length = super::read_length(&lease.0.reader, &path, offset, length)
                        .await
                        .map_err(super::core_error)?;
                    let bytes = lease
                        .0
                        .reader
                        .read_range_with_resolver(
                            &path,
                            offset,
                            length,
                            resolver
                                .as_ref()
                                .map(|r| r.as_ref() as &dyn ObjectResolver)
                                .or(lease.0.resolver.as_deref()),
                        )
                        .await
                        .map_err(super::core_error)?;
                    Python::attach(|py| Ok(PyBytes::new(py, &bytes).unbind()))
                })
                .await
        })
    }
    fn _readinto<'py>(
        &self,
        py: Python<'py>,
        path: String,
        offset: u64,
        buffer: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.lease()?;
        let buffer = PyBuffer::<u8>::get(buffer)?;
        let length = buffer.item_count() as u64;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            lease
                .run(async {
                    let data = lease
                        .0
                        .reader
                        .read_range_with_resolver(
                            &path,
                            offset,
                            length,
                            lease.0.resolver.as_deref(),
                        )
                        .await
                        .map_err(super::core_error)?;
                    Python::attach(|py| {
                        buffer.copy_from_slice(py, &data)?;
                        Ok(data.len())
                    })
                })
                .await
        })
    }
}

/// An async cursor over owned entry metadata. Closing its archive invalidates it.
#[pyclass(module = "zstar", frozen)]
pub struct AsyncEntries {
    archive: Py<AsyncArchive>,
    prefix: Option<String>,
    cursor: Arc<tokio::sync::Mutex<(Option<zstar::EntryCursor>, bool)>>,
}
#[pymethods]
impl AsyncEntries {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.archive.get().lease()?;
        let cursor = self.cursor.clone();
        let prefix = self.prefix.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Self::next_batch(lease, cursor, prefix, 1)
                .await?
                .pop()
                .ok_or_else(|| pyo3::exceptions::PyStopAsyncIteration::new_err(()))
        })
    }
    fn _next_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let lease = self.archive.get().lease()?;
        let cursor = self.cursor.clone();
        let prefix = self.prefix.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, Self::next_batch(lease, cursor, prefix, 64))
    }
}
impl AsyncEntries {
    async fn next_batch(
        lease: Lease,
        cursor: Arc<tokio::sync::Mutex<(Option<zstar::EntryCursor>, bool)>>,
        prefix: Option<String>,
        limit: usize,
    ) -> PyResult<Vec<Py<PyAny>>> {
        lease
            .run(async {
                let mut state = cursor.lock().await;
                if state.1 {
                    return Ok(Vec::new());
                }
                // Commit the checkpoint after metadata and conversion succeed.
                let position = match state.0.clone() {
                    Some(position) => position,
                    None => match &prefix {
                        Some(prefix) => lease.0.reader.cursor_prefix(prefix).await,
                        None => lease.0.reader.cursor().await,
                    }
                    .map_err(super::core_error)?,
                };
                let (position, entries) = lease
                    .0
                    .reader
                    .next_entries(position, limit)
                    .await
                    .map_err(super::core_error)?;
                let output = Python::attach(|py| {
                    entries
                        .iter()
                        .map(|entry| super::manifest_api::to_py(py, &entry.to_value()))
                        .collect::<PyResult<Vec<_>>>()
                })?;
                state.0 = Some(position);
                state.1 = output.is_empty();
                Ok(output)
            })
            .await
    }
}
