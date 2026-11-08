//! Shared Python error, buffer, and callback synchronization rules.
use pyo3::{buffer::PyBuffer, prelude::*};
use zstar::Error;

/// Python numeric contract for public nonnegative integer arguments.
#[derive(Clone, Copy)]
pub struct Unsigned(pub u64);
impl FromPyObject<'_, '_> for Unsigned {
    type Error = PyErr;
    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<pyo3::types::PyBool>()
            || !value.is_instance_of::<pyo3::types::PyInt>()
        {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "expected an integer",
            ));
        }
        if value.lt(0)? {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "expected a nonnegative integer",
            ));
        }
        value.extract::<u64>().map(Self)
    }
}
pyo3::create_exception!(zstar, ZstarError, pyo3::exceptions::PyValueError);
/// Lock a state mutex whose protected value stays consistent across a panic
/// (short critical sections that only swap or clone an `Option`/`Arc`).
/// Poisoning is therefore ignored instead of cascading panics into every
/// Python thread. Never hold such a guard while calling into Python.
pub fn lock_state<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A Python exception raised by a user callback, carried through the Rust
/// error chain so it can be restored as `__cause__`. The display text is fixed
/// because callback exceptions can contain credentials or signed URLs.
#[derive(Debug)]
pub struct PyCause {
    message: String,
    cause: PyErr,
}
impl PyCause {
    pub fn io(message: impl Into<String>, cause: PyErr) -> std::io::Error {
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

/// Copy any contiguous or strided bytes-like object exactly once.
pub fn buffer_vec(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let buffer = PyBuffer::<u8>::get(data)?;
    buffer.to_vec(py)
}

/// Share immutable `bytes` without copying; snapshot any other buffer once.
pub fn shared_bytes(data: &Bound<'_, PyAny>) -> PyResult<bytes::Bytes> {
    if let Ok(bytes) = data.cast::<pyo3::types::PyBytes>() {
        return Ok(bytes::Bytes::from_owner(
            pyo3::pybacked::PyBackedBytes::from(bytes.clone()),
        ));
    }
    Ok(bytes::Bytes::from(buffer_vec(data.py(), data)?))
}

/// Pass immutable `bytes` through; snapshot any other bytes-like object once.
pub fn owned_bytes<'py>(data: &Bound<'py, PyAny>) -> PyResult<Bound<'py, pyo3::types::PyBytes>> {
    if let Ok(bytes) = data.cast::<pyo3::types::PyBytes>() {
        return Ok(bytes.clone());
    }
    let py = data.py();
    Ok(pyo3::types::PyBytes::new(py, &buffer_vec(py, data)?))
}

pub fn local_source_error(error: zstar::Error) -> PyErr {
    match error {
        zstar::Error::Io(error) => error.into(),
        other => core_error(other),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
pub fn core_error(error: zstar::Error) -> PyErr {
    core_error_ref(&error)
}

fn core_error_ref(error: &zstar::Error) -> PyErr {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    for _ in 0..64 {
        if let Some(group) = cause.downcast_ref::<zstar::s3::UploadCleanupError>() {
            return exception_group(
                "archive publication and cleanup failed",
                vec![
                    core_error_ref(&group.publication),
                    core_error_ref(&group.cleanup),
                ],
            );
        }
        let io = if let Some(zstar::Error::Io(io)) = cause.downcast_ref::<zstar::Error>() {
            Some(io)
        } else {
            cause.downcast_ref::<std::io::Error>()
        };
        if let Some(inner) = io.and_then(std::io::Error::get_ref) {
            cause = inner;
            continue;
        }
        let Some(inner) = cause.source() else { break };
        cause = inner;
    }
    if let zstar::Error::MissingEntry(path) = error {
        return pyo3::exceptions::PyKeyError::new_err(path.clone());
    }
    let exception = ZstarError::new_err(error.to_string());
    let _ = Python::attach(|py| {
        // The category is independent of human-readable diagnostic text.
        exception.value(py).setattr("category", error.category())?;
        if let Some(found) = python_cause(error) {
            exception.set_cause(py, Some(found.cause.clone_ref(py)));
        }
        Ok::<_, PyErr>(())
    });
    exception
}

pub fn exception_group(message: &'static str, errors: Vec<PyErr>) -> PyErr {
    Python::attach(|py| {
        let errors: Vec<_> = errors
            .into_iter()
            .map(|error| error.into_value(py))
            .collect();
        pyo3::exceptions::PyBaseExceptionGroup::new_err((message, errors))
    })
}

pub fn py_io(error: PyErr) -> std::io::Error {
    crate::support::PyCause::io("Python callback failed", error)
}
/// Enter the process-wide Tokio runtime without holding the interpreter.
/// Sync adapters never create or nest a Python event loop.
pub fn block_on<T: Send + 'static>(
    py: Python<'_>,
    work: impl std::future::Future<Output = PyResult<T>> + Send + 'static,
) -> PyResult<T> {
    let context = py
        .import("contextvars")?
        .call_method0("copy_context")?
        .unbind();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let result = crate::dispatch::scope_context(context, work).await;
        let _ = send.send(result);
    });
    py.detach(move || receive.recv()).map_err(|_| {
        core_error(Error::Io(std::io::Error::other(
            "native operation worker failed",
        )))
    })?
}

/// A blocking job that could not be admitted or joined.
pub struct WorkerFailure(Error);
impl From<WorkerFailure> for PyErr {
    fn from(failure: WorkerFailure) -> Self {
        core_error(failure.0)
    }
}
impl From<WorkerFailure> for Error {
    fn from(failure: WorkerFailure) -> Self {
        failure.0
    }
}
impl From<WorkerFailure> for std::io::Error {
    fn from(failure: WorkerFailure) -> Self {
        failure.0.into()
    }
}

/// Bound admitted blocking jobs, keeping capacity charged until a running job
/// finishes even when its awaiting future is cancelled.
pub async fn blocking<T: Send + 'static, E: From<WorkerFailure> + Send + 'static>(
    capacity: &'static tokio::sync::Semaphore,
    work: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, E> {
    let permit = capacity
        .acquire()
        .await
        .map_err(|_| WorkerFailure(Error::Unsupported("worker pool closed")))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| WorkerFailure(Error::Io(std::io::Error::other("blocking worker failed"))))?
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

#[pyfunction]
pub fn member_name(path: &str) -> PyResult<String> {
    if path.contains(['\0', '\\']) || path.split('/').any(|part| part == "..") {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "member paths must be archive-relative without '..', NUL, or backslashes",
        ));
    }
    Ok(path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/"))
}
