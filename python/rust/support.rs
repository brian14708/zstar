//! Shared Python error, buffer, and callback synchronization rules.
use pyo3::{buffer::PyBuffer, prelude::*, types::PyBytes};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use zstar::Error;
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

/// Lock a mutex guarding long-running work whose partial effects are unknown
/// after a panic. A poisoned guard is reported as an error, never as a panic.
pub fn lock_work<T>(mutex: &std::sync::Mutex<T>) -> zstar::Result<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| zstar::Error::Unsupported("object poisoned by an earlier panic"))
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

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
pub fn filesystem_error(error: Box<dyn std::error::Error + Send + Sync>) -> PyErr {
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

pub fn py_io(error: PyErr) -> std::io::Error {
    crate::support::PyCause::io(error.to_string(), error)
}
/// Copy a bytes-like object returned by a Python callback into `out`,
/// rejecting anything longer than the requested bound.
pub fn copy_bytes_like(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    out: &mut [u8],
    what: &'static str,
) -> PyResult<usize> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        let bytes = bytes.as_bytes();
        if bytes.len() > out.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(what));
        }
        out[..bytes.len()].copy_from_slice(bytes);
        return Ok(bytes.len());
    }
    let buffer = pyo3::buffer::PyBuffer::<u8>::get(value)
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("expected a bytes-like object"))?;
    if buffer.item_count() > out.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(what));
    }
    let length = buffer.item_count();
    buffer.copy_to_slice(py, &mut out[..length])?;
    Ok(length)
}
pub fn write_python(
    object: &Py<PyAny>,
    bytes: &[u8],
    oversized: &'static str,
) -> std::io::Result<usize> {
    Python::attach(|py| -> PyResult<usize> {
        let result = object.call_method1(py, "write", (PyBytes::new(py, bytes),))?;
        if result.is_none(py) {
            return Err(pyo3::exceptions::PyBlockingIOError::new_err(
                "output stream would block",
            ));
        }
        let size = result.extract(py)?;
        if size > bytes.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(oversized));
        }
        Ok(size)
    })
    .map_err(py_io)
}

thread_local! {
    static THREAD_TOKEN: u64 = {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.fetch_add(1, Ordering::Relaxed)
    };
}
fn thread_token() -> u64 {
    THREAD_TOKEN.with(|token| *token)
}

struct ClearOwner<'a>(&'a AtomicU64);
impl Drop for ClearOwner<'_> {
    fn drop(&mut self) {
        self.0.store(0, Ordering::Release);
    }
}

/// Serializes operations on a mutable native object that calls back into
/// Python (sources, resolvers, sinks, spill stores).
///
/// * Other threads wait for the running operation *detached from Python*, so a
///   waiter never blocks the thread the operation's callbacks need.
/// * A callback that re-enters the same object on the operating thread is
///   rejected with `ZstarError` (category `unsupported-operation`) instead of
///   deadlocking or aliasing the mutable reader.
/// * `closed` is an atomic readable at any time without waiting.
pub struct Gate<T> {
    value: Mutex<Option<T>>,
    owner: AtomicU64,
    closed: AtomicBool,
}
impl<T: Send> Gate<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self {
            value: Mutex::new(Some(value)),
            owner: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(crate) fn reject_reentry(&self) -> Result<(), Error> {
        if self.owner.load(Ordering::Acquire) == thread_token() {
            return Err(Error::Unsupported(
                "re-entrant use of an archive from its own callback",
            ));
        }
        Ok(())
    }
    /// Run `work` with exclusive access, detached from the interpreter.
    pub(crate) fn run<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut T) -> Result<R, Error> + Send,
    ) -> PyResult<R> {
        self.run_py(py, |value| work(value).map_err(core_error))
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Serialize the operation through all callbacks."
    )]
    pub(crate) fn run_py<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut T) -> PyResult<R> + Send,
    ) -> PyResult<R> {
        self.reject_reentry().map_err(core_error)?;
        py.detach(|| {
            let mut guard = crate::support::lock_work(&self.value).map_err(core_error)?;
            let value = guard
                .as_mut()
                .ok_or_else(|| core_error(Error::Unsupported("reader closed")))?;
            self.owner.store(thread_token(), Ordering::Release);
            let _clear = ClearOwner(&self.owner);
            work(value)
        })
    }
    /// Mark closed and take the value. Waits (detached) for a running
    /// operation. The caller drops or finishes the value after the lock is
    /// released, so destructors and final callbacks run lock-free.
    pub(crate) fn take(&self, py: Python<'_>) -> PyResult<Option<T>> {
        self.reject_reentry().map_err(core_error)?;
        self.closed.store(true, Ordering::Release);
        Ok(py.detach(|| crate::support::lock_state(&self.value).take()))
    }
    pub(crate) fn close(&self, py: Python<'_>) -> PyResult<()> {
        let value = self.take(py)?;
        py.detach(|| drop(value));
        Ok(())
    }
}

/// Enter the process-wide Tokio runtime without holding the interpreter.
/// Sync adapters never create or nest a Python event loop.
pub fn block_on<T: Send>(
    py: Python<'_>,
    work: impl std::future::Future<Output = PyResult<T>> + Send,
) -> PyResult<T> {
    py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(work))
}

/// Bound admitted blocking jobs, keeping capacity charged until a running job
/// finishes even when its awaiting future is cancelled.
pub async fn blocking<T: Send + 'static>(
    capacity: &'static tokio::sync::Semaphore,
    work: impl FnOnce() -> PyResult<T> + Send + 'static,
) -> PyResult<T> {
    let permit = capacity
        .acquire()
        .await
        .map_err(|_| core_error(zstar::Error::Unsupported("worker pool closed")))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|_| {
        core_error(zstar::Error::Io(std::io::Error::other(
            "blocking callback worker failed",
        )))
    })?
}
