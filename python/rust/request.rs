//! Bridge async Python request providers onto their originating asyncio loop.
//! A dropped Rust future cancels the thread-safe Python task handle.
//!
//! Event-loop affinity: the provider coroutine always runs on the loop that was
//! running when the archive was opened (`run_coroutine_threadsafe`). Reads may
//! originate from any thread or loop, but that loop must stay running and open
//! for the archive's lifetime; a closed loop surfaces as a `ZstarError` whose
//! `__cause__` is the scheduling failure.
use pyo3::{prelude::*, types::PyDict};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use zstar::http::{
    ExpiringRequest, HttpRangeSource, PresignedRequest, RequestProvider, ReqwestTransport,
};

/// Out-of-band carrier for the last exception raised by a user provider.
///
/// The core crate deliberately replaces provider error text (it can contain
/// signed URLs or credentials) and drops the source chain, so the original
/// exception travels here and is attached as `__cause__` of the redacted
/// `ZstarError`. Best effort: with several concurrent failures the most recent
/// exception wins.
#[derive(Default)]
pub(crate) struct FailureSlot(std::sync::Mutex<Option<PyErr>>);
impl FailureSlot {
    fn record(&self, error: PyErr) {
        let previous = crate::lock_state(&self.0).replace(error);
        drop(previous); // After the guard: dropping may run Python code.
    }
    pub(crate) fn annotate(&self, error: PyErr) -> PyErr {
        Python::attach(|py| {
            if error.is_instance_of::<crate::ZstarError>(py)
                && error
                    .value(py)
                    .to_string()
                    .contains("presigned provider failed")
            {
                let cause = crate::lock_state(&self.0).take();
                if let Some(cause) = cause {
                    error.set_cause(py, Some(cause));
                }
            }
            error
        })
    }
}
fn failed(slot: &FailureSlot, cause: PyErr) -> std::io::Error {
    // The message stays fixed: exception text can contain signed URLs,
    // credentials, and headers.
    slot.record(cause);
    std::io::Error::other("Python request provider failed")
}
struct CancelTask(Option<Py<PyAny>>);
impl Drop for CancelTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            Python::attach(|py| {
                let _ = task.call_method0(py, "cancel");
            });
        }
    }
}
struct PythonProvider {
    failures: Arc<FailureSlot>,
    callback: Py<PyAny>,
    locals: pyo3_async_runtimes::TaskLocals,
}
#[async_trait::async_trait]
impl RequestProvider for PythonProvider {
    async fn request(&self, method: &str, force_refresh: bool) -> std::io::Result<ExpiringRequest> {
        let (future, mut task) = Python::attach(|py| -> PyResult<_> {
            let asyncio = py.import("asyncio")?;
            // Each request gets its own Context: concurrent native reads must
            // never enter the same Python Context on two Tokio workers.
            let context = self.locals.context(py).call_method0("copy")?;
            let loop_ = self.locals.event_loop(py);
            let coroutine =
                context.call_method1("run", (self.callback.bind(py), method, force_refresh))?;
            let scheduled = context
                .call_method1(
                    "run",
                    (
                        asyncio.getattr("run_coroutine_threadsafe")?,
                        &coroutine,
                        &loop_,
                    ),
                )
                .inspect_err(|_| {
                    // A closed event loop must not leave an unawaited coroutine.
                    let _ = coroutine.call_method0("close");
                })?;
            let task = CancelTask(Some(scheduled.clone().unbind()));
            let kwargs = PyDict::new(py);
            kwargs.set_item("loop", loop_)?;
            let wrapped = asyncio.call_method("wrap_future", (scheduled,), Some(&kwargs))?;
            Ok((
                pyo3_async_runtimes::into_future_with_locals(&self.locals, wrapped)?,
                task,
            ))
        })
        .map_err(|e| failed(&self.failures, e))?;
        let value = future.await.map_err(|e| failed(&self.failures, e))?;
        // The callback completed. No cancellation is needed when parsing it.
        task.0 = None;
        Python::attach(|py| -> PyResult<ExpiringRequest> {
            let value = value.bind(py).cast::<PyDict>()?;
            let url = value
                .get_item("url")?
                .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("missing URL"))?
                .extract::<String>()?;
            let headers = value
                .get_item("headers")?
                .filter(|v| !v.is_none())
                .map(|v| v.extract::<BTreeMap<String, String>>())
                .transpose()?
                .unwrap_or_default();
            let expires_at = value
                .get_item("expires_in")?
                .filter(|v| !v.is_none())
                .map(|v| -> PyResult<_> {
                    let seconds = v.extract::<f64>()?;
                    if !seconds.is_finite() || !(0.0..=604800.0).contains(&seconds) {
                        return Err(pyo3::exceptions::PyValueError::new_err("invalid expiry"));
                    }
                    Ok(Instant::now() + Duration::from_secs_f64(seconds))
                })
                .transpose()?;
            Ok(ExpiringRequest {
                request: PresignedRequest {
                    method: method.into(),
                    url,
                    headers: headers.into_iter().collect(),
                },
                expires_at,
            })
        })
        .map_err(|e| failed(&self.failures, e))
    }
}

pub fn source(
    py: Python<'_>,
    method: &str,
    url: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    provider: Option<Py<PyAny>>,
    failures: Arc<FailureSlot>,
) -> PyResult<HttpRangeSource> {
    let transport = Arc::new(ReqwestTransport::new().map_err(|e| super::core_error(e.into()))?);
    match (url, provider) {
        (Some(url), None) => Ok(HttpRangeSource::new(
            transport,
            PresignedRequest {
                method: method.into(),
                url,
                headers: headers.unwrap_or_default().into_iter().collect(),
            },
        )),
        (None, Some(callback)) if headers.is_none() => {
            let inspect = py.import("inspect")?;
            let callable = callback.bind(py);
            let asynchronous = inspect
                .call_method1("iscoroutinefunction", (callable,))?
                .is_truthy()?
                || callable
                    .getattr("__call__")
                    .ok()
                    .map(|call| -> PyResult<bool> {
                        inspect
                            .call_method1("iscoroutinefunction", (call,))?
                            .is_truthy()
                    })
                    .transpose()?
                    .unwrap_or(false);
            if !asynchronous {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "provider must be an async callable",
                ));
            }
            Ok(HttpRangeSource::with_provider(
                transport,
                Arc::new(PythonProvider {
                    failures,
                    callback,
                    locals: pyo3_async_runtimes::tokio::get_current_locals(py)?,
                }),
            ))
        }
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            "pass either url with optional headers or provider",
        )),
    }
}

/// Bounded async presigned PUT. The caller supplies the encoded archive/object.
#[pyfunction]
#[pyo3(signature=(data,url=None,*,headers=None,provider=None))]
pub fn put_presigned<'py>(
    py: Python<'py>,
    data: &[u8],
    url: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    provider: Option<Py<PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    if data.len() > 64 << 20 {
        return Err(super::core_error(zstar::Error::Budget(
            "presigned PUT body",
        )));
    }
    let failures = Arc::new(FailureSlot::default());
    let source = source(py, "PUT", url, headers, provider, failures.clone())?;
    let body = bytes::Bytes::copy_from_slice(data);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        source
            .put(body)
            .await
            .map_err(|e| failures.annotate(super::core_error(e.into())))
    })
}
