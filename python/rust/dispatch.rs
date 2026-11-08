//! Explicit callback modes, operation context, bounded jobs, and GC roots.
use crate::support::{core_error, lock_state};
use pyo3::{
    PyTraverseError, PyVisit,
    prelude::*,
    types::{PyDict, PyTuple},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore};

/// Counts admitted work so closers can wait until none remains. Guards owning
/// the counter's holder call [`JobCounter::exit`] when dropped.
#[derive(Default)]
pub struct JobCounter {
    active: AtomicUsize,
    drained: Notify,
}
impl JobCounter {
    pub fn enter(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }
    pub fn exit(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
    pub async fn drain(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                break;
            }
            drained.await;
        }
    }
}

/// `zstar._callbacks` entry points, resolved once rather than per callback.
pub struct Callbacks {
    pub invoke: Py<PyAny>,
    pub ainvoke: Py<PyAny>,
    pub reject: Py<PyAny>,
    in_blocking_callback: Py<PyAny>,
}
pub fn callbacks(py: Python<'_>) -> PyResult<&'static Callbacks> {
    static CALLBACKS: pyo3::sync::PyOnceLock<Callbacks> = pyo3::sync::PyOnceLock::new();
    CALLBACKS.get_or_try_init(py, || {
        let module = py.import("zstar._callbacks")?;
        Ok(Callbacks {
            invoke: module.getattr("invoke")?.unbind(),
            ainvoke: module.getattr("ainvoke")?.unbind(),
            reject: module.getattr("reject")?.unbind(),
            in_blocking_callback: module.getattr("in_blocking_callback")?.unbind(),
        })
    })
}

pub enum Mode {
    Sync,
    Async(Py<PyAny>),
}
pub struct Scope {
    pub id: u64,
    mode: Mode,
    roots: Mutex<Vec<Weak<Callable>>>,
    jobs: JobCounter,
}
struct Callable {
    object: Py<PyAny>,
    gate: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct Callback {
    scope: Arc<Scope>,
    callable: Arc<Callable>,
    method: &'static str,
    blocking: bool,
}
fn object_gate(object: &Py<PyAny>) -> Arc<Semaphore> {
    static GATES: OnceLock<Mutex<HashMap<usize, Weak<Semaphore>>>> = OnceLock::new();
    let mut gates = lock_state(GATES.get_or_init(Mutex::default));
    let key = object.as_ptr() as usize;
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return gate;
    }
    gates.retain(|_, gate| gate.strong_count() != 0);
    let gate = Arc::new(Semaphore::new(1));
    gates.insert(key, Arc::downgrade(&gate));
    gate
}
impl Scope {
    pub fn new(py: Python<'_>, asynchronous: bool) -> PyResult<Arc<Self>> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mode = if asynchronous {
            Mode::Async(
                pyo3_async_runtimes::tokio::get_current_locals(py)?
                    .event_loop(py)
                    .unbind(),
            )
        } else {
            Mode::Sync
        };
        Ok(Arc::new(Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            mode,
            roots: Mutex::new(Vec::new()),
            jobs: JobCounter::default(),
        }))
    }
    pub fn reject(&self, py: Python<'_>) -> PyResult<()> {
        let objects: Vec<_> = lock_state(&self.roots)
            .iter()
            .filter_map(Weak::upgrade)
            .map(|root| root.object.as_ptr() as usize)
            .collect();
        callbacks(py)?.reject.call1(py, (self.id, objects))?;
        if let Mode::Async(loop_) = &self.mode {
            let current = py.import("asyncio")?.call_method0("get_running_loop")?;
            if !current.is(loop_.bind(py)) {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "object belongs to another event loop",
                ));
            }
        }
        Ok(())
    }
    pub fn callback(
        self: &Arc<Self>,
        py: Python<'_>,
        object: Py<PyAny>,
        method: &'static str,
    ) -> PyResult<Callback> {
        callbacks(py)?
            .reject
            .call1(py, (self.id, vec![object.as_ptr() as usize]))?;
        if !object.getattr(py, method)?.bind(py).is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "{method} must be callable"
            )));
        }
        let mut roots = lock_state(&self.roots);
        roots.retain(|root| root.strong_count() != 0);
        let callable = roots
            .iter()
            .filter_map(Weak::upgrade)
            .find(|root| root.object.as_ptr() == object.as_ptr())
            .unwrap_or_else(|| {
                let root = Arc::new(Callable {
                    gate: object_gate(&object),
                    object,
                });
                roots.push(Arc::downgrade(&root));
                root
            });
        drop(roots);
        Ok(Callback {
            scope: self.clone(),
            callable,
            method,
            blocking: false,
        })
    }
    pub fn protocol_callback(
        self: &Arc<Self>,
        py: Python<'_>,
        object: Py<PyAny>,
        method: &'static str,
    ) -> PyResult<Callback> {
        if let Ok(adapter) = object
            .bind(py)
            .extract::<PyRef<'_, crate::adapters::BlockingAdapter>>()
        {
            let original = adapter.original(py)?;
            let mut callback = self.callback(py, original, method)?;
            callback.blocking = true;
            return Ok(callback);
        }
        self.callback(py, object, method)
    }
    pub fn job(self: &Arc<Self>) -> Job {
        self.jobs.enter();
        Job(self.clone())
    }
    pub async fn drain(&self) {
        self.jobs.drain().await;
    }
    pub fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        let roots: Vec<_> = lock_state(&self.roots)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for root in roots {
            visit.call(&root.object)?;
        }
        if let Mode::Async(loop_) = &self.mode {
            visit.call(loop_)?;
        }
        Ok(())
    }
}
pub struct Job(Arc<Scope>);
impl Drop for Job {
    fn drop(&mut self) {
        self.0.jobs.exit();
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct CallbackTicket {
    _job: Job,
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CallbackTicket>()?;
    Ok(())
}
struct Cancel(Option<(Py<PyAny>, Py<PyAny>)>);
impl Drop for Cancel {
    fn drop(&mut self) {
        if let Some((loop_, future)) = self.0.take() {
            Python::attach(|py| {
                if let Ok(cancel) = future.getattr(py, "cancel") {
                    let _ = loop_.call_method1(py, "call_soon_threadsafe", (cancel,));
                }
            });
        }
    }
}
impl Callback {
    /// Capture the caller's context and reject reentry from inside it.
    fn checked_context(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let context = current_context(py)?;
        context.call_method1(
            py,
            "run",
            (
                &callbacks(py)?.reject,
                self.scope.id,
                vec![self.callable.object.as_ptr() as usize],
            ),
        )?;
        Ok(context)
    }
    pub async fn call(&self, args: Py<PyTuple>) -> PyResult<Py<PyAny>> {
        let blocking = self.blocking || matches!(self.scope.mode, Mode::Sync);
        // One GIL acquisition covers the reentry check and nesting probe.
        let (context, nested) = Python::attach(|py| -> PyResult<_> {
            let context = self.checked_context(py)?;
            let nested = blocking && in_blocking_callback(py, &context)?;
            Ok((context, nested))
        })?;
        match (blocking, &self.scope.mode) {
            (true, _) => {
                static CAPACITY: Semaphore = Semaphore::const_new(8);
                // Acquire the per-object permit first, so a backlog for one
                // object cannot monopolize the entire blocking callback pool.
                let gate = self
                    .callable
                    .gate
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| core_error(zstar::Error::Unsupported("callback closed")))?;
                // Nested callbacks use their outer callback's admission. That
                // worker may be waiting for this callback through a sync API.
                let capacity = if nested {
                    None
                } else {
                    Some(CAPACITY.acquire().await.map_err(|_| {
                        core_error(zstar::Error::Unsupported("callback pool closed"))
                    })?)
                };
                let callable = self.callable.clone();
                let method = self.method;
                let token = self.scope.id;
                let job = self.scope.job();
                tokio::task::spawn_blocking(move || {
                    let (_gate, _capacity, _job) = (gate, capacity, job);
                    Python::attach(|py| invoke(py, &context, token, &callable, method, args))
                })
                .await
                .map_err(|_| {
                    core_error(zstar::Error::Io(std::io::Error::other(
                        "callback worker failed",
                    )))
                })?
            }
            (false, Mode::Sync) => unreachable!("sync scopes always block"),
            (false, Mode::Async(owner)) => {
                let (future, mut cancel) = Python::attach(|py| -> PyResult<_> {
                    let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
                    let loop_ = owner.bind(py);
                    if !locals.event_loop(py).is(loop_) {
                        return Err(pyo3::exceptions::PyRuntimeError::new_err(
                            "callback belongs to another event loop",
                        ));
                    }
                    let context = locals.context(py).call_method0("copy")?;
                    let ticket = CallbackTicket {
                        _job: self.scope.job(),
                    };
                    let coroutine = callbacks(py)?.ainvoke.bind(py).call1((
                        self.scope.id,
                        self.callable.object.as_ptr() as usize,
                        self.callable.object.getattr(py, self.method)?,
                        args,
                        ticket,
                    ))?;
                    let asyncio = py.import("asyncio")?;
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
                            let _ = coroutine.call_method0("close");
                        })?;
                    let cancel = Cancel(Some((owner.clone_ref(py), scheduled.clone().unbind())));
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("loop", loop_)?;
                    let wrapped =
                        asyncio.call_method("wrap_future", (scheduled,), Some(&kwargs))?;
                    Ok((
                        pyo3_async_runtimes::into_future_with_locals(&locals, wrapped)?,
                        cancel,
                    ))
                })?;
                let value = future.await?;
                cancel.0 = None;
                Ok(value)
            }
        }
    }
}

tokio::task_local! { static SYNC_CONTEXT: Py<PyAny>; }
fn in_blocking_callback(py: Python<'_>, context: &Py<PyAny>) -> PyResult<bool> {
    context
        .call_method1(py, "run", (&callbacks(py)?.in_blocking_callback,))?
        .extract::<bool>(py)
}
fn invoke(
    py: Python<'_>,
    context: &Py<PyAny>,
    token: u64,
    callable: &Callable,
    method: &str,
    args: Py<PyTuple>,
) -> PyResult<Py<PyAny>> {
    context.call_method1(
        py,
        "run",
        (
            &callbacks(py)?.invoke,
            token,
            callable.object.as_ptr() as usize,
            callable.object.getattr(py, method)?,
            args,
        ),
    )
}
pub async fn scope_context<T>(context: Py<PyAny>, work: impl std::future::Future<Output = T>) -> T {
    SYNC_CONTEXT.scope(context, work).await
}
pub fn current_context(py: Python<'_>) -> PyResult<Py<PyAny>> {
    if let Ok(context) = SYNC_CONTEXT.try_with(|context| context.clone_ref(py)) {
        return context.call_method0(py, "copy");
    }
    if let Ok(locals) = pyo3_async_runtimes::tokio::get_current_locals(py) {
        return Ok(locals.context(py).call_method0("copy")?.unbind());
    }
    Ok(py
        .import("contextvars")?
        .call_method0("copy_context")?
        .unbind())
}
