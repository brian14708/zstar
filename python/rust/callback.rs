//! Cancellable Python storage coroutines bound to their originating event loop.
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

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

pub struct AsyncCallback {
    method: Py<PyAny>,
    locals: pyo3_async_runtimes::TaskLocals,
}
impl AsyncCallback {
    pub fn is_async_method(py: Python<'_>, object: &Py<PyAny>, name: &str) -> PyResult<bool> {
        py.import("inspect")?
            .call_method1("iscoroutinefunction", (object.getattr(py, name)?,))?
            .extract()
    }
    /// Sync callbacks keep the existing bounded blocking-worker path.
    pub fn detect(py: Python<'_>, object: &Py<PyAny>, name: &str) -> PyResult<Option<Self>> {
        let method = object.getattr(py, name)?;
        if !Self::is_async_method(py, object, name)? {
            return Ok(None);
        }
        Ok(Some(Self {
            method,
            locals: pyo3_async_runtimes::tokio::get_current_locals(py)?,
        }))
    }
    pub async fn call(&self, args: Py<PyTuple>) -> PyResult<Py<PyAny>> {
        let (future, mut task) = Python::attach(|py| -> PyResult<_> {
            let asyncio = py.import("asyncio")?;
            let context = self.locals.context(py).call_method0("copy")?;
            let mut values = vec![self.method.clone_ref(py)];
            values.extend(args.bind(py).iter().map(Bound::unbind));
            let coroutine = context.call_method1("run", PyTuple::new(py, values)?)?;
            let loop_ = self.locals.event_loop(py);
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
            let task = CancelTask(Some(scheduled.clone().unbind()));
            let kwargs = PyDict::new(py);
            kwargs.set_item("loop", loop_)?;
            let wrapped = asyncio.call_method("wrap_future", (scheduled,), Some(&kwargs))?;
            Ok((
                pyo3_async_runtimes::into_future_with_locals(&self.locals, wrapped)?,
                task,
            ))
        })?;
        let result = future.await?;
        task.0 = None;
        Ok(result)
    }
}
