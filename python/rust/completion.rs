//! One retained native terminal operation, independent of Python waiter cancellation.
use crate::support::lock_state;
use pyo3::prelude::*;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

type ResultValue = Result<(), Py<PyAny>>;
pub struct Completion {
    started: Mutex<bool>,
    result: watch::Sender<Option<Arc<ResultValue>>>,
}
impl Default for Completion {
    fn default() -> Self {
        Self {
            started: Mutex::new(false),
            result: watch::channel(None).0,
        }
    }
}
impl Completion {
    pub fn clear(&self) {
        let completed = self.result.borrow().is_some();
        if completed {
            drop(self.result.send_replace(Some(Arc::new(Ok(())))));
        }
    }
    pub fn start<F: Future<Output = PyResult<()>> + Send + 'static>(
        &self,
        py: Python<'_>,
        work: impl FnOnce() -> F,
    ) -> PyResult<()> {
        let context = crate::dispatch::current_context(py)?;
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py).ok();
        let mut started = lock_state(&self.started);
        if *started {
            return Ok(());
        }
        let work = work();
        *started = true;
        drop(started);
        let result = self.result.clone();
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let work = crate::dispatch::scope_context(context, work);
            let output = match locals {
                Some(locals) => pyo3_async_runtimes::tokio::scope(locals, work).await,
                None => work.await,
            };
            let output =
                output.map_err(|error| Python::attach(|py| error.into_value(py).into_any()));
            result.send_replace(Some(Arc::new(output)));
        });
        Ok(())
    }
    pub fn wait(&self) -> impl Future<Output = PyResult<()>> + Send + 'static {
        let mut result = self.result.subscribe();
        async move {
            let value = result
                .wait_for(Option::is_some)
                .await
                .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("cleanup task stopped"))?
                .as_ref()
                .unwrap()
                .clone();
            match value.as_ref() {
                Ok(()) => Ok(()),
                Err(error) => Err(Python::attach(|py| {
                    PyErr::from_value(error.bind(py).clone())
                })),
            }
        }
    }
    pub fn traverse(&self, visit: &pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(result) = self.result.borrow().as_ref()
            && let Err(error) = result.as_ref()
        {
            visit.call(error)?;
        }
        Ok(())
    }
}
