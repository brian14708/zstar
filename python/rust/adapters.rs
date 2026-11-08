//! Explicit adaptation of borrowed blocking Python protocols to awaitables.
use crate::{
    dispatch::Scope,
    support::{Unsigned, lock_state},
};
use pyo3::{prelude::*, types::PyTuple};
use std::sync::{Arc, Mutex};

#[pyclass(module = "zstar._native", frozen)]
pub struct BlockingAdapter {
    object: Mutex<Option<Py<PyAny>>>,
    scope: Arc<Scope>,
}
impl BlockingAdapter {
    pub fn original(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        lock_state(&self.object)
            .as_ref()
            .map(|object| object.clone_ref(py))
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("adapter cleared"))
    }
    fn invoke<'py>(
        &self,
        py: Python<'py>,
        name: &'static str,
        args: Py<PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.scope.reject(py)?;
        let callback = self.scope.callback(py, self.original(py)?, name)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move { callback.call(args).await })
    }
}
#[pymethods]
impl BlockingAdapter {
    #[new]
    fn new(py: Python<'_>, value: Py<PyAny>) -> PyResult<Self> {
        Ok(Self {
            object: Mutex::new(Some(value)),
            scope: Scope::new(py, false)?,
        })
    }
    fn size<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(py, "size", PyTuple::empty(py).unbind())
    }
    fn read_at<'py>(
        &self,
        py: Python<'py>,
        offset: Unsigned,
        length: Unsigned,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(
            py,
            "read_at",
            (offset.0, length.0).into_pyobject(py)?.unbind(),
        )
    }
    fn read<'py>(&self, py: Python<'py>, size: Unsigned) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(py, "read", (size.0,).into_pyobject(py)?.unbind())
    }
    fn write<'py>(&self, py: Python<'py>, data: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let data = crate::support::owned_bytes(data)?;
        self.invoke(py, "write", (data,).into_pyobject(py)?.unbind())
    }
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(py, "flush", PyTuple::empty(py).unbind())
    }
    fn get_encoded<'py>(
        &self,
        py: Python<'py>,
        address: String,
        hint: Unsigned,
        max_bytes: Unsigned,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(
            py,
            "get_encoded",
            (address, hint.0, max_bytes.0).into_pyobject(py)?.unbind(),
        )
    }
    fn put_encoded<'py>(
        &self,
        py: Python<'py>,
        address: String,
        encoded: &Bound<'_, PyAny>,
        raw_size: Unsigned,
    ) -> PyResult<Bound<'py, PyAny>> {
        let encoded = crate::support::owned_bytes(encoded)?;
        self.invoke(
            py,
            "put_encoded",
            (address, encoded, raw_size.0).into_pyobject(py)?.unbind(),
        )
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        if let Some(object) = lock_state(&self.object).as_ref() {
            visit.call(object)?;
        }
        self.scope.traverse(&visit)
    }
    fn __clear__(&self) {
        let object = lock_state(&self.object).take();
        drop(object);
    }
}
