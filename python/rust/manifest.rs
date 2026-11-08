use crate::support::core_error;
use ciborium::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyList, PyString, PyTuple};

pub fn from_py(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > 64 {
        return Err(core_error(zstar::Error::budget(
            zstar::BudgetResource::CborItems,
            "CBOR nesting limit",
        )));
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(value.extract()?));
    }
    if value.is_instance_of::<PyInt>() {
        let integer: i128 = value.extract()?;
        let integer = ciborium::value::Integer::try_from(integer).map_err(|_| {
            pyo3::exceptions::PyOverflowError::new_err("integer is outside CBOR range")
        })?;
        return Ok(Value::Integer(integer));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(Value::Text(value.extract()?));
    }
    if value.is_instance_of::<PyBytes>() {
        return Ok(Value::Bytes(value.extract()?));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let mut map = Vec::new();
        for (key, val) in dict.iter() {
            map.push((Value::Text(key.extract()?), from_py(&val, depth + 1)?));
        }
        return Ok(Value::Map(map));
    }
    if value.is_instance_of::<PyTuple>() || value.is_instance_of::<PyList>() {
        let mut values = Vec::new();
        for item in value.try_iter()? {
            values.push(from_py(&item?, depth + 1)?);
        }
        return Ok(Value::Array(values));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected restricted CBOR value",
    ))
}
pub fn to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Integer(n) => i128::from(*n).into_pyobject(py)?.into_any().unbind(),
        Value::Bool(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Text(s) => s.into_pyobject(py)?.into_any().unbind(),
        Value::Bytes(b) => PyBytes::new(py, b).into_any().unbind(),
        Value::Array(values) => PyTuple::new(
            py,
            values
                .iter()
                .map(|v| to_py(py, v))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any()
        .unbind(),
        Value::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(to_py(py, key)?, to_py(py, value)?)?;
            }
            dict.into_any().unbind()
        }
        _ => {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "unsupported CBOR value",
            ));
        }
    })
}

pub fn extensions_to_py(py: Python<'_>, extensions: &zstar::Extensions) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (key, value) in extensions {
        dict.set_item(key, to_py(py, value)?)?;
    }
    Ok(dict.into_any().unbind())
}
