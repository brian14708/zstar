//! External-object chunking and Python resolver callbacks.
use crate::support::{PyCause, ZstarError, core_error};
use bytes::Bytes;
use pyo3::{prelude::*, types::PyBytes};
use std::sync::Arc;

/// Encode one external chunk on the bounded codec pool.
pub async fn encode_chunk(
    raw: Bytes,
    level: i32,
    limits: zstar::Limits,
) -> zstar::Result<([u8; 32], Vec<u8>)> {
    static ENCODERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);
    crate::support::blocking(&ENCODERS, move || {
        zstar::backend::encode_object(&raw, level, limits)
    })
    .await
}

/// The extent for a stored whole object.
pub const fn object_extent(
    size: u64,
    digest: [u8; 32],
    encoded_size: u64,
) -> zstar::layout::Extent {
    zstar::layout::Extent {
        size,
        source: zstar::layout::Source::Object(digest),
        encoded_size,
        offset: 0,
    }
}

struct ProtocolResolver(crate::dispatch::Callback);
pub fn protocol_resolver(
    py: Python<'_>,
    object: Py<PyAny>,
    scope: &Arc<crate::dispatch::Scope>,
) -> PyResult<Arc<dyn zstar::ObjectResolver>> {
    Ok(Arc::new(ProtocolResolver(scope.protocol_callback(
        py,
        object,
        "get_encoded",
    )?)))
}
#[async_trait::async_trait]
impl zstar::ObjectResolver for ProtocolResolver {
    async fn get_encoded(
        &self,
        digest: [u8; 32],
        hint: u64,
        max_bytes: u64,
    ) -> zstar::Result<Bytes> {
        let args = Python::attach(|py| {
            (
                zstar::backend::manifest::object_address(&digest),
                hint,
                max_bytes,
            )
                .into_pyobject(py)
                .map(Bound::unbind)
        })
        .map_err(|error| resolver_error(error, digest))?;
        let value = self
            .0
            .call(args)
            .await
            .map_err(|error| resolver_error(error, digest))?;
        Python::attach(|py| {
            let bytes = value.bind(py).cast::<PyBytes>()?;
            if bytes.as_bytes().len() as u64 > max_bytes {
                return Err(core_error(zstar::Error::budget(
                    zstar::BudgetResource::EncodedObjectBytes,
                    "encoded resolver output size",
                )));
            }
            Ok(Bytes::copy_from_slice(bytes.as_bytes()))
        })
        .map_err(|error| resolver_error(error, digest))
    }
}

fn resolver_error(error: PyErr, digest: [u8; 32]) -> zstar::Error {
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
            zstar::Error::budget(
                zstar::BudgetResource::EncodedObjectBytes,
                "encoded resolver output size",
            )
        } else {
            zstar::Error::Io(PyCause::io("Python encoded-object resolver failed", error))
        }
    })
}
