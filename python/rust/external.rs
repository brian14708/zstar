//! External-object chunking and Python resolver callbacks.
use crate::support::{PyCause, ZstarError, core_error};
use bytes::Bytes;
use pyo3::{prelude::*, types::PyBytes};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};
use zstar::chunking::{Chunking, external_file};

struct CallbackWriter {
    callback: Py<PyAny>,
    level: i32,
}

#[async_trait::async_trait]
impl zstar::chunking::ObjectWriter for CallbackWriter {
    async fn put_chunk(&mut self, raw: bytes::Bytes) -> zstar::Result<zstar::Extent> {
        let digest = *zstar::backend::Digest::sha256(&raw).as_bytes();
        let encoded = zstar::backend::encode_frame(&raw, self.level)?;
        let encoded_size = Python::attach(|py| {
            self.callback
                .call1(
                    py,
                    (
                        zstar::backend::manifest::object_address(&digest),
                        PyBytes::new(py, &encoded),
                        raw.len(),
                    ),
                )
                .and_then(|value| value.extract::<u64>(py))
                .map_err(|error| {
                    zstar::Error::Io(PyCause::io("external object upload failed", error))
                })
        })?;
        Ok(zstar::Extent {
            size: raw.len() as u64,
            source: zstar::Source::Object(digest),
            encoded_size,
            offset: 0,
        })
    }
}

pub fn externalize_with_strategy(
    py: Python<'_>,
    fileobj: Py<PyAny>,
    size: u64,
    callback: Py<PyAny>,
    strategy: Chunking,
    compression_level: i32,
) -> PyResult<Vec<zstar::Extent>> {
    py.detach(|| {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut body = Body {
                object: Arc::new(fileobj),
                pending: None,
                buffer: Bytes::new(),
            };
            let mut writer = CallbackWriter {
                callback,
                level: compression_level,
            };
            let extents = external_file(&mut body, size, strategy, &mut writer).await?;
            Ok(extents)
        })
    })
    .map_err(core_error)
}
type ReadJob = Pin<Box<dyn Future<Output = std::io::Result<Bytes>> + Send>>;
struct Body {
    object: Arc<Py<PyAny>>,
    pending: Option<ReadJob>,
    buffer: Bytes,
}
impl AsyncRead for Body {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.buffer.is_empty() {
            if this.pending.is_none() {
                let object = this.object.clone();
                this.pending = Some(Box::pin(async move {
                    static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
                        std::sync::OnceLock::new();
                    let permit = PERMITS
                        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| std::io::Error::other("body reader closed"))?;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        Python::attach(|py| {
                            let value = object.call_method1(py, "read", (64 << 10,))?;
                            let raw = crate::support::buffer_vec(py, value.bind(py))?;
                            if raw.len() > 64 << 10 {
                                return Err(pyo3::exceptions::PyValueError::new_err(
                                    "oversized body read",
                                ));
                            }
                            Ok(Bytes::from(raw))
                        })
                        .map_err(|cause: PyErr| {
                            crate::support::PyCause::io("body read failed", cause)
                        })
                    })
                    .await
                    .map_err(|_| std::io::Error::other("body worker failed"))?
                }));
            }
            match this.pending.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    this.pending = None;
                    this.buffer = result?;
                }
            }
        }
        let n = this.buffer.len().min(out.remaining());
        out.put_slice(&this.buffer.split_to(n));
        Poll::Ready(Ok(()))
    }
}
pub fn sync_binding(py: Python<'_>, object: Py<PyAny>) -> PyResult<Arc<dyn zstar::ObjectResolver>> {
    if crate::callback::AsyncCallback::is_async_method(py, &object, "get_encoded")? {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "use AsyncArchive for async resolvers",
        ));
    }
    binding(py, object)
}
pub fn binding(py: Python<'_>, object: Py<PyAny>) -> PyResult<Arc<dyn zstar::ObjectResolver>> {
    let callback = crate::callback::AsyncCallback::detect(py, &object, "get_encoded")?;
    Ok(Arc::new(PythonResolver {
        object: Arc::new(object),
        callback,
    }))
}

/// Shared callback validation for blocking manifest and async indexed readers.
pub fn resolve_encoded(
    object: &Py<PyAny>,
    digest: [u8; 32],
    hint: u64,
    max_bytes: u64,
) -> zstar::Result<Vec<u8>> {
    Python::attach(|py| -> PyResult<Vec<u8>> {
        let value = object.call_method1(
            py,
            "get_encoded",
            (
                zstar::backend::manifest::object_address(&digest),
                hint,
                max_bytes,
            ),
        )?;
        if value.bind(py).len()? as u64 > max_bytes {
            return Err(core_error(zstar::Error::Budget(
                "encoded resolver output size",
            )));
        }
        value.extract(py)
    })
    .map_err(|error| resolver_error(error, digest))
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
            zstar::Error::Budget("encoded resolver output size")
        } else {
            zstar::Error::Io(PyCause::io("Python encoded-object resolver failed", error))
        }
    })
}

struct PythonResolver {
    object: Arc<Py<PyAny>>,
    callback: Option<crate::callback::AsyncCallback>,
}

#[async_trait::async_trait]
impl zstar::ObjectResolver for PythonResolver {
    #[expect(
        clippy::items_after_statements,
        reason = "Keep local helpers next to the operation that uses them."
    )]
    async fn get_encoded(
        &self,
        digest: [u8; 32],
        hint: u64,
        max_bytes: u64,
    ) -> zstar::Result<bytes::Bytes> {
        if let Some(callback) = &self.callback {
            let args = Python::attach(|py| {
                (
                    zstar::backend::manifest::object_address(&digest),
                    hint,
                    max_bytes,
                )
                    .into_pyobject(py)
                    .map(Bound::unbind)
            })
            .map_err(|e| resolver_error(e, digest))?;
            let value = callback
                .call(args)
                .await
                .map_err(|e| resolver_error(e, digest))?;
            return Python::attach(|py| -> PyResult<Bytes> {
                if value.bind(py).len()? as u64 > max_bytes {
                    return Err(core_error(zstar::Error::Budget(
                        "encoded resolver output size",
                    )));
                }
                value.extract::<Vec<u8>>(py).map(Bytes::from)
            })
            .map_err(|e| resolver_error(e, digest));
        }
        let object = self.object.clone();
        // A cancelled await cannot interrupt Python code already running.
        // Retain a global callback permit until that code actually completes.
        static PERMITS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> =
            std::sync::OnceLock::new();
        let permit = PERMITS
            .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(8)))
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| zstar::Error::Unsupported("resolver closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            resolve_encoded(&object, digest, hint, max_bytes).map(bytes::Bytes::from)
        })
        .await
        .map_err(|_| zstar::Error::Io(std::io::Error::other("Python resolver worker failed")))?
    }
}
