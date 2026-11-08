//! Owned async I/O adapters for explicitly selected Python callback modes.
use crate::{
    dispatch::Callback,
    support::{Unsigned, py_io},
};
use pyo3::{
    prelude::*,
    pybacked::PyBackedBytes,
    types::{PyBytes, PyTuple},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type Job<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send>>;
/// Start a job when the slot is idle, then poll it, clearing the slot when ready.
fn poll_job<T>(
    slot: &mut Option<Job<T>>,
    cx: &mut Context<'_>,
    start: impl FnOnce() -> io::Result<Job<T>>,
) -> Poll<io::Result<T>> {
    let job = match slot {
        Some(job) => job,
        None => slot.insert(start()?),
    };
    let result = std::task::ready!(job.as_mut().poll(cx));
    *slot = None;
    Poll::Ready(result)
}
pub struct Body {
    read: Callback,
    pending: Option<Job<PyBackedBytes>>,
}
impl Body {
    pub fn new(read: Callback) -> Self {
        Self {
            read,
            pending: None,
        }
    }
}
impl AsyncRead for Body {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let size = output.remaining().min(64 << 10);
        let callback = &this.read;
        let bytes = std::task::ready!(poll_job(&mut this.pending, cx, || {
            let callback = callback.clone();
            Ok(Box::pin(async move {
                let args = Python::attach(|py| (size,).into_pyobject(py).map(Bound::unbind))
                    .map_err(py_io)?;
                let result = callback.call(args).await.map_err(py_io)?;
                Python::attach(|py| {
                    let bytes = result.bind(py).cast::<PyBytes>()?;
                    if bytes.as_bytes().len() > size {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "oversized body read",
                        ));
                    }
                    // Borrow the returned object; the read buffer receives the only copy.
                    Ok(PyBackedBytes::from(bytes.clone()))
                })
                .map_err(py_io)
            }))
        }))?;
        if bytes.len() > output.remaining() {
            return Poll::Ready(Err(io::Error::other("body read buffer shrank")));
        }
        output.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}
pub struct Sink {
    write: Callback,
    flush: Callback,
    pending_write: Option<Job<usize>>,
    pending_flush: Option<Job<()>>,
}
impl Sink {
    pub fn new(write: Callback, flush: Callback) -> Self {
        Self {
            write,
            flush,
            pending_write: None,
            pending_flush: None,
        }
    }
}
impl AsyncWrite for Sink {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let size = bytes.len().min(64 << 10);
        let callback = &this.write;
        poll_job(&mut this.pending_write, cx, || {
            let args = Python::attach(|py| {
                PyTuple::new(py, [PyBytes::new(py, &bytes[..size])]).map(Bound::unbind)
            })
            .map_err(py_io)?;
            let callback = callback.clone();
            Ok(Box::pin(async move {
                let result = callback.call(args).await.map_err(py_io)?;
                let count = Python::attach(|py| result.bind(py).extract::<Unsigned>())
                    .map_err(py_io)?
                    .0;
                if count == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "sink made no progress",
                    ));
                }
                if count > size as u64 {
                    return Err(io::Error::other("oversized sink write"));
                }
                Ok(usize::try_from(count).unwrap())
            }))
        })
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let callback = &this.flush;
        poll_job(&mut this.pending_flush, cx, || {
            let args = Python::attach(|py| PyTuple::empty(py).unbind());
            let callback = callback.clone();
            Ok(Box::pin(async move {
                let result = callback.call(args).await.map_err(py_io)?;
                Python::attach(|py| {
                    if result.is_none(py) {
                        Ok(())
                    } else {
                        Err(io::Error::other("flush must return None"))
                    }
                })
            }))
        })
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
