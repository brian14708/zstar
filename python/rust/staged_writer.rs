//! Filesystem writer policy shared by synchronous and asynchronous adapters.
//! Python supplies temporary streams and backend publication; native state owns
//! member admission, parent creation, serialization, and failure propagation.
use pyo3::{exceptions, prelude::*};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use zstar::{Entry, EntryKind};

use crate::{
    support::{Gate, core_error},
    writer::EntryArchiveWriter,
};

#[pyfunction]
pub fn member_name(path: &str) -> PyResult<String> {
    if path.contains(['\0', '\\']) || path.split('/').any(|part| part == "..") {
        return Err(exceptions::PyValueError::new_err(
            "member paths must be archive-relative without '..', NUL, or backslashes",
        ));
    }
    Ok(path
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect::<Vec<_>>()
        .join("/"))
}
fn metadata(path: String, kind: EntryKind) -> Entry {
    Entry {
        path,
        kind,
        mode: if kind == EntryKind::Directory {
            0o755
        } else {
            0o644
        },
        uid: 0,
        gid: 0,
        mtime_ns: 0,
        extents: vec![],
        linkname: None,
        attributes: BTreeMap::new(),
    }
}
struct Active {
    name: String,
    body: Py<PyAny>,
    closed: Arc<AtomicBool>,
}
struct State {
    writer: Py<EntryArchiveWriter>,
    spool: Py<PyAny>,
    external: Option<Py<PyAny>>,
    threshold: u64,
    compression_level: i32,
    entries: BTreeMap<String, (EntryKind, u64)>,
    active: Option<Active>,
    failed: bool,
}
impl State {
    fn check(&self) -> PyResult<()> {
        if self.failed {
            Err(exceptions::PyValueError::new_err(
                "archive writer failed; discard this filesystem",
            ))
        } else {
            Ok(())
        }
    }
    fn add(&mut self, mut entry: Entry, body: Option<Py<PyAny>>, size: u64) -> PyResult<()> {
        let path = entry.path.clone();
        let kind = entry.kind;
        let result = Python::attach(|py| {
            if let Some(callback) = &self.external
                && entry.kind == EntryKind::File
                && size > 0
                && size >= self.threshold
            {
                let external = super::external::externalize_with_strategy(
                    py,
                    body.ok_or_else(|| {
                        exceptions::PyValueError::new_err("nonempty file requires a body")
                    })?,
                    size,
                    callback.clone_ref(py),
                    zstar::chunking::Chunking::FastCdc {
                        min: 2 << 20,
                        target: 8 << 20,
                        max: 16 << 20,
                    },
                    self.compression_level,
                )?;
                entry.extents = external;
                self.writer.get().add_entry(py, entry, None, 0)
            } else {
                self.writer.get().add_entry(py, entry, body, size)
            }
        });
        if result.is_err() {
            self.failed = true;
        }
        result?;
        self.entries.insert(path, (kind, size));
        Ok(())
    }
    fn mkdir(&mut self, path: &str, parents: bool, exist_ok: bool) -> PyResult<()> {
        self.check()?;
        if path.is_empty() {
            return if exist_ok {
                Ok(())
            } else {
                Err(exceptions::PyFileExistsError::new_err(path.to_owned()))
            };
        }
        if let Some((kind, _)) = self.entries.get(path) {
            return if *kind == EntryKind::Directory && exist_ok {
                Ok(())
            } else {
                Err(exceptions::PyFileExistsError::new_err(path.to_owned()))
            };
        }
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.name == path)
        {
            return Err(exceptions::PyFileExistsError::new_err(path.to_owned()));
        }
        if let Some((parent, _)) = path.rsplit_once('/') {
            if let Some((kind, _)) = self.entries.get(parent) {
                if *kind != EntryKind::Directory {
                    return Err(exceptions::PyNotADirectoryError::new_err(parent.to_owned()));
                }
            } else if parents {
                // Create ancestors from the root, keeping stack usage constant
                // even for an adversarial path with many components.
                for ancestor in parent
                    .match_indices('/')
                    .map(|(end, _)| &parent[..end])
                    .chain(std::iter::once(parent))
                {
                    self.mkdir(ancestor, false, true)?;
                }
            } else {
                return Err(exceptions::PyFileNotFoundError::new_err(parent.to_owned()));
            }
        }
        self.add(metadata(path.to_owned(), EntryKind::Directory), None, 0)
    }
    fn prepare(&mut self, path: &str) -> PyResult<()> {
        self.check()?;
        if path.is_empty() {
            return Err(exceptions::PyIsADirectoryError::new_err(path.to_owned()));
        }
        if self.active.is_some() {
            return Err(exceptions::PyRuntimeError::new_err(
                "close the active member before opening another",
            ));
        }
        if let Some((kind, _)) = self.entries.get(path) {
            return Err(if *kind == EntryKind::Directory {
                exceptions::PyIsADirectoryError::new_err(path.to_owned())
            } else {
                exceptions::PyFileExistsError::new_err(path.to_owned())
            });
        }
        if let Some((parent, _)) = path.rsplit_once('/') {
            if self
                .entries
                .get(parent)
                .is_some_and(|(kind, _)| *kind != EntryKind::Directory)
            {
                return Err(exceptions::PyNotADirectoryError::new_err(parent.to_owned()));
            }
            self.mkdir(parent, true, true)?;
        }
        Ok(())
    }
    fn finish_member(&mut self, abort: bool) -> PyResult<()> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let result = self.check().and_then(|()| {
            if abort {
                self.failed = true;
                return Ok(());
            }
            Python::attach(|py| {
                let size = active.body.call_method1(py, "seek", (0, 2))?.extract(py)?;
                active.body.call_method1(py, "seek", (0,))?;
                self.add(
                    metadata(active.name.clone(), EntryKind::File),
                    Some(active.body.clone_ref(py)),
                    size,
                )
            })
        });
        if result.is_err() {
            self.failed = true;
        }
        active.closed.store(true, Ordering::Release);
        let cleanup = Python::attach(|py| active.body.call_method0(py, "close").map(|_| ()));
        if cleanup.is_err() {
            self.failed = true;
        }
        combine(result, cleanup)
    }
}
fn combine(result: PyResult<()>, cleanup: PyResult<()>) -> PyResult<()> {
    match (result, cleanup) {
        (Err(primary), Err(cleanup)) => Python::attach(|py| {
            let errors = vec![
                primary.value(py).clone().into_any(),
                cleanup.value(py).clone().into_any(),
            ];
            let group = py
                .import("builtins")?
                .getattr("BaseExceptionGroup")?
                .call1(("member cleanup failed", errors))?;
            Err(PyErr::from_value(group))
        }),
        (Err(error), _) | (_, Err(error)) => Err(error),
        _ => Ok(()),
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct StagedWriter {
    inner: Arc<Gate<State>>,
}
#[pymethods]
impl StagedWriter {
    #[new]
    #[pyo3(signature = (writer, spool, external=None, external_threshold=16 << 20, compression_level=3))]
    fn new(
        writer: Py<EntryArchiveWriter>,
        spool: Py<PyAny>,
        external: Option<Py<PyAny>>,
        external_threshold: u64,
        compression_level: i32,
    ) -> Self {
        Self {
            inner: Arc::new(Gate::new(State {
                writer,
                spool,
                external,
                threshold: external_threshold,
                compression_level,
                entries: BTreeMap::new(),
                active: None,
                failed: false,
            })),
        }
    }
    #[pyo3(signature = (path, parents=true, exist_ok=false))]
    fn mkdir(&self, py: Python<'_>, path: &str, parents: bool, exist_ok: bool) -> PyResult<()> {
        let path = member_name(path)?;
        self.inner
            .run_py(py, |state| state.mkdir(&path, parents, exist_ok))
    }
    fn open(&self, py: Python<'_>, path: &str) -> PyResult<StagedMember> {
        let name = member_name(path)?;
        let closed = self.inner.run_py(py, |state| {
            state.prepare(&name)?;
            let body = Python::attach(|py| state.spool.call0(py))?;
            let closed = Arc::new(AtomicBool::new(false));
            state.active = Some(Active {
                name: name.clone(),
                body,
                closed: closed.clone(),
            });
            Ok(closed)
        })?;
        Ok(StagedMember {
            inner: self.inner.clone(),
            name,
            closed,
        })
    }
    fn add_file(&self, py: Python<'_>, path: &str, body: Py<PyAny>, size: u64) -> PyResult<()> {
        let path = member_name(path)?;
        self.inner.run_py(py, |state| {
            state.prepare(&path)?;
            state.add(metadata(path, EntryKind::File), Some(body), size)
        })
    }
    fn snapshot(&self, py: Python<'_>) -> PyResult<Vec<(Py<PyAny>, u64)>> {
        self.inner.run_py(py, |state| {
            state.check()?;
            Python::attach(|py| {
                state
                    .entries
                    .iter()
                    .map(|(path, (kind, size))| {
                        Ok((
                            super::manifest::to_py(py, &metadata(path.clone(), *kind).to_value())?,
                            *size,
                        ))
                    })
                    .collect()
            })
        })
    }
    #[pyo3(signature = (abort=false))]
    fn finish(&self, py: Python<'_>, abort: bool) -> PyResult<(bool, Vec<Py<PyAny>>)> {
        let Some(mut state) = self.inner.take(py)? else {
            return Ok((abort, Vec::new()));
        };
        py.detach(|| {
            let mut errors = Vec::new();
            if let Some(active) = state.active.take() {
                active.closed.store(true, Ordering::Release);
                if let Err(error) = Python::attach(|py| active.body.call_method0(py, "close")) {
                    errors.push(error);
                }
                if !abort {
                    errors.push(exceptions::PyRuntimeError::new_err(
                        "archive closed with an open writable member",
                    ));
                }
            }
            if state.failed && !abort && errors.is_empty() {
                errors.push(exceptions::PyValueError::new_err("archive writer failed"));
            }
            let mut abort = abort || state.failed || !errors.is_empty();
            let result = Python::attach(|py| {
                if abort {
                    state.writer.get().abort(py)
                } else {
                    state.writer.get().close(py)
                }
            });
            if let Err(error) = result {
                errors.push(error);
                abort = true;
            }
            Python::attach(|py| {
                Ok((
                    abort,
                    errors
                        .into_iter()
                        .map(|error| error.value(py).clone().into_any().unbind())
                        .collect(),
                ))
            })
        })
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct StagedMember {
    inner: Arc<Gate<State>>,
    #[pyo3(get)]
    name: String,
    closed: Arc<AtomicBool>,
}
impl StagedMember {
    fn with_body<R: Send>(
        &self,
        py: Python<'_>,
        work: impl FnOnce(Python<'_>, &Py<PyAny>) -> PyResult<R> + Send,
    ) -> PyResult<R> {
        self.inner.run_py(py, |state| {
            state.check()?;
            if self.closed.load(Ordering::Acquire) {
                return Err(exceptions::PyValueError::new_err(
                    "I/O operation on closed file",
                ));
            }
            let active = state
                .active
                .as_ref()
                .ok_or_else(|| core_error(zstar::Error::Unsupported("no active member")))?;
            Python::attach(|py| work(py, &active.body))
        })
    }
}
#[pymethods]
impl StagedMember {
    #[getter]
    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.inner.is_closed()
    }
    fn write(&self, py: Python<'_>, data: Py<PyAny>) -> PyResult<usize> {
        self.with_body(py, |py, body| {
            body.call_method1(py, "write", (data,))?.extract(py)
        })
    }
    #[pyo3(signature = (offset, whence=0))]
    fn seek(&self, py: Python<'_>, offset: i64, whence: i32) -> PyResult<u64> {
        self.with_body(py, |py, body| {
            body.call_method1(py, "seek", (offset, whence))?.extract(py)
        })
    }
    fn tell(&self, py: Python<'_>) -> PyResult<u64> {
        self.with_body(py, |py, body| body.call_method0(py, "tell")?.extract(py))
    }
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.with_body(py, |py, body| body.call_method0(py, "flush").map(|_| ()))
    }
    #[pyo3(signature = (abort=false))]
    fn close(&self, py: Python<'_>, abort: bool) -> PyResult<()> {
        if self.closed() {
            return Ok(());
        }
        self.inner.run_py(py, |state| {
            if self.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            state.finish_member(abort)
        })
    }
}
