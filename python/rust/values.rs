//! Owned immutable metadata values; physical extents stay in Rust until indexed.
use crate::manifest::{extensions_to_py, from_py};
use crate::support::{Unsigned, core_error};
use pyo3::{
    exceptions::PyIndexError,
    prelude::*,
    types::{PyDict, PySlice, PyTuple},
};
use std::sync::Arc;
use zstar::EntryKind;
use zstar::layout::{Entry, Extent, Source};

#[derive(Clone)]
#[pyclass(name = "Entry", module = "zstar", frozen)]
pub struct EntryValue {
    pub inner: Arc<Entry>,
    size: u64,
}
impl EntryValue {
    pub fn new(entry: Entry) -> PyResult<Self> {
        let size = entry.size().map_err(core_error)?;
        Ok(Self {
            inner: Arc::new(entry),
            size,
        })
    }
}
fn kind(py: Python<'_>, value: EntryKind) -> PyResult<Py<PyAny>> {
    // Enum members are singletons; resolve them once per interpreter.
    static KINDS: pyo3::sync::PyOnceLock<[Py<PyAny>; 3]> = pyo3::sync::PyOnceLock::new();
    let kinds = KINDS.get_or_try_init(py, || -> PyResult<_> {
        let kind = py.import("zstar._metadata")?.getattr("EntryKind")?;
        let member = |name: &str| kind.call1((name,)).map(Bound::unbind);
        Ok([member("file")?, member("directory")?, member("symlink")?])
    })?;
    let index = match value {
        EntryKind::File => 0,
        EntryKind::Directory => 1,
        EntryKind::Symlink => 2,
    };
    Ok(kinds[index].clone_ref(py))
}
#[pymethods]
impl EntryValue {
    #[getter]
    fn path(&self) -> &str {
        &self.inner.path
    }
    #[getter]
    fn kind(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        kind(py, self.inner.kind)
    }
    #[getter]
    const fn size(&self) -> u64 {
        self.size
    }
    #[getter]
    fn mode(&self) -> u64 {
        self.inner.mode
    }
    #[getter]
    fn uid(&self) -> u64 {
        self.inner.uid
    }
    #[getter]
    fn gid(&self) -> u64 {
        self.inner.gid
    }
    #[getter]
    fn mtime_ns(&self) -> i64 {
        self.inner.mtime_ns
    }
    #[getter]
    fn linkname(&self) -> Option<&str> {
        self.inner.linkname.as_deref()
    }
    #[getter]
    fn extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        extensions_to_py(py, &self.inner.attributes)
    }
    #[getter]
    fn is_external(&self) -> bool {
        self.inner.is_external()
    }
    #[getter]
    fn extents(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = Extents {
            entry: self.inner.clone(),
        };
        Ok(py
            .import("zstar._metadata")?
            .getattr("_Extents")?
            .call1((values,))?
            .unbind())
    }
    fn to_header(&self) -> HeaderValue {
        let entry = Entry {
            path: self.inner.path.clone(),
            kind: self.inner.kind,
            mode: self.inner.mode,
            uid: self.inner.uid,
            gid: self.inner.gid,
            mtime_ns: self.inner.mtime_ns,
            extents: Vec::new(),
            linkname: self.inner.linkname.clone(),
            attributes: self.inner.attributes.clone(),
        };
        HeaderValue {
            entry,
            size: self.size,
        }
    }
    fn __repr__(&self) -> String {
        format!("Entry(path={:?}, size={})", self.inner.path, self.size)
    }
}

#[derive(Clone)]
#[pyclass(name = "Extent", module = "zstar", frozen)]
pub struct ExtentValue(Extent);
#[pymethods]
impl ExtentValue {
    #[getter]
    const fn size(&self) -> u64 {
        self.0.size
    }
    #[getter]
    const fn encoded_size(&self) -> u64 {
        self.0.encoded_size
    }
    #[getter]
    const fn offset(&self) -> u64 {
        self.0.offset
    }
    #[getter]
    fn source(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self.0.source {
            Source::Archive(offset) => offset.into_pyobject(py)?.into_any().unbind(),
            Source::Object(hash) => zstar::backend::manifest::object_address(&hash)
                .into_pyobject(py)?
                .into_any()
                .unbind(),
        })
    }
    fn __repr__(&self) -> String {
        format!(
            "Extent(size={}, encoded_size={}, offset={})",
            self.0.size, self.0.encoded_size, self.0.offset
        )
    }
}

#[pyclass(module = "zstar._native", frozen, sequence)]
pub struct Extents {
    entry: Arc<Entry>,
}
#[pymethods]
impl Extents {
    fn __len__(&self) -> usize {
        self.entry.extents.len()
    }
    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(slice) = index.cast::<PySlice>() {
            let count = isize::try_from(self.entry.extents.len())
                .map_err(|_| PyIndexError::new_err("extent count"))?;
            let indices = slice.indices(count)?;
            let mut at = indices.start;
            let values = (0..indices.slicelength)
                .map(|i| {
                    let index =
                        usize::try_from(at).map_err(|_| PyIndexError::new_err("extent index"))?;
                    if i + 1 < indices.slicelength {
                        at = at
                            .checked_add(indices.step)
                            .ok_or_else(|| PyIndexError::new_err("extent index"))?;
                    }
                    Py::new(py, ExtentValue(self.entry.extents[index].clone()))
                })
                .collect::<PyResult<Vec<_>>>()?;
            return Ok(PyTuple::new(py, values)?.into_any().unbind());
        }
        let index: isize = index.extract()?;
        let count = isize::try_from(self.entry.extents.len())
            .map_err(|_| PyIndexError::new_err("extent count"))?;
        let index = if index < 0 {
            index.checked_add(count)
        } else {
            Some(index)
        };
        let index = index.and_then(|index| usize::try_from(index).ok());
        let extent = index
            .and_then(|index| self.entry.extents.get(index))
            .ok_or_else(|| PyIndexError::new_err("extent index out of range"))?;
        Ok(Py::new(py, ExtentValue(extent.clone()))?.into_any())
    }
}

#[derive(Clone)]
#[pyclass(name = "Header", module = "zstar", frozen)]
pub struct HeaderValue {
    pub entry: Entry,
    pub size: u64,
}
impl HeaderValue {
    fn create(
        mut entry: Entry,
        size: u64,
        mode: u64,
        uid: u64,
        gid: u64,
        mtime_ns: i64,
        extensions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        entry.mode = mode;
        entry.uid = uid;
        entry.gid = gid;
        entry.mtime_ns = mtime_ns;
        entry.attributes = extensions
            .into_iter()
            .flat_map(PyDictMethods::iter)
            .map(|(key, value)| Ok((key.extract()?, from_py(&value, 1)?)))
            .collect::<PyResult<_>>()?;
        entry.validate(u64::MAX).map_err(core_error)?;
        Ok(Self { entry, size })
    }
    pub fn native_header(&self) -> zstar::Header {
        let mut header = match self.entry.kind {
            EntryKind::File => zstar::Header::file(&self.entry.path, self.size),
            EntryKind::Directory => zstar::Header::directory(&self.entry.path),
            EntryKind::Symlink => zstar::Header::symlink(
                &self.entry.path,
                self.entry.linkname.as_deref().unwrap_or_default(),
            ),
        };
        header.set_mode(self.entry.mode);
        header.set_uid(self.entry.uid);
        header.set_gid(self.entry.gid);
        header.set_mtime_ns(self.entry.mtime_ns);
        header.set_attributes(self.entry.attributes.clone());
        header
    }
}
#[pymethods]
impl HeaderValue {
    #[staticmethod]
    #[pyo3(signature = (path, size, *, mode = Unsigned(0o644), uid = Unsigned(0), gid = Unsigned(0), mtime_ns = 0, extensions = None))]
    fn file(
        path: String,
        size: Unsigned,
        mode: Unsigned,
        uid: Unsigned,
        gid: Unsigned,
        mtime_ns: i64,
        extensions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        Self::create(
            Entry::file(path),
            size.0,
            mode.0,
            uid.0,
            gid.0,
            mtime_ns,
            extensions,
        )
    }
    #[staticmethod]
    #[pyo3(signature = (path, *, mode = Unsigned(0o755), uid = Unsigned(0), gid = Unsigned(0), mtime_ns = 0, extensions = None))]
    fn directory(
        path: String,
        mode: Unsigned,
        uid: Unsigned,
        gid: Unsigned,
        mtime_ns: i64,
        extensions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        Self::create(
            Entry::directory(path),
            0,
            mode.0,
            uid.0,
            gid.0,
            mtime_ns,
            extensions,
        )
    }
    #[staticmethod]
    #[pyo3(signature = (path, target, *, mode = Unsigned(0o777), uid = Unsigned(0), gid = Unsigned(0), mtime_ns = 0, extensions = None))]
    fn symlink(
        path: String,
        target: String,
        mode: Unsigned,
        uid: Unsigned,
        gid: Unsigned,
        mtime_ns: i64,
        extensions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        Self::create(
            Entry::symlink(path, target),
            0,
            mode.0,
            uid.0,
            gid.0,
            mtime_ns,
            extensions,
        )
    }
    #[getter]
    fn path(&self) -> &str {
        &self.entry.path
    }
    #[getter]
    fn kind(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        kind(py, self.entry.kind)
    }
    #[getter]
    const fn size(&self) -> u64 {
        self.size
    }
    #[getter]
    const fn mode(&self) -> u64 {
        self.entry.mode
    }
    #[getter]
    const fn uid(&self) -> u64 {
        self.entry.uid
    }
    #[getter]
    const fn gid(&self) -> u64 {
        self.entry.gid
    }
    #[getter]
    const fn mtime_ns(&self) -> i64 {
        self.entry.mtime_ns
    }
    #[getter]
    fn linkname(&self) -> Option<&str> {
        self.entry.linkname.as_deref()
    }
    #[getter]
    fn extensions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        extensions_to_py(py, &self.entry.attributes)
    }
    fn __repr__(&self) -> String {
        format!("Header(path={:?}, size={})", self.entry.path, self.size)
    }
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<EntryValue>()?;
    module.add_class::<HeaderValue>()?;
    module.add_class::<ExtentValue>()?;
    module.add_class::<Extents>()?;
    Ok(())
}
