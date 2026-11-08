//! Strict dictionary extraction and owned policy snapshots at the Python boundary.
use crate::{manifest::from_py, support::Unsigned};
use pyo3::{
    exceptions::{PyOverflowError, PyTypeError, PyValueError},
    prelude::*,
    types::PyDict,
};
use std::{collections::BTreeMap, sync::Arc};

trait Options: for<'a, 'py> FromPyObject<'a, 'py, Error = PyErr> {
    const KEYS: &'static [&'static str];
    fn defaults(py: Python<'_>) -> PyResult<Bound<'_, PyDict>>;
}

// Keep accepted keys and Rust defaults beside the derived fields. PyO3 rejects
// derives with every field defaulted, so merge defaults before derived extraction.
macro_rules! options {
    ($(#[$meta:meta])* $vis:vis struct $name:ident($py:ident) { $( $field:ident: $ty:ty = $default:expr $(=> $extract:path)? ),* $(,)? }) => {
        $(#[$meta])*
        #[derive(FromPyObject)]
        #[pyo3(from_item_all)]
        $vis struct $name {
            $( $( #[pyo3(from_py_with = $extract)] )? $field: $ty, )*
        }
        impl Options for $name {
            const KEYS: &'static [&'static str] = &[$(stringify!($field)),*];
            fn defaults($py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
                let defaults = PyDict::new($py);
                $(defaults.set_item(stringify!($field), $default)?;)*
                Ok(defaults)
            }
        }
    };
}

fn parse<T: Options>(value: &Bound<'_, PyAny>) -> PyResult<T> {
    let dict = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("options must be a dictionary"))?;
    for key in dict.keys() {
        let key: String = key
            .extract()
            .map_err(|_| PyTypeError::new_err("option keys must be strings"))?;
        if !T::KEYS.contains(&key.as_str()) {
            return Err(PyTypeError::new_err(format!("unknown option: {key}")));
        }
    }
    let values = T::defaults(value.py())?;
    values.update(dict.as_mapping())?;
    values.extract()
}
fn optional<T: Options>(value: &Bound<'_, PyAny>) -> PyResult<Option<T>> {
    if value.is_none() {
        Ok(None)
    } else {
        parse(value).map(Some)
    }
}
fn policy<T: Options>(value: Option<&Bound<'_, PyAny>>) -> PyResult<T> {
    value.filter(|value| !value.is_none()).map_or_else(
        || Python::attach(|py| parse(PyDict::new(py).as_any())),
        parse,
    )
}
fn unsigned(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    Ok(value.extract::<Unsigned>()?.0)
}
fn size(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize::try_from(unsigned(value)?).map_err(|_| PyOverflowError::new_err("option exceeds usize"))
}
fn level(value: &Bound<'_, PyAny>) -> PyResult<i32> {
    if value.is_instance_of::<pyo3::types::PyBool>()
        || !value.is_instance_of::<pyo3::types::PyInt>()
    {
        return Err(PyTypeError::new_err("compression_level must be an integer"));
    }
    value.extract()
}
fn callable(value: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    if value.is_none() {
        return Ok(None);
    }
    if !value.is_callable() {
        return Err(PyTypeError::new_err("expected a callable"));
    }
    Ok(Some(value.clone().unbind()))
}
fn object_resolver(value: &Bound<'_, PyAny>) -> PyResult<Option<Py<PyAny>>> {
    if value.is_none() {
        return Ok(None);
    }
    if !value.getattr("get_encoded")?.is_callable() {
        return Err(PyTypeError::new_err(
            "resolver.get_encoded must be callable",
        ));
    }
    Ok(Some(value.clone().unbind()))
}
fn extensions(value: &Bound<'_, PyAny>) -> PyResult<zstar::Extensions> {
    value
        .cast::<PyDict>()?
        .iter()
        .map(|(key, value)| {
            let key: String = key.extract()?;
            if !key.starts_with("x-") {
                return Err(PyValueError::new_err("extension names must start with x-"));
            }
            Ok((key, from_py(&value, 1)?))
        })
        .collect()
}
fn pages(value: &Bound<'_, PyAny>) -> PyResult<BTreeMap<String, Vec<ciborium::Value>>> {
    extensions(value)?
        .into_iter()
        .map(|(key, value)| {
            let ciborium::Value::Array(values) = value else {
                return Err(PyTypeError::new_err("extension pages must be sequences"));
            };
            Ok((key, values))
        })
        .collect()
}

options! { #[expect(clippy::struct_field_names, reason = "Names match the public limit policy.")] struct Limits(py) {
    max_frame_encoded_bytes: u64 = zstar::Limits::default().max_frame_encoded_bytes => unsigned,
    max_frame_decoded_bytes: u64 = zstar::Limits::default().max_frame_decoded_bytes => unsigned,
    max_window_bytes: u64 = zstar::Limits::default().max_window_bytes => unsigned,
    max_entry_bytes: u64 = zstar::Limits::default().max_entry_bytes => unsigned,
    max_extents: u64 = zstar::Limits::default().max_extents => unsigned,
    max_pointer_bytes: u64 = zstar::Limits::default().max_pointer_bytes => unsigned,
} }

impl From<Limits> for zstar::Limits {
    fn from(value: Limits) -> Self {
        Self {
            max_frame_encoded_bytes: value.max_frame_encoded_bytes,
            max_frame_decoded_bytes: value.max_frame_decoded_bytes,
            max_window_bytes: value.max_window_bytes,
            max_entry_bytes: value.max_entry_bytes,
            max_extents: value.max_extents,
            max_pointer_bytes: value.max_pointer_bytes,
        }
    }
}
options! { #[expect(clippy::struct_field_names, reason = "Names match the public metadata policy.")] struct MetadataReaderOptions(py) {
    max_page_bytes: u64 = zstar::MetadataReaderOptions::default().max_page_bytes => unsigned,
    max_retained_entry_bytes: u64 = zstar::MetadataReaderOptions::default().max_retained_entry_bytes => unsigned,
    max_entries: u64 = zstar::MetadataReaderOptions::default().max_entries => unsigned,
} }

options! { struct MetadataWriterOptions(py) {
    max_page_bytes: u64 = zstar::MetadataWriterOptions::default().max_page_bytes => unsigned,
    max_buffer_bytes: u64 = zstar::MetadataWriterOptions::default().max_buffer_bytes => unsigned,
    block_target: usize = zstar::MetadataWriterOptions::default().block_target => size,
    extension_pages: BTreeMap<String, Vec<ciborium::Value>> = PyDict::new(py) => pages,
} }

options! { struct ReaderOptions(py) {
    limits: Limits = PyDict::new(py) => parse,
    metadata: MetadataReaderOptions = PyDict::new(py) => parse,
    max_read_bytes: u64 = zstar::ReaderOptions::default().max_read_bytes => unsigned,
    verify_external: bool = zstar::ReaderOptions::default().verify_external,
    concurrent_chunks: usize = zstar::ReaderOptions::default().concurrent_chunks => size,
    resolver: Option<Py<PyAny>> = py.None() => object_resolver,
} }

options! { struct WriterOptions(py) {
    limits: Limits = PyDict::new(py) => parse,
    metadata: MetadataWriterOptions = PyDict::new(py) => parse,
    frame_size: usize = zstar::WriterOptions::default().frame_size => size,
    compression_level: i32 = zstar::WriterOptions::default().compression_level => level,
    workers: usize = zstar::WriterOptions::default().workers => size,
    root_extensions: zstar::Extensions = PyDict::new(py) => extensions,
} }

#[pyclass(module = "zstar._native", frozen)]
pub struct ReaderConfig {
    policy: zstar::ReaderOptions,
    resolver: Option<Py<PyAny>>,
}
#[pymethods]
impl ReaderConfig {
    #[new]
    fn new(options: &Bound<'_, PyAny>) -> PyResult<Self> {
        let value: ReaderOptions = policy(Some(options))?;
        let metadata = value.metadata;
        let config = Self {
            resolver: value.resolver,
            policy: zstar::ReaderOptions {
                limits: value.limits.into(),
                max_read_bytes: value.max_read_bytes,
                verify_external: value.verify_external,
                concurrent_chunks: value.concurrent_chunks,
                metadata: zstar::MetadataReaderOptions {
                    max_page_bytes: metadata.max_page_bytes,
                    max_retained_entry_bytes: metadata.max_retained_entry_bytes,
                    max_entries: metadata.max_entries,
                },
                resolver: None,
            },
        };
        config
            .policy
            .validate()
            .map_err(crate::support::core_error)?;
        Ok(config)
    }
    fn with_resolver(&self, resolver: Option<Py<PyAny>>) -> Self {
        Self {
            policy: self.policy.clone(),
            resolver,
        }
    }
    fn __traverse__(&self, visit: pyo3::PyVisit<'_>) -> Result<(), pyo3::PyTraverseError> {
        visit.call(&self.resolver)
    }
}
impl ReaderConfig {
    pub fn bind(
        &self,
        py: Python<'_>,
        scope: &Arc<crate::dispatch::Scope>,
    ) -> PyResult<zstar::ReaderOptions> {
        let mut policy = self.policy.clone();
        if let Some(resolver) = &self.resolver {
            policy.resolver = Some(crate::external::protocol_resolver(
                py,
                resolver.clone_ref(py),
                scope,
            )?);
        }
        Ok(policy)
    }
}
#[pyclass(module = "zstar._native", frozen)]
pub struct WriterConfig {
    policy: zstar::WriterOptions,
}
#[pymethods]
impl WriterConfig {
    #[new]
    fn new(options: &Bound<'_, PyAny>) -> PyResult<Self> {
        let value: WriterOptions = policy(Some(options))?;
        let metadata = value.metadata;
        let config = Self {
            policy: zstar::WriterOptions {
                limits: value.limits.into(),
                frame_size: value.frame_size,
                compression_level: value.compression_level,
                workers: value.workers,
                root_extensions: value.root_extensions,
                metadata: zstar::MetadataWriterOptions {
                    max_page_bytes: metadata.max_page_bytes,
                    max_buffer_bytes: metadata.max_buffer_bytes,
                    block_target: metadata.block_target,
                    extension_pages: metadata.extension_pages,
                },
            },
        };
        config
            .policy
            .validate()
            .map_err(crate::support::core_error)?;
        Ok(config)
    }
}
impl WriterConfig {
    pub fn policy(&self) -> zstar::WriterOptions {
        self.policy.clone()
    }
}

options! { struct RangeCacheOptions(py) {
    capacity_bytes: usize = zstar::RangeCacheOptions::default().capacity_bytes => size,
    max_entries: usize = zstar::RangeCacheOptions::default().max_entries => size,
    read_ahead_bytes: usize = zstar::RangeCacheOptions::default().read_ahead_bytes => size,
    max_fetch_bytes: usize = zstar::RangeCacheOptions::default().max_fetch_bytes => size,
    max_in_flight: usize = zstar::RangeCacheOptions::default().max_in_flight => size,
} }

impl RangeCacheOptions {
    fn into_policy(self) -> PyResult<zstar::RangeCacheOptions> {
        if self.max_entries == 0
            || self.max_fetch_bytes == 0
            || !(1..=1024).contains(&self.max_in_flight)
        {
            return Err(PyValueError::new_err("invalid range_cache limits"));
        }
        Ok(zstar::RangeCacheOptions {
            capacity_bytes: self.capacity_bytes,
            max_entries: self.max_entries,
            read_ahead_bytes: self.read_ahead_bytes,
            max_fetch_bytes: self.max_fetch_bytes,
            max_in_flight: self.max_in_flight,
        })
    }
}
options! { struct Credentials(py) {
    access_key_id: String = String::new(),
    secret_access_key: String = String::new(),
    session_token: Option<String> = py.None(),
} }

options! { pub struct S3Options(py) {
    region: String = String::new(),
    endpoint: Option<String> = py.None(),
    anonymous: bool = false,
    credentials: Option<Credentials> = py.None() => optional,
} }

impl S3Options {
    pub fn builder(self, bucket: String) -> PyResult<zstar::s3::AmazonS3Builder> {
        if self.region.is_empty() {
            return Err(PyValueError::new_err("region must be nonempty"));
        }
        if self.anonymous == self.credentials.is_some() {
            return Err(PyValueError::new_err(
                "choose anonymous access or explicit credentials",
            ));
        }
        let mut builder = zstar::s3::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(self.region)
            .with_skip_signature(self.anonymous);
        if let Some(endpoint) = self.endpoint {
            builder = builder
                .with_allow_http(endpoint.starts_with("http://"))
                .with_endpoint(endpoint);
        }
        if let Some(credentials) = self.credentials {
            if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
                return Err(PyValueError::new_err("credential keys must be nonempty"));
            }
            builder = builder
                .with_access_key_id(credentials.access_key_id)
                .with_secret_access_key(credentials.secret_access_key);
            if let Some(token) = credentials.session_token {
                builder = builder.with_token(token);
            }
        }
        Ok(builder)
    }
}
options! { pub struct SourceOptions(py) {
    headers: BTreeMap<String, String> = PyDict::new(py),
    s3: Option<S3Options> = py.None() => optional,
    request_provider: Option<Py<PyAny>> = py.None() => callable,
    range_cache: Option<RangeCacheOptions> = py.None() => optional,
} }

impl SourceOptions {
    pub fn parse(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let options: Self = policy(value)?;
        if options.request_provider.is_some() && !options.headers.is_empty() {
            return Err(PyValueError::new_err(
                "signed providers supply their own headers",
            ));
        }
        Ok(options)
    }
    pub fn range_cache(&mut self) -> PyResult<Option<zstar::RangeCacheOptions>> {
        self.range_cache
            .take()
            .map(RangeCacheOptions::into_policy)
            .transpose()
    }
    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<String, String>,
        Option<S3Options>,
        Option<Py<PyAny>>,
    ) {
        (self.headers, self.s3, self.request_provider)
    }
}
options! { pub struct SinkOptions(py) {
    s3: Option<S3Options> = py.None() => optional,
} }

impl SinkOptions {
    pub fn parse(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<S3Options>> {
        let options: Self = policy(value)?;
        Ok(options.s3)
    }
}
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ReaderConfig>()?;
    module.add_class::<WriterConfig>()?;
    Ok(())
}
