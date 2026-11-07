//! Shared native S3 configuration for synchronous and asynchronous bindings.

use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyMemoryView, PySlice},
};
use std::{num::NonZeroUsize, sync::Arc};
use zstar::s3::{AmazonS3Builder, Path, S3BlobStore, S3RangeSource};

pub fn builder(
    bucket: String,
    endpoint: Option<String>,
    region: Option<String>,
    anon: Option<bool>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
) -> PyResult<AmazonS3Builder> {
    let region = region
        .filter(|value| !value.is_empty())
        .ok_or_else(|| PyValueError::new_err("region is required for raw S3 access"))?;
    let anonymous = anon.unwrap_or(false);
    if anonymous
        && (access_key_id.is_some() || secret_access_key.is_some() || session_token.is_some())
    {
        return Err(PyValueError::new_err(
            "anonymous S3 access cannot include credentials",
        ));
    }
    if !anonymous
        && (access_key_id.as_deref().is_none_or(str::is_empty)
            || secret_access_key.as_deref().is_none_or(str::is_empty))
    {
        return Err(PyValueError::new_err(
            "raw S3 access requires access_key_id and secret_access_key, or anon=True",
        ));
    }
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region);
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_allow_http(endpoint.starts_with("http://"))
            .with_endpoint(endpoint);
    }
    if let Some(key) = access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(secret) = secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(token) = session_token {
        builder = builder.with_token(token);
    }
    if let Some(anon) = anon {
        builder = builder.with_skip_signature(anon);
    }
    Ok(builder)
}

pub fn source(
    builder: AmazonS3Builder,
    key: String,
    object_prefix: Option<String>,
    options: &mut zstar::ReaderOptions,
) -> zstar::Result<Arc<dyn zstar::AsyncRangeReader>> {
    let store = Arc::new(
        builder
            .build()
            .map_err(|_| zstar::Error::Unsupported("S3 configuration failed"))?,
    );
    if options.resolver.is_none() {
        options.resolver = object_prefix.map(|prefix| {
            Arc::new(S3BlobStore::new(
                store.clone(),
                Path::from(prefix),
                NonZeroUsize::new(8).unwrap(),
            )) as Arc<dyn zstar::ObjectResolver>
        });
    }
    Ok(Arc::new(S3RangeSource::new(store, Path::from(key))))
}

#[pyfunction]
pub fn s3_archive_metadata(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    let view = PyMemoryView::from(data)?.call_method1("cast", ("B",))?;
    let source_size = view.len()? as u64;
    let tail = view.get_item(PySlice::new(
        py,
        -(zstar::backend::FOOTER_SIZE.cast_signed()),
        isize::MAX,
        1,
    ))?;
    let footer = crate::support::buffer_vec(py, &tail)?;
    let footer = zstar::backend::parse_footer(&footer).map_err(crate::support::core_error)?;
    let attributes =
        zstar::s3::archive_attributes(&footer, source_size).map_err(crate::support::core_error)?;
    Ok(attributes
        .iter()
        .filter_map(|(key, value)| {
            if let zstar::s3::object_store::Attribute::Metadata(key) = key {
                Some((key.to_string(), value.as_ref().to_owned()))
            } else {
                None
            }
        })
        .collect())
}
