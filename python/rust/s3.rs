//! Shared native S3 configuration for synchronous and asynchronous bindings.

use std::{num::NonZeroUsize, sync::Arc};
use zstar::s3::{AmazonS3Builder, Path, S3BlobStore, S3RangeSource};

pub(crate) fn builder(
    bucket: String,
    endpoint: Option<String>,
    region: Option<String>,
    anon: Option<bool>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
) -> AmazonS3Builder {
    // Capture environment configuration before releasing the Python GIL.
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_allow_http(endpoint.starts_with("http://"))
            .with_endpoint(endpoint);
    }
    if let Some(region) = region {
        builder = builder.with_region(region);
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
    builder
}

pub(crate) fn source(
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
