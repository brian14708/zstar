//! Configure and open the native reader shared by Python archive constructors.
use pyo3::prelude::*;
use std::sync::Arc;
use zstar::ReaderOptions;

pub type Reader = zstar::Archive<dyn zstar::AsyncRangeReader>;

/// Owned metadata/active chunk only: the archive lease is supplied per read.
#[pyclass(module = "zstar._native", frozen)]
pub struct MemberCursor {
    pub(crate) inner: Arc<tokio::sync::Mutex<zstar::FileCursor>>,
}
impl MemberCursor {
    pub(crate) async fn open(reader: &Reader, path: &str) -> PyResult<Self> {
        let cursor = reader.open_cursor(path).await.map_err(super::core_error)?;
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(cursor)),
        })
    }
}

pub async fn open_reader(
    source: Arc<dyn zstar::AsyncRangeReader>,
    options: ReaderOptions,
) -> PyResult<Reader> {
    let reader = Reader::open_with_options(source, options)
        .await
        .map_err(super::core_error)?;
    Ok(reader)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Shared Python constructor keyword options."
)]
pub fn reader_options(
    py: Python<'_>,
    concurrent_chunks: usize,
    verify_external: bool,
    memory_bytes: u64,
    max_encoded_bytes: u64,
    max_decoded_bytes: u64,
    max_read_bytes: u64,
    max_retained_entry_bytes: u64,
    max_entries: u64,
    max_spool_bytes: u64,
    create_spill_store: Option<Py<PyAny>>,
    resolver: Option<Py<PyAny>>,
) -> PyResult<ReaderOptions> {
    let options = ReaderOptions {
        concurrent_chunks,
        verify_external,
        resolver: resolver.map(|r| super::pack::binding(py, r)),
        manifest: zstar::ManifestOptions {
            memory_bytes,
            max_encoded_bytes,
            max_decoded_bytes,
            max_read_bytes,
            max_retained_entry_bytes,
            max_entries,
            spool_budget: Arc::new(zstar::SpoolBudget::new(max_spool_bytes)),
            create_spill_store: super::manifest_api::spill_factory(py, create_spill_store)?,
            ..Default::default()
        },
    };
    Ok(options)
}
