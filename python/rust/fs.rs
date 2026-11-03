//! Native filesystem capture, restoration, and standalone frame utilities.
use crate::support::{core_error, filesystem_error};
use pyo3::prelude::*;
#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
/// Decode one independent external Zstandard object with an explicit expected decoded size.
#[pyfunction]
fn decode_object(
    py: Python<'_>,
    encoded: Vec<u8>,
    size: usize,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    if size == 0 {
        return Err(core_error(zstar::Error::MalformedExternalContent(
            "object size outside format bounds".into(),
        )));
    }
    let raw = py
        .detach(|| zstar::decode_frame(&encoded, size))
        .map_err(|error| core_error(zstar::Error::MalformedExternalContent(error.to_string())))?;
    Ok(pyo3::types::PyBytes::new(py, &raw).unbind())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
#[pyfunction]
fn pack_file(py: Python<'_>, path: &str, data: Vec<u8>) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| {
            let mut writer = zstar::backend::BlockingBuilder::new(Vec::new())?;
            writer.add_file(
                zstar::manifest::Entry {
                    path: path.to_owned(),
                    kind: zstar::manifest::EntryKind::File,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: std::collections::BTreeMap::default(),
                },
                data.len() as u64,
                &mut data.as_slice(),
            )?;
            writer.finish()
        })
        .map_err(core_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
#[pyfunction]
#[pyo3(
    signature = (
        path,
        *,
        preserve_metadata = false,
    )
)]
fn pack_directory(
    py: Python<'_>,
    path: std::path::PathBuf,
    preserve_metadata: bool,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| {
            let mut bytes = Vec::new();
            zstar::fs::pack_to_with_options(
                &path,
                &mut bytes,
                zstar::fs::PackOptions {
                    preserve_metadata,
                    ..Default::default()
                },
            )?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(bytes)
        })
        .map_err(filesystem_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
#[pyfunction]
#[pyo3(
    signature = (
        data,
        destination,
        *,
        objects = None,
        allow_special_bits = false,
        verify_external = false,
    )
)]
fn extract(
    py: Python<'_>,
    data: Vec<u8>,
    destination: std::path::PathBuf,
    objects: Option<std::path::PathBuf>,
    allow_special_bits: bool,
    verify_external: bool,
) -> PyResult<()> {
    py.detach(|| -> zstar::fs::AnyResult<()> {
        let mut archive = zstar::backend::BlockingArchive::open(
            zstar::SliceReader::new(&data),
            zstar::ManifestOptions::default(),
        )?;
        let objects = objects
            .as_deref()
            .map(zstar::fs::LocalObjects::open)
            .transpose()?;
        zstar::fs::extract_with_options(
            &mut archive,
            &destination,
            objects,
            zstar::fs::ExtractOptions {
                allow_special_bits,
                verify_external,
                ..Default::default()
            },
        )
    })
    .map_err(filesystem_error)
}
#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
#[pyfunction]
#[pyo3(
    signature = (
        data,
        *,
        objects = None,
        verify_external = false,
    )
)]
fn materialize(
    py: Python<'_>,
    data: Vec<u8>,
    objects: Option<std::path::PathBuf>,
    verify_external: bool,
) -> PyResult<Py<pyo3::types::PyBytes>> {
    let bytes = py
        .detach(|| -> zstar::fs::AnyResult<Vec<u8>> {
            let mut archive = zstar::backend::BlockingArchive::open(
                zstar::SliceReader::new(&data),
                zstar::ManifestOptions::default(),
            )?;
            let objects = objects
                .as_deref()
                .map(zstar::fs::LocalObjects::open)
                .transpose()?;
            let mut output = Vec::new();
            zstar::fs::materialize_to(
                &mut archive,
                &mut output,
                objects,
                verify_external,
                zstar::fs::PackOptions::default(),
            )?;
            Ok(output)
        })
        .map_err(filesystem_error)?;
    Ok(pyo3::types::PyBytes::new(py, &bytes).unbind())
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(decode_object, m)?)?;
    m.add_function(wrap_pyfunction!(pack_file, m)?)?;
    m.add_function(wrap_pyfunction!(pack_directory, m)?)?;
    m.add_function(wrap_pyfunction!(extract, m)?)?;
    m.add_function(wrap_pyfunction!(materialize, m)?)?;
    Ok(())
}
