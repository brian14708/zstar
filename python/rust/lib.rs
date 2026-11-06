//! Python adapters for native archive readers, writers, and storage.
use pyo3::prelude::*;
mod async_archive;
mod callback;
mod external;
mod fs;
mod manifest;
mod reader;
mod request;
mod s3;
mod staged_writer;
mod support;
mod sync_archive;
mod writer;

/// All classes are internally synchronized. Callback gates reject reentry and
/// state locks are released before Python callbacks, including without the GIL.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ZstarError", m.py().get_type::<support::ZstarError>())?;
    fs::register(m)?;
    manifest::register(m)?;
    m.add_class::<writer::EntryArchiveWriter>()?;
    m.add_function(wrap_pyfunction!(writer::_writer_call, m)?)?;
    m.add_class::<staged_writer::StagedWriter>()?;
    m.add_class::<staged_writer::StagedMember>()?;
    m.add_function(wrap_pyfunction!(staged_writer::member_name, m)?)?;
    m.add_function(wrap_pyfunction!(request::put_presigned, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_archive_metadata, m)?)?;
    m.add_class::<sync_archive::SyncArchive>()?;
    m.add_class::<sync_archive::SyncEntries>()?;
    m.add_class::<reader::MemberCursor>()?;
    m.add_class::<async_archive::AsyncArchive>()?;
    m.add_class::<async_archive::AsyncEntries>()?;
    Ok(())
}
