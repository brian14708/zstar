//! Python adapters for native archive readers, writers, and storage.
use pyo3::prelude::*;
mod adapters;
mod async_archive;
mod completion;
mod config;
mod dispatch;
mod external;
mod manifest;
mod protocol_io;
mod reader;
mod request;
mod sinks;
mod sources;
mod storage;
mod support;
mod sync_archive;
mod values;
mod writer_session;

/// All classes are internally synchronized. Callback gates reject reentry and
/// state locks are released before Python callbacks, including without the GIL.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ZstarError", m.py().get_type::<support::ZstarError>())?;
    m.add_class::<adapters::BlockingAdapter>()?;
    values::register(m)?;
    config::register(m)?;
    dispatch::register(m)?;
    sources::register(m)?;
    sinks::register(m)?;
    m.add_class::<writer_session::WriterSession>()?;
    m.add_function(wrap_pyfunction!(support::_writer_call, m)?)?;
    m.add_function(wrap_pyfunction!(support::member_name, m)?)?;
    m.add_function(wrap_pyfunction!(request::put_presigned, m)?)?;
    m.add_class::<sync_archive::SyncArchive>()?;
    m.add_class::<sync_archive::SyncEntries>()?;
    m.add_class::<reader::MemberCursor>()?;
    m.add_class::<async_archive::AsyncArchive>()?;
    m.add_class::<async_archive::AsyncEntries>()?;
    Ok(())
}
