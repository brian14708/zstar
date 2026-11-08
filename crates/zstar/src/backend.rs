//! Hidden workspace support for low-level format and physical validation.
//! Ordinary Rust applications should use the supported public APIs.
#[cfg(feature = "async")]
pub use crate::file::FileCursor;
pub use crate::frame::encode_object;
pub use crate::manifest_io::EntryCursor;
/// Creates a binding cursor that borrows a live archive on each operation.
///
/// # Errors
/// Returns metadata, missing-entry, wrong-kind, or resource-limit errors.
#[cfg(feature = "async")]
pub async fn open_cursor<R: crate::AsyncRangeReader + ?Sized + 'static>(
    archive: &crate::Archive<R>,
    path: &str,
) -> crate::Result<FileCursor> {
    archive.open_cursor(path).await
}
pub use crate::primitives::{
    Digest, FOOTER_SIZE, Footer, checked_range, encode_footer, encode_frame, parse_footer,
};
pub use crate::tar::parse_header_prefix as parse_member_header;
/// Internal manifest encoding and validation helpers.
pub mod manifest {
    pub use crate::manifest::{decode_block, object_address, parse_object_address};
}
