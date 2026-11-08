//! Bounded logical read requests.
/// Maximum number of ranges accepted by one batch read call.
pub const MAX_BATCH_REQUESTS: usize = 4096;
/// One logical file range in a grouped batch.
#[derive(Clone, Copy, Debug)]
pub struct ReadRequest<'a> {
    /// Archive-relative path to a regular file.
    pub path: &'a str,
    /// Start position within the logical file.
    pub offset: u64,
    /// Number of bytes to return.
    pub length: u64,
}
