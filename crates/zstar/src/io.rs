//! Shared storage interfaces and bounded batch requests.
use crate::Result;
use bytes::Bytes;
use std::ops::Range;

/// Maximum number of ranges accepted by one [`crate::Archive::read_many`] call.
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

/// Exact byte ranges from an immutable archive. Dropping a returned future
/// cancels pending work; implementations must not detach network requests.
#[async_trait::async_trait]
pub trait AsyncRangeReader: Send + Sync {
    /// Whether retained decoded content may be reused under the source policy.
    fn cache_allowed(&self) -> bool {
        true
    }
    /// Returns the immutable archive size in bytes.
    async fn size(&self) -> Result<u64>;
    /// Optional location hint for the encoded manifest region, excluding the footer.
    /// Readers validate bounds and the terminal footer before using this hint.
    async fn manifest_range(&self) -> Result<Option<Range<u64>>> {
        Ok(None)
    }
    /// Returns exactly the requested half-open byte range.
    ///
    /// Return an error for reversed or out-of-bounds ranges and short reads.
    /// Concurrent calls must not share a mutable seek position.
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes>;
}

/// In-memory archives use shared zero-copy exact slices.
#[async_trait::async_trait]
impl AsyncRangeReader for Bytes {
    async fn size(&self) -> Result<u64> {
        Ok(self.len() as u64)
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or(crate::Error::Range)?;
        crate::checked_range(range.start, length, self.len() as u64)?;
        Ok(self.slice(range.start as usize..range.end as usize))
    }
}
