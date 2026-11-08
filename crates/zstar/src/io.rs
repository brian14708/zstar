//! Shared storage interfaces and bounded batch requests.
use crate::Result;
use bytes::Bytes;
use std::ops::Range;

/// Exact byte ranges from an immutable archive. Dropping a returned future
/// cancels pending work; implementations must not detach network requests.
///
/// Raw-byte caching and prefetching belong to the source implementation; the
/// archive does not retain encoded ranges across operations.
/// [`crate::CachedRangeReader`] can decorate any immutable source to add bounded
/// interval caching, shared fetches, and optional forward read-ahead.
#[async_trait::async_trait]
pub trait AsyncRangeReader: Send + Sync {
    /// Returns the immutable archive size in bytes.
    async fn size(&self) -> Result<u64>;
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
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    async fn read_range(&self, range: Range<u64>) -> Result<Self> {
        let length = range
            .end
            .checked_sub(range.start)
            .ok_or(crate::Error::Range)?;
        crate::checked_range(range.start, length, self.len() as u64)?;
        Ok(self.slice(range.start as usize..range.end as usize))
    }
}
