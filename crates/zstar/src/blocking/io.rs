//! Immutable blocking sources and external object resolution.
use crate::{Error, Result};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

pub use crate::primitives::RangeReader;

/// Resolves complete encoded objects with a mutable, sequential contract.
/// Implementations need not be `Send` or `Sync`.
pub trait ObjectResolver {
    /// Fetches an object under an actual encoded-byte limit.
    ///
    /// `hint` is advisory and may differ from the stored encoding's length.
    /// Enforce `max_bytes` while fetching, before buffering unbounded input.
    ///
    /// # Errors
    /// Return [`Error::MissingObject`] when absent, or a transport/budget error.
    fn get_encoded(&mut self, address: [u8; 32], hint: u64, max_bytes: u64) -> Result<Vec<u8>>;
}

/// An immutable memory snapshot, owning or borrowing its backing bytes.
#[derive(Clone, Debug)]
pub struct MemorySource<B>(B);
impl<B: AsRef<[u8]>> MemorySource<B> {
    /// Wraps immutable archive bytes without copying them.
    pub const fn new(bytes: B) -> Self {
        Self(bytes)
    }
}
impl<B: AsRef<[u8]>> RangeReader for MemorySource<B> {
    fn len(&self) -> u64 {
        self.0.as_ref().len() as u64
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, length, self.len())?;
        let start = usize::try_from(range.start).map_err(|_| Error::Range)?;
        let end = usize::try_from(range.end).map_err(|_| Error::Range)?;
        Ok(self.0.as_ref()[start..end].to_vec())
    }
}

/// A `Read + Seek` source with a size recorded once at construction.
///
/// The adapter owns the seek position. The caller must keep contents immutable
/// for the archive lifetime, including when another handle can modify the file.
pub struct SeekSource<R> {
    reader: R,
    size: u64,
}
impl<R: Read + Seek> SeekSource<R> {
    /// Records the size and restores the initial cursor position.
    ///
    /// # Errors
    /// Returns seek errors while obtaining the size or restoring the cursor.
    pub fn new(mut reader: R) -> Result<Self> {
        let position = reader.stream_position()?;
        let size = reader.seek(SeekFrom::End(0))?;
        reader.seek(SeekFrom::Start(position))?;
        Ok(Self { reader, size })
    }
    /// Returns the reader with its current seek position.
    pub fn into_inner(self) -> R {
        self.reader
    }
}
impl<R: Read + Seek> RangeReader for SeekSource<R> {
    fn len(&self) -> u64 {
        self.size
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, length, self.size)?;
        let length = usize::try_from(length).map_err(|_| {
            crate::Error::budget(crate::BudgetResource::RangeBytes, "source range size")
        })?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|_| {
            crate::Error::budget(
                crate::BudgetResource::AllocationBytes,
                "source range allocation",
            )
        })?;
        bytes.resize(length, 0);
        self.reader.seek(SeekFrom::Start(range.start))?;
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}
