//! Borrowed bounded member streams with standard cursor semantics.
use super::{Archive, RangeReader};
use crate::decoded_body::DecodedBody;
use crate::read_plan::{Key, Selection};
use crate::{Entry, EntryMetadata, Error, Result};
use std::io::{self, Read, Seek, SeekFrom};

const DECODED_CACHE_BYTES: usize = 2 << 20;

/// A regular member implementing standard [`Read`] and [`Seek`].
///
/// The handle exclusively borrows its archive. Seeks past EOF are allowed;
/// reads clamp at EOF. Each selected frame/object is completely decoded and its
/// declared extent validated before delivery. A content failure makes future
/// reads fail; seeking does not clear that failure. At most one decoded source
/// is kept between reads: the current one while the cursor remains inside its
/// extent, and otherwise only within a 2 MiB retention budget.
pub struct File<'a, R: RangeReader> {
    archive: &'a mut Archive<R>,
    metadata: EntryMetadata,
    selection: Selection,
    position: u64,
    cached: Option<(Key, DecodedBody)>,
    failed: bool,
}
impl<'a, R: RangeReader> File<'a, R> {
    pub(crate) fn new(archive: &'a mut Archive<R>, entry: Entry) -> Result<Self> {
        let metadata = EntryMetadata::from_entry(&entry)?;
        let selection = Selection::new(entry)?;
        Ok(Self {
            archive,
            metadata,
            selection,
            position: 0,
            cached: None,
            failed: false,
        })
    }
    /// Returns the logical metadata backing this handle.
    #[must_use]
    pub const fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        self.metadata.path()
    }
    /// Reads an exact range without moving the stream position, reusing the
    /// handle's bounded decoded cache. A content failure poisons this handle.
    ///
    /// # Errors
    /// Returns range, budget, source, or content-validation errors.
    pub fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if self.failed {
            return Err(Error::Unsupported("content reader failed"));
        }
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, length, self.size())?;
        if length > self.archive.options.max_read_bytes {
            return Err(Error::budget_exceeded(
                crate::BudgetResource::ReadBytes,
                "returned read size",
                self.archive.options.max_read_bytes,
                length,
            ));
        }
        let length = usize::try_from(length).map_err(|_| Error::Range)?;
        let mut output = Vec::new();
        output.try_reserve_exact(length).map_err(|_| {
            Error::budget(crate::BudgetResource::AllocationBytes, "read allocation")
        })?;
        let position = self.position;
        self.position = range.start;
        let result = (|| {
            while output.len() < length {
                self.next_chunk(length - output.len(), |bytes| {
                    output.extend_from_slice(bytes);
                    Ok(())
                })?;
            }
            Ok(output)
        })();
        self.position = position;
        if self
            .cached
            .as_ref()
            .is_some_and(|(_, raw)| !retainable(raw))
        {
            self.cached = None;
        }
        self.poison_on_error(result)
    }
    /// Returns the logical file length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.selection.size
    }

    /// Delivers up to `limit` bytes from the current extent to `deliver`.
    fn next_chunk(
        &mut self,
        limit: usize,
        deliver: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<usize> {
        if self.failed {
            return Err(Error::Unsupported("content reader failed"));
        }
        if limit == 0 || self.position >= self.selection.size {
            return Ok(0);
        }
        let index = self.selection.extent_index(self.position);
        let extent = &self.selection.entry.extents[index];
        let key = Key::of(extent);
        let raw = if self
            .cached
            .as_ref()
            .is_some_and(|(cached, _)| *cached == key)
        {
            self.cached.take().unwrap().1
        } else {
            // Drop a previous decoded source before fetching the next.
            self.cached = None;
            let verify_external = self.archive.options.verify_external;
            self.archive.engine.load(
                &key,
                extent.encoded_size,
                &mut self.archive.options.resolver(),
                verify_external,
            )?
        };
        crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
        let logical_start = if index == 0 {
            0
        } else {
            self.selection.ends[index - 1]
        };
        let within = self.position - logical_start;
        let count =
            usize::try_from((extent.size - within).min(limit as u64)).map_err(|_| Error::Range)?;
        let start = usize::try_from(extent.offset + within).map_err(|_| Error::Range)?;
        deliver(&raw[start..start + count])?;
        self.position += count as u64;
        // Sequential reads keep the current source regardless of size; only
        // retention beyond its extent is budgeted.
        if self.position < logical_start + extent.size || retainable(&raw) {
            self.cached = Some((key, raw));
        }
        Ok(count)
    }
    /// Copy the remaining content, preserving codec and resolver error types.
    #[cfg(any(test, all(feature = "fs", unix)))]
    pub(crate) fn copy_to(&mut self, output: &mut impl std::io::Write) -> Result<u64> {
        let mut size = 0_u64;
        loop {
            let result = self.next_chunk(usize::MAX, |bytes| Ok(output.write_all(bytes)?));
            let count = self.poison_on_error(result)?;
            if count == 0 {
                return Ok(size);
            }
            size = size.checked_add(count as u64).ok_or(Error::Range)?;
        }
    }
    /// A failure makes all future reads fail and releases the decoded source.
    fn poison_on_error<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.failed = true;
            self.cached = None;
        }
        result
    }
}
fn retainable(raw: &DecodedBody) -> bool {
    raw.len()
        .saturating_add(std::mem::size_of::<(Key, DecodedBody)>())
        <= DECODED_CACHE_BYTES
}
impl<R: RangeReader> Read for File<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let result = self.next_chunk(output.len(), |bytes| {
            output[..bytes.len()].copy_from_slice(bytes);
            Ok(())
        });
        self.poison_on_error(result).map_err(Into::into)
    }
}
impl<R: RangeReader> Seek for File<'_, R> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
            SeekFrom::End(delta) => self.selection.size.checked_add_signed(delta),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid member seek"))?;
        if position != self.position
            && self
                .cached
                .as_ref()
                .is_some_and(|(_, raw)| !retainable(raw))
        {
            self.cached = None;
        }
        self.position = position;
        Ok(position)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}
