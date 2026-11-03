//! Owned logical file handles over an immutable, shared archive.
use crate::archive::Key;
use crate::{Archive, AsyncRangeReader, Entry, EntryKind, Error, Result};
use bytes::Bytes;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

type Load = Pin<Box<dyn Future<Output = Result<Bytes>> + Send>>;

// Immutable entry metadata and extent interpretation shared by both handles.
// Logical stream position and pending I/O remain with File.
struct FileSelection {
    entry: Entry,
    ends: Vec<u64>,
    size: u64,
}
impl FileSelection {
    fn new(entry: Entry) -> Result<Self> {
        let size = entry.size()?;
        let mut ends = Vec::new();
        ends.try_reserve_exact(entry.extents.len())
            .map_err(|_| Error::Budget("file cursor index"))?;
        let mut position = 0_u64;
        for extent in &entry.extents {
            position = position.checked_add(extent.size).ok_or(Error::Range)?;
            ends.push(position);
        }
        Ok(Self { entry, ends, size })
    }

    fn extent_index(&self, offset: u64) -> usize {
        self.ends.partition_point(|end| *end <= offset)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    fn read<R: AsyncRangeReader + ?Sized + 'static>(
        &self,
        archive: &Archive<R>,
        offset: u64,
        length: u64,
    ) -> Load {
        // Prepare synchronously so the pending future owns only its selected
        // extent or read plan, without cloning the handle's complete metadata.
        let prepared = (|| -> Result<Load> {
            crate::checked_range(offset, length, self.size)?;
            if length > archive.max_read_bytes() {
                return Err(Error::Budget("returned read size"));
            }
            let length =
                usize::try_from(length).map_err(|_| Error::Budget("returned read size"))?;
            if length == 0 {
                return Ok(Box::pin(std::future::ready(Ok(Bytes::new()))));
            }
            let index = self.extent_index(offset);
            let first = &self.entry.extents[index];
            let start = if index == 0 { 0 } else { self.ends[index - 1] };
            let within = offset - start;
            if length as u64 > first.size - within {
                // Plan from retained metadata, preserving bounded batch execution.
                return Ok(Box::pin(archive.read_entry_range(
                    &self.entry,
                    &self.ends,
                    offset,
                    length as u64,
                )));
            }
            let extent = first.clone();
            let archive = archive.clone();
            Ok(Box::pin(async move {
                let raw = archive
                    .content(&Key::of(&extent), extent.encoded_size, archive.resolver())
                    .await?;
                crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
                let start = (extent.offset + within) as usize;
                Ok(raw.slice(start..start + length))
            }))
        })();
        prepared.unwrap_or_else(|error| Box::pin(std::future::ready(Err(error))))
    }
}

/// Reusable random-access member cursor retaining owned metadata.
/// The caller supplies the live archive for each operation.
pub struct FileCursor {
    pub(crate) owner: Option<std::sync::Weak<crate::ManifestReader>>,
    selection: FileSelection,
}
impl FileCursor {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub fn new(entry: Entry) -> Result<Self> {
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        Ok(Self {
            owner: None,
            selection: FileSelection::new(entry)?,
        })
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads an exact bounded range with the archive's hash/resolver policy.
    /// Cancellation does not change any logical stream position.
    pub async fn read<R: AsyncRangeReader + ?Sized + 'static>(
        &mut self,
        archive: &Archive<R>,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        if self
            .owner
            .as_ref()
            .is_some_and(|owner| !archive.owns_cursor(owner))
        {
            return Err(Error::Unsupported("cursor belongs to another archive"));
        }
        self.selection.read(archive, offset, length).await
    }
}

/// An owned archive entry with metadata and async file I/O.
///
/// Regular files implement [`AsyncRead`] and [`AsyncSeek`]. Directory and
/// symlink handles expose metadata and reject reads. Seeking beyond EOF is
/// permitted; negative and overflowing positions return an invalid-input error.
///
/// The handle retains its archive and pending read state. Dropping it cancels
/// pending source I/O. Completed reads do not retain decoded content.
pub struct File<R: AsyncRangeReader + ?Sized> {
    archive: Archive<R>,
    selection: FileSelection,
    position: u64,
    pending: Option<(u64, Load)>,
}
impl<R: AsyncRangeReader + ?Sized + 'static> File<R> {
    pub(crate) fn new(archive: Archive<R>, entry: Entry) -> Result<Self> {
        let selection = FileSelection::new(entry)?;
        Ok(Self {
            archive,
            selection,
            position: 0,
            pending: None,
        })
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.selection.entry.path
    }
    /// Returns the logical payload length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.selection.size
    }
    /// Returns the entry kind.
    #[must_use]
    pub const fn kind(&self) -> EntryKind {
        self.selection.entry.kind
    }
    /// Borrows the entry's immutable metadata and extents.
    #[must_use]
    pub const fn metadata(&self) -> &Entry {
        &self.selection.entry
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> AsyncRead for File<R> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.selection.entry.kind != EntryKind::File {
            return Poll::Ready(Err(io::Error::from(Error::Unsupported(
                "entry is not a file",
            ))));
        }
        if buffer.remaining() == 0 || this.position >= this.selection.size {
            return Poll::Ready(Ok(()));
        }
        if this.pending.is_none() {
            let start = this.position;
            let index = this.selection.extent_index(start);
            let end = this.selection.ends[index];
            let length = (end - start)
                .min(this.archive.max_read_bytes())
                .min(buffer.remaining() as u64);
            let future = this.selection.read(&this.archive, start, length);
            this.pending = Some((start, future));
        }
        let (start, future) = this.pending.as_mut().unwrap();
        let start = *start;
        let raw = match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                this.pending = None;
                return Poll::Ready(Err(io::Error::from(error)));
            }
            Poll::Ready(Ok(raw)) => raw,
        };
        this.pending = None;
        let offset = (this.position - start) as usize;
        let count = (raw.len() - offset).min(buffer.remaining());
        buffer.put_slice(&raw[offset..offset + count]);
        this.position += count as u64;
        Poll::Ready(Ok(()))
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> AsyncSeek for File<R> {
    fn start_seek(self: Pin<&mut Self>, seek: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let position = match seek {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::End(offset) => i128::from(this.selection.size) + i128::from(offset),
            SeekFrom::Current(offset) => i128::from(this.position) + i128::from(offset),
        };
        this.position = u64::try_from(position).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "negative or overflowing file seek",
            )
        })?;
        this.pending = None;
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
