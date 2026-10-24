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

/// Reusable random-access member cursor. Retains owned metadata and at most one
/// validated decoded source, independently of the archive's retained cache.
/// The caller supplies the live archive for each operation.
pub struct FileCursor {
    pub(crate) owner: Option<std::sync::Weak<crate::ManifestReader>>,
    entry: Entry,
    starts: Vec<u64>,
    size: u64,
    cached: Option<(Key, Bytes)>,
}
impl FileCursor {
    pub fn new(entry: Entry) -> Result<Self> {
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        let size = entry.size()?;
        let mut starts = Vec::new();
        starts
            .try_reserve_exact(entry.extents.len())
            .map_err(|_| Error::Budget("file cursor index"))?;
        let mut position = 0_u64;
        for extent in &entry.extents {
            starts.push(position);
            position = position.checked_add(extent.size).ok_or(Error::Range)?;
        }
        Ok(Self {
            owner: None,
            entry,
            starts,
            size,
            cached: None,
        })
    }

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
        crate::checked_range(offset, length, self.size)?;
        if length > archive.max_read_bytes() {
            return Err(Error::Budget("returned read size"));
        }
        let length = usize::try_from(length).map_err(|_| Error::Budget("returned read size"))?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut index = self
            .starts
            .partition_point(|start| *start <= offset)
            .saturating_sub(1);
        let first = &self.entry.extents[index];
        let key = Key::of(first);
        if self
            .cached
            .as_ref()
            .is_none_or(|(previous, _)| *previous != key)
        {
            let raw = archive
                .content(&key, first.encoded_size, archive.resolver())
                .await?;
            self.cached = Some((key, raw));
        }
        let raw = &self.cached.as_ref().unwrap().1;
        crate::checked_range(first.offset, first.size, raw.len() as u64)?;
        let within = offset - self.starts[index];
        if length as u64 <= first.size - within {
            let start = (first.offset + within) as usize;
            // Sharing a validated chunk avoids an intermediate returned-read
            // allocation, especially when filling a Python readinto buffer.
            return Ok(raw.slice(start..start + length));
        }
        let mut output = Vec::new();
        output
            .try_reserve_exact(length)
            .map_err(|_| Error::Budget("returned read allocation"))?;
        while output.len() < length {
            let extent = &self.entry.extents[index];
            let key = Key::of(extent);
            if self
                .cached
                .as_ref()
                .is_none_or(|(previous, _)| *previous != key)
            {
                let raw = archive
                    .content(&key, extent.encoded_size, archive.resolver())
                    .await?;
                self.cached = Some((key, raw));
            }
            let raw = &self.cached.as_ref().unwrap().1;
            crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
            let within = offset + output.len() as u64 - self.starts[index];
            let count = (extent.size - within).min((length - output.len()) as u64) as usize;
            let start = (extent.offset + within) as usize;
            output.extend_from_slice(&raw[start..start + count]);
            index += 1;
        }
        Ok(Bytes::from(output))
    }
}

/// An owned archive entry with metadata and async file I/O.
///
/// Regular files implement [`AsyncRead`] and [`AsyncSeek`]. Directory and
/// symlink handles expose metadata and reject reads. Seeking beyond EOF is
/// permitted; negative and overflowing positions return an invalid-input error.
///
/// The handle retains its archive and one validated decoded chunk independently
/// of the archive cache budget. Dropping it cancels pending source I/O.
pub struct File<R: AsyncRangeReader + ?Sized> {
    archive: Archive<R>,
    entry: Entry,
    size: u64,
    starts: Vec<u64>,
    position: u64,
    cached: Option<(Key, Bytes)>,
    pending: Option<(Key, Load)>,
}
impl<R: AsyncRangeReader + ?Sized + 'static> File<R> {
    pub(crate) fn new(archive: Archive<R>, entry: Entry) -> Result<Self> {
        let size = entry.size()?;
        let mut starts = Vec::with_capacity(entry.extents.len());
        let mut at = 0_u64;
        for extent in &entry.extents {
            starts.push(at);
            at = at.checked_add(extent.size).ok_or(Error::Range)?;
        }
        Ok(Self {
            archive,
            entry,
            size,
            starts,
            position: 0,
            cached: None,
            pending: None,
        })
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.entry.path
    }
    /// Returns the logical payload length in bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Returns the entry kind.
    #[must_use]
    pub fn kind(&self) -> EntryKind {
        self.entry.kind
    }
    /// Borrows the entry's immutable metadata and extents.
    #[must_use]
    pub fn metadata(&self) -> &Entry {
        &self.entry
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> AsyncRead for File<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.entry.kind != EntryKind::File {
            return Poll::Ready(Err(io::Error::from(Error::Unsupported(
                "entry is not a file",
            ))));
        }
        if buffer.remaining() == 0 || this.position >= this.size {
            return Poll::Ready(Ok(()));
        }
        let index = this
            .starts
            .partition_point(|start| *start <= this.position)
            .saturating_sub(1);
        let extent = &this.entry.extents[index];
        let key = Key::of(extent);
        if this
            .cached
            .as_ref()
            .is_none_or(|(cached, _)| cached != &key)
        {
            if this.pending.is_none() {
                let archive = this.archive.clone();
                let key = key.clone();
                let hint = extent.encoded_size;
                let future =
                    Box::pin(async move { archive.content(&key, hint, archive.resolver()).await });
                this.pending = Some((Key::of(extent), future));
            }
            let (_, future) = this.pending.as_mut().unwrap();
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.pending = None;
                    return Poll::Ready(Err(io::Error::from(error)));
                }
                Poll::Ready(Ok(raw)) => {
                    this.pending = None;
                    this.cached = Some((key, raw));
                }
            }
        }
        let raw = &this.cached.as_ref().unwrap().1;
        if let Err(error) = crate::checked_range(extent.offset, extent.size, raw.len() as u64) {
            return Poll::Ready(Err(io::Error::from(error)));
        }
        let offset = this.position - this.starts[index];
        let count = (extent.size - offset).min(buffer.remaining() as u64) as usize;
        let Some(start) = extent.offset.checked_add(offset) else {
            return Poll::Ready(Err(io::Error::from(Error::Range)));
        };
        let start = start as usize;
        buffer.put_slice(&raw[start..start + count]);
        this.position += count as u64;
        Poll::Ready(Ok(()))
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> AsyncSeek for File<R> {
    fn start_seek(self: Pin<&mut Self>, seek: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        let position = match seek {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::End(offset) => i128::from(this.size) + i128::from(offset),
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
