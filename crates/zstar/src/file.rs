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
        let index = self
            .starts
            .partition_point(|start| *start <= offset)
            .saturating_sub(1);
        let first = &self.entry.extents[index];
        let within = offset - self.starts[index];
        if length as u64 > first.size - within {
            // A multi-frame request communicates its complete miss plan, so
            // packed and adjacent archive frames can share bounded range GETs.
            return archive
                .read_range_with_resolver(
                    &self.entry.path,
                    offset,
                    length as u64,
                    archive.resolver(),
                )
                .await;
        }
        let key = Key::of(first);
        let raw = if archive.cache_allowed(&key, archive.resolver()) {
            self.cached
                .as_ref()
                .filter(|(previous, _)| *previous == key)
                .map(|(_, raw)| raw.clone())
        } else {
            None
        };
        let raw = match raw {
            Some(raw) => raw,
            None => {
                archive
                    .content(&key, first.encoded_size, archive.resolver())
                    .await?
            }
        };
        self.cached = if archive.cache_allowed(&key, archive.resolver()) {
            Some((key, raw.clone()))
        } else {
            None
        };
        crate::checked_range(first.offset, first.size, raw.len() as u64)?;
        let start = (first.offset + within) as usize;
        // Share the validated single chunk for bounded readinto operations.
        Ok(raw.slice(start..start + length))
    }
}

/// An owned archive entry with metadata and async file I/O.
///
/// Regular files implement [`AsyncRead`] and [`AsyncSeek`]. Directory and
/// symlink handles expose metadata and reject reads. Seeking beyond EOF is
/// permitted; negative and overflowing positions return an invalid-input error.
///
/// The handle retains its archive and a bounded validated read window independently
/// of the archive cache budget. Sequential reads may coalesce up to four extents
/// and 8 MiB; seeks reset read-ahead detection. Dropping it cancels pending source I/O.
pub struct File<R: AsyncRangeReader + ?Sized> {
    archive: Archive<R>,
    entry: Entry,
    size: u64,
    starts: Vec<u64>,
    position: u64,
    last_end: Option<u64>,
    cached: Option<(u64, Bytes)>,
    pending: Option<(u64, Load)>,
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
            last_end: None,
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
        if !this.archive.cache_allowed(
            &Key::of(&this.entry.extents[index]),
            this.archive.resolver(),
        ) {
            this.cached = None;
        }
        let cache_hit = this.cached.as_ref().is_some_and(|(start, bytes)| {
            this.position >= *start && this.position - *start < bytes.len() as u64
        });
        if !cache_hit {
            if this.pending.is_none() {
                let start = this.position;
                let extent = &this.entry.extents[index];
                let mut end = this.starts[index] + extent.size;
                // Read ahead only after a successful sequential read. Random
                // seeks reset detection. Both bytes and extent count are bounded.
                let cap = this.archive.max_read_bytes().min(8 << 20);
                if this.last_end == Some(start) {
                    for next in index + 1..this.entry.extents.len().min(index + 4) {
                        let next_end = this.starts[next] + this.entry.extents[next].size;
                        if next_end - start > cap {
                            break;
                        }
                        end = next_end;
                    }
                }
                let length = (end - start).min(this.archive.max_read_bytes());
                let archive = this.archive.clone();
                let path = this.entry.path.clone();
                let future = Box::pin(async move {
                    archive
                        .read_range_with_resolver(&path, start, length, archive.resolver())
                        .await
                });
                this.pending = Some((start, future));
            }
            let (start, future) = this.pending.as_mut().unwrap();
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.pending = None;
                    return Poll::Ready(Err(io::Error::from(error)));
                }
                Poll::Ready(Ok(raw)) => {
                    this.cached = Some((*start, raw));
                    this.pending = None;
                }
            }
        }
        let (start, raw) = this.cached.as_ref().unwrap();
        let offset = (this.position - start) as usize;
        let count = (raw.len() - offset).min(buffer.remaining());
        buffer.put_slice(&raw[offset..offset + count]);
        this.position += count as u64;
        this.last_end = Some(this.position);
        if !this.archive.cache_allowed(
            &Key::of(&this.entry.extents[index]),
            this.archive.resolver(),
        ) {
            this.cached = None;
        }
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
        this.last_end = None;
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}
