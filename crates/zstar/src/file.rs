//! Owned logical file handles over an immutable, shared archive.
use crate::read_plan::{Key, Selection};
use crate::{Archive, AsyncRangeReader, Entry, EntryMetadata, Error, Result};
use bytes::Bytes;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

// Private per-member decoded-source retention, including bookkeeping.
const DECODED_CACHE_BYTES: usize = 2 << 20;

type Load = Pin<Box<dyn Future<Output = Result<Bytes>> + Send>>;

// Immutable entry metadata and extent interpretation shared by both handles.
// Logical stream position and pending I/O remain with File.
struct FileSelection {
    selected: Selection,
    cache: Arc<DecodedCache>,
}
struct CachedSource {
    key: Key,
    bytes: Bytes,
}
// The handle fixes archive identity and verification policy for this cache.
#[derive(Default)]
pub struct DecodedCache(Mutex<Option<CachedSource>>);
impl DecodedCache {
    pub(crate) fn get(&self, key: &Key) -> Option<Bytes> {
        let mut cache = self.0.lock().unwrap();
        if cache.as_ref().is_some_and(|cached| &cached.key == key) {
            cache.as_ref().map(|cached| cached.bytes.clone())
        } else {
            *cache = None;
            None
        }
    }

    pub(crate) fn retain(&self, key: Key, bytes: &Bytes) {
        let bookkeeping = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        if bytes.len().saturating_add(bookkeeping) <= DECODED_CACHE_BYTES {
            *self.0.lock().unwrap() = Some(CachedSource {
                key,
                bytes: bytes.clone(),
            });
        }
    }
}
impl FileSelection {
    fn new(entry: Entry) -> Result<Self> {
        Ok(Self {
            selected: Selection::new(entry)?,
            cache: Arc::new(DecodedCache::default()),
        })
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
            crate::checked_range(offset, length, self.selected.size)?;
            if length > archive.max_read_bytes() {
                return Err(crate::Error::budget_exceeded(
                    crate::BudgetResource::ReadBytes,
                    "returned read size",
                    archive.max_read_bytes(),
                    length,
                ));
            }
            let length = usize::try_from(length).map_err(|_| {
                crate::Error::budget(crate::BudgetResource::ReadBytes, "returned read size")
            })?;
            if length == 0 {
                return Ok(Box::pin(std::future::ready(Ok(Bytes::new()))));
            }
            let index = self.selected.extent_index(offset);
            let first = &self.selected.entry.extents[index];
            let start = if index == 0 {
                0
            } else {
                self.selected.ends[index - 1]
            };
            let within = offset - start;
            if length as u64 > first.size - within {
                // Plan from retained metadata, preserving bounded batch execution.
                return Ok(Box::pin(archive.read_entry_range(
                    &self.selected.entry,
                    &self.selected.ends,
                    offset,
                    length as u64,
                    self.cache.clone(),
                )));
            }
            let extent = first.clone();
            let archive = archive.clone();
            let cache = self.cache.clone();
            Ok(Box::pin(async move {
                let key = Key::of(&extent);
                let raw = if let Some(raw) = cache.get(&key) {
                    raw
                } else {
                    archive
                        .content(&key, extent.encoded_size, archive.resolver())
                        .await?
                };
                crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
                cache.retain(key, &raw);
                let start = (extent.offset + within) as usize;
                let selected = raw.slice(start..start + length);
                Ok(if length < raw.len() / 4 {
                    Bytes::copy_from_slice(&selected)
                } else {
                    selected
                })
            }))
        })();
        prepared.unwrap_or_else(|error| Box::pin(std::future::ready(Err(error))))
    }
}

/// Validated random-access member selection created by [`crate::backend::open_cursor`].
///
/// The caller supplies the live archive for each operation. Positional reads
/// borrow the cursor immutably and can run concurrently. Each cursor privately
/// retains one decoded source within a 2 MiB budget; dropping it releases the cache.
/// Separate handles do not share completed decoded caches.
pub struct FileCursor {
    owner: Weak<crate::ManifestDirectory>,
    selection: FileSelection,
}
impl FileCursor {
    /// Returns the logical payload length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.selection.selected.size
    }

    pub(crate) fn new(entry: Entry, owner: Weak<crate::ManifestDirectory>) -> Result<Self> {
        Ok(Self {
            owner,
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
        &self,
        archive: &Archive<R>,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        if !archive.owns_cursor(&self.owner) {
            return Err(Error::Unsupported("cursor belongs to another archive"));
        }
        self.selection.read(archive, offset, length).await
    }
}

/// An owned regular-file handle with metadata and async file I/O.
///
/// Implements [`AsyncRead`] and [`AsyncSeek`]. Seeking beyond EOF is
/// permitted; negative and overflowing positions return an invalid-input error.
///
/// The handle retains its archive and pending read state. Dropping it cancels
/// pending source I/O. One decoded source is retained within a 2 MiB cache budget.
pub struct File<R: AsyncRangeReader + ?Sized> {
    archive: Archive<R>,
    metadata: EntryMetadata,
    selection: FileSelection,
    position: u64,
    pending: Option<(u64, Load)>,
}
impl<R: AsyncRangeReader + ?Sized + 'static> File<R> {
    pub(crate) fn new(archive: Archive<R>, entry: Entry) -> Result<Self> {
        let metadata = EntryMetadata::from_entry(&entry)?;
        let selection = FileSelection::new(entry)?;
        Ok(Self {
            archive,
            metadata,
            selection,
            position: 0,
            pending: None,
        })
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.selection.selected.entry.path
    }
    /// Returns the logical payload length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.selection.selected.size
    }
    /// Borrows the entry's immutable logical metadata.
    #[must_use]
    pub const fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }
    /// Reads an exact range without changing the stream position.
    ///
    /// Concurrent positional reads share this handle's bounded decoded cache.
    /// Cancellation leaves the stream position unchanged.
    ///
    /// # Errors
    /// Returns range, budget, source, or content-validation errors.
    pub fn read_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static + use<R> {
        match range.end.checked_sub(range.start) {
            Some(length) => self.selection.read(&self.archive, range.start, length),
            None => Box::pin(std::future::ready(Err(Error::Range))),
        }
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
        if buffer.remaining() == 0 || this.position >= this.selection.selected.size {
            return Poll::Ready(Ok(()));
        }
        if this.pending.is_none() {
            let start = this.position;
            let index = this.selection.selected.extent_index(start);
            let end = this.selection.selected.ends[index];
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
            SeekFrom::End(offset) => i128::from(this.selection.selected.size) + i128::from(offset),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Builder;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountedSource {
        bytes: Bytes,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl AsyncRangeReader for CountedSource {
        async fn size(&self) -> Result<u64> {
            self.bytes.size().await
        }
        async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.bytes.read_range(range).await
        }
    }

    #[tokio::test]
    async fn cursors_keep_archive_identity_across_clones_and_full_scans() {
        let mut builder = Builder::try_new(Vec::new()).await.unwrap();
        builder.append_bytes("file", b"hello").await.unwrap();
        let source = Arc::new(Bytes::from(builder.finish().await.unwrap()));
        let archive = Archive::open(source.clone()).await.unwrap();
        let cursor = archive.open_cursor("file").await.unwrap();
        let clone = archive.clone();
        clone.validate_all_metadata().await.unwrap();
        drop(archive);
        assert_eq!(cursor.read(&clone, 1, 3).await.unwrap(), b"ell"[..]);

        let other = Archive::open(source).await.unwrap();
        assert!(matches!(
            cursor.read(&other, 0, 1).await,
            Err(Error::Unsupported("cursor belongs to another archive"))
        ));
        drop(clone);
        assert!(cursor.owner.upgrade().is_none());
    }

    #[tokio::test]
    async fn cross_extent_reads_reuse_handle_cache_and_validate_every_selection() {
        let mut builder = Builder::try_new(Vec::new()).await.unwrap();
        builder.append_bytes("file", b"abcdef").await.unwrap();
        let source = Arc::new(CountedSource {
            bytes: builder.finish().await.unwrap().into(),
            calls: AtomicUsize::new(0),
        });
        let archive = Archive::open(source.clone()).await.unwrap();
        let mut cursor = archive.open_cursor("file").await.unwrap();
        let mut entry = cursor.selection.selected.entry.clone();
        assert_eq!(entry.extents.len(), 1);
        let mut second = entry.extents[0].clone();
        entry.extents[0].size = 3;
        second.offset += 3;
        second.size -= 3;
        entry.extents.push(second);
        cursor.selection = FileSelection::new(entry).unwrap();
        source.calls.store(0, Ordering::SeqCst);
        for _ in 0..2 {
            assert_eq!(cursor.read(&archive, 2, 2).await.unwrap(), b"cd"[..]);
        }
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        cursor.selection.selected.entry.extents[1].offset = u64::MAX;
        assert!(matches!(
            cursor.read(&archive, 3, 1).await,
            Err(Error::Range)
        ));
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    }
}
