//! Directory-first reads with independent, deduplicated metadata page loads.
use crate::{
    Archive, AsyncRangeReader, Entry, EntryKind, Error, ManifestDirectory, ManifestReader,
    ReadRequest, ReaderOptions, Result,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};

type CachedPage = Mutex<Option<Arc<ManifestReader>>>;

/// An archive opened by validating its footer and manifest directory.
/// Entry pages are cached on demand; full metadata and physical checks are explicit.
pub struct PagedState<R: AsyncRangeReader + ?Sized> {
    directory: Arc<ManifestDirectory>,
    options: ReaderOptions,
    source: Arc<R>,
    pages: Vec<CachedPage>,
    budget: Mutex<(u64, u64)>,
    directory_retained: u64,
    scan: RwLock<()>,
    full: tokio::sync::OnceCell<Arc<ManifestReader>>,
    jobs: Arc<Semaphore>,
}
impl<R: AsyncRangeReader + ?Sized + 'static> PagedState<R> {
    /// Opens the footer and directory, without retaining encoded bytes.
    /// Entry and optional extension pages are decoded only when needed.
    ///
    /// # Errors
    /// Returns transport, footer, directory, feature, or resource errors.
    pub async fn open_with_options(source: Arc<R>, options: ReaderOptions) -> Result<Self> {
        if !(1..=1024).contains(&options.concurrent_chunks) {
            return Err(Error::Budget("invalid content concurrency"));
        }
        let size = source.size().await?;
        if size < crate::FOOTER_SIZE as u64 {
            return Err(Error::NotZstar);
        }
        let raw = source
            .read_range(size - crate::FOOTER_SIZE as u64..size)
            .await?;
        if raw.len() != crate::FOOTER_SIZE {
            return Err(Error::MalformedToc("short footer".into()));
        }
        let footer = crate::parse_footer(&raw)?;
        let metadata = footer.metadata_range(size)?;
        if footer.toc_encoded_len > options.metadata.max_encoded_bytes
            || footer.toc_decoded_len > options.metadata.max_decoded_bytes
        {
            return Err(Error::Budget("manifest size"));
        }
        let start = metadata.end - footer.directory_encoded_len;
        crate::Limits::check(
            footer.directory_decoded_len,
            crate::manifest::HEADER_MAX,
            "directory size",
        )?;
        let jobs = Arc::new(Semaphore::new(2));
        let encoded = read_wrapper(
            source.as_ref(),
            start,
            footer.directory_encoded_len,
            footer.directory_decoded_len,
            options.limits,
            jobs.clone(),
        )
        .await?;
        let retained = (encoded.len() as u64).saturating_mul(8);
        if retained > options.metadata.max_retained_entry_bytes {
            return Err(Error::Budget("retained directory"));
        }
        let directory = Arc::new(
            metadata_worker(jobs.clone(), move || {
                ManifestDirectory::decode(&encoded, &footer, size)
            })
            .await?,
        );
        let pages = (0..directory.pages.len())
            .map(|_| Mutex::new(None))
            .collect();
        Ok(Self {
            directory,
            options,
            source,
            pages,
            budget: Mutex::new((retained, 0)),
            directory_retained: retained,
            scan: RwLock::new(()),
            full: tokio::sync::OnceCell::new(),
            jobs,
        })
    }
    /// Returns the validated directory without claiming page or namespace validity.
    #[must_use]
    pub fn directory(&self) -> &ManifestDirectory {
        &self.directory
    }
    async fn page(&self, index: usize) -> Result<Arc<ManifestReader>> {
        let mut cached = self.pages[index].lock().await;
        if let Some(entries) = cached.as_ref() {
            return Ok(entries.clone());
        }
        let reference = &self.directory.pages[index];
        let raw = read_wrapper(
            self.source.as_ref(),
            self.directory.data_end + reference.offset,
            reference.encoded_len,
            reference.decoded_len,
            self.options.limits,
            self.jobs.clone(),
        )
        .await?;
        let directory = self.directory.clone();
        let limits = self.options.limits;
        let (entries, cost) = metadata_worker(self.jobs.clone(), move || {
            let entries = directory.decode_entry_page(index, &raw, limits)?;
            let slots = entries.capacity() * std::mem::size_of::<Entry>()
                + (entries.len() + 1) * std::mem::size_of::<usize>();
            let cost = entries.iter().try_fold(slots as u64, |cost, entry| {
                cost.checked_add(crate::manifest::entry_allocation(entry) as u64)
                    .and_then(|cost| cost.checked_add((entry.extents.len() * 8) as u64))
                    .ok_or(Error::Budget("retained page size"))
            })?;
            Ok((entries, cost))
        })
        .await?;
        let mut budget = self.budget.lock().await;
        let retained = budget
            .0
            .checked_add(cost)
            .filter(|n| *n <= self.options.metadata.max_retained_entry_bytes)
            .ok_or(Error::Budget("retained metadata pages"))?;
        let count = budget
            .1
            .checked_add(entries.len() as u64)
            .ok_or(Error::Budget("entry count"))?;
        if self.options.metadata.max_entries != 0 && count > self.options.metadata.max_entries {
            return Err(Error::Budget("entry count"));
        }
        let root = crate::manifest::ManifestRoot {
            requires: Vec::new(),
            entry_count: entries.len() as u64,
            extensions: BTreeMap::new(),
        };
        let entries = Arc::new(
            metadata_worker(self.jobs.clone(), move || {
                ManifestReader::from_entries(root, entries, false)
            })
            .await?,
        );
        *budget = (retained, count);
        drop(budget);
        *cached = Some(entries.clone());
        drop(cached);
        Ok(entries)
    }
    async fn local_page(&self, path: &str) -> Result<Option<Arc<ManifestReader>>> {
        let _scan = self.scan.read().await;
        let page = if let Some(full) = self.full.get() {
            full.clone()
        } else if let Some(index) = self.directory.find_page(path) {
            self.page(index).await?
        } else {
            return Ok(None);
        };
        Ok(page.range_entry(path).is_some().then_some(page))
    }
    async fn selected_page(&self, path: &str) -> Result<Option<Arc<ManifestReader>>> {
        let Some(page) = self.local_page(path).await? else {
            return Ok(None);
        };
        for (i, c) in path.char_indices() {
            if c == '/' {
                let parent = &path[..i];
                let ancestor = self.local_page(parent).await?;
                if ancestor
                    .as_ref()
                    .and_then(|p| p.range_entry(parent))
                    .is_none_or(|(e, _)| e.kind != EntryKind::Directory)
                {
                    return Err(Error::MalformedToc("missing ancestor directory".into()));
                }
            }
        }
        Ok(Some(page))
    }
    /// Looks up an exact path and checks each proper ancestor as an explicit directory.
    ///
    /// # Errors
    /// Returns page, namespace, transport, or resource errors.
    pub async fn entry(&self, path: &str) -> Result<Option<Entry>> {
        let Some(page) = self.selected_page(path).await? else {
            return Ok(None);
        };
        if let crate::manifest_io::EntryLookup::Ready(entry) = page.entry_cached(path) {
            return Ok(entry);
        }
        let path = path.to_owned();
        metadata_worker(self.jobs.clone(), move || page.entry(&path)).await
    }
    pub(crate) async fn selected_manifests(
        &self,
        requests: &[ReadRequest<'_>],
    ) -> Result<BTreeMap<String, Arc<ManifestReader>>> {
        let mut pages = BTreeMap::new();
        for request in requests {
            if !pages.contains_key(request.path) {
                let page = self
                    .selected_page(request.path)
                    .await?
                    .ok_or_else(|| Error::MissingEntry(request.path.into()))?;
                pages.insert(request.path.to_owned(), page);
            }
        }
        Ok(pages)
    }
    pub(crate) async fn full_manifest(&self) -> Result<Arc<ManifestReader>> {
        if let Some(full) = self.full.get() {
            return Ok(full.clone());
        }
        let _scan = self.scan.write().await;
        // Replace the page cache with the complete index rather than retaining both.
        for page in &self.pages {
            *page.lock().await = None;
        }
        *self.budget.lock().await = (self.directory_retained, 0);
        self.full
            .get_or_try_init(|| {
                Archive::<R>::load_full_manifest(self.source.clone(), self.options.clone())
            })
            .await
            .cloned()
    }
}
async fn read_wrapper<R: AsyncRangeReader + ?Sized>(
    source: &R,
    start: u64,
    encoded_len: u64,
    decoded_len: u64,
    limits: crate::Limits,
    jobs: Arc<Semaphore>,
) -> Result<Vec<u8>> {
    crate::Limits::check(
        encoded_len - 8,
        limits.max_frame_encoded_bytes,
        "encoded metadata frame size",
    )?;
    crate::Limits::check(
        decoded_len,
        limits.max_frame_decoded_bytes,
        "decoded metadata frame size",
    )?;
    let permit = jobs
        .acquire_owned()
        .await
        .map_err(|_| Error::Unsupported("reader closed"))?;
    let bytes = source
        .read_range(start..start.checked_add(encoded_len).ok_or(Error::Range)?)
        .await?;
    if bytes.len() as u64 != encoded_len
        || bytes.len() < 13
        || bytes[..4] != crate::TOC_MAGIC.to_le_bytes()
        || u64::from(u32::from_le_bytes(
            bytes[4..8].try_into().map_err(|_| Error::Range)?,
        )) + 8
            != encoded_len
        || bytes[12] & 4 == 0
    {
        return Err(Error::MalformedToc("metadata wrapper".into()));
    }
    let limit =
        usize::try_from(decoded_len).map_err(|_| Error::Budget("metadata page address space"))?;
    run_metadata_worker(permit, move || {
        let raw =
            crate::decode_bounded_with_limits(&bytes[8..], limit, limits).map_err(|error| {
                match error {
                    Error::Budget("decoded frame size")
                        if limits.max_frame_decoded_bytes == 0
                            || decoded_len < limits.max_frame_decoded_bytes =>
                    {
                        Error::MalformedToc("page decoded length".into())
                    }
                    Error::Budget(_) | Error::Integrity => error,
                    other => Error::MalformedToc(other.to_string()),
                }
            })?;
        if raw.len() != limit {
            return Err(Error::MalformedToc("metadata decoded length".into()));
        }
        Ok(raw)
    })
    .await
}

async fn metadata_worker<T: Send + 'static>(
    jobs: Arc<Semaphore>,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let permit = jobs
        .acquire_owned()
        .await
        .map_err(|_| Error::Unsupported("reader closed"))?;
    run_metadata_worker(permit, operation).await
}

async fn run_metadata_worker<T: Send + 'static>(
    permit: tokio::sync::OwnedSemaphorePermit,
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(|_| Error::Unsupported("metadata worker failed"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_metadata_workers_keep_their_permits_until_finished() {
        let jobs = Arc::new(Semaphore::new(1));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::channel();
        let worker = tokio::spawn(metadata_worker(jobs.clone(), move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
            Ok(())
        }));
        ready.await.unwrap();
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
        let available = jobs.available_permits();
        finish.send(()).unwrap();
        assert_eq!(available, 0);
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(5), jobs.acquire())
            .await
            .unwrap()
            .unwrap();
    }
}
