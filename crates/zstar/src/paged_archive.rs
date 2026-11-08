//! Directory-first reads with independent, deduplicated metadata page loads.
use crate::manifest_io::ManifestIngest;
use crate::{
    AsyncRangeReader, Entry, EntryKind, EntryMetadata, Error, ManifestDirectory, ManifestReader,
    ReadRequest, ReaderOptions, Result,
};
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};

struct CachedPage {
    entries: Arc<ManifestReader>,
    cost: u64,
}
struct PageCache {
    pages: BTreeMap<usize, CachedPage>,
    order: VecDeque<usize>,
    retained: u64,
    seen: Vec<bool>,
    count: u64,
}
impl PageCache {
    fn new(retained: u64, page_count: usize) -> Self {
        Self {
            pages: BTreeMap::new(),
            order: VecDeque::new(),
            retained,
            seen: vec![false; page_count],
            count: 0,
        }
    }
    fn get(&mut self, index: usize) -> Option<Arc<ManifestReader>> {
        let entries = self.pages.get(&index)?.entries.clone();
        self.order.retain(|&i| i != index);
        self.order.push_back(index);
        Some(entries)
    }
}

/// An archive opened by validating its footer and manifest directory.
/// Entry pages are cached on demand; full metadata and physical checks are explicit.
pub struct PagedState<R: AsyncRangeReader + ?Sized> {
    directory: Arc<ManifestDirectory>,
    options: ReaderOptions,
    source: Arc<R>,
    page_gates: Vec<Mutex<()>>,
    cache: Mutex<PageCache>,
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
        options.validate()?;

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
        let start = metadata.end - footer.directory_encoded_len;
        crate::Limits::check(
            footer.directory_decoded_len,
            crate::manifest::HEADER_MAX,
            "directory size",
            crate::BudgetResource::MetadataPageBytes,
        )?;
        let jobs = Arc::new(Semaphore::new(2));
        let encoded = read_wrapper(
            source.as_ref(),
            start,
            footer.directory_encoded_len,
            footer.directory_decoded_len,
            options.limits,
            options.metadata.max_page_bytes,
            jobs.clone(),
        )
        .await?;
        let retained = (encoded.len() as u64).saturating_mul(8);
        if retained > options.metadata.max_retained_entry_bytes {
            return Err(crate::Error::budget_exceeded(
                crate::BudgetResource::MetadataRetainedBytes,
                "retained directory",
                options.metadata.max_retained_entry_bytes,
                retained,
            ));
        }
        let directory = Arc::new(
            metadata_worker(jobs.clone(), move || {
                ManifestDirectory::decode(&encoded, &footer, size)
            })
            .await?,
        );
        let page_gates = (0..directory.pages.len()).map(|_| Mutex::new(())).collect();
        let cache = Mutex::new(PageCache::new(retained, directory.pages.len()));
        Ok(Self {
            directory,
            options,
            source,
            page_gates,
            cache,
            directory_retained: retained,
            scan: RwLock::new(()),
            full: tokio::sync::OnceCell::new(),
            jobs,
        })
    }
    /// Returns the validated directory without claiming page or namespace validity.
    #[must_use]
    pub const fn directory(&self) -> &Arc<ManifestDirectory> {
        &self.directory
    }
    pub(crate) fn first_page(&self, prefix: &str) -> usize {
        self.directory
            .pages
            .partition_point(|page| page.last_path.as_deref().is_some_and(|last| last < prefix))
    }
    pub(crate) async fn entries_page(
        &self,
        index: usize,
        prefix: &str,
    ) -> Result<Option<Vec<EntryMetadata>>> {
        if index >= self.directory.pages.len() {
            return Ok(None);
        }
        if index > 0
            && self.directory.pages[index - 1]
                .last_path
                .as_deref()
                .is_some_and(|last| last > prefix && !last.starts_with(prefix))
        {
            return Ok(None);
        }
        let page = {
            let _scan = self.scan.read().await;
            if let Some(full) = self.full.get() {
                full.clone()
            } else {
                self.page(index).await?
            }
        };
        let lower = index
            .checked_sub(1)
            .and_then(|i| self.directory.pages[i].last_path.clone());
        let upper = self.directory.pages[index].last_path.clone();
        let prefix = prefix.to_owned();
        let entries = metadata_worker(self.jobs.clone(), move || {
            let all = page.iter().as_slice();
            let first = lower.as_deref().map_or(0, |lower| {
                all.partition_point(|entry| entry.path.as_str() <= lower)
            });
            let end = upper.as_deref().map_or(all.len(), |upper| {
                all.partition_point(|entry| entry.path.as_str() <= upper)
            });
            all[first..end]
                .iter()
                .filter(|entry| entry.path.starts_with(&prefix))
                .map(EntryMetadata::from_entry)
                .collect::<Result<Vec<_>>>()
        })
        .await?;
        for entry in &entries {
            self.validate_ancestors(entry.path()).await?;
        }
        Ok(Some(entries))
    }
    async fn page(&self, index: usize) -> Result<Arc<ManifestReader>> {
        let _gate = self.page_gates[index].lock().await;
        let cached = self.cache.lock().await.get(index);
        if let Some(entries) = cached {
            return Ok(entries);
        }
        let reference = &self.directory.pages[index];
        let raw = read_wrapper(
            self.source.as_ref(),
            self.directory.data_end + reference.offset,
            reference.encoded_len,
            reference.decoded_len,
            self.options.limits,
            self.options.metadata.max_page_bytes,
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
                    .ok_or(crate::Error::budget(
                        crate::BudgetResource::MetadataRetainedBytes,
                        "retained page size",
                    ))
            })?;
            Ok((entries, cost))
        })
        .await?;
        let minimum = self.directory_retained.checked_add(cost).ok_or_else(|| {
            Error::budget(
                crate::BudgetResource::MetadataRetainedBytes,
                "retained page size",
            )
        })?;
        crate::Limits::check(
            minimum,
            self.options.metadata.max_retained_entry_bytes,
            "retained metadata page",
            crate::BudgetResource::MetadataRetainedBytes,
        )?;
        let entry_count = entries.len() as u64;
        let root = crate::manifest::ManifestRoot {
            requires: Vec::new(),
            entry_count: entries.len() as u64,
            extensions: BTreeMap::new(),
        };
        let entries = Arc::new(
            metadata_worker(self.jobs.clone(), move || {
                ManifestReader::from_entries(root, entries)
            })
            .await?,
        );
        let mut cache = self.cache.lock().await;
        let count = if cache.seen[index] {
            cache.count
        } else {
            cache.count.checked_add(entry_count).ok_or_else(|| {
                Error::budget(crate::BudgetResource::MetadataEntries, "entry count")
            })?
        };
        crate::Limits::check(
            count,
            self.options.metadata.max_entries,
            "entry count",
            crate::BudgetResource::MetadataEntries,
        )?;
        while cache
            .retained
            .checked_add(cost)
            .is_none_or(|size| size > self.options.metadata.max_retained_entry_bytes)
        {
            let oldest = cache.order.pop_front().ok_or_else(|| {
                Error::budget(
                    crate::BudgetResource::MetadataRetainedBytes,
                    "retained metadata page",
                )
            })?;
            cache.retained -= cache.pages.remove(&oldest).unwrap().cost;
        }
        cache.retained += cost;
        cache.count = count;
        cache.seen[index] = true;
        cache.order.push_back(index);
        cache.pages.insert(
            index,
            CachedPage {
                entries: entries.clone(),
                cost,
            },
        );
        drop(cache);
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
        self.validate_ancestors(path).await?;
        Ok(Some(page))
    }
    async fn validate_ancestors(&self, path: &str) -> Result<()> {
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
        Ok(())
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
        metadata_worker(self.jobs.clone(), move || Ok(page.entry(&path))).await
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
        // Replace cached pages with the complete index rather than retaining both.
        *self.cache.lock().await =
            PageCache::new(self.directory_retained, self.directory.pages.len());
        self.full
            .get_or_try_init(|| self.load_full_manifest())
            .await
            .cloned()
    }
    async fn load_full_manifest(&self) -> Result<Arc<ManifestReader>> {
        let size = self.source.size().await?;
        if size < crate::FOOTER_SIZE as u64 {
            return Err(Error::NotZstar);
        }
        let footer = crate::parse_footer(
            &self
                .source
                .read_range(size - crate::FOOTER_SIZE as u64..size)
                .await?,
        )?;
        let metadata_range = footer.metadata_range(size)?;
        let mut ranges = MetadataSource {
            source: self.source.as_ref(),
            end: metadata_range.end,
            start: metadata_range.start,
            buffer: Bytes::new(),
        };
        let options = crate::options::reader_manifest_options(
            &self.options.metadata,
            self.options.limits,
            self.options.max_read_bytes,
        );
        let mut ingest = ManifestIngest::new(footer, size, options)?;
        while !ingest.complete() {
            let at = ingest.position();
            let header = ranges.read(ingest.header_range()?).await?;
            let len = ingest.encoded_length(&header)?;
            let encoded = ranges.read(at + 8..at + 8 + len).await?;
            // Workers own the ingest and retain a permit through cancellation.
            ingest = metadata_worker(self.jobs.clone(), move || {
                ingest.push(&header, &encoded)?;
                Ok(ingest)
            })
            .await?;
        }
        metadata_worker(self.jobs.clone(), move || ingest.finish())
            .await
            .map(Arc::new)
    }
}
struct MetadataSource<'a, R: AsyncRangeReader + ?Sized> {
    source: &'a R,
    end: u64,
    start: u64,
    buffer: Bytes,
}
impl<R: AsyncRangeReader + ?Sized> MetadataSource<'_, R> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    async fn read(&mut self, range: Range<u64>) -> Result<Bytes> {
        crate::checked_range(range.start, range.end - range.start, self.end)?;
        if range.start >= self.start && range.end - self.start <= self.buffer.len() as u64 {
            return Ok(self
                .buffer
                .slice((range.start - self.start) as usize..(range.end - self.start) as usize));
        }
        let length = (range.end - range.start)
            .max(64 << 10)
            .min(self.end - range.start);
        self.buffer = self
            .source
            .read_range(range.start..range.start + length)
            .await?;
        if self.buffer.len() as u64 != length {
            return Err(Error::MalformedToc("short metadata range".into()));
        }
        self.start = range.start;
        Ok(self.buffer.slice(..(range.end - range.start) as usize))
    }
}

async fn read_wrapper<R: AsyncRangeReader + ?Sized>(
    source: &R,
    start: u64,
    encoded_len: u64,
    decoded_len: u64,
    limits: crate::Limits,
    max_page_bytes: u64,
    jobs: Arc<Semaphore>,
) -> Result<Vec<u8>> {
    crate::manifest_io::check_page_size(encoded_len - 8, max_page_bytes)?;
    crate::manifest_io::check_page_size(decoded_len, max_page_bytes)?;
    crate::Limits::check(
        encoded_len - 8,
        limits.max_frame_encoded_bytes,
        "encoded metadata frame size",
        crate::BudgetResource::EncodedFrameBytes,
    )?;
    crate::Limits::check(
        decoded_len,
        limits.max_frame_decoded_bytes,
        "decoded metadata frame size",
        crate::BudgetResource::DecodedFrameBytes,
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
    let limit = usize::try_from(decoded_len).map_err(|_| {
        crate::Error::budget(
            crate::BudgetResource::MetadataPageBytes,
            "metadata page address space",
        )
    })?;
    run_metadata_worker(permit, move || {
        let raw =
            crate::decode_bounded_with_limits(&bytes[8..], limit, limits).map_err(|error| {
                match error {
                    Error::Budget(crate::BudgetError {
                        resource: crate::BudgetResource::DecodedFrameBytes,
                        ..
                    }) if limits.max_frame_decoded_bytes == 0
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
