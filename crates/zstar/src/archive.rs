//! Concurrent selective reads for the entry manifest, without a frame table.
use crate::decoded_body::DecodedBody;
use crate::manifest::Entry;
use crate::manifest_io::{EntryCursor, ManifestReader};
use crate::read_plan::{Key, Plan, build_plan, record, validate_copy};
use crate::{
    AsyncRangeReader, EntryMetadata, Error, MAX_BATCH_REQUESTS, ReadReport, ReadRequest, Result,
};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

/// Resolves external objects by the SHA-256 digest of their decoded bytes.
///
/// Implementations return bounded encoded bytes; [`Archive`] handles decoding
/// and the configured hash policy.
#[async_trait::async_trait]
pub trait ObjectResolver: Send + Sync {
    /// Returns a complete encoded object of at most `max_bytes` bytes.
    ///
    /// `hint` is an advisory encoded size, not a decoded-size bound. Return
    /// [`Error::MissingObject`] if the object is unavailable.
    async fn get_encoded(&self, address: [u8; 32], hint: u64, max_bytes: u64) -> Result<Bytes>;
}

/// Metadata, concurrency, and external-object policies for an [`Archive`].
#[derive(Clone)]
pub struct ReaderOptions {
    /// Metadata parsing, retained index, and temporary storage policy.
    pub metadata: crate::MetadataReaderOptions,
    /// Per-source frame/window and entry/extent/pointer limits. Zero disables each limit.
    pub limits: crate::Limits,
    /// Maximum bytes returned by one allocating read or batch; defaults to 64 MiB.
    pub max_read_bytes: u64,
    /// Bounds both outstanding content requests and detached decode jobs.
    pub concurrent_chunks: usize,
    /// Fixed reader-local external hash policy. Data checksums are mandatory.
    pub verify_external: bool,
    /// External objects are resolved through this reader-wide policy.
    pub resolver: Option<Arc<dyn ObjectResolver>>,
}
impl ReaderOptions {
    /// Finite resource policy with four concurrent chunks and an 8 MiB returned
    /// buffer budget. Encoded/decoded frame and window limits are each 64 MiB.
    /// Live file caches and caller-retained buffers have separate ownership.
    #[must_use]
    pub fn bounded() -> Self {
        Self {
            metadata: crate::MetadataReaderOptions::bounded(),
            limits: crate::Limits::bounded(),
            max_read_bytes: 8 << 20,
            concurrent_chunks: 4,
            ..Self::default()
        }
    }
    /// Checks configuration without reading the source.
    ///
    /// # Errors
    /// Returns an invalid-options error for zero read budgets, invalid concurrency,
    /// or invalid metadata page limits.
    pub fn validate(&self) -> Result<()> {
        if !(1..=1024).contains(&self.concurrent_chunks) {
            return Err(Error::InvalidOptions {
                option: "concurrent_chunks",
                reason: "must be between 1 and 1024",
            });
        }
        if self.max_read_bytes == 0 {
            return Err(Error::InvalidOptions {
                option: "max_read_bytes",
                reason: "must be positive",
            });
        }
        self.metadata.validate()
    }
    crate::options::with_fields! { const
        /// Sets metadata parsing and storage policy.
        with_metadata(metadata: crate::MetadataReaderOptions);
    }
    crate::options::with_fields! { const
        /// Sets per-frame and per-entry resource limits.
        with_limits(limits: crate::Limits);
        /// Sets the maximum returned bytes per allocating read or batch.
        with_max_read_bytes(max_read_bytes: u64);
        /// Enables SHA-256 verification of decoded external objects.
        with_verify_external(verify_external: bool);
    }
    /// Sets the reader-wide external object resolver.
    #[must_use]
    pub fn with_resolver(mut self, value: Arc<dyn ObjectResolver>) -> Self {
        self.resolver = Some(value);
        self
    }
    crate::options::with_fields! { const
        /// Sets concurrent content requests and decode jobs, from one through 1024.
        with_concurrent_chunks(concurrent_chunks: usize);
    }
}
impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            metadata: crate::MetadataReaderOptions::default(),
            limits: crate::Limits::default(),
            max_read_bytes: crate::options::DEFAULT_MAX_READ_BYTES,
            concurrent_chunks: 8,
            verify_external: false,
            resolver: None,
        }
    }
}
impl std::fmt::Debug for ReaderOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderOptions")
            .field("metadata", &self.metadata)
            .field("limits", &self.limits)
            .field("max_read_bytes", &self.max_read_bytes)
            .field("concurrent_chunks", &self.concurrent_chunks)
            .field("verify_external", &self.verify_external)
            .field("resolver", &self.resolver.as_ref().map(|_| "configured"))
            .finish()
    }
}
struct Flight {
    gate: AsyncMutex<()>,
    value: Mutex<Option<DecodedBody>>,
    /// Leader failure for archive-frame keys, handed to waiters that queued
    /// behind the leader instead of letting each repeat the failed load.
    failure: Mutex<Option<Error>>,
}
type Flights = Mutex<HashMap<Key, Weak<Flight>>>;
/// Owns one participant's handle to a flight. The handle is released while the
/// map lock is held, so exactly the last participant observes a count of one
/// and removes the dead entry (no leaked `Weak`).
struct Cleanup<'a> {
    flights: &'a Flights,
    key: &'a Key,
    flight: Option<Arc<Flight>>,
}
impl Cleanup<'_> {
    const fn flight(&self) -> &Arc<Flight> {
        self.flight.as_ref().unwrap()
    }
}
impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock().unwrap();
        let flight = self.flight.take().unwrap();
        if Arc::strong_count(&flight) == 1
            && flights
                .get(self.key)
                .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(&flight)))
        {
            flights.remove(self.key);
        }
        drop(flight);
        drop(flights);
    }
}
/// An indexed archive backed by an immutable [`AsyncRangeReader`].
///
/// Opening validates the metadata directory and loads entry pages on demand.
/// Clones share the source, metadata cache, and concurrency limits.
/// Selected content is validated before it is returned.
///
/// Dropping a read future cancels pending transport I/O.
/// CPU jobs that have already started retain their permits until completion.
///
/// # Metadata integrity
///
/// Validated metadata is immutable and cannot be replaced through the reader:
///
/// ```compile_fail
/// use bytes::Bytes;
/// use zstar::Archive;
/// fn replace(mut archive: Archive<Bytes>, other: Archive<Bytes>) {
///     archive.paged = other.paged;
/// }
/// ```
pub struct Archive<R: AsyncRangeReader + ?Sized> {
    source: Arc<R>,
    paged: Arc<crate::paged_archive::PagedState<R>>,
    options: ReaderOptions,
    semaphore: Arc<Semaphore>,
    metadata_semaphore: Arc<Semaphore>,
    flights: Arc<Flights>,
}
impl Archive<Bytes> {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Opens in-memory archive bytes with default [`ReaderOptions`].
    pub async fn from_bytes(bytes: impl Into<Bytes>) -> Result<Self> {
        Self::open(Arc::new(bytes.into())).await
    }
}
#[cfg(feature = "fs")]
impl Archive<crate::fs::AsyncLocalFileSource> {
    /// Opens an immutable local archive path using positional filesystem reads.
    ///
    /// # Errors
    /// Returns filesystem, format, validation, or resource-limit errors.
    pub async fn open_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::open_path_with_options(path, ReaderOptions::default()).await
    }
    /// Opens an immutable local path with explicit reader policy.
    ///
    /// # Errors
    /// Returns filesystem, format, validation, or resource-limit errors.
    pub async fn open_path_with_options(
        path: impl AsRef<std::path::Path>,
        options: ReaderOptions,
    ) -> Result<Self> {
        Self::open_with_options(
            Arc::new(crate::fs::AsyncLocalFileSource::open(path).await?),
            options,
        )
        .await
    }
}
impl<R: AsyncRangeReader + ?Sized> Clone for Archive<R> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            paged: self.paged.clone(),
            options: self.options.clone(),
            semaphore: self.semaphore.clone(),
            metadata_semaphore: self.metadata_semaphore.clone(),
            flights: self.flights.clone(),
        }
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> Archive<R> {
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Opens an immutable source with default [`ReaderOptions`].
    pub async fn open(source: Arc<R>) -> Result<Self> {
        Self::open_with_options(source, ReaderOptions::default()).await
    }
    pub(crate) fn resolver(&self) -> Option<&dyn ObjectResolver> {
        self.options.resolver.as_deref()
    }
    /// Scans metadata pages and returns the derived member count.
    ///
    /// # Errors
    /// Returns errors from any metadata page or the complete namespace.
    pub async fn entry_count(&self) -> Result<u64> {
        Ok(self.paged.full_manifest().await?.root().entry_count)
    }
    /// Maximum bytes returned by a single allocating read operation.
    #[must_use]
    pub const fn max_read_bytes(&self) -> u64 {
        self.options.max_read_bytes
    }
    /// Returns the archive-level extension values.
    #[must_use]
    pub fn root_extensions(&self) -> &crate::Extensions {
        &self.paged.directory().root.extensions
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads an entire regular file within the configured returned-read budget.
    ///
    /// Use [`Self::open_file`] to stream files larger than that budget.
    pub async fn read(&self, path: &str) -> Result<Bytes> {
        let entry = self
            .entry_layout(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        self.read_range(path, 0..entry.size()?).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads a half-open byte range from a regular file.
    ///
    /// Returns an error for a missing entry, an invalid range, or an exceeded budget.
    pub async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        self.read_range_with_resolver(path, range.start, length, self.resolver())
            .await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads an entire file and reports the integrity checks completed.
    pub async fn read_with_report(&self, path: &str) -> Result<(Bytes, ReadReport)> {
        let entry = self
            .entry_layout(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        self.read_range_with_report(path, 0..entry.size()?).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads a half-open file range and reports the integrity checks completed.
    pub async fn read_range_with_report(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Result<(Bytes, ReadReport)> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        self.read_range_with_report_and_resolver(path, range.start, length, self.resolver())
            .await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Opens an owned regular-file handle implementing Tokio async I/O traits.
    ///
    /// Returns an error if the path is missing or identifies a directory or symlink.
    pub async fn open_file(&self, path: &str) -> Result<crate::File<R>> {
        let entry = self
            .entry_layout(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        self.file(entry).await
    }
    /// Opens a reusable cursor without retaining this archive's ownership.
    pub(crate) async fn open_cursor(&self, path: &str) -> Result<crate::file::FileCursor> {
        let entry = self
            .entry_layout(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        crate::file::FileCursor::new(entry, Arc::downgrade(self.paged.directory()))
    }
    async fn file(&self, entry: Entry) -> Result<crate::File<R>> {
        if entry.extents.len() <= 64 {
            return crate::File::new(self.clone(), entry);
        }
        // Constructing a large seek index is CPU work, like cloning its metadata.
        let archive = self.clone();
        tokio::task::spawn_blocking(move || crate::File::new(archive, entry))
            .await
            .map_err(worker_error)?
    }
    /// Streams immutable metadata in UTF-8 path order, loading pages on demand.
    ///
    /// Each yielded entry and its ancestors are validated. Optional extension
    /// pages are not scanned; use [`Self::validate_all_metadata`] for a complete
    /// metadata check. The stream can outlive this archive and is `Unpin`.
    pub fn entries(
        &self,
    ) -> impl stream::FusedStream<Item = Result<EntryMetadata>> + Send + Unpin + 'static + use<R>
    {
        self.entries_with_prefix("")
    }

    /// Streams paths beginning with the exact string prefix. Loads candidate
    /// entry pages and any pages required to validate ancestors.
    pub fn entries_with_prefix(
        &self,
        prefix: &str,
    ) -> impl stream::FusedStream<Item = Result<EntryMetadata>> + Send + Unpin + 'static + use<R>
    {
        let page = self.paged.first_page(prefix);
        let state = (
            self.clone(),
            page,
            std::collections::VecDeque::new(),
            prefix.to_owned(),
        );
        stream::try_unfold(
            state,
            |(archive, mut page, mut pending, prefix)| async move {
                loop {
                    if let Some(entry) = pending.pop_front() {
                        return Ok(Some((entry, (archive, page, pending, prefix))));
                    }
                    let Some(entries) = archive.paged.entries_page(page, &prefix).await? else {
                        return Ok(None);
                    };
                    page += 1;
                    pending.extend(entries);
                }
            },
        )
        .boxed()
        .fuse()
    }

    /// Validates every metadata page, optional extension, and the complete namespace.
    /// Content frames and external objects are checked only when read.
    ///
    /// # Errors
    /// Returns metadata, source, or resource-limit errors from the complete scan.
    pub async fn validate_all_metadata(&self) -> Result<()> {
        self.paged.full_manifest().await.map(|_| ())
    }

    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Opens and validates an immutable source using the supplied policies.
    pub async fn open_with_options(source: Arc<R>, options: ReaderOptions) -> Result<Self> {
        let paged =
            crate::paged_archive::PagedState::open_with_options(source.clone(), options.clone())
                .await?;
        Ok(Self {
            source,
            paged: Arc::new(paged),
            semaphore: Arc::new(Semaphore::new(options.concurrent_chunks)),
            metadata_semaphore: Arc::new(Semaphore::new(2)),
            flights: Arc::new(Mutex::new(HashMap::new())),
            options,
        })
    }
    async fn metadata<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&ManifestReader) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let manifest = self.paged.full_manifest().await?;
        let permit = self
            .metadata_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("reader closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(&manifest)
        })
        .await
        .map_err(worker_error)?
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Returns immutable logical metadata, or `None` if the path is absent.
    pub async fn entry(&self, path: &str) -> Result<Option<EntryMetadata>> {
        self.entry_layout(path)
            .await?
            .as_ref()
            .map(EntryMetadata::from_entry)
            .transpose()
    }
    /// Returns the validated physical layout for format tooling and bindings.
    /// Prefer [`Self::entry`] for ordinary metadata access.
    ///
    /// # Errors
    /// Returns metadata, ancestor validation, source, or resource-limit errors.
    pub async fn entry_layout(&self, path: &str) -> Result<Option<Entry>> {
        self.paged.entry(path).await
    }
    /// Creates a cursor for bounded metadata batches.
    ///
    /// Prefer [`Self::entries`] for ordinary enumeration.
    #[doc(hidden)]
    pub async fn cursor(&self) -> Result<EntryCursor> {
        self.cursor_prefix("").await
    }
    /// Creates a bounded cursor over paths beginning with `prefix`.
    #[doc(hidden)]
    pub async fn cursor_prefix(&self, prefix: &str) -> Result<EntryCursor> {
        let prefix = prefix.to_owned();
        self.metadata(move |manifest| Ok(manifest.cursor_prefix(&prefix)))
            .await
    }
    /// Returns the advanced cursor with at most `limit` owned entries.
    #[doc(hidden)]
    pub async fn next_entries(
        &self,
        mut cursor: EntryCursor,
        limit: usize,
    ) -> Result<(EntryCursor, Vec<Entry>)> {
        if limit == 0 || limit > MAX_BATCH_REQUESTS {
            return Err(crate::Error::budget(
                crate::BudgetResource::BatchRequests,
                "cursor batch size",
            ));
        }
        self.metadata(move |manifest| {
            let mut entries = Vec::new();
            for _ in 0..limit {
                let Some(entry) = manifest.next_entry(&mut cursor)? else {
                    break;
                };
                entries.push(entry);
            }
            Ok((cursor, entries))
        })
        .await
    }
    /// Returns a symlink target verbatim, without resolving or following it.
    ///
    /// # Errors
    /// Returns an error if the entry is missing, is not a symlink, or cannot be read.
    pub async fn read_link(&self, path: &str) -> Result<String> {
        self.entry_layout(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?
            .linkname
            .ok_or(Error::Unsupported("entry is not a symlink"))
    }
    pub(crate) fn owns_cursor(&self, owner: &Weak<crate::ManifestDirectory>) -> bool {
        Weak::ptr_eq(owner, &Arc::downgrade(self.paged.directory()))
    }
    #[expect(
        clippy::option_if_let_else,
        reason = "Explicit branches keep validation and state transitions readable."
    )]
    pub(crate) async fn content(
        &self,
        key: &Key,
        hint: u64,
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<Bytes> {
        // Per-call resolvers have no stable namespace. Unchecked objects may
        // share a group within a batch, but cannot share retained bytes or a
        // flight with another operation's resolver.
        if matches!(key, Key::Object(_)) && !self.options.verify_external {
            return Ok(self.load_content(key, hint, resolver).await?.into_bytes());
        }
        let flight = {
            let mut flights = self.flights.lock().unwrap();
            if let Some(flight) = flights.get(key).and_then(Weak::upgrade) {
                flight
            } else {
                let flight = Arc::new(Flight {
                    gate: AsyncMutex::new(()),
                    value: Mutex::new(None),
                    failure: Mutex::new(None),
                });
                flights.insert(key.clone(), Arc::downgrade(&flight));
                flight
            }
        };
        let cleanup = Cleanup {
            flights: &self.flights,
            key,
            flight: Some(flight),
        };
        let flight = cleanup.flight();
        let _guard = flight.gate.lock().await;
        let cached = flight.value.lock().unwrap().clone();
        if let Some(raw) = cached {
            return Ok(raw.into_bytes());
        }
        if let Some(error) = flight.failure.lock().unwrap().as_ref() {
            return Err(error.duplicate());
        }
        let raw = match self.load_content(key, hint, resolver).await {
            Ok(raw) => raw,
            Err(error) => {
                // Object keys depend on each caller's resolver, so only
                // archive frames (same immutable source) share the failure.
                if matches!(key, Key::Archive(..)) {
                    *flight.failure.lock().unwrap() = Some(error.duplicate());
                }
                return Err(error);
            }
        };
        *flight.value.lock().unwrap() = Some(raw.clone());
        Ok(raw.into_bytes())
    }
    async fn load_content(
        &self,
        key: &Key,
        hint: u64,
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<DecodedBody> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("reader closed"))?;
        let encoded = match key {
            Key::Archive(at, size) => {
                let range = *at..at.checked_add(*size).ok_or(Error::Range)?;
                let encoded = self.source.read_range(range).await?;
                if encoded.len() as u64 != *size {
                    return Err(Error::MalformedFrame("short archive frame".into()));
                }
                encoded
            }
            Key::Object(hash) => {
                resolver
                    .ok_or(Error::Unsupported("external read requires resolver"))?
                    .get_encoded(*hash, hint, self.options.limits.encoded_limit())
                    .await?
            }
        };
        let key_copy = key.clone();
        let verify = self.options.verify_external;
        let limits = self.options.limits;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let raw = crate::read_plan::decode(&key_copy, &encoded, verify, limits)?;
            Ok::<_, Error>(raw)
        })
        .await
        .map_err(worker_error)?
    }
    async fn plan(&self, requests: &[ReadRequest<'_>]) -> Result<Plan> {
        if requests.len() > MAX_BATCH_REQUESTS {
            return Err(crate::Error::budget(
                crate::BudgetResource::BatchRequests,
                "batch request count",
            ));
        }
        let selected = self.paged.selected_manifests(requests).await?;
        let requests: Vec<_> = requests
            .iter()
            .map(|r| (r.path.to_owned(), r.offset, r.length))
            .collect();
        let budget = self.options.max_read_bytes;
        tokio::task::spawn_blocking(move || {
            build_plan(
                requests
                    .iter()
                    .map(|(path, offset, length)| (path.as_str(), *offset, *length)),
                budget,
                |path| Ok(selected.get(path).and_then(|page| page.range_entry(path))),
            )
        })
        .await
        .map_err(worker_error)?
    }
    // Handles already own validated metadata; do not look up their paths
    // again when a logical selection crosses extent boundaries.
    pub(crate) fn read_entry_range(
        &self,
        entry: &Entry,
        ends: &[u64],
        offset: u64,
        length: u64,
        cache: Arc<crate::file::DecodedCache>,
    ) -> impl std::future::Future<Output = Result<Bytes>> + Send + use<R> {
        let plan = build_plan(
            std::iter::once((entry.path.as_str(), offset, length)),
            self.max_read_bytes(),
            |_| Ok(Some((entry, ends))),
        );
        let archive = self.clone();
        async move {
            let mut outputs = archive
                .execute_plan(plan?, archive.resolver(), None, Some(&cache))
                .await?;
            Ok(outputs.pop().unwrap())
        }
    }
    #[doc(hidden)]
    pub async fn read_range_with_resolver(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<Bytes> {
        let mut outputs = self
            .execute(
                &[ReadRequest {
                    path,
                    offset,
                    length,
                }],
                resolver,
                None,
                true,
            )
            .await?;
        Ok(outputs.pop().unwrap())
    }
    #[doc(hidden)]
    pub async fn read_range_with_report_and_resolver(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<(Bytes, ReadReport)> {
        let mut report = ReadReport {
            metadata_validated: true,
            ..Default::default()
        };
        let mut outputs = self
            .execute(
                &[ReadRequest {
                    path,
                    offset,
                    length,
                }],
                resolver,
                Some(&mut report),
                true,
            )
            .await?;
        Ok((outputs.pop().unwrap(), report))
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Reads file ranges in request order, sharing loads of common content.
    pub async fn read_many(&self, requests: &[ReadRequest<'_>]) -> Result<Vec<Bytes>> {
        self.read_many_with_resolver(requests, self.options.resolver.as_deref())
            .await
    }
    #[doc(hidden)]
    pub async fn read_many_with_resolver(
        &self,
        requests: &[ReadRequest<'_>],
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<Vec<Bytes>> {
        self.execute(requests, resolver, None, false).await
    }
    async fn execute(
        &self,
        requests: &[ReadRequest<'_>],
        resolver: Option<&dyn ObjectResolver>,
        mut report: Option<&mut ReadReport>,
        borrow_single: bool,
    ) -> Result<Vec<Bytes>> {
        let plan = self.plan(requests).await?;
        if borrow_single && plan.groups.len() == 1 {
            let (key, group) = plan.groups.into_iter().next().unwrap();
            if group.copies.len() == 1 {
                let raw = self.content(&key, group.hint, resolver).await?;
                let copy = &group.copies[0];
                validate_copy(copy, raw.len())?;
                record(report.as_deref_mut(), &key, self.options.verify_external);
                let whole = raw;
                let slice = whole.slice(copy.source.clone());
                // A small borrowed slice would pin the whole decoded frame
                // for as long as the caller keeps it. Copy
                // out when the slice is under a quarter of the frame.
                return Ok(vec![if slice.len() < whole.len() / 4 {
                    Bytes::copy_from_slice(&slice)
                } else {
                    slice
                }]);
            }
            return self
                .execute_plan(
                    Plan {
                        sizes: plan.sizes,
                        groups: BTreeMap::from([(key, group)]),
                    },
                    resolver,
                    report,
                    None,
                )
                .await;
        }
        self.execute_plan(plan, resolver, report, None).await
    }
    async fn execute_plan(
        &self,
        plan: Plan,
        resolver: Option<&dyn ObjectResolver>,
        mut report: Option<&mut ReadReport>,
        cache: Option<&crate::file::DecodedCache>,
    ) -> Result<Vec<Bytes>> {
        let mut outputs: Vec<_> = plan
            .sizes
            .iter()
            .map(|size| BytesMut::zeroed(*size))
            .collect();
        let mut jobs = stream::iter(plan.groups.into_iter().map(|(key, group)| async move {
            let raw = if let Some(raw) = cache.and_then(|cache| cache.get(&key)) {
                raw
            } else {
                self.content(&key, group.hint, resolver).await?
            };
            for copy in &group.copies {
                validate_copy(copy, raw.len())?;
            }
            if let Some(cache) = cache {
                cache.retain(key.clone(), &raw);
            }
            Ok::<_, Error>((key, group, raw))
        }))
        .buffer_unordered(self.options.concurrent_chunks);
        while let Some(job) = jobs.next().await {
            let (key, group, raw) = job?;
            record(report.as_deref_mut(), &key, self.options.verify_external);
            for copy in group.copies {
                outputs[copy.output][copy.destination].copy_from_slice(&raw[copy.source]);
            }
        }
        Ok(outputs.into_iter().map(BytesMut::freeze).collect())
    }
}
fn worker_error(_: tokio::task::JoinError) -> Error {
    Error::Io(std::io::Error::other("entry reader worker failed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntryKind;
    use crate::blocking::Builder as BlockingBuilder;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    fn fixture() -> Vec<u8> {
        let mut writer = BlockingBuilder::try_with_options(
            Vec::new(),
            crate::WriterOptions {
                frame_size: 2048,
                compression_level: 3,
                ..Default::default()
            },
        )
        .unwrap();
        for (path, raw) in [
            ("a", b"hello".to_vec()),
            ("b", b"world".to_vec()),
            ("c", vec![7; 10000]),
        ] {
            writer
                .add_file(
                    Entry {
                        path: path.into(),
                        kind: EntryKind::File,
                        mode: 420,
                        uid: 0,
                        gid: 0,
                        mtime_ns: 0,
                        extents: vec![],
                        linkname: None,
                        attributes: BTreeMap::default(),
                    },
                    raw.len() as u64,
                    &mut raw.as_slice(),
                )
                .unwrap();
        }
        writer.finish().unwrap()
    }
    struct Source {
        bytes: Bytes,
        data_end: u64,
        calls: AtomicUsize,
        data_calls: AtomicUsize,
        blocked: AtomicBool,
        active: AtomicUsize,
        started: Notify,
        release: Notify,
    }
    impl Source {
        fn new(bytes: Vec<u8>) -> Self {
            let data_end = crate::parse_footer(&bytes[bytes.len() - crate::FOOTER_SIZE..])
                .unwrap()
                .metadata_range(bytes.len() as u64)
                .unwrap()
                .start;
            Self {
                bytes: bytes.into(),
                data_end,
                calls: AtomicUsize::new(0),
                data_calls: AtomicUsize::new(0),
                blocked: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                started: Notify::new(),
                release: Notify::new(),
            }
        }
    }
    struct Active<'a>(&'a AtomicUsize);
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl AsyncRangeReader for Source {
        async fn size(&self) -> Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
        )]
        async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if range.start < self.data_end && range.end <= self.data_end {
                self.data_calls.fetch_add(1, Ordering::SeqCst);
                self.active.fetch_add(1, Ordering::SeqCst);
                let _active = Active(&self.active);
                self.started.notify_one();
                if self.blocked.load(Ordering::SeqCst) {
                    self.release.notified().await;
                }
            }
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }
    #[tokio::test]
    async fn selective_and_batch_reads_share_chunks_and_validate_before_content_io() {
        let source = Arc::new(Source::new(fixture()));
        let reader = Archive::open_with_options(source.clone(), ReaderOptions::default())
            .await
            .unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 2); // Footer and directory.
        let requests = [
            ReadRequest {
                path: "a",
                offset: 1,
                length: 3,
            },
            ReadRequest {
                path: "b",
                offset: 0,
                length: 5,
            },
            ReadRequest {
                path: "a",
                offset: 5,
                length: 0,
            },
            ReadRequest {
                path: "a",
                offset: 1,
                length: 3,
            },
        ];
        assert_eq!(
            reader.read_many(&requests).await.unwrap(),
            [
                Bytes::from_static(b"ell"),
                Bytes::from_static(b"world"),
                Bytes::new(),
                Bytes::from_static(b"ell")
            ]
        );
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        let bad = [
            requests[0],
            ReadRequest {
                path: "c",
                offset: 10000,
                length: 1,
            },
        ];
        assert!(reader.read_many(&bad).await.is_err());
        assert!(
            reader
                .read_many(&vec![requests[2]; MAX_BATCH_REQUESTS + 1])
                .await
                .is_err()
        );
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            reader.read_range("c", 1023..6023).await.unwrap(),
            vec![7; 5000]
        );
        let (raw, report) = reader
            .read_range_with_report_and_resolver("a", 0, 5, None)
            .await
            .unwrap();
        assert_eq!(raw, b"hello".as_slice());
        assert!(report.metadata_validated);
        assert_eq!(report.data_frames_checked.len(), 1);
        let mut cursor = reader.cursor().await.unwrap();
        let mut names = Vec::new();
        loop {
            let (next, entries) = reader.next_entries(cursor, 2).await.unwrap();
            cursor = next;
            if entries.is_empty() {
                break;
            }
            names.extend(entries.into_iter().map(|e| e.path));
        }
        assert_eq!(names, ["a", "b", "c"]);
    }
    #[tokio::test]
    async fn concurrent_misses_coalesce_even_without_a_retained_cache() {
        let source = Arc::new(Source::new(fixture()));
        let reader = Arc::new(
            Archive::open_with_options(source.clone(), ReaderOptions::default())
                .await
                .unwrap(),
        );
        source.blocked.store(true, Ordering::SeqCst);
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.read_range("a", 0..5).await.unwrap() })
            })
            .collect();
        timeout(Duration::from_secs(2), async {
            loop {
                let waiting = reader
                    .flights
                    .lock()
                    .unwrap()
                    .values()
                    .any(|w| w.strong_count() == 8);
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        source.release.notify_waiters();
        for job in jobs {
            assert_eq!(job.await.unwrap(), b"hello".as_slice());
        }
        assert!(reader.flights.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn cancellation_drops_pending_io_and_releases_chunk_slots() {
        let source = Arc::new(Source::new(fixture()));
        let reader = Arc::new(
            Archive::open_with_options(
                source.clone(),
                ReaderOptions {
                    concurrent_chunks: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        source.blocked.store(true, Ordering::SeqCst);
        let first = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read_range("a", 0..5).await })
        };
        timeout(Duration::from_secs(2), source.started.notified())
            .await
            .unwrap();
        let next = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read_range("c", 9000..9001).await })
        };
        assert_eq!(source.active.load(Ordering::SeqCst), 1);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        timeout(Duration::from_secs(2), source.started.notified())
            .await
            .unwrap();
        assert_eq!(source.active.load(Ordering::SeqCst), 1);
        next.abort();
        assert!(next.await.unwrap_err().is_cancelled());
        assert_eq!(source.active.load(Ordering::SeqCst), 0);
        source.blocked.store(false, Ordering::SeqCst);
        assert_eq!(
            reader.read_range("a", 0..5).await.unwrap(),
            b"hello".as_slice()
        );
    }
    #[tokio::test]
    async fn reads_and_batches_share_the_metadata_path() {
        let reader =
            Archive::open_with_options(Arc::new(Source::new(fixture())), ReaderOptions::default())
                .await
                .unwrap();
        reader.read_range("a", 0..5).await.unwrap();
        let raw = timeout(Duration::from_secs(1), reader.read_range("a", 1..4))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw, b"ell".as_slice());
        assert_eq!(
            timeout(Duration::from_secs(1), reader.entry("a"))
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .path(),
            "a"
        );
        let requests = [
            ReadRequest {
                path: "a",
                offset: 1,
                length: 3,
            },
            ReadRequest {
                path: "b",
                offset: 0,
                length: 5,
            },
            ReadRequest {
                path: "a",
                offset: 5,
                length: 0,
            },
        ];
        let batch = timeout(Duration::from_secs(1), reader.read_many(&requests))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            batch,
            vec![
                Bytes::from_static(b"ell"),
                Bytes::from_static(b"world"),
                Bytes::new()
            ]
        );
        let bad = [
            requests[0],
            ReadRequest {
                path: "a",
                offset: 6,
                length: 0,
            },
        ];
        assert!(matches!(reader.read_many(&bad).await, Err(Error::Range)));
    }
    struct Objects {
        bytes: Bytes,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectResolver for Objects {
        async fn get_encoded(&self, _: [u8; 32], _: u64, _: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes.clone())
        }
    }
    #[tokio::test]
    async fn object_hints_are_advisory_and_hash_policy_is_explicit() {
        let source = Arc::new(Source::new(
            include_bytes!("../../../testdata/entry-manifest/thin.zstar").to_vec(),
        ));
        let reader = Archive::open_with_options(source.clone(), ReaderOptions::default())
            .await
            .unwrap();
        let objects = Objects {
            bytes: crate::encode_frame(b"hello\nworld\n", 3).unwrap().into(),
            calls: AtomicUsize::new(0),
        };
        let requests = [
            ReadRequest {
                path: "repeat",
                offset: 0,
                length: 12,
            },
            ReadRequest {
                path: "world.txt",
                offset: 0,
                length: 6,
            },
        ];
        assert_eq!(
            reader
                .read_many_with_resolver(&requests, Some(&objects))
                .await
                .unwrap(),
            [
                Bytes::from_static(b"hello\nhello\n"),
                Bytes::from_static(b"world\n")
            ]
        );
        assert_eq!(objects.calls.load(Ordering::SeqCst), 1);
        let wrong = Objects {
            bytes: crate::encode_frame(b"wrong\nbytes\n", 3).unwrap().into(),
            calls: AtomicUsize::new(0),
        };
        let checked = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                verify_external: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            checked
                .read_range_with_resolver("hello.txt", 0, 6, Some(&wrong))
                .await
                .is_err()
        );
        let unchecked = Archive::open_with_options(
            source,
            ReaderOptions {
                verify_external: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let (raw, report) = unchecked
            .read_range_with_report_and_resolver("hello.txt", 0, 6, Some(&wrong))
            .await
            .unwrap();
        assert_eq!(raw, b"wrong\n".as_slice());
        assert_eq!(report.external_objects_unchecked.len(), 1);
        assert_eq!(report.external_objects_sha256_checked, [] as [[u8; 32]; 0]);
        // Same address, different unchecked resolvers: no cache or flight reuse.
        let prior_calls = wrong.calls.load(Ordering::SeqCst);
        let (first, second) = tokio::join!(
            unchecked.read_range_with_resolver("hello.txt", 0, 6, Some(&objects)),
            unchecked.read_range_with_resolver("hello.txt", 0, 6, Some(&wrong)),
        );
        assert_eq!(first.unwrap(), b"hello\n".as_slice());
        assert_eq!(second.unwrap(), b"wrong\n".as_slice());
        assert_eq!(wrong.calls.load(Ordering::SeqCst), prior_calls + 1);
        let short = Objects {
            bytes: crate::encode_frame(b"hello\n", 3).unwrap().into(),
            calls: AtomicUsize::new(0),
        };
        let uncached = Archive::open_with_options(
            Arc::new(Source::new(
                include_bytes!("../../../testdata/entry-manifest/thin.zstar").to_vec(),
            )),
            ReaderOptions {
                verify_external: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            uncached
                .read_range_with_resolver("world.txt", 0, 1, Some(&short))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn mandatory_checksums_and_validated_metadata_cannot_be_skipped() {
        let bytes = include_bytes!("../../../testdata/entry-manifest/invalid/data-checksum.zstar");
        let reader = Archive::open_with_options(
            Arc::new(Source::new(bytes.to_vec())),
            ReaderOptions {
                verify_external: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            reader.read_range("a-b", 0..5).await,
            Err(Error::Integrity)
        ));
        let bytes =
            include_bytes!("../../../testdata/entry-manifest/invalid/metadata-checksum.zstar");
        assert!(
            Archive::open_with_options(
                Arc::new(Source::new(bytes.to_vec())),
                ReaderOptions::default()
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn leader_failure_reaches_queued_waiters_and_flights_are_reclaimed() {
        let mut bytes = fixture();
        bytes[12] ^= 0xff; // damage the first data frame
        let source = Arc::new(Source::new(bytes));
        let reader = Arc::new(
            Archive::open_with_options(source.clone(), ReaderOptions::default())
                .await
                .unwrap(),
        );
        source.blocked.store(true, Ordering::SeqCst);
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.read_range("a", 0..5).await })
            })
            .collect();
        timeout(Duration::from_secs(2), async {
            while !reader
                .flights
                .lock()
                .unwrap()
                .values()
                .any(|w| w.strong_count() == 8)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        source.release.notify_waiters();
        for job in jobs {
            assert!(job.await.unwrap().is_err());
        }
        // One load served every queued waiter, and no dead entry remains.
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        assert!(reader.flights.lock().unwrap().is_empty());
    }
}
