//! Concurrent selective reads for the entry manifest, without a frame table.
use crate::backend::{decode_content_with_limits, decode_object};
use crate::decoded_body::DecodedBody;
use crate::manifest::{Entry, EntryKind, Extent};
use crate::manifest_io::{
    EntryCursor, EntryLookup, ManifestIngest, ManifestOptions, ManifestReader, Spool,
};
use crate::{AsyncRangeReader, Error, MAX_BATCH_REQUESTS, ReadRequest, Result};
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
    fn manifest_options(&self) -> ManifestOptions {
        let mut options = ManifestOptions {
            limits: self.limits,
            max_read_bytes: self.max_read_bytes,
            ..Default::default()
        };
        self.metadata.clone().apply(&mut options);
        options
    }
}
impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            metadata: crate::MetadataReaderOptions::default(),
            limits: crate::Limits::default(),
            max_read_bytes: 64 << 20,
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
/// Checks completed by a selective read; does not certify the whole archive.
#[derive(Clone, Debug, Default)]
pub struct ReadReport {
    /// Whether directory, selected entry pages, and ancestor validation completed.
    /// This does not claim validation of unrelated metadata pages.
    pub metadata_validated: bool,
    /// Encoded offsets of the data frames checked.
    pub data_frames_checked: Vec<u64>,
    /// Decoded object hashes checked against their addresses.
    pub external_objects_sha256_checked: Vec<[u8; 32]>,
    /// Object addresses read with external hash verification disabled.
    pub external_objects_unchecked: Vec<[u8; 32]>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    Archive(u64, u64),
    Object([u8; 32]),
}
impl Key {
    pub(crate) const fn of(extent: &Extent) -> Self {
        match extent.source {
            crate::manifest::Source::Archive(at) => Self::Archive(at, extent.encoded_size),
            crate::manifest::Source::Object(hash) => Self::Object(hash),
        }
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
struct Copy {
    output: usize,
    selection: Range<u64>,
    source: Range<usize>,
    destination: Range<usize>,
}
struct Group {
    hint: u64,
    copies: Vec<Copy>,
}
struct Plan {
    sizes: Vec<usize>,
    groups: BTreeMap<Key, Group>,
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
/// Authenticated metadata is immutable and cannot be replaced through the reader:
///
/// ```compile_fail
/// use bytes::Bytes;
/// use zstar::Archive;
/// fn replace(mut archive: Archive<Bytes>, other: Archive<Bytes>) {
///     archive.manifest = other.manifest;
/// }
/// ```
pub struct Archive<R: AsyncRangeReader + ?Sized> {
    source: Arc<R>,
    manifest: Arc<ManifestReader>,
    paged: Option<Arc<crate::paged_archive::PagedState<R>>>,
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
impl<R: AsyncRangeReader + ?Sized> Clone for Archive<R> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            manifest: self.manifest.clone(),
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
        Ok(self.full_manifest().await?.root().entry_count)
    }
    /// Maximum bytes returned by a single allocating read operation.
    #[must_use]
    pub const fn max_read_bytes(&self) -> u64 {
        self.options.max_read_bytes
    }
    /// Returns the archive-level extension values.
    #[must_use]
    pub fn root_extensions(&self) -> &crate::Extensions {
        &self.manifest.root().extensions
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
            .entry(path)
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
            .entry(path)
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
            .entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        self.file(entry).await
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Opens a reusable cursor without retaining this archive's ownership.
    pub async fn open_cursor(&self, path: &str) -> Result<crate::FileCursor> {
        let entry = self
            .entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        crate::FileCursor::new(entry, Arc::downgrade(&self.manifest))
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
    /// Owned stream of sorted entry handles. Skipping a handle performs no content I/O.
    /// Pin the stream to use [`futures_util::TryStreamExt::try_next`], or consume it with stream
    /// combinators. Both the stream and its handles can outlive this archive.
    pub fn entries(
        &self,
    ) -> impl stream::FusedStream<Item = Result<crate::File<R>>> + Send + 'static {
        let state = (self.clone(), None, std::collections::VecDeque::new());
        stream::try_unfold(state, |(archive, cursor, mut pending)| async move {
            let cursor = match cursor {
                Some(cursor) => cursor,
                None => archive.cursor().await?,
            };
            let (cursor, entry) = if let Some(entry) = pending.pop_front() {
                (cursor, entry)
            } else {
                let (cursor, entries) = archive.next_entries(cursor, 128).await?;
                pending.extend(entries);
                let Some(entry) = pending.pop_front() else {
                    return Ok(None);
                };
                (cursor, entry)
            };
            let file = archive.file(entry).await?;
            Ok(Some((file, (archive, Some(cursor), pending))))
        })
        .fuse()
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
        let root = paged.directory().root.clone();
        let mut archive = Self::from_directory(
            source,
            ManifestReader::from_entries(root, Vec::new(), false)?,
            options,
        );
        archive.paged = Some(Arc::new(paged));
        Ok(archive)
    }
    pub(crate) async fn load_full_manifest(
        source: Arc<R>,
        options: ReaderOptions,
    ) -> Result<Arc<ManifestReader>> {
        Ok(Self::open_indexed(source, options).await?.manifest)
    }
    async fn full_manifest(&self) -> Result<Arc<ManifestReader>> {
        match &self.paged {
            Some(paged) => paged.full_manifest().await,
            None => Ok(self.manifest.clone()),
        }
    }
    async fn open_indexed(source: Arc<R>, options: ReaderOptions) -> Result<Self> {
        if !(1..=1024).contains(&options.concurrent_chunks) {
            return Err(Error::Budget("invalid content concurrency"));
        }
        let size = source.size().await?;
        if size < crate::FOOTER_SIZE as u64 {
            return Err(Error::NotZstar);
        }
        let footer = crate::parse_footer(
            &source
                .read_range(size - crate::FOOTER_SIZE as u64..size)
                .await?,
        )?;
        let buffer = Bytes::new();
        let metadata_range = footer.metadata_range(size)?;
        let mut ranges = MetadataSource {
            source: source.as_ref(),
            end: metadata_range.end,
            start: metadata_range.start,
            buffer,
        };
        let mut ingest = ManifestIngest::new(footer, size, Spool::new(options.manifest_options()))?;
        while !ingest.complete() {
            let at = ingest.position();
            let header = ranges.read(ingest.header_range()?).await?;
            let len = ingest.encoded_length(&header)?;
            let encoded = ranges.read(at + 8..at + 8 + len).await?;
            // Ingest is moved, never shared with abandoned work. Cancellation
            // releases its temporary storage when the single worker finishes.
            ingest = tokio::task::spawn_blocking(move || {
                ingest.push(&header, &encoded)?;
                Ok::<_, Error>(ingest)
            })
            .await
            .map_err(worker_error)??;
        }
        let manifest = tokio::task::spawn_blocking(move || ingest.finish())
            .await
            .map_err(worker_error)??;
        Ok(Self {
            source,
            manifest: Arc::new(manifest),
            paged: None,
            semaphore: Arc::new(Semaphore::new(options.concurrent_chunks)),
            metadata_semaphore: Arc::new(Semaphore::new(2)),
            flights: Arc::new(Mutex::new(HashMap::new())),
            options,
        })
    }
    pub(crate) fn from_directory(
        source: Arc<R>,
        manifest: ManifestReader,
        options: ReaderOptions,
    ) -> Self {
        Self {
            source,
            manifest: Arc::new(manifest),
            paged: None,
            semaphore: Arc::new(Semaphore::new(options.concurrent_chunks)),
            metadata_semaphore: Arc::new(Semaphore::new(2)),
            flights: Arc::new(Mutex::new(HashMap::new())),
            options,
        }
    }
    async fn metadata<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&ManifestReader) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let manifest = self.full_manifest().await?;
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
    fn cached_entry(&self, path: &str) -> EntryLookup {
        self.manifest.entry_cached(path)
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    /// Returns owned metadata, or `None` if the path is absent.
    pub async fn entry(&self, path: &str) -> Result<Option<Entry>> {
        if let Some(paged) = &self.paged {
            return paged.entry(path).await;
        }
        if let EntryLookup::Ready(entry) = self.cached_entry(path) {
            return Ok(entry);
        }
        let path = path.to_owned();
        self.metadata(move |manifest| manifest.entry(&path)).await
    }
    /// Creates a cursor for bounded metadata batches.
    ///
    /// Prefer [`Self::entries`] for ordinary enumeration.
    #[doc(hidden)]
    pub async fn cursor(&self) -> Result<EntryCursor> {
        self.metadata(|manifest| Ok(manifest.cursor())).await
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
            return Err(Error::Budget("cursor batch size"));
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
        self.entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?
            .linkname
            .ok_or(Error::Unsupported("entry is not a symlink"))
    }
    pub(crate) fn owns_cursor(&self, owner: &std::sync::Weak<ManifestReader>) -> bool {
        std::sync::Weak::ptr_eq(owner, &Arc::downgrade(&self.manifest))
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
            let raw = match key_copy {
                Key::Archive(..) => decode_content_with_limits(&encoded, true, limits)?,
                Key::Object(_) => decode_object(&encoded, limits)?,
            };
            if let Key::Object(hash) = key_copy
                && verify
            {
                crate::verify_sha256(&raw, hash)?;
            }
            let raw = DecodedBody::compact(raw);
            Ok::<_, Error>(raw)
        })
        .await
        .map_err(worker_error)?
    }
    async fn plan(&self, requests: &[ReadRequest<'_>]) -> Result<Plan> {
        if requests.len() > MAX_BATCH_REQUESTS {
            return Err(Error::Budget("batch request count"));
        }
        let requests: Vec<_> = requests
            .iter()
            .map(|r| (r.path.to_owned(), r.offset, r.length))
            .collect();
        let budget = self.options.max_read_bytes;
        let selected = match &self.paged {
            Some(paged) => Some(
                paged
                    .selected_manifests(
                        &requests
                            .iter()
                            .map(|(path, offset, length)| ReadRequest {
                                path,
                                offset: *offset,
                                length: *length,
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await?,
            ),
            None => None,
        };
        let manifest = self.manifest.clone();
        tokio::task::spawn_blocking(move || {
            build_plan(
                requests
                    .iter()
                    .map(|(path, offset, length)| (path.as_str(), *offset, *length)),
                budget,
                |path| {
                    let page = selected
                        .as_ref()
                        .and_then(|pages| pages.get(path))
                        .unwrap_or(&manifest);
                    Ok(page.range_entry(path))
                },
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
#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
fn build_plan<'a, 'm>(
    requests: impl Iterator<Item = (&'a str, u64, u64)>,
    budget: u64,
    mut lookup: impl FnMut(&str) -> Result<Option<(&'m Entry, &'m [u64])>>,
) -> Result<Plan> {
    let mut plan = Plan {
        sizes: Vec::new(),
        groups: BTreeMap::new(),
    };
    let mut total = 0_u64;
    let mut copies = 0_usize;
    for (output, (path, offset, length)) in requests.enumerate() {
        total = total
            .checked_add(length)
            .ok_or(Error::Budget("batch output size"))?;
        if total > budget || usize::try_from(total).is_err() {
            return Err(Error::Budget("batch output size"));
        }
        let (entry, ends) = lookup(path)?.ok_or_else(|| Error::MissingEntry(path.into()))?;
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        let range = crate::checked_range(offset, length, ends.last().copied().unwrap_or(0))?;
        plan.sizes.push(length as usize);
        if length == 0 {
            continue;
        }
        let first = ends.partition_point(|end| *end <= offset);
        let mut position = if first == 0 { 0 } else { ends[first - 1] };
        for extent in &entry.extents[first..] {
            let next = position.checked_add(extent.size).ok_or(Error::Range)?;
            if position >= range.end {
                break;
            }
            if next > range.start {
                copies += 1;
                if copies > 65_536 {
                    return Err(Error::Budget("batch extent copies"));
                }
                let start = range.start.max(position);
                let end = range.end.min(next);
                let at = extent
                    .offset
                    .checked_add(start - position)
                    .ok_or(Error::Range)?;
                let selection_end = extent.offset.checked_add(extent.size).ok_or(Error::Range)?;
                let group = plan.groups.entry(Key::of(extent)).or_insert_with(|| Group {
                    hint: extent.encoded_size,
                    copies: Vec::new(),
                });
                group.copies.push(Copy {
                    output,
                    selection: extent.offset..selection_end,
                    source: at as usize..(at + end - start) as usize,
                    destination: (start - range.start) as usize..(end - range.start) as usize,
                });
            }
            position = next;
        }
    }
    Ok(plan)
}
fn worker_error(_: tokio::task::JoinError) -> Error {
    Error::Io(std::io::Error::other("entry reader worker failed"))
}
fn validate_copy(copy: &Copy, decoded: usize) -> Result<()> {
    crate::checked_range(
        copy.selection.start,
        copy.selection.end - copy.selection.start,
        decoded as u64,
    )?;
    Ok(())
}
fn record(report: Option<&mut ReadReport>, key: &Key, verify: bool) {
    if let Some(report) = report {
        match key {
            Key::Archive(at, _) => report.data_frames_checked.push(*at),
            Key::Object(hash) if verify => report.external_objects_sha256_checked.push(*hash),
            Key::Object(hash) => report.external_objects_unchecked.push(*hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BlockingBuilder;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::time::{Duration, timeout};

    fn fixture() -> Vec<u8> {
        let mut writer =
            BlockingBuilder::with_options(Vec::new(), 2048, 3, ManifestOptions::default()).unwrap();
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
                .path,
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
