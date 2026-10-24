//! Concurrent selective reads for the entry manifest, without a frame table.
use crate::backend::{decode_content, decode_object};
use crate::decoded_cache::{BuiltinLookup, DecodedBody, DecodedCacheKey, SharedDecodedCache};
use crate::manifest::{ENCODED_FRAME_MAX, Entry, EntryKind, Extent};
use crate::manifest_io::{
    EntryCursor, EntryLookup, ManifestIngest, ManifestOptions, ManifestReader, Spool,
};
use crate::{AsyncRangeReader, Error, MAX_BATCH_REQUESTS, ReadRequest, Result};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use lru::LruCache;
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
    /// Whether retained content for this address may be reused.
    fn cache_allowed(&self, _address: [u8; 32]) -> bool {
        true
    }
    /// Returns a complete encoded object of at most `max_bytes` bytes.
    ///
    /// `hint` is an advisory encoded size, not a decoded-size bound. Return
    /// [`Error::MissingObject`] if the object is unavailable.
    async fn get_encoded(&self, address: [u8; 32], hint: u64, max_bytes: u64) -> Result<Bytes>;
    /// Optional planning capability. The fallback preserves existing resolver APIs.
    fn supports_batch(&self) -> bool {
        false
    }
    /// Actual stored frame length when cheaply known from an immutable catalog.
    fn encoded_length(&self, _address: [u8; 32]) -> Option<u64> {
        None
    }
    /// Optional immutable physical ordering for bounded reader miss windows.
    /// The token groups one pack; offsets locate frames only for planning.
    fn encoded_location(&self, _address: [u8; 32]) -> Option<([u8; 32], u64)> {
        None
    }
    async fn get_encoded_many(&self, requests: &[ObjectRequest]) -> Result<Vec<Bytes>> {
        let mut output = Vec::with_capacity(requests.len());
        for request in requests {
            output.push(
                self.get_encoded(request.address, request.hint, request.max_bytes)
                    .await?,
            );
        }
        Ok(output)
    }
}
/// One complete encoded object requested by a reader miss plan.
#[derive(Clone, Copy, Debug)]
pub struct ObjectRequest {
    pub address: [u8; 32],
    pub hint: u64,
    pub max_bytes: u64,
}
/// Metadata, cache, concurrency, and external-object policies for an [`Archive`].
#[derive(Clone)]
pub struct ReaderOptions {
    /// Metadata parsing and storage limits, including the returned-read budget.
    pub manifest: ManifestOptions,
    /// Charged retained decoded content. Active jobs/results are additional.
    pub cache_bytes: usize,
    /// Bounds both outstanding content requests and detached decode jobs.
    pub concurrent_chunks: usize,
    /// Fixed reader-local external hash policy. Data checksums are mandatory.
    pub verify_external: bool,
    /// External objects are resolved through this reader-wide policy.
    pub resolver: Option<Arc<dyn ObjectResolver>>,
}
impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            manifest: ManifestOptions::default(),
            cache_bytes: 16 << 20,
            concurrent_chunks: 8,
            verify_external: true,
            resolver: None,
        }
    }
}
impl std::fmt::Debug for ReaderOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderOptions")
            .field("manifest", &self.manifest)
            .field("cache_bytes", &self.cache_bytes)
            .field("concurrent_chunks", &self.concurrent_chunks)
            .field("verify_external", &self.verify_external)
            .field("resolver", &self.resolver.as_ref().map(|_| "configured"))
            .finish()
    }
}
/// Selects whether verification also resolves external objects.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VerificationScope {
    /// Verifies metadata and every physical frame and tar member.
    #[default]
    Archive,
    /// Also resolves and validates every external-object selection.
    ExternalClosure,
}
/// Checks completed by a successful [`Archive::verify`] call.
#[derive(Clone, Debug, Default)]
pub struct VerificationReport {
    /// Successful verification of this scope includes metadata, tar and data checksums.
    pub scope: VerificationScope,
    /// Logical tar members, excluding PAX headers and end records.
    pub tar_members: u64,
    /// Tar members that contain external-object pointers.
    pub pointer_members: u64,
    /// Data frames decoded and checked.
    pub data_frames: u64,
    /// Decoded object hashes checked against their addresses.
    pub external_objects_sha256_checked: Vec<[u8; 32]>,
    /// Object addresses read with external hash verification disabled.
    pub external_objects_unchecked: Vec<[u8; 32]>,
}
/// Checks completed by a selective read; does not certify the whole archive.
#[derive(Clone, Debug, Default)]
pub struct ReadReport {
    /// Whether manifest authentication completed.
    pub metadata_authenticated: bool,
    /// Selective reads do not claim full namespace or physical tar validation.
    pub full_archive_validated: bool,
    /// Encoded offsets of the data frames checked.
    pub data_frames_checked: Vec<u64>,
    /// Decoded object hashes checked against their addresses.
    pub external_objects_sha256_checked: Vec<[u8; 32]>,
    /// Object addresses read with external hash verification disabled.
    pub external_objects_unchecked: Vec<[u8; 32]>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Key {
    Archive(u64, u64),
    Object([u8; 32]),
}
impl Key {
    pub(crate) fn of(extent: &Extent) -> Self {
        match extent.source {
            crate::manifest::Source::Archive(at) => Self::Archive(at, extent.encoded_size),
            crate::manifest::Source::Object(hash) => Self::Object(hash),
        }
    }
}
struct Cache {
    entries: LruCache<Key, DecodedBody>,
    limit: usize,
    used: usize,
}
impl Cache {
    const OVERHEAD: usize = 256;
    fn put(&mut self, key: Key, raw: DecodedBody) {
        let charge = raw.allocated().saturating_add(Self::OVERHEAD);
        if charge > self.limit {
            return;
        }
        if let Some(previous) = self.entries.pop(&key) {
            self.used -= previous.allocated() + Self::OVERHEAD;
        }
        while self.used > self.limit - charge {
            let (_, previous) = self.entries.pop_lru().unwrap();
            self.used -= previous.allocated() + Self::OVERHEAD;
        }
        self.used += charge;
        self.entries.put(key, raw);
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
    fn flight(&self) -> &Arc<Flight> {
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

struct VerificationSource<R: AsyncRangeReader + ?Sized> {
    source: Arc<R>,
    size: u64,
    handle: tokio::runtime::Handle,
    cancel: tokio::sync::watch::Receiver<bool>,
}
impl<R: AsyncRangeReader + ?Sized> crate::RangeReader for VerificationSource<R> {
    fn len(&self) -> u64 {
        self.size
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        if *self.cancel.borrow() {
            return Err(std::io::Error::from(std::io::ErrorKind::Interrupted).into());
        }
        self.handle.block_on(async {
            tokio::select! {
                biased;
                _ = self.cancel.changed() => Err(std::io::Error::from(std::io::ErrorKind::Interrupted).into()),
                bytes = self.source.read_range(range) => bytes.map(|bytes| bytes.to_vec()),
            }
        })
    }
}

/// An indexed archive backed by an immutable [`AsyncRangeReader`].
///
/// Opening authenticates the complete manifest and retains a sorted index in
/// O(N) memory. Clones share the source, index, cache, and concurrency limits.
/// Selected content is validated before it is returned. Use [`Self::verify`]
/// to check the physical archive or its external-object closure.
///
/// Dropping a read or verification future cancels pending transport I/O.
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
    options: ReaderOptions,
    semaphore: Arc<Semaphore>,
    metadata_semaphore: Arc<Semaphore>,
    cache: Arc<Mutex<Cache>>,
    flights: Arc<Flights>,
    metadata_digest: [u8; 32],
    data_end: u64,
    shared_cache: Option<SharedDecodedCache>,
}
impl Archive<Bytes> {
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
            options: self.options.clone(),
            semaphore: self.semaphore.clone(),
            metadata_semaphore: self.metadata_semaphore.clone(),
            cache: self.cache.clone(),
            flights: self.flights.clone(),
            metadata_digest: self.metadata_digest,
            data_end: self.data_end,
            shared_cache: self.shared_cache.clone(),
        }
    }
}
impl<R: AsyncRangeReader + ?Sized + 'static> Archive<R> {
    /// Opens an immutable source with default [`ReaderOptions`].
    pub async fn open(source: Arc<R>) -> Result<Self> {
        Self::open_with_options(source, ReaderOptions::default()).await
    }
    pub(crate) fn resolver(&self) -> Option<&dyn ObjectResolver> {
        self.options.resolver.as_deref()
    }
    /// Returns the number of entries in the authenticated manifest.
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.manifest.root().entry_count
    }
    /// Maximum bytes returned by a single allocating read operation.
    #[must_use]
    pub fn max_read_bytes(&self) -> u64 {
        self.options.manifest.max_read_bytes
    }
    /// Returns the archive-level extension values.
    #[must_use]
    pub fn root_extensions(&self) -> &crate::Extensions {
        &self.manifest.root().extensions
    }
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
    /// Reads a half-open byte range from a regular file.
    ///
    /// Returns an error for a missing entry, an invalid range, or an exceeded budget.
    pub async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        self.read_range_with_resolver(path, range.start, length, self.resolver())
            .await
    }
    /// Reads an entire file and reports the integrity checks completed.
    pub async fn read_with_report(&self, path: &str) -> Result<(Bytes, ReadReport)> {
        let entry = self
            .entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        self.read_range_with_report(path, 0..entry.size()?).await
    }
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
    /// Opens a reusable cursor without retaining this archive's ownership.
    pub async fn open_cursor(&self, path: &str) -> Result<crate::FileCursor> {
        let entry = self
            .entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        let mut cursor = if entry.extents.len() <= 64 {
            crate::FileCursor::new(entry)?
        } else {
            self.metadata(move |_| crate::FileCursor::new(entry))
                .await?
        };
        cursor.owner = Some(Arc::downgrade(&self.manifest));
        Ok(cursor)
    }
    async fn file(&self, entry: Entry) -> Result<crate::File<R>> {
        if entry.extents.len() <= 64 {
            return crate::File::new(self.clone(), entry);
        }
        // Constructing a large seek index is CPU work, like cloning its metadata.
        let archive = self.clone();
        self.metadata(move |_| crate::File::new(archive, entry))
            .await
    }
    /// Owned stream of sorted entry handles. Skipping a handle performs no content I/O.
    /// Pin the stream to use [`futures_util::TryStreamExt::try_next`], or consume it with stream
    /// combinators. Both the stream and its handles can outlive this archive.
    pub fn entries(
        &self,
    ) -> impl stream::FusedStream<Item = Result<crate::File<R>>> + Send + 'static {
        let state = (
            self.clone(),
            self.manifest.cursor(),
            std::collections::VecDeque::new(),
        );
        stream::try_unfold(state, |(archive, cursor, mut pending)| async move {
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
            Ok(Some((file, (archive, cursor, pending))))
        })
        .fuse()
    }
    /// Verifies the complete physical archive and the requested external scope.
    ///
    /// Cancellation drops pending transport futures; worker permits remain held
    /// until any already running frame decode completes.
    pub async fn verify(&self, scope: VerificationScope) -> Result<VerificationReport> {
        let (cancel, receiver) = tokio::sync::watch::channel(false);
        struct Cancel(tokio::sync::watch::Sender<bool>);
        impl Drop for Cancel {
            fn drop(&mut self) {
                let _ = self.0.send(true);
            }
        }
        let _cancel = Cancel(cancel);
        let source = VerificationSource {
            source: self.source.clone(),
            size: self.source.size().await?,
            handle: tokio::runtime::Handle::current(),
            cancel: receiver,
        };
        let manifest = self.manifest.clone();
        let options = self.options.manifest.clone();
        let data_end = self.data_end;
        let digest = self.metadata_digest;
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("reader closed"))?;
        let physical = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            crate::backend::BlockingArchive::from_manifest(
                source, manifest, data_end, options, digest,
            )
            .visit_payloads_with_report(|_, _| Ok(()))
        })
        .await
        .map_err(worker_error)??;
        let mut report = VerificationReport {
            scope,
            tar_members: physical.members,
            pointer_members: physical.pointers,
            data_frames: physical.frames,
            ..Default::default()
        };
        if scope == VerificationScope::ExternalClosure {
            let entries = self.entries();
            futures_util::pin_mut!(entries);
            let mut checked = std::collections::BTreeSet::new();
            while let Some(file) = futures_util::TryStreamExt::try_next(&mut entries).await? {
                if !file.metadata().is_external() {
                    continue;
                }
                let mut previous: Option<(Key, Bytes)> = None;
                for extent in &file.metadata().extents {
                    let key = Key::of(extent);
                    if previous.as_ref().is_none_or(|(cached, _)| cached != &key) {
                        previous = Some((
                            key.clone(),
                            self.content(&key, extent.encoded_size, self.resolver())
                                .await?,
                        ));
                    }
                    let raw = &previous.as_ref().unwrap().1;
                    crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
                    if let Key::Object(hash) = key {
                        checked.insert(hash);
                    }
                }
            }
            if self.options.verify_external {
                report.external_objects_sha256_checked.extend(checked);
            } else {
                report.external_objects_unchecked.extend(checked);
            }
        }
        Ok(report)
    }

    /// Opens and authenticates an immutable source using the supplied policies.
    pub async fn open_with_options(source: Arc<R>, options: ReaderOptions) -> Result<Self> {
        if !(1..=1024).contains(&options.concurrent_chunks) {
            return Err(Error::Budget("invalid content concurrency"));
        }
        let size = source.size().await?;
        if size < crate::FOOTER_SIZE as u64 {
            return Err(Error::NotZstar);
        }
        let mut prefetched = None;
        if let Some(hint) = source.manifest_range().await?
            && hint.start > 0
            && hint.end == size - 80
            && hint.start < hint.end
            && hint.end - hint.start > 8
            && hint.end - hint.start <= options.manifest.max_encoded_bytes
            // Keep speculative hint reads within one bounded metadata window.
            && size - hint.start <= 1 << 20
        {
            let bytes = source.read_range(hint.start..size).await?;
            if bytes.len() as u64 != size - hint.start {
                return Err(Error::MalformedToc("short hinted metadata range".into()));
            }
            let end = bytes.len() - crate::FOOTER_SIZE;
            if let Ok(footer) = crate::parse_footer(&bytes[end..])
                && footer.toc_offset == hint.start
                && footer.toc_encoded_len == hint.end - hint.start
            {
                prefetched = Some((footer, bytes.slice(..end)));
            }
        }
        let (footer, buffer) = match prefetched {
            Some(result) => result,
            None => (
                crate::parse_footer(&source.read_range(size - 80..size).await?)?,
                Bytes::new(),
            ),
        };
        let metadata_end = footer
            .toc_offset
            .checked_add(footer.toc_encoded_len)
            .ok_or(Error::MalformedFooter("manifest coverage"))?;
        let mut ranges = MetadataSource {
            source: source.as_ref(),
            end: metadata_end,
            start: footer.toc_offset,
            buffer,
        };
        let metadata_digest = footer.toc_sha256;
        let data_end = footer.toc_offset;
        let mut ingest = ManifestIngest::new(footer, size, Spool::new(options.manifest.clone()))?;
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
            semaphore: Arc::new(Semaphore::new(options.concurrent_chunks)),
            metadata_semaphore: Arc::new(Semaphore::new(2)),
            cache: Arc::new(Mutex::new(Cache {
                entries: LruCache::unbounded(),
                limit: options.cache_bytes,
                used: 0,
            })),
            flights: Arc::new(Mutex::new(HashMap::new())),
            options,
            metadata_digest,
            data_end,
            shared_cache: None,
        })
    }
    /// Attach a caller-owned decoded cache before sharing this reader.
    /// The binding identity must name an immutable encoded archive snapshot.
    #[must_use]
    pub fn with_shared_decoded_cache(mut self, shared: SharedDecodedCache) -> Self {
        self.shared_cache = Some(shared);
        self
    }
    pub(crate) fn cache_allowed(&self, key: &Key, resolver: Option<&dyn ObjectResolver>) -> bool {
        match key {
            Key::Archive(..) => self.source.cache_allowed(),
            Key::Object(address) => resolver.is_some_and(|r| r.cache_allowed(*address)),
        }
    }
    fn shared_key(&self, key: &Key) -> Option<DecodedCacheKey> {
        self.shared_cache.as_ref().and_then(|shared| match key {
            Key::Archive(at, size) => Some(shared.archive_key(self.metadata_digest, *at, *size)),
            Key::Object(hash) if self.options.verify_external => {
                Some(DecodedCacheKey::Object(*hash))
            }
            Key::Object(_) => None,
        })
    }
    async fn metadata<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&ManifestReader) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let manifest = self.manifest.clone();
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
    /// Returns owned metadata, or `None` if the path is absent.
    pub async fn entry(&self, path: &str) -> Result<Option<Entry>> {
        if let EntryLookup::Ready(entry) = self.cached_entry(path) {
            return Ok(entry);
        }
        let path = path.to_owned();
        self.metadata(move |manifest| manifest.entry(&path)).await
    }
    /// Collects all owned entry metadata into a vector.
    ///
    /// Use [`Self::entries`] to enumerate without allocating a second complete list.
    pub async fn collect_entries(&self) -> Result<Vec<Entry>> {
        self.metadata(ManifestReader::collect_entries).await
    }
    /// Creates a cursor for bounded metadata batches.
    ///
    /// Prefer [`Self::entries`] for ordinary enumeration.
    pub async fn cursor(&self) -> Result<EntryCursor> {
        self.metadata(|manifest| Ok(manifest.cursor())).await
    }
    /// Creates a bounded cursor over paths beginning with `prefix`.
    pub async fn cursor_prefix(&self, prefix: &str) -> Result<EntryCursor> {
        let prefix = prefix.to_owned();
        self.metadata(move |manifest| Ok(manifest.cursor_prefix(&prefix)))
            .await
    }
    /// Returns the advanced cursor with at most `limit` owned entries.
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
    #[doc(hidden)]
    pub async fn validate_metadata(&self) -> Result<()> {
        self.metadata(ManifestReader::validate_metadata).await
    }
    /// Returns a symlink target verbatim, without resolving or following it.
    pub async fn read_link(&self, path: &str) -> Result<String> {
        self.entry(path)
            .await?
            .ok_or_else(|| Error::MissingEntry(path.into()))?
            .linkname
            .ok_or(Error::Unsupported("entry is not a symlink"))
    }
    /// Returns charged bytes retained by this archive's decoded cache.
    #[must_use]
    pub fn cache_len_bytes(&self) -> usize {
        self.cache.lock().unwrap().used
    }
    pub(crate) fn owns_cursor(&self, owner: &std::sync::Weak<ManifestReader>) -> bool {
        std::sync::Weak::ptr_eq(owner, &Arc::downgrade(&self.manifest))
    }
    pub(crate) async fn content(
        &self,
        key: &Key,
        hint: u64,
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<Bytes> {
        self.content_prefetched(key, hint, resolver, None).await
    }
    async fn content_prefetched(
        &self,
        key: &Key,
        hint: u64,
        resolver: Option<&dyn ObjectResolver>,
        encoded: Option<Bytes>,
    ) -> Result<Bytes> {
        // Per-call resolvers have no stable namespace. Unchecked objects may
        // share a group within a batch, but cannot share retained bytes or a
        // flight with another operation's resolver.
        if matches!(key, Key::Object(_)) && !self.options.verify_external {
            return Ok(self
                .load_content(key, hint, resolver, encoded)
                .await?
                .into_bytes());
        }
        if let Some(raw) = self
            .cache
            .lock()
            .unwrap()
            .entries
            .get(key)
            .cloned()
            .filter(|_| self.cache_allowed(key, resolver))
        {
            return Ok(raw.into_bytes());
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
        if let Some(raw) = flight.value.lock().unwrap().clone() {
            return Ok(raw.into_bytes());
        }
        if let Some(error) = flight.failure.lock().unwrap().as_ref() {
            return Err(error.duplicate());
        }
        if let Some(raw) = self
            .cache
            .lock()
            .unwrap()
            .entries
            .get(key)
            .cloned()
            .filter(|_| self.cache_allowed(key, resolver))
        {
            return Ok(raw.into_bytes());
        }
        let raw = match self.load_content(key, hint, resolver, encoded).await {
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
        // Failed or cancelled loads never enter the cache or flight result.
        if self.cache_allowed(key, resolver) {
            self.cache.lock().unwrap().put(key.clone(), raw.clone());
        }
        *flight.value.lock().unwrap() = Some(raw.clone());
        Ok(raw.into_bytes())
    }
    async fn load_content(
        &self,
        key: &Key,
        hint: u64,
        resolver: Option<&dyn ObjectResolver>,
        prefetched: Option<Bytes>,
    ) -> Result<DecodedBody> {
        let shared_key = self
            .cache_allowed(key, resolver)
            .then(|| self.shared_key(key))
            .flatten();
        if let (Some(cache_key), Some(shared)) = (shared_key, self.shared_cache.clone()) {
            let cache_hit = match shared.memory_get(&cache_key) {
                BuiltinLookup::Result(value) => value,
                BuiltinLookup::Unavailable => {
                    let permit = self
                        .semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| Error::Unsupported("reader closed"))?;
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        shared.get(&cache_key)
                    })
                    .await
                    .map_err(worker_error)?
                }
            };
            if let Some(raw) = cache_hit {
                return Ok(raw);
            }
        }
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("reader closed"))?;
        let encoded = if let Some(encoded) = prefetched {
            encoded
        } else {
            match key {
                Key::Archive(at, size) => {
                    let encoded = self
                        .source
                        .read_range(*at..at.checked_add(*size).ok_or(Error::Range)?)
                        .await?;
                    if encoded.len() as u64 != *size {
                        return Err(Error::MalformedFrame("short archive frame".into()));
                    }
                    encoded
                }
                Key::Object(hash) => {
                    resolver
                        .ok_or(Error::Unsupported("external read requires resolver"))?
                        .get_encoded(*hash, hint, ENCODED_FRAME_MAX)
                        .await?
                }
            }
        };
        let key_copy = key.clone();
        let verify = self.options.verify_external;
        let shared = self
            .cache_allowed(key, resolver)
            .then(|| self.shared_cache.clone())
            .flatten();
        let shared_key = self
            .cache_allowed(key, resolver)
            .then(|| self.shared_key(key))
            .flatten();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let raw = match key_copy {
                Key::Archive(..) => decode_content(&encoded, true)?,
                Key::Object(_) => decode_object(&encoded)?,
            };
            if let Key::Object(hash) = key_copy
                && verify
            {
                crate::verify_sha256(&raw, hash)?;
            }
            let raw = DecodedBody::from_vec(raw);
            if let (Some(shared), Some(shared_key)) = (shared.as_ref(), shared_key) {
                shared.put(shared_key, raw.clone());
            }
            Ok::<_, Error>(raw)
        })
        .await
        .map_err(worker_error)?
    }
    async fn plan(&self, requests: &[ReadRequest<'_>]) -> Result<Plan> {
        if requests.len() > MAX_BATCH_REQUESTS {
            return Err(Error::Budget("batch request count"));
        }
        // Small batches use the immutable in-memory index directly, avoiding
        // path copies and a blocking worker round trip. Keep larger batches
        // off the runtime thread; extent-copy budgets still apply to both.
        if requests.len() <= 16 {
            return build_plan(
                requests.iter().map(|r| (r.path, r.offset, r.length)),
                self.options.manifest.max_read_bytes,
                |path| Ok(self.manifest.range_entry(path)),
            );
        }
        let requests: Vec<_> = requests
            .iter()
            .map(|r| (r.path.to_owned(), r.offset, r.length))
            .collect();
        let budget = self.options.manifest.max_read_bytes;
        self.metadata(move |manifest| {
            build_plan(
                requests
                    .iter()
                    .map(|(path, offset, length)| (path.as_str(), *offset, *length)),
                budget,
                |path| Ok(manifest.range_entry(path)),
            )
        })
        .await
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
            metadata_authenticated: true,
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
                // (up to 16 MiB) for as long as the caller keeps it. Copy
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
                )
                .await;
        }
        self.execute_plan(plan, resolver, report).await
    }
    async fn execute_plan(
        &self,
        plan: Plan,
        resolver: Option<&dyn ObjectResolver>,
        mut report: Option<&mut ReadReport>,
    ) -> Result<Vec<Bytes>> {
        let mut outputs: Vec<_> = plan
            .sizes
            .iter()
            .map(|size| BytesMut::zeroed(*size))
            .collect();
        let mut groups: Vec<_> = plan.groups.into_iter().collect();
        // Hash ordering scatters otherwise adjacent frames across miss windows.
        // Keep physical neighbors together without widening decode admission.
        groups.sort_by_cached_key(|(key, _)| match key {
            Key::Archive(offset, _) => (0, [0; 32], *offset, key.clone()),
            Key::Object(address) => resolver
                .and_then(|r| r.encoded_location(*address))
                .map_or_else(
                    || (2, [0; 32], 0, key.clone()),
                    |(pack, offset)| (1, pack, offset, key.clone()),
                ),
        });
        let mut groups = groups.into_iter();
        loop {
            let window: Vec<_> = groups
                .by_ref()
                .take(self.options.concurrent_chunks)
                .collect();
            if window.is_empty() {
                break;
            }
            let mut encoded = self.prefetch_window(&window, resolver).await?;
            let mut jobs = stream::iter(window.into_iter().map(|(key, group)| {
                let bytes = encoded.remove(&key);
                async move {
                    let raw = self
                        .content_prefetched(&key, group.hint, resolver, bytes)
                        .await?;
                    for copy in &group.copies {
                        validate_copy(copy, raw.len())?;
                    }
                    Ok::<_, Error>((key, group, raw))
                }
            }))
            .buffer_unordered(self.options.concurrent_chunks);
            while let Some(job) = jobs.next().await {
                let (key, group, raw) = job?;
                record(report.as_deref_mut(), &key, self.options.verify_external);
                for copy in group.copies {
                    outputs[copy.output][copy.destination].copy_from_slice(&raw[copy.source]);
                }
            }
        }
        Ok(outputs.into_iter().map(BytesMut::freeze).collect())
    }
    async fn prefetch_window(
        &self,
        window: &[(Key, Group)],
        resolver: Option<&dyn ObjectResolver>,
    ) -> Result<BTreeMap<Key, Bytes>> {
        let mut output = BTreeMap::new();
        // Advisory custom shared caches can require blocking callbacks. Let
        // existing content loads consult them before issuing speculative I/O.
        if self.shared_cache.is_some() {
            return Ok(output);
        }
        let missing: Vec<_> = window
            .iter()
            .filter(|(key, _)| {
                (!self.cache_allowed(key, resolver)
                    || !self.cache.lock().unwrap().entries.contains(key))
                    && self
                        .flights
                        .lock()
                        .unwrap()
                        .get(key)
                        .and_then(Weak::upgrade)
                        .is_none()
            })
            .collect();
        let mut cursor = 0;
        while cursor < missing.len() {
            let Key::Archive(start, size) = missing[cursor].0 else {
                break;
            };
            let first = cursor;
            let mut end = start + size;
            cursor += 1;
            while cursor < missing.len() {
                if let Key::Archive(offset, length) = missing[cursor].0
                    && offset == end
                    && offset + length - start <= 8 << 20
                {
                    end += length;
                    cursor += 1;
                } else {
                    break;
                }
            }
            if cursor - first > 1 {
                let _permit = self
                    .semaphore
                    .acquire()
                    .await
                    .map_err(|_| Error::Unsupported("reader closed"))?;
                let bytes = self.source.read_range(start..end).await?;
                if bytes.len() as u64 != end - start {
                    return Err(Error::MalformedFrame(
                        "short coalesced archive range".into(),
                    ));
                }
                for (key, _) in &missing[first..cursor] {
                    let Key::Archive(offset, length) = key else {
                        unreachable!()
                    };
                    output.insert(
                        key.clone(),
                        Bytes::copy_from_slice(
                            &bytes[(offset - start) as usize..(offset - start + length) as usize],
                        ),
                    );
                }
            }
        }
        if let Some(resolver) = resolver.filter(|resolver| resolver.supports_batch()) {
            let requests: Vec<_> = missing
                .iter()
                .filter_map(|(key, group)| {
                    if let Key::Object(address) = key {
                        Some(ObjectRequest {
                            address: *address,
                            hint: group.hint,
                            max_bytes: ENCODED_FRAME_MAX,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            let size = requests.iter().try_fold(0_u64, |sum, request| {
                sum.checked_add(
                    resolver
                        .encoded_length(request.address)
                        .unwrap_or(request.max_bytes),
                )
            });
            if requests.len() > 1 && size.is_some_and(|size| size <= 64 << 20) {
                let _permit = self
                    .semaphore
                    .acquire()
                    .await
                    .map_err(|_| Error::Unsupported("reader closed"))?;
                let bytes = resolver.get_encoded_many(&requests).await?;
                if bytes.len() != requests.len() {
                    return Err(Error::MalformedExternalContent(
                        "batch resolver result count".into(),
                    ));
                }
                for (request, bytes) in requests.iter().zip(bytes) {
                    if bytes.len() as u64 > request.max_bytes {
                        return Err(Error::Budget("encoded batch resolver output"));
                    }
                    output.insert(Key::Object(request.address), bytes);
                }
            }
        }
        Ok(output)
    }
}
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
            BlockingBuilder::with_options(Vec::new(), 2048, 3, Default::default()).unwrap();
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
                        attributes: Default::default(),
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
            let data_end = crate::parse_footer(&bytes[bytes.len() - 80..])
                .unwrap()
                .toc_offset;
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
        async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if range.start < self.data_end {
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
        let reader = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(source.calls.load(Ordering::SeqCst), 2); // footer + read-ahead metadata
        reader.validate_metadata().await.unwrap();
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
        assert!(report.metadata_authenticated);
        assert!(!report.full_archive_validated);
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
            Archive::open_with_options(
                source.clone(),
                ReaderOptions {
                    cache_bytes: 0,
                    ..Default::default()
                },
            )
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
        assert_eq!(reader.cache_len_bytes(), 0);
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
                    cache_bytes: 0,
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
    async fn small_cached_reads_do_not_need_a_metadata_worker_slot() {
        let reader =
            Archive::open_with_options(Arc::new(Source::new(fixture())), ReaderOptions::default())
                .await
                .unwrap();
        reader.read_range("a", 0..5).await.unwrap();
        let _first = reader
            .metadata_semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let _second = reader
            .metadata_semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();
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
    #[tokio::test]
    async fn fragmented_range_reads_do_not_wait_for_metadata_workers() {
        let payload: Vec<_> = (0..100_000).map(|i| (i % 251) as u8).collect();
        let mut writer =
            BlockingBuilder::with_options(Vec::new(), 512, 3, Default::default()).unwrap();
        writer
            .add_file(
                Entry::file("file"),
                payload.len() as u64,
                &mut payload.as_slice(),
            )
            .unwrap();
        let reader = Archive::from_bytes(writer.finish().unwrap()).await.unwrap();
        assert!(reader.entry("file").await.unwrap().unwrap().extents.len() > 64);
        let _permits = reader
            .metadata_semaphore
            .clone()
            .acquire_many_owned(2)
            .await
            .unwrap();
        for (offset, size) in [(99_500, 500), (99_000, 750), (100_000, 0)] {
            let result = timeout(
                Duration::from_secs(1),
                reader.read_range("file", offset..offset + size),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                result.as_ref(),
                &payload[offset as usize..(offset + size) as usize]
            );
        }
        assert!(matches!(
            reader.read_range("file", 100_000..100_001).await,
            Err(Error::Range)
        ));
    }
    #[tokio::test]
    async fn cache_is_bounded_and_selected_bytes_keep_their_owner_after_eviction() {
        let source = Arc::new(Source::new(fixture()));
        let reader = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: usize::MAX,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let owned = reader.read_range("a", 0..5).await.unwrap();
        let one_chunk_budget = reader.cache_len_bytes();
        reader.cache.lock().unwrap().limit = one_chunk_budget;
        assert_eq!(
            reader.read_range("b", 0..5).await.unwrap(),
            b"world".as_slice()
        );
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        reader.read_range("c", 9000..9001).await.unwrap();
        reader.read_range("a", 0..5).await.unwrap();
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 3);
        assert!(reader.cache_len_bytes() <= one_chunk_budget);
        drop(reader);
        assert_eq!(owned, b"hello".as_slice());
    }
    struct Objects {
        bytes: Bytes,
        calls: AtomicUsize,
    }
    #[tokio::test]
    async fn shared_cache_reuses_sync_content_and_isolates_new_snapshots() {
        use crate::{MemoryDecodedCache, SharedDecodedCache};
        let raw = fixture();
        let memory = Arc::new(MemoryDecodedCache::new(1 << 20));
        let binding = SharedDecodedCache::new(b"snapshot-one", memory.clone()).unwrap();
        let mut sync = crate::backend::BlockingArchive::open(
            crate::SliceReader::new(&raw),
            Default::default(),
        )
        .unwrap();
        sync.set_shared_decoded_cache(Some(binding.clone()));
        sync.read("a", 0, 5, None, true).unwrap();
        drop(sync);
        let source = Arc::new(Source::new(raw));
        let reader = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_shared_decoded_cache(binding);
        let owned = reader.read_range("a", 0..5).await.unwrap();
        assert_eq!(owned, b"hello".as_slice());
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 0);
        drop(reader);
        let replaced = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_shared_decoded_cache(
            SharedDecodedCache::new(b"snapshot-two", memory.clone()).unwrap(),
        );
        assert_eq!(
            replaced.read_range("a", 0..5).await.unwrap(),
            b"hello".as_slice()
        );
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        let third = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_shared_decoded_cache(
            SharedDecodedCache::new(b"snapshot-two", memory.clone()).unwrap(),
        );
        third.read_range("a", 0..5).await.unwrap();
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 1);
        memory.clear();
        drop(replaced);
        drop(third);
        assert_eq!(owned, b"hello".as_slice());
    }

    struct BlockedCache {
        calls: AtomicUsize,
        started: Notify,
        released: (Mutex<bool>, std::sync::Condvar),
    }
    impl crate::DecodedCache for BlockedCache {
        fn get(&self, _: &crate::DecodedCacheKey) -> std::io::Result<Option<crate::CachedChunk>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            let (lock, notify) = &self.released;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = notify.wait(released).unwrap();
            }
            Ok(None)
        }
        fn put(&self, _: crate::CachedChunk) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn cancelled_cache_callbacks_keep_their_worker_permit_until_completion() {
        let cache = Arc::new(BlockedCache {
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            released: (Mutex::new(false), std::sync::Condvar::new()),
        });
        // Release blocking workers even if an assertion unwinds the test.
        struct Release(Arc<BlockedCache>);
        impl Drop for Release {
            fn drop(&mut self) {
                *self.0.released.0.lock().unwrap() = true;
                self.0.released.1.notify_all();
            }
        }
        let release = Release(cache.clone());
        let source = Arc::new(Source::new(fixture()));
        let reader = Arc::new(
            Archive::open_with_options(
                source.clone(),
                ReaderOptions {
                    cache_bytes: 0,
                    concurrent_chunks: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .with_shared_decoded_cache(
                crate::SharedDecodedCache::new(b"immutable", cache.clone()).unwrap(),
            ),
        );
        reader.entry("a").await.unwrap();
        reader.entry("c").await.unwrap();
        let first = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read_range("a", 0..5).await })
        };
        timeout(Duration::from_secs(2), cache.started.notified())
            .await
            .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(reader.semaphore.available_permits(), 0);
        let mut next = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read_range("c", 9000..9001).await })
        };
        assert!(timeout(Duration::from_millis(30), &mut next).await.is_err());
        assert_eq!(cache.calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.data_calls.load(Ordering::SeqCst), 0);
        drop(release);
        assert_eq!(
            timeout(Duration::from_secs(2), next)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            [7].as_slice()
        );
        assert_eq!(reader.semaphore.available_permits(), 1);
    }
    #[async_trait::async_trait]
    impl ObjectResolver for Objects {
        async fn get_encoded(&self, _: [u8; 32], _: u64, _: u64) -> Result<Bytes> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.bytes.clone())
        }
    }
    #[tokio::test]
    async fn object_hints_are_not_cache_identity_and_hash_policy_is_explicit() {
        let source = Arc::new(Source::new(
            include_bytes!("../../../testdata/entry-manifest/thin.zstar").to_vec(),
        ));
        let reader = Archive::open_with_options(
            source.clone(),
            ReaderOptions {
                cache_bytes: 0,
                ..Default::default()
            },
        )
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
        assert!(
            reader
                .read_range_with_resolver("hello.txt", 0, 6, Some(&wrong))
                .await
                .is_err()
        );
        assert_eq!(reader.cache_len_bytes(), 0);
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
        assert_eq!(unchecked.cache_len_bytes(), 0);
        // Same address, different unchecked resolvers: no cache or flight reuse.
        let prior_calls = wrong.calls.load(Ordering::SeqCst);
        let (first, second) = tokio::join!(
            unchecked.read_range_with_resolver("hello.txt", 0, 6, Some(&objects)),
            unchecked.read_range_with_resolver("hello.txt", 0, 6, Some(&wrong)),
        );
        assert_eq!(first.unwrap(), b"hello\n".as_slice());
        assert_eq!(second.unwrap(), b"wrong\n".as_slice());
        assert_eq!(wrong.calls.load(Ordering::SeqCst), prior_calls + 1);
        assert_eq!(unchecked.cache_len_bytes(), 0);
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
                cache_bytes: 0,
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
    async fn mandatory_checksums_and_authenticated_metadata_cannot_be_skipped() {
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
        reader.validate_metadata().await.unwrap();
        assert!(matches!(
            reader.read_range("a-b", 0..5).await,
            Err(Error::MalformedFrame(_))
        ));
        assert_eq!(reader.cache_len_bytes(), 0);
        let bytes = include_bytes!("../../../testdata/entry-manifest/invalid/metadata-hash.zstar");
        assert!(
            Archive::open_with_options(Arc::new(Source::new(bytes.to_vec())), Default::default())
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
            Archive::open_with_options(
                source.clone(),
                ReaderOptions {
                    cache_bytes: 0,
                    ..Default::default()
                },
            )
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
