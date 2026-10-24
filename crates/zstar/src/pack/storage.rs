use super::{Catalog, CatalogLimits, ChunkRecord, PackRecord, Snapshot};
use crate::manifest_io::{ManifestOptions, Spool};
use crate::{Digest, Error, Extent, ObjectResolver, Result, Source};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    ops::Range,
    sync::{Arc, Mutex, Weak},
};

/// Runtime configuration owns credentials and endpoints. New objects are
/// immutable and uniquely keyed. A range must match the supplied identity and
/// exact length; cancelling its future cancels pending requests.
pub trait PackStore: Send + Sync {
    /// Whether retained content may be reused under this snapshot's response policy.
    fn cache_allowed(&self, _snapshot: &Snapshot) -> bool {
        true
    }
    fn put(&self, key: String, data: Spool) -> impl Future<Output = Result<Snapshot>> + Send;
    fn get_range(
        &self,
        snapshot: &Snapshot,
        range: Range<u64>,
    ) -> impl Future<Output = Result<Bytes>> + Send;
}

#[derive(Clone, Debug)]
pub struct PackWriterOptions {
    pub prefix: String,
    pub target_bytes: u64,
    pub compression_level: i32,
    pub catalog: CatalogLimits,
    pub spool: ManifestOptions,
}
impl Default for PackWriterOptions {
    fn default() -> Self {
        Self {
            prefix: "objects".into(),
            target_bytes: 64 << 20,
            compression_level: 3,
            catalog: CatalogLimits::default(),
            spool: ManifestOptions::default(),
        }
    }
}
/// One bounded spool, one compression job, and one upload at a time. The
/// catalog index is separately bounded. Reuse is local to an explicit base
/// catalog/session; successful uploads may be orphaned after a later failure.
pub struct PackWriter<S> {
    store: Arc<S>,
    options: PackWriterOptions,
    state: Option<PackState>,
    failed: bool,
    completed: Vec<Snapshot>,
}

// Move the complete CPU state into one blocking job per chunk. Its compressor
// and encoded buffer survive jobs; maps move with it rather than being cloned.
struct PackState {
    catalog: Catalog,
    pending: BTreeMap<[u8; 32], ChunkRecord>,
    spool: Option<Spool>,
    hash: Sha256,
    compressor: zstd::bulk::Compressor<'static>,
    encoded: Vec<u8>,
}
enum PreparedChunk {
    Ready(Extent),
    Flush {
        digest: [u8; 32],
        decoded_length: u64,
    },
}
impl PackState {
    fn prepare(&mut self, raw: Bytes, options: &PackWriterOptions) -> Result<PreparedChunk> {
        let digest = *Digest::sha256(&raw).as_bytes();
        let decoded_length = raw.len() as u64;
        if let Some(chunk) = self
            .catalog
            .chunks()
            .get(&digest)
            .or_else(|| self.pending.get(&digest))
        {
            if chunk.decoded_length != decoded_length {
                return Err(Error::MalformedCatalog("inconsistent chunk size".into()));
            }
            return Ok(PreparedChunk::Ready(Extent {
                size: decoded_length,
                source: Source::Object(digest),
                encoded_size: chunk.encoded_length,
                offset: 0,
            }));
        }
        let count = self.catalog.chunks().len() + self.pending.len() + 1;
        if count > options.catalog.max_chunks || count > options.catalog.max_memory_bytes / 512 {
            return Err(Error::Budget("pack hash index"));
        }
        self.encoded.clear();
        self.encoded
            .reserve(zstd::zstd_safe::compress_bound(raw.len()));
        self.compressor
            .compress_to_buffer(&raw, &mut self.encoded)?;
        let spool = self.spool.as_ref().unwrap();
        if !spool.is_empty()
            && spool
                .len()
                .checked_add(self.encoded.len() as u64)
                .is_none_or(|n| n > options.target_bytes)
        {
            return Ok(PreparedChunk::Flush {
                digest,
                decoded_length,
            });
        }
        Ok(PreparedChunk::Ready(
            self.append_chunk(digest, decoded_length)?,
        ))
    }
    fn append_chunk(&mut self, digest: [u8; 32], decoded_length: u64) -> Result<Extent> {
        let encoded_length = self.encoded.len() as u64;
        let offset = self.spool.as_mut().unwrap().append(&self.encoded)?;
        self.hash.update(&self.encoded);
        self.pending.insert(
            digest,
            ChunkRecord {
                digest,
                pack: [0; 32],
                offset,
                encoded_length,
                decoded_length,
            },
        );
        Ok(Extent {
            size: decoded_length,
            source: Source::Object(digest),
            encoded_size: encoded_length,
            offset: 0,
        })
    }
}
impl<S: PackStore> PackWriter<S> {
    pub fn new(store: Arc<S>, base: Catalog, options: PackWriterOptions) -> Result<Self> {
        if !crate::manifest::valid_path(&options.prefix)
            || options.target_bytes == 0
            || options.target_bytes > options.spool.max_encoded_bytes
            || !(-131_072..=22).contains(&options.compression_level)
        {
            return Err(Error::Unsupported("invalid pack writer settings"));
        }
        base.encode(options.catalog)?;
        let mut compressor = zstd::bulk::Compressor::new(options.compression_level)?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        Ok(Self {
            store,
            state: Some(PackState {
                catalog: base,
                pending: BTreeMap::new(),
                spool: Some(Spool::new(options.spool.clone())),
                hash: Sha256::new(),
                compressor,
                encoded: Vec::new(),
            }),
            options,
            failed: false,
            completed: Vec::new(),
        })
    }
    /// Uploaded packs owned by this session. Never delete base/shared packs as
    /// failure cleanup. Callers may record these keys for orphan accounting.
    #[must_use]
    pub fn completed_packs(&self) -> &[Snapshot] {
        &self.completed
    }
    /// Cancel this session and release pending spool storage. Completed packs
    /// remain immutable and are exposed for orphan accounting.
    pub fn abort(&mut self) {
        self.failed = true;
        self.state = None;
    }
    fn check_open(&self) -> Result<()> {
        if self.failed || self.state.is_none() {
            return Err(Error::Unsupported("pack writer failed or closed"));
        }
        Ok(())
    }
    pub async fn put_chunk(&mut self, raw: Bytes) -> Result<Extent> {
        self.check_open()?;
        if raw.is_empty() || raw.len() > crate::DATA_FRAME_MAX {
            return Err(Error::Budget("external object size"));
        }
        // Mark before awaiting: cancellation poisons the session rather than
        // silently omitting or reordering an admitted chunk.
        self.failed = true;
        let mut state = self.state.take().unwrap();
        let options = self.options.clone();
        let (state, prepared) = tokio::task::spawn_blocking(move || {
            let prepared = state.prepare(raw, &options)?;
            Ok::<_, Error>((state, prepared))
        })
        .await
        .map_err(|_| Error::Unsupported("pack chunk worker failed"))??;
        self.state = Some(state);
        let extent = match prepared {
            PreparedChunk::Ready(extent) => extent,
            PreparedChunk::Flush {
                digest,
                decoded_length,
            } => {
                self.flush_pending().await?;
                let mut state = self.state.take().unwrap();
                let (state, extent) = tokio::task::spawn_blocking(move || {
                    let extent = state.append_chunk(digest, decoded_length)?;
                    Ok::<_, Error>((state, extent))
                })
                .await
                .map_err(|_| Error::Unsupported("pack spool worker failed"))??;
                self.state = Some(state);
                extent
            }
        };
        self.failed = false;
        Ok(extent)
    }
    async fn flush_pending(&mut self) -> Result<()> {
        let state = self.state.as_mut().unwrap();
        if state.pending.is_empty() {
            return Ok(());
        }
        if state.catalog.packs().len() == self.options.catalog.max_packs {
            return Err(Error::Budget("pack count"));
        }
        let id: [u8; 32] = state.hash.clone().finalize().into();
        let address = crate::manifest::object_address(&id);
        let key = format!("{}/packs/{}.zstp", self.options.prefix, &address[7..]);
        let spool = state.spool.take().unwrap();
        let length = spool.len();
        let snapshot = self.store.put(key.clone(), spool).await?;
        snapshot.validate()?;
        if snapshot.key != key || snapshot.length != length {
            return Err(Error::ChangedPack);
        }
        self.completed.push(snapshot.clone());
        let mut packs: Vec<_> = state.catalog.packs().values().cloned().collect();
        let mut chunks: Vec<_> = state.catalog.chunks().values().cloned().collect();
        packs.push(PackRecord { id, snapshot });
        chunks.extend(
            std::mem::take(&mut state.pending)
                .into_values()
                .map(|mut chunk| {
                    chunk.pack = id;
                    chunk
                }),
        );
        state.catalog = Catalog::new(packs, chunks, self.options.catalog)?;
        state.spool = Some(Spool::new(self.options.spool.clone()));
        state.hash = Sha256::new();
        Ok(())
    }
    pub async fn finish(&mut self) -> Result<Catalog> {
        self.check_open()?;
        self.failed = true;
        self.flush_pending().await?;
        let state = self.state.take().unwrap();
        state.catalog.encode(self.options.catalog)?;
        Ok(state.catalog)
    }
}
impl<S: PackStore> crate::chunking::ObjectWriter for PackWriter<S> {
    async fn put_chunk(&mut self, raw: Bytes) -> Result<Extent> {
        self.put_chunk(raw).await
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResolverOptions {
    pub max_range_bytes: u64,
    pub max_overfetch_bytes: u64,
    pub max_batch_bytes: u64,
    pub cache_bytes: usize,
    /// Global concurrent range requests, including overlapping batches.
    pub concurrent_requests: usize,
}
impl Default for ResolverOptions {
    fn default() -> Self {
        Self {
            max_range_bytes: 8 << 20,
            max_overfetch_bytes: 0,
            max_batch_bytes: 64 << 20,
            cache_bytes: 0,
            concurrent_requests: 8,
        }
    }
}
type Flight = tokio::sync::Mutex<Option<Bytes>>;
struct EncodedCache {
    entries: BTreeMap<[u8; 32], Bytes>,
    used: usize,
}
/// A resolver owns one immutable catalog. Encoded flights/caches are separate
/// from the archive's decoded cache and never bypass its hash policy.
pub struct PackedObjectResolver<S> {
    store: Arc<S>,
    catalog: Arc<Catalog>,
    options: ResolverOptions,
    flights: Mutex<BTreeMap<[u8; 32], Weak<Flight>>>,
    cache: Mutex<EncodedCache>,
    requests: tokio::sync::Semaphore,
}
impl<S: PackStore> PackedObjectResolver<S> {
    pub fn new(store: Arc<S>, catalog: Catalog, options: ResolverOptions) -> Result<Self> {
        if !(1..=32).contains(&options.concurrent_requests)
            || options.max_range_bytes == 0
            || options.max_range_bytes > options.max_batch_bytes
            || options.max_batch_bytes > 256 << 20
            || options.max_overfetch_bytes > options.max_range_bytes
        {
            return Err(Error::Unsupported("invalid packed resolver settings"));
        }
        Ok(Self {
            store,
            catalog: Arc::new(catalog),
            requests: tokio::sync::Semaphore::new(options.concurrent_requests),
            options,
            flights: Mutex::new(BTreeMap::new()),
            cache: Mutex::new(EncodedCache {
                entries: BTreeMap::new(),
                used: 0,
            }),
        })
    }
    pub async fn get_many(&self, requests: &[crate::archive::ObjectRequest]) -> Result<Vec<Bytes>> {
        if requests.len() > crate::MAX_BATCH_REQUESTS {
            return Err(Error::Budget("packed batch count"));
        }
        let mut requested = BTreeMap::new();
        let mut total = 0_u64;
        for request in requests {
            let chunk = self
                .catalog
                .chunks()
                .get(&request.address)
                .ok_or(Error::MissingObject(request.address))?;
            if chunk.encoded_length > request.max_bytes {
                return Err(Error::Budget("encoded packed object size"));
            }
            total = total
                .checked_add(chunk.encoded_length)
                .ok_or(Error::Budget("packed batch bytes"))?;
            if total > self.options.max_batch_bytes {
                return Err(Error::Budget("packed batch bytes"));
            }
            requested.insert(request.address, chunk);
        }
        self.flights
            .lock()
            .unwrap()
            .retain(|_, flight| flight.strong_count() != 0);
        let mut guards = Vec::new();
        for (&digest, chunk) in &requested {
            let flight = {
                let mut flights = self.flights.lock().unwrap();
                if let Some(flight) = flights.get(&digest).and_then(Weak::upgrade) {
                    flight
                } else {
                    let flight = Arc::new(Flight::new(None));
                    flights.insert(digest, Arc::downgrade(&flight));
                    flight
                }
            };
            let mut guard = flight.lock_owned().await;
            if guard.is_none()
                && self
                    .store
                    .cache_allowed(&self.catalog.packs()[&chunk.pack].snapshot)
            {
                *guard = self.cache.lock().unwrap().entries.get(&digest).cloned();
            }
            guards.push((digest, *chunk, guard));
        }
        let mut misses: Vec<_> = guards
            .iter()
            .enumerate()
            .filter(|(_, (_, _, guard))| guard.is_none())
            .map(|(i, (_, chunk, _))| (chunk.pack, chunk.offset, i))
            .collect();
        misses.sort_unstable();
        let mut groups = Vec::new();
        let mut aggregate_overfetch = 0;
        let mut cursor = 0;
        while cursor < misses.len() {
            let (pack, start, index) = misses[cursor];
            let mut end = start + guards[index].1.encoded_length;
            let first = cursor;
            cursor += 1;
            while cursor < misses.len() {
                let (next_pack, offset, index) = misses[cursor];
                let next_end = offset + guards[index].1.encoded_length;
                let gap = offset.saturating_sub(end);
                if next_pack != pack
                    || next_end - start > self.options.max_range_bytes
                    || gap
                        > self
                            .options
                            .max_overfetch_bytes
                            .saturating_sub(aggregate_overfetch)
                    || total + aggregate_overfetch + gap > self.options.max_batch_bytes
                {
                    break;
                }
                aggregate_overfetch += gap;
                end = next_end;
                cursor += 1;
            }
            groups.push((pack, start, end, first, cursor));
        }
        // Stream results so completed group buffers are released promptly.
        // Dropping a batch cancels its futures and releases global permits.
        let mut loads = stream::iter(groups.into_iter().map(
            |(pack, start, end, first, last)| async move {
                let _permit = self
                    .requests
                    .acquire()
                    .await
                    .map_err(|_| Error::Unsupported("resolver closed"))?;
                let snapshot = &self.catalog.packs()[&pack].snapshot;
                let data = self.store.get_range(snapshot, start..end).await?;
                if data.len() as u64 != end - start {
                    return Err(Error::MalformedExternalContent("short packed range".into()));
                }
                Ok::<_, Error>((start, first, last, data))
            },
        ))
        .buffer_unordered(self.options.concurrent_requests);
        while let Some((start, first, last, data)) = loads.try_next().await? {
            for &(_, offset, index) in &misses[first..last] {
                let chunk = guards[index].1;
                let bytes = Bytes::copy_from_slice(
                    &data[(offset - start) as usize
                        ..(offset - start + chunk.encoded_length) as usize],
                );
                *guards[index].2 = Some(bytes.clone());
                let charge = bytes.len() + 128;
                if charge <= self.options.cache_bytes
                    && self
                        .store
                        .cache_allowed(&self.catalog.packs()[&chunk.pack].snapshot)
                {
                    let mut cache = self.cache.lock().unwrap();
                    while cache.used > self.options.cache_bytes - charge {
                        if let Some((_, removed)) = cache.entries.pop_first() {
                            cache.used -= removed.len() + 128;
                        } else {
                            break;
                        }
                    }
                    if let Some(previous) = cache.entries.insert(chunk.digest, bytes) {
                        cache.used -= previous.len() + 128;
                    }
                    cache.used += charge;
                }
            }
        }
        let values: BTreeMap<_, _> = guards
            .iter()
            .map(|(digest, _, value)| (*digest, value.as_ref().unwrap().clone()))
            .collect();
        Ok(requests
            .iter()
            .map(|request| values[&request.address].clone())
            .collect())
    }
}
#[async_trait::async_trait]
impl<S: PackStore> ObjectResolver for PackedObjectResolver<S> {
    fn cache_allowed(&self, address: [u8; 32]) -> bool {
        self.catalog.chunks().get(&address).is_some_and(|chunk| {
            self.store
                .cache_allowed(&self.catalog.packs()[&chunk.pack].snapshot)
        })
    }
    async fn get_encoded(&self, address: [u8; 32], hint: u64, max_bytes: u64) -> Result<Bytes> {
        Ok(self
            .get_many(&[crate::archive::ObjectRequest {
                address,
                hint,
                max_bytes,
            }])
            .await?
            .pop()
            .unwrap())
    }
    async fn get_encoded_many(
        &self,
        requests: &[crate::archive::ObjectRequest],
    ) -> Result<Vec<Bytes>> {
        self.get_many(requests).await
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn encoded_location(&self, address: [u8; 32]) -> Option<([u8; 32], u64)> {
        self.catalog
            .chunks()
            .get(&address)
            .map(|chunk| (chunk.pack, chunk.offset))
    }
    fn encoded_length(&self, address: [u8; 32]) -> Option<u64> {
        self.catalog
            .chunks()
            .get(&address)
            .map(|chunk| chunk.encoded_length)
    }
}
