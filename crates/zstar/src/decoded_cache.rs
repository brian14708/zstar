//! Caller-owned decoded caches receive opaque, immutable validated chunks.
use crate::{Error, Result};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// The source identity must identify an immutable encoded archive snapshot.
/// Metadata hashes alone cannot identify inline bytes. Object keys identify
/// complete SHA-256-verified decoded content and omit recompression size hints.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum DecodedCacheKey {
    Archive {
        identity: [u8; 32],
        metadata: [u8; 32],
        offset: u64,
        encoded_size: u64,
    },
    Object([u8; 32]),
}

#[derive(Clone, Debug)]
pub(crate) enum DecodedBody {
    Shared(Arc<[u8]>),
    #[cfg(feature = "async")]
    Async {
        raw: bytes::Bytes,
        allocated: usize,
    },
}
impl DecodedBody {
    #[cfg(any(feature = "async", test))]
    pub(crate) fn from_vec(raw: Vec<u8>) -> Self {
        #[cfg(feature = "async")]
        {
            let allocated = raw.capacity();
            Self::Async {
                raw: raw.into(),
                allocated,
            }
        }
        #[cfg(not(feature = "async"))]
        {
            Self::Shared(raw.into())
        }
    }
    pub(crate) fn compact(raw: Vec<u8>) -> Self {
        Self::Shared(raw.into())
    }
    pub(crate) fn allocated(&self) -> usize {
        match self {
            Self::Shared(raw) => raw.len(),
            #[cfg(feature = "async")]
            Self::Async { allocated, .. } => *allocated,
        }
    }
    #[cfg(feature = "async")]
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        match self {
            Self::Shared(raw) => bytes::Bytes::from_owner(raw),
            Self::Async { raw, .. } => raw,
        }
    }
}
impl Deref for DecodedBody {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Shared(raw) => raw,
            #[cfg(feature = "async")]
            Self::Async { raw, .. } => raw,
        }
    }
}

/// Only readers can create these values, after framing/checksum validation and
/// (for objects) complete decoded SHA-256 verification. Backend implementations
/// may retain/clone them but cannot safely forge or mutate their provenance.
#[derive(Clone, Debug)]
pub struct CachedChunk {
    key: DecodedCacheKey,
    body: DecodedBody,
}
impl CachedChunk {
    #[must_use]
    pub fn key(&self) -> &DecodedCacheKey {
        &self.key
    }
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.body
    }
    /// Retained payload allocation plus conservative key/owner/LRU overhead.
    #[must_use]
    pub fn weight(&self) -> usize {
        self.body.allocated().saturating_add(512)
    }
    pub(crate) fn validated(key: DecodedCacheKey, body: DecodedBody) -> Self {
        Self { key, body }
    }
}

/// Errors are advisory: a failed get is a miss, a failed put does not fail a
/// read. Implementations must bound their storage and must not re-enter readers.
/// Reader close does not close a caller-owned cache. Native async readers run
/// custom callbacks on bounded blocking workers; canceled work retains its
/// permit. The built-in memory backend uses a direct bounded lookup.
pub trait DecodedCache: Any + Send + Sync {
    fn get(&self, key: &DecodedCacheKey) -> io::Result<Option<CachedChunk>>;
    fn put(&self, value: CachedChunk) -> io::Result<()>;
}

#[derive(Clone)]
pub struct SharedDecodedCache {
    identity: [u8; 32],
    backend: Arc<dyn DecodedCache>,
}
#[cfg(feature = "async")]
pub(crate) enum BuiltinLookup {
    Unavailable,
    Result(Option<DecodedBody>),
}
impl std::fmt::Debug for SharedDecodedCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedDecodedCache")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl SharedDecodedCache {
    /// Supply an immutable source version (e.g. qualified object version/ETag,
    /// or a complete archive digest). Different snapshots need different IDs.
    pub fn new(identity: &[u8], backend: Arc<dyn DecodedCache>) -> Result<Self> {
        if identity.is_empty() || identity.len() > 65536 {
            return Err(Error::Budget(
                "decoded cache identity must contain 1..65536 bytes",
            ));
        }
        let mut hash = Sha256::new();
        hash.update(b"zstar/decoded-cache-identity/v1\0");
        hash.update(identity);
        Ok(Self {
            identity: hash.finalize().into(),
            backend,
        })
    }
    pub(crate) fn archive_key(
        &self,
        metadata: [u8; 32],
        offset: u64,
        encoded_size: u64,
    ) -> DecodedCacheKey {
        DecodedCacheKey::Archive {
            identity: self.identity,
            metadata,
            offset,
            encoded_size,
        }
    }
    pub(crate) fn get(&self, key: &DecodedCacheKey) -> Option<DecodedBody> {
        let value = self.backend.get(key).ok()??;
        // A backend may return an unrelated, otherwise valid token. It must
        // never be accepted for another source, locator, or object identity.
        (value.key == *key).then_some(value.body)
    }
    #[cfg(feature = "async")]
    pub(crate) fn memory_get(&self, key: &DecodedCacheKey) -> BuiltinLookup {
        // Only the concrete built-in backend can bypass blocking workers.
        // User implementations cannot mark arbitrary callbacks as nonblocking.
        let backend: &dyn Any = self.backend.as_ref();
        if backend.downcast_ref::<MemoryDecodedCache>().is_none() {
            return BuiltinLookup::Unavailable;
        }
        BuiltinLookup::Result(self.get(key))
    }
    pub(crate) fn put(&self, key: DecodedCacheKey, body: DecodedBody) {
        let _ = self.backend.put(CachedChunk::validated(key, body));
    }
}

struct MemoryState {
    values: HashMap<DecodedCacheKey, (CachedChunk, u64)>,
    ages: BTreeMap<u64, DecodedCacheKey>,
    used: usize,
    clock: u64,
}
/// A bounded cache that can be shared across synchronous/asynchronous readers.
pub struct MemoryDecodedCache {
    capacity: usize,
    state: Mutex<MemoryState>,
}
impl MemoryDecodedCache {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(MemoryState {
                values: HashMap::new(),
                ages: BTreeMap::new(),
                used: 0,
                clock: 0,
            }),
        }
    }
    pub fn used(&self) -> usize {
        self.state.lock().unwrap().used
    }
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.values.clear();
        state.ages.clear();
        state.used = 0;
    }
}
impl DecodedCache for MemoryDecodedCache {
    fn get(&self, key: &DecodedCacheKey) -> io::Result<Option<CachedChunk>> {
        let mut state = self.state.lock().unwrap();
        let Some((value, previous)) = state.values.get(key).cloned() else {
            return Ok(None);
        };
        state.clock = state
            .clock
            .checked_add(1)
            .ok_or_else(|| io::Error::other("decoded cache clock exhausted"))?;
        let age = state.clock;
        state.ages.remove(&previous);
        state.ages.insert(age, *key);
        state.values.get_mut(key).unwrap().1 = age;
        Ok(Some(value))
    }
    fn put(&self, value: CachedChunk) -> io::Result<()> {
        let weight = value.weight();
        if weight > self.capacity {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        state.clock = state
            .clock
            .checked_add(1)
            .ok_or_else(|| io::Error::other("decoded cache clock exhausted"))?;
        let age = state.clock;
        if let Some((previous, stamp)) = state.values.remove(&value.key) {
            state.used -= previous.weight();
            state.ages.remove(&stamp);
        }
        while state.used > self.capacity - weight {
            let (_, oldest) = state.ages.pop_first().unwrap();
            state.used -= state.values.remove(&oldest).unwrap().0.weight();
        }
        state.used += weight;
        state.ages.insert(age, value.key);
        state.values.insert(value.key, (value, age));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{BlockingArchive, EncodedObjectResolver};
    use crate::{RangeReader, SliceReader};
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Source<'a> {
        raw: &'a [u8],
        calls: Arc<AtomicUsize>,
        data_end: u64,
    }
    impl<'a> Source<'a> {
        fn new(raw: &'a [u8], calls: Arc<AtomicUsize>) -> Self {
            Self {
                raw,
                calls,
                data_end: crate::parse_footer(&raw[raw.len() - 80..])
                    .unwrap()
                    .toc_offset,
            }
        }
    }
    impl RangeReader for Source<'_> {
        fn len(&self) -> u64 {
            self.raw.len() as u64
        }
        fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
            if range.start < self.data_end {
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(self.raw[range.start as usize..range.end as usize].to_vec())
        }
    }
    const INLINE: &[u8] = include_bytes!("../../../testdata/entry-manifest/inline.zstar");
    const THIN: &[u8] = include_bytes!("../../../testdata/entry-manifest/thin.zstar");

    #[test]
    fn shared_reads_survive_reader_drop_and_local_cache_reset() {
        let memory = Arc::new(MemoryDecodedCache::new(1 << 20));
        let binding = SharedDecodedCache::new(b"immutable-version-1", memory.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut first =
            BlockingArchive::open(Source::new(INLINE, calls.clone()), Default::default()).unwrap();
        first.set_shared_decoded_cache(Some(binding.clone()));
        assert_eq!(first.read("a-b", 0, 5, None, true).unwrap(), b"hello");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(first);
        let mut second =
            BlockingArchive::open(Source::new(INLINE, calls.clone()), Default::default()).unwrap();
        second.set_shared_decoded_cache(Some(binding.clone()));
        second.set_content_cache_bytes(0);
        assert_eq!(second.read("a-b", 0, 5, None, true).unwrap(), b"hello");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let extent = second.entry("a-b").unwrap().unwrap().extents.remove(0);
        let crate::manifest::Source::Archive(offset) = extent.source else {
            panic!()
        };
        let key = binding.archive_key(
            crate::parse_footer(&INLINE[INLINE.len() - 80..])
                .unwrap()
                .toc_sha256,
            offset,
            extent.encoded_size,
        );
        let token = memory.get(&key).unwrap().unwrap();
        memory.clear();
        drop(second);
        assert_ne!(token.as_bytes(), [] as [u8; 0]);
        assert_eq!(memory.used(), 0);
    }

    struct WrongKey(CachedChunk);
    impl DecodedCache for WrongKey {
        fn get(&self, _: &DecodedCacheKey) -> io::Result<Option<CachedChunk>> {
            Ok(Some(self.0.clone()))
        }
        fn put(&self, _: CachedChunk) -> io::Result<()> {
            Err(io::Error::other("offline"))
        }
    }
    struct Offline;
    impl DecodedCache for Offline {
        fn get(&self, _: &DecodedCacheKey) -> io::Result<Option<CachedChunk>> {
            Err(io::Error::other("offline"))
        }
        fn put(&self, _: CachedChunk) -> io::Result<()> {
            Err(io::Error::other("offline"))
        }
    }

    #[test]
    fn identities_metadata_and_locators_isolate_shared_tokens() {
        let memory = Arc::new(MemoryDecodedCache::new(4096));
        let a = SharedDecodedCache::new(b"version-a", memory.clone()).unwrap();
        let b = SharedDecodedCache::new(b"version-b", memory.clone()).unwrap();
        let key = a.archive_key([1; 32], 0, 30);
        a.put(key, DecodedBody::from_vec(vec![42]));
        assert!(b.get(&b.archive_key([1; 32], 0, 30)).is_none());
        assert!(a.get(&a.archive_key([2; 32], 0, 30)).is_none());
        assert!(a.get(&a.archive_key([1; 32], 1, 30)).is_none());
        assert!(a.get(&a.archive_key([1; 32], 0, 31)).is_none());
        let wrong = SharedDecodedCache::new(
            b"version-b",
            Arc::new(WrongKey(memory.get(&key).unwrap().unwrap())),
        )
        .unwrap();
        assert!(wrong.get(&wrong.archive_key([1; 32], 0, 30)).is_none());
        for backend in [
            Arc::new(Offline) as Arc<dyn DecodedCache>,
            Arc::new(WrongKey(memory.get(&key).unwrap().unwrap())),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let mut reader =
                BlockingArchive::open(Source::new(INLINE, calls.clone()), Default::default())
                    .unwrap();
            reader.set_shared_decoded_cache(Some(
                SharedDecodedCache::new(b"version-a", backend).unwrap(),
            ));
            assert_eq!(reader.read("a-b", 0, 5, None, true).unwrap(), b"hello");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        assert!(SharedDecodedCache::new(b"", memory.clone()).is_err());
        assert!(SharedDecodedCache::new(&vec![0; 65537], memory).is_err());
    }

    #[test]
    fn memory_cache_bounds_lru_and_retains_immutable_owners() {
        let one = CachedChunk::validated(
            DecodedCacheKey::Object([1; 32]),
            DecodedBody::from_vec(vec![1; 128]),
        );
        let two = CachedChunk::validated(
            DecodedCacheKey::Object([2; 32]),
            DecodedBody::from_vec(vec![2; 128]),
        );
        let three = CachedChunk::validated(
            DecodedCacheKey::Object([3; 32]),
            DecodedBody::from_vec(vec![3; 128]),
        );
        let cache = MemoryDecodedCache::new(one.weight() * 2);
        cache.put(one.clone()).unwrap();
        cache.put(two.clone()).unwrap();
        let owner = cache.get(one.key()).unwrap().unwrap();
        cache.put(three).unwrap();
        assert!(cache.get(two.key()).unwrap().is_none());
        assert_eq!(cache.used(), one.weight() * 2);
        cache.clear();
        assert_eq!(owner.as_bytes(), &[1; 128]);
        let zero = MemoryDecodedCache::new(0);
        zero.put(one).unwrap();
        assert_eq!(zero.used(), 0);
        let mut excess = Vec::with_capacity(4096);
        excess.extend_from_slice(b"short");
        let body = DecodedBody::from_vec(excess);
        #[cfg(feature = "async")]
        assert_eq!(body.allocated(), 4096);
        assert!(body.allocated() >= body.len());
    }

    struct Objects {
        encoded: Vec<u8>,
        calls: usize,
    }
    impl EncodedObjectResolver for Objects {
        fn get_encoded(&mut self, _: [u8; 32], _: u64, _: u64) -> Result<Vec<u8>> {
            self.calls += 1;
            Ok(self.encoded.clone())
        }
    }
    struct Observed {
        memory: MemoryDecodedCache,
        gets: AtomicUsize,
        puts: AtomicUsize,
    }
    impl DecodedCache for Observed {
        fn get(&self, key: &DecodedCacheKey) -> io::Result<Option<CachedChunk>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.memory.get(key)
        }
        fn put(&self, value: CachedChunk) -> io::Result<()> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.memory.put(value)
        }
    }
    #[test]
    fn only_verified_objects_publish_and_unchecked_operations_bypass_shared_cache() {
        let cache = Arc::new(Observed {
            memory: MemoryDecodedCache::new(1 << 20),
            gets: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
        });
        let binding = SharedDecodedCache::new(b"snapshot-a", cache.clone()).unwrap();
        let mut reader = BlockingArchive::open(SliceReader::new(THIN), Default::default()).unwrap();
        reader.set_shared_decoded_cache(Some(binding));
        let mut wrong = Objects {
            encoded: crate::encode_frame(b"wrong\nbytes\n", 3).unwrap(),
            calls: 0,
        };
        assert!(
            reader
                .read("hello.txt", 0, 6, Some(&mut wrong), true)
                .is_err()
        );
        assert_eq!(cache.puts.load(Ordering::SeqCst), 0);
        assert_eq!(
            reader
                .read("hello.txt", 0, 6, Some(&mut wrong), false)
                .unwrap(),
            b"wrong\n"
        );
        assert_eq!(cache.gets.load(Ordering::SeqCst), 1);
        assert_eq!(cache.puts.load(Ordering::SeqCst), 0);
        let mut good = Objects {
            encoded: crate::encode_frame(b"hello\nworld\n", 3).unwrap(),
            calls: 0,
        };
        reader
            .read("hello.txt", 0, 6, Some(&mut good), true)
            .unwrap();
        assert_eq!(cache.puts.load(Ordering::SeqCst), 1);
        drop(reader);
        let mut second = BlockingArchive::open(SliceReader::new(THIN), Default::default()).unwrap();
        second.set_shared_decoded_cache(Some(
            SharedDecodedCache::new(b"other-archive", cache.clone()).unwrap(),
        ));
        assert_eq!(
            second.read("world.txt", 0, 6, None, true).unwrap(),
            b"world\n"
        );
        assert_eq!(good.calls, 1);
        // Even a verified shared hit cannot override a caller's unchecked resolver.
        assert_eq!(
            second
                .read("hello.txt", 0, 6, Some(&mut wrong), false)
                .unwrap(),
            b"wrong\n"
        );
        assert_eq!(cache.puts.load(Ordering::SeqCst), 1);
    }
}
