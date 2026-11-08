//! Bounded compressed-byte caching above an immutable range source.
use crate::{AsyncRangeReader, Error, Result};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    io,
    ops::Range,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

/// Storage and concurrency policy for [`CachedRangeReader`].
#[derive(Clone, Debug)]
pub struct RangeCacheOptions {
    /// Maximum retained payload bytes; zero disables completed-range caching.
    /// Outstanding fetches and buffers held by callers are additional memory.
    pub capacity_bytes: usize,
    /// Maximum number of retained intervals, including very small ranges.
    pub max_entries: usize,
    /// Extra bytes fetched after a miss; zero (the default) avoids speculation.
    /// This can also satisfy nearby concurrent reads from the same fetch.
    pub read_ahead_bytes: usize,
    /// Maximum expanded fetch length. Larger caller requests are fetched exactly
    /// without read-ahead. Configure this within the underlying source's limits.
    pub max_fetch_bytes: usize,
    /// Maximum outstanding fetch intervals, including their waiting readers.
    pub max_in_flight: usize,
}
impl Default for RangeCacheOptions {
    fn default() -> Self {
        Self {
            capacity_bytes: 16 << 20,
            max_entries: 1024,
            read_ahead_bytes: 0,
            max_fetch_bytes: 8 << 20,
            max_in_flight: 8,
        }
    }
}
impl RangeCacheOptions {
    /// Checks interval storage and fetch concurrency settings without source I/O.
    ///
    /// # Errors
    /// Returns an invalid-options error for zero entry/fetch limits or concurrency
    /// outside one through 1024. Zero cache capacity remains valid.
    pub const fn validate(&self) -> Result<()> {
        if self.max_entries == 0 {
            return Err(Error::InvalidOptions {
                option: "range_cache.max_entries",
                reason: "must be positive",
            });
        }
        if self.max_fetch_bytes == 0 {
            return Err(Error::InvalidOptions {
                option: "range_cache.max_fetch_bytes",
                reason: "must be positive",
            });
        }
        if self.max_in_flight == 0 || self.max_in_flight > 1024 {
            return Err(Error::InvalidOptions {
                option: "range_cache.max_in_flight",
                reason: "must be between 1 and 1024",
            });
        }
        Ok(())
    }
}

/// Cumulative counters and current retained storage. Counters are approximate
/// snapshots during concurrent operations and do not include HTTP headers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RangeCacheStats {
    /// All calls, including empty or invalid ranges.
    pub read_requests: u64,
    /// Calls satisfied by a completed cached interval.
    pub cache_hits: u64,
    /// Calls joining an existing fetch interval (possibly already completed).
    pub coalesced_reads: u64,
    /// Underlying range calls started, including errors and cancellations.
    pub source_reads: u64,
    /// Bytes returned successfully by exact-length underlying calls.
    pub source_bytes: u64,
    /// Intervals removed to meet capacity or entry limits.
    pub evictions: u64,
    /// Sum of retained interval lengths, excluding outstanding fetches/outputs.
    pub cached_bytes: usize,
    /// Number of retained intervals.
    pub cached_entries: usize,
}
#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    hits: AtomicU64,
    coalesced: AtomicU64,
    reads: AtomicU64,
    bytes: AtomicU64,
    evictions: AtomicU64,
}
struct Cached {
    range: Range<u64>,
    bytes: Bytes,
}
struct Flight {
    range: Range<u64>,
    value: OnceCell<Bytes>,
    // Retained by every participant, including a waiter taking over after cancellation.
    _permit: OwnedSemaphorePermit,
}
#[derive(Default)]
struct State {
    cache: VecDeque<Cached>,
    bytes: usize,
    flights: Vec<Weak<Flight>>,
}
enum Selection {
    Hit(Bytes),
    Flight(Arc<Flight>),
}

/// An optional decorator that caches compressed bytes, not decoded frames.
///
/// Cached intervals are evicted in least-recently-used order. A containing
/// interval serves a requested subrange; arbitrary partially overlapping
/// intervals are not stitched together. Concurrent requests contained in one
/// in-flight interval share its fetch. Optional forward read-ahead lets nearby
/// requests share an interval, without a queueing delay or detached I/O task.
/// It does not merge arbitrary adjacent requests outside that interval.
///
/// The source must represent one immutable snapshot for the decorator's entire
/// lifetime. Wrap a pinned HTTP source or an immutable open local file; never
/// reuse a decorator across object replacements. Cache hits intentionally do
/// not contact the source to revalidate it. Source errors are returned unchanged
/// and not cached; waiting readers can retry a failed or cancelled fetch.
/// Dropping the last reader of a fetch cancels its underlying future. If other
/// readers remain, one can take over a cancelled leader's work.
///
/// Capacity bounds retained payload lengths and entry count, not all process
/// memory: returned `Bytes` slices can outlive eviction, and outstanding fetches
/// each require their own response buffer. No compressed frame is decoded here.
///
/// ```
/// # async fn example() -> zstar::Result<()> {
/// use std::sync::Arc;
/// use zstar::{AsyncRangeReader, CachedRangeReader, RangeCacheOptions};
/// let source = Arc::new(bytes::Bytes::from_static(b"abcdef"));
/// let source = CachedRangeReader::new(source, RangeCacheOptions::default())?;
/// assert_eq!(source.read_range(0..4).await?.as_ref(), b"abcd");
/// assert_eq!(source.read_range(1..3).await?.as_ref(), b"bc");
/// assert_eq!(source.stats().source_reads, 1);
/// # Ok(())
/// # }
/// ```
pub struct CachedRangeReader<R: AsyncRangeReader + ?Sized> {
    source: Arc<R>,
    size: OnceCell<u64>,
    fetches: FetchCoordinator,
}

// Own admission, shared fetches, and cache publication together. The decorator
// itself only validates requests and delegates to its immutable source.
struct FetchCoordinator {
    options: RangeCacheOptions,
    slots: Arc<Semaphore>,
    state: Mutex<State>,
    counters: Counters,
}
impl<R: AsyncRangeReader + ?Sized> CachedRangeReader<R> {
    /// Decorates one source. Share the result through an `Arc` to share its cache.
    ///
    /// # Errors
    /// Rejects zero entry/fetch limits or concurrency outside `1..=1024`.
    pub fn new(source: Arc<R>, options: RangeCacheOptions) -> Result<Self> {
        Ok(Self {
            source,
            size: OnceCell::new(),
            fetches: FetchCoordinator::new(options)?,
        })
    }

    /// Returns counters without resetting them or changing cache recency.
    ///
    /// # Panics
    /// Panics if a previous panic poisoned the internal cache-state mutex.
    #[must_use]
    pub fn stats(&self) -> RangeCacheStats {
        self.fetches.stats()
    }
}

impl FetchCoordinator {
    fn new(options: RangeCacheOptions) -> Result<Self> {
        options.validate()?;
        Ok(Self {
            slots: Arc::new(Semaphore::new(options.max_in_flight)),
            options,
            state: Mutex::new(State::default()),
            counters: Counters::default(),
        })
    }

    fn record_request(&self) {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
    }

    fn stats(&self) -> RangeCacheStats {
        let state = self.state.lock().unwrap();
        RangeCacheStats {
            read_requests: self.counters.requests.load(Ordering::Relaxed),
            cache_hits: self.counters.hits.load(Ordering::Relaxed),
            coalesced_reads: self.counters.coalesced.load(Ordering::Relaxed),
            source_reads: self.counters.reads.load(Ordering::Relaxed),
            source_bytes: self.counters.bytes.load(Ordering::Relaxed),
            evictions: self.counters.evictions.load(Ordering::Relaxed),
            cached_bytes: state.bytes,
            cached_entries: state.cache.len(),
        }
    }

    fn lookup(&self, state: &mut State, range: &Range<u64>) -> Option<Selection> {
        if let Some(index) = state
            .cache
            .iter()
            .position(|item| contains(&item.range, range))
        {
            let cached = state.cache.remove(index).unwrap();
            let bytes = select(&cached.bytes, &cached.range, range);
            state.cache.push_back(cached);
            self.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Some(Selection::Hit(bytes));
        }
        state.flights.retain(|item| item.strong_count() != 0);
        let flight = state
            .flights
            .iter()
            .filter_map(Weak::upgrade)
            .find(|flight| contains(&flight.range, range));
        flight.map(|flight| {
            self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
            Selection::Flight(flight)
        })
    }

    fn insert(&self, range: Range<u64>, bytes: Bytes) {
        if bytes.len() > self.options.capacity_bytes {
            return;
        }
        let mut state = self.state.lock().unwrap();
        // Don't replace a larger interval completed by a concurrent fetch.
        if state.cache.iter().any(|item| contains(&item.range, &range)) {
            return;
        }
        let mut removed = 0;
        state.cache.retain(|item| {
            let keep = !contains(&range, &item.range);
            if !keep {
                removed += item.bytes.len();
            }
            keep
        });
        state.bytes -= removed;
        while state.bytes > self.options.capacity_bytes - bytes.len()
            || state.cache.len() >= self.options.max_entries
        {
            let removed = state.cache.pop_front().unwrap();
            state.bytes -= removed.bytes.len();
            self.counters.evictions.fetch_add(1, Ordering::Relaxed);
        }
        state.bytes += bytes.len();
        state.cache.push_back(Cached { range, bytes });
    }

    async fn resolve<R: AsyncRangeReader + ?Sized>(
        &self,
        source: &R,
        selection: Selection,
        range: &Range<u64>,
    ) -> Result<Bytes> {
        match selection {
            Selection::Hit(bytes) => Ok(bytes),
            Selection::Flight(flight) => {
                let bytes = flight
                    .value
                    .get_or_try_init(|| async {
                        let bytes = flight.fetch(source, &self.counters).await?;
                        self.insert(flight.range.clone(), bytes.clone());
                        Ok::<_, Error>(bytes)
                    })
                    .await?;
                Ok(select(bytes, &flight.range, range))
            }
        }
    }

    async fn read<R: AsyncRangeReader + ?Sized>(
        &self,
        source: &R,
        range: Range<u64>,
        size: u64,
    ) -> Result<Bytes> {
        if let Some(found) = {
            let mut state = self.state.lock().unwrap();
            self.lookup(&mut state, &range)
        } {
            return self.resolve(source, found, &range).await;
        }
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Io(io::Error::other("range cache closed")))?;
        // Admission may have waited for another fetch to finish. Recheck and
        // register under one lock so concurrent misses cannot create duplicates.
        let (selection, unused_permit) = {
            let mut state = self.state.lock().unwrap();
            if let Some(found) = self.lookup(&mut state, &range) {
                (found, Some(permit))
            } else {
                let length = range.end - range.start;
                let extra = (self.options.read_ahead_bytes as u64)
                    .min((self.options.max_fetch_bytes as u64).saturating_sub(length));
                let expanded = range.start..range.end.saturating_add(extra).min(size);
                let flight = Arc::new(Flight {
                    range: expanded,
                    value: OnceCell::new(),
                    _permit: permit,
                });
                state.flights.push(Arc::downgrade(&flight));
                (Selection::Flight(flight), None)
            }
        };
        // Release unused admission outside the state lock before resolving a hit.
        drop(unused_permit);
        self.resolve(source, selection, &range).await
    }
}

impl Flight {
    // OnceCell elects a leader and lets waiters retry errors or cancellation.
    async fn fetch<R: AsyncRangeReader + ?Sized>(
        &self,
        source: &R,
        counters: &Counters,
    ) -> Result<Bytes> {
        counters.reads.fetch_add(1, Ordering::Relaxed);
        let bytes = source.read_range(self.range.clone()).await?;
        if bytes.len() as u64 != self.range.end - self.range.start {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "range cache source returned an inexact range",
            )
            .into());
        }
        counters
            .bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(bytes)
    }
}

const fn contains(outer: &Range<u64>, inner: &Range<u64>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}
fn select(bytes: &Bytes, outer: &Range<u64>, inner: &Range<u64>) -> Bytes {
    // All selections are contained in a successfully validated Bytes length.
    let start = usize::try_from(inner.start - outer.start).unwrap();
    let end = usize::try_from(inner.end - outer.start).unwrap();
    bytes.slice(start..end)
}

#[async_trait::async_trait]
impl<R: AsyncRangeReader + ?Sized> AsyncRangeReader for CachedRangeReader<R> {
    async fn size(&self) -> Result<u64> {
        self.size
            .get_or_try_init(|| self.source.size())
            .await
            .copied()
    }
    async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.fetches.record_request();
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        let size = self.size().await?;
        crate::checked_range(range.start, length, size)?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        self.fetches.read(self.source.as_ref(), range, size).await
    }
}
