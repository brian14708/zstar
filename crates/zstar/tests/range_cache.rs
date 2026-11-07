#![cfg(feature = "async")]
#![expect(
    clippy::significant_drop_tightening,
    reason = "Tests retain caches through complete traces and post-operation assertions."
)]
use bytes::Bytes;
use std::{
    io,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use zstar::{Archive, AsyncRangeReader, Builder, CachedRangeReader, Error, RangeCacheOptions};

struct Source {
    bytes: Bytes,
    calls: Mutex<Vec<Range<u64>>>,
    sizes: AtomicUsize,
    fail: AtomicUsize,
    short: bool,
    gate: Option<Semaphore>,
    started: Notify,
    active: AtomicUsize,
    peak: AtomicUsize,
}
impl Source {
    fn new() -> Self {
        Self {
            bytes: (0..=255).collect::<Vec<u8>>().into(),
            calls: Mutex::new(Vec::new()),
            sizes: AtomicUsize::new(0),
            fail: AtomicUsize::new(0),
            short: false,
            gate: None,
            started: Notify::new(),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
    fn ranges(&self) -> Vec<Range<u64>> {
        self.calls.lock().unwrap().clone()
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
    async fn size(&self) -> zstar::Result<u64> {
        self.sizes.fetch_add(1, Ordering::SeqCst);
        Ok(self.bytes.len() as u64)
    }
    async fn read_range(&self, range: Range<u64>) -> zstar::Result<Bytes> {
        self.calls.lock().unwrap().push(range.clone());
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _active = Active(&self.active);
        self.started.notify_one();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self.fail.swap(0, Ordering::SeqCst) != 0 {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "source failure").into());
        }
        let end = range.end - u64::from(self.short);
        self.bytes.read_range(range.start..end).await
    }
}
fn options() -> RangeCacheOptions {
    RangeCacheOptions {
        capacity_bytes: 16,
        max_entries: 4,
        max_fetch_bytes: 16,
        max_in_flight: 2,
        ..Default::default()
    }
}
async fn until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn containment_lru_and_byte_budget() {
    let source = Arc::new(Source::new());
    let cache = CachedRangeReader::new(source.clone(), options()).unwrap();
    assert_eq!(
        cache.read_range(0..8).await.unwrap().as_ref(),
        &[0, 1, 2, 3, 4, 5, 6, 7]
    );
    cache.read_range(8..16).await.unwrap();
    assert_eq!(cache.read_range(2..4).await.unwrap().as_ref(), &[2, 3]);
    cache.read_range(16..24).await.unwrap(); // Evicts 8..16, not recently touched 0..8.
    cache.read_range(0..8).await.unwrap();
    cache.read_range(8..16).await.unwrap();
    assert_eq!(source.ranges(), vec![0..8, 8..16, 16..24, 8..16]);
    let stats = cache.stats();
    assert_eq!(stats.cache_hits, 2);
    assert_eq!(stats.evictions, 2);
    assert_eq!(stats.cached_bytes, 16);
    assert_eq!(stats.cached_entries, 2);
    assert_eq!(stats.source_bytes, 32);
    assert_eq!(source.sizes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn read_ahead_bounds_exact_outputs_and_large_request_bypass() {
    let source = Arc::new(Source::new());
    let cache = CachedRangeReader::new(
        source.clone(),
        RangeCacheOptions {
            read_ahead_bytes: 32,
            ..options()
        },
    )
    .unwrap();
    assert_eq!(
        cache.read_range(3..7).await.unwrap().as_ref(),
        &[3, 4, 5, 6]
    );
    assert_eq!(
        cache.read_range(7..19).await.unwrap().as_ref(),
        &(7..19).collect::<Vec<_>>()
    );
    assert_eq!(source.ranges(), vec![3..19]);
    cache.read_range(250..256).await.unwrap(); // Clamp to EOF.
    cache.read_range(32..64).await.unwrap(); // Caller may exceed max_fetch_bytes/capacity.
    assert_eq!(source.ranges(), vec![3..19, 250..256, 32..64]);
    assert!(cache.stats().cached_bytes <= 16);
}

#[tokio::test]
async fn entry_limit_replacement_and_disabled_cache() {
    let source = Arc::new(Source::new());
    let cache = CachedRangeReader::new(
        source.clone(),
        RangeCacheOptions {
            max_entries: 1,
            ..options()
        },
    )
    .unwrap();
    cache.read_range(0..1).await.unwrap();
    cache.read_range(1..2).await.unwrap();
    assert_eq!(cache.stats().cached_entries, 1);
    assert_eq!(cache.stats().cached_bytes, 1);
    cache.read_range(0..8).await.unwrap(); // Replaces the contained interval.
    assert_eq!(cache.stats().cached_entries, 1);
    assert_eq!(cache.stats().cached_bytes, 8);
    assert_eq!(cache.read_range(1..2).await.unwrap().as_ref(), &[1]);
    let uncached = CachedRangeReader::new(
        source.clone(),
        RangeCacheOptions {
            capacity_bytes: 0,
            ..options()
        },
    )
    .unwrap();
    uncached.read_range(32..40).await.unwrap();
    uncached.read_range(32..40).await.unwrap();
    assert_eq!(uncached.stats().cached_entries, 0);
    assert_eq!(uncached.stats().source_reads, 2);
}

#[tokio::test]
async fn validation_short_reads_and_failures_are_not_cached() {
    let source = Arc::new(Source::new());
    let cache = CachedRangeReader::new(source.clone(), options()).unwrap();
    for range in [Range { start: 9, end: 8 }, 0..257, 257..257] {
        assert!(matches!(cache.read_range(range).await, Err(Error::Range)));
    }
    assert!(cache.read_range(256..256).await.unwrap().is_empty());
    assert_eq!(cache.stats().source_reads, 0);
    source.fail.store(1, Ordering::SeqCst);
    assert!(
        matches!(cache.read_range(0..8).await,Err(Error::Io(e)) if e.kind()==io::ErrorKind::PermissionDenied)
    );
    assert_eq!(cache.read_range(0..8).await.unwrap().len(), 8);
    assert_eq!(cache.stats().source_reads, 2);
    assert_eq!(cache.stats().source_bytes, 8);
    let short = Arc::new(Source {
        short: true,
        ..Source::new()
    });
    let cache = CachedRangeReader::new(short, options()).unwrap();
    for _ in 0..2 {
        assert!(
            matches!(cache.read_range(0..8).await,Err(Error::Io(e)) if e.kind()==io::ErrorKind::UnexpectedEof)
        );
    }
    assert_eq!(cache.stats().cached_entries, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_nearby_ranges_share_read_ahead() {
    let source = Arc::new(Source {
        gate: Some(Semaphore::new(0)),
        ..Source::new()
    });
    let cache = Arc::new(
        CachedRangeReader::new(
            source.clone(),
            RangeCacheOptions {
                read_ahead_bytes: 12,
                ..options()
            },
        )
        .unwrap(),
    );
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(0..4).await })
    };
    source.started.notified().await;
    let second = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(8..12).await })
    };
    until(|| cache.stats().coalesced_reads == 1).await;
    source.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().as_ref(), &[0, 1, 2, 3]);
    assert_eq!(second.await.unwrap().unwrap().as_ref(), &[8, 9, 10, 11]);
    assert_eq!(source.ranges(), vec![0..16]);
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancellation_releases_io_and_a_waiter_can_take_over() {
    let source = Arc::new(Source {
        gate: Some(Semaphore::new(0)),
        ..Source::new()
    });
    let cache = Arc::new(
        CachedRangeReader::new(
            source.clone(),
            RangeCacheOptions {
                max_in_flight: 1,
                ..options()
            },
        )
        .unwrap(),
    );
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(0..8).await })
    };
    source.started.notified().await;
    let waiter = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(2..4).await })
    };
    until(|| cache.stats().coalesced_reads == 1).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    until(|| source.ranges().len() == 2).await;
    assert_eq!(source.active.load(Ordering::SeqCst), 1);
    source.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(waiter.await.unwrap().unwrap().as_ref(), &[2, 3]);
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    // A sole cancelled leader leaves no detached work or exhausted slot.
    let lone = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(16..24).await })
    };
    until(|| source.active.load(Ordering::SeqCst) == 1).await;
    lone.abort();
    assert!(lone.await.unwrap_err().is_cancelled());
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    source.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(cache.read_range(32..40).await.unwrap().len(), 8);
}

#[tokio::test]
async fn concurrent_distinct_fetches_obey_limit_and_cancel_queued_work() {
    let source = Arc::new(Source {
        gate: Some(Semaphore::new(0)),
        ..Source::new()
    });
    let cache = Arc::new(CachedRangeReader::new(source.clone(), options()).unwrap());
    let mut jobs = Vec::new();
    for i in 0..10 {
        let cache = cache.clone();
        jobs.push(tokio::spawn(async move {
            cache.read_range(i * 8..i * 8 + 8).await
        }));
    }
    until(|| source.active.load(Ordering::SeqCst) == 2).await;
    tokio::task::yield_now().await;
    assert_eq!(source.ranges().len(), 2);
    for job in &jobs {
        job.abort();
    }
    for job in jobs {
        assert!(job.await.unwrap_err().is_cancelled());
    }
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    assert_eq!(source.ranges().len(), 2);
    source.gate.as_ref().unwrap().add_permits(1);
    cache.read_range(160..168).await.unwrap();
    assert_eq!(source.peak.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn archive_reads_remain_valid_and_shared_frames_fetch_once() {
    let mut builder = Builder::try_new(Vec::new()).await.unwrap();
    for i in 0..32 {
        builder
            .append_bytes(format!("f{i}"), &[u8::try_from(i).unwrap(); 1024])
            .await
            .unwrap();
    }
    let source: Bytes = builder.finish().await.unwrap().into();
    let cache =
        Arc::new(CachedRangeReader::new(Arc::new(source), RangeCacheOptions::default()).unwrap());
    let archive = Archive::open(cache.clone()).await.unwrap();
    for i in 0..32 {
        assert_eq!(
            archive.read(&format!("f{i}")).await.unwrap().as_ref(),
            &[u8::try_from(i).unwrap(); 1024]
        );
    }
    let before = cache.stats();
    assert_eq!(before.source_reads, 4); // Footer, directory, entry page, shared data frame.
    assert_eq!(before.cache_hits, 31);
    let (_, report) = archive.read_range_with_report("f0", 0..4).await.unwrap();
    assert!(report.metadata_validated);
    assert_eq!(report.data_frames_checked.len(), 1);
    assert_eq!(cache.stats().source_reads, before.source_reads);
}

#[test]
fn invalid_options_are_rejected() {
    for option in [
        RangeCacheOptions {
            max_entries: 0,
            ..options()
        },
        RangeCacheOptions {
            max_fetch_bytes: 0,
            ..options()
        },
        RangeCacheOptions {
            max_in_flight: 0,
            ..options()
        },
        RangeCacheOptions {
            max_in_flight: 1025,
            ..options()
        },
    ] {
        assert!(CachedRangeReader::new(Arc::new(Bytes::new()), option).is_err());
    }
}

#[tokio::test]
async fn varied_overlapping_traces_return_exact_bytes_with_bounded_retention() {
    for capacity_bytes in [0, 7, 16, 64] {
        for read_ahead_bytes in [0, 4, 32] {
            let source = Arc::new(Source::new());
            let cache = CachedRangeReader::new(
                source,
                RangeCacheOptions {
                    capacity_bytes,
                    read_ahead_bytes,
                    ..options()
                },
            )
            .unwrap();
            let mut seed = 0x1234_5678_u64;
            for _ in 0..256 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let start = seed % 257;
                let end = (start + seed.rotate_left(23) % 48).min(256);
                let got = cache.read_range(start..end).await.unwrap();
                assert_eq!(
                    got.as_ref(),
                    (start..end)
                        .map(|n| u8::try_from(n).unwrap())
                        .collect::<Vec<_>>()
                );
                let stats = cache.stats();
                assert!(stats.cached_bytes <= capacity_bytes);
                assert!(stats.cached_entries <= 4);
            }
        }
    }
}

#[derive(Debug)]
struct SourceFailure;
impl std::fmt::Display for SourceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("typed source error")
    }
}
impl std::error::Error for SourceFailure {}
struct FailingSource;
#[async_trait::async_trait]
impl AsyncRangeReader for FailingSource {
    async fn size(&self) -> zstar::Result<u64> {
        Ok(8)
    }
    async fn read_range(&self, _range: Range<u64>) -> zstar::Result<Bytes> {
        Err(io::Error::other(SourceFailure).into())
    }
}
#[tokio::test]
async fn underlying_typed_errors_are_preserved() {
    let cache = CachedRangeReader::new(Arc::new(FailingSource), options()).unwrap();
    for _ in 0..2 {
        let Error::Io(error) = cache.read_range(0..4).await.unwrap_err() else {
            panic!("expected I/O error")
        };
        assert!(error.get_ref().unwrap().is::<SourceFailure>());
    }
    assert_eq!(cache.stats().source_reads, 2);
    assert_eq!(cache.stats().cached_bytes, 0);
}

#[tokio::test]
async fn a_waiter_retries_a_failed_fetch_without_caching_the_failure() {
    let source = Arc::new(Source {
        gate: Some(Semaphore::new(0)),
        ..Source::new()
    });
    source.fail.store(1, Ordering::SeqCst);
    let cache = Arc::new(CachedRangeReader::new(source.clone(), options()).unwrap());
    let first = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(0..8).await })
    };
    source.started.notified().await;
    let waiter = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(2..4).await })
    };
    until(|| cache.stats().coalesced_reads == 1).await;
    source.gate.as_ref().unwrap().add_permits(2);
    assert!(
        matches!(first.await.unwrap(),Err(Error::Io(e)) if e.kind()==io::ErrorKind::PermissionDenied)
    );
    assert_eq!(waiter.await.unwrap().unwrap().as_ref(), &[2, 3]);
    assert_eq!(cache.read_range(0..8).await.unwrap().len(), 8);
    assert_eq!(cache.stats().source_reads, 2);
    assert_eq!(cache.stats().source_bytes, 8);
    assert_eq!(cache.stats().cache_hits, 1);
}

#[tokio::test]
async fn queued_misses_recheck_cache_after_admission() {
    let source = Arc::new(Source {
        gate: Some(Semaphore::new(0)),
        ..Source::new()
    });
    let cache = Arc::new(
        CachedRangeReader::new(
            source.clone(),
            RangeCacheOptions {
                max_in_flight: 1,
                ..options()
            },
        )
        .unwrap(),
    );
    let occupying = {
        let cache = cache.clone();
        tokio::spawn(async move { cache.read_range(0..8).await })
    };
    source.started.notified().await;
    // Both miss before their interval has a registered fetch, then wait for
    // admission. The second must reuse what the first eventually cached.
    let mut queued = Vec::new();
    for _ in 0..2 {
        let cache = cache.clone();
        queued.push(tokio::spawn(async move { cache.read_range(16..24).await }));
    }
    until(|| cache.stats().read_requests == 3).await;
    assert_eq!(source.ranges(), vec![0..8]);
    source.gate.as_ref().unwrap().add_permits(2);
    assert_eq!(occupying.await.unwrap().unwrap().len(), 8);
    for task in queued {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .as_ref(),
            &[16, 17, 18, 19, 20, 21, 22, 23]
        );
    }
    assert_eq!(source.ranges(), vec![0..8, 16..24]);
    assert_eq!(cache.stats().cache_hits, 1);
    // The permit acquired for that cache hit must also have been released.
    source.gate.as_ref().unwrap().add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), cache.read_range(32..40))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn overlapping_fetches_retain_the_containing_interval_in_either_completion_order() {
    struct GatedSource {
        bytes: Bytes,
        gates: [Semaphore; 2],
    }
    #[async_trait::async_trait]
    impl AsyncRangeReader for GatedSource {
        async fn size(&self) -> zstar::Result<u64> {
            self.bytes.size().await
        }
        async fn read_range(&self, range: Range<u64>) -> zstar::Result<Bytes> {
            let gate = match range.start {
                4 => Some(&self.gates[0]),
                0 => Some(&self.gates[1]),
                _ => None,
            };
            if let Some(gate) = gate {
                gate.acquire().await.unwrap().forget();
            }
            self.bytes.read_range(range).await
        }
    }
    for larger_first in [false, true] {
        let source = Arc::new(GatedSource {
            bytes: (0..64).collect::<Vec<u8>>().into(),
            gates: [Semaphore::new(0), Semaphore::new(0)],
        });
        let cache = CachedRangeReader::new(
            source.clone(),
            RangeCacheOptions {
                capacity_bytes: 12,
                max_entries: 2,
                ..options()
            },
        )
        .unwrap();
        cache.read_range(32..36).await.unwrap();
        // Poll the subrange first so both fetches are admitted independently.
        let mut small = std::pin::pin!(cache.read_range(4..8));
        assert!(futures_util::poll!(&mut small).is_pending());
        let mut large = std::pin::pin!(cache.read_range(0..8));
        assert!(futures_util::poll!(&mut large).is_pending());
        if larger_first {
            source.gates[1].add_permits(1);
            large.await.unwrap();
            source.gates[0].add_permits(1);
            small.await.unwrap();
        } else {
            source.gates[0].add_permits(1);
            small.await.unwrap();
            source.gates[1].add_permits(1);
            large.await.unwrap();
        }
        for range in [32..36, 0..8, 4..8] {
            assert_eq!(
                cache.read_range(range.clone()).await.unwrap(),
                source.bytes.read_range(range).await.unwrap()
            );
        }
        let stats = cache.stats();
        assert_eq!(stats.cached_entries, 2);
        assert_eq!(stats.cached_bytes, 12);
        assert_eq!(stats.source_reads, 3);
        assert_eq!(stats.cache_hits, 3);
        assert_eq!(stats.evictions, 0);
    }
}

#[tokio::test]
async fn size_discovery_retries_errors_and_cancellation() {
    struct SizeSource(AtomicUsize);
    #[async_trait::async_trait]
    impl AsyncRangeReader for SizeSource {
        async fn size(&self) -> zstar::Result<u64> {
            match self.0.fetch_add(1, Ordering::SeqCst) {
                0 => Err(io::Error::other(SourceFailure).into()),
                1 => std::future::pending().await,
                _ => Ok(8),
            }
        }
        async fn read_range(&self, _range: Range<u64>) -> zstar::Result<Bytes> {
            unreachable!("size discovery must not read ranges")
        }
    }
    let source = Arc::new(SizeSource(AtomicUsize::new(0)));
    let cache = CachedRangeReader::new(source.clone(), options()).unwrap();
    let Error::Io(error) = cache.size().await.unwrap_err() else {
        panic!("expected original source error")
    };
    assert!(error.get_ref().unwrap().is::<SourceFailure>());
    let mut leader = Box::pin(cache.size());
    assert!(futures_util::poll!(&mut leader).is_pending());
    let mut waiter = std::pin::pin!(cache.size());
    assert!(futures_util::poll!(&mut waiter).is_pending());
    assert_eq!(source.0.load(Ordering::SeqCst), 2);
    drop(leader);
    assert_eq!(waiter.await.unwrap(), 8);
    assert_eq!(cache.size().await.unwrap(), 8);
    assert_eq!(source.0.load(Ordering::SeqCst), 3);
}
