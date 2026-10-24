#![allow(clippy::unused_async_trait_impl)] // In-memory fixtures implement the native async storage contract.
#![cfg(feature = "async")]
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use zstar::{
    Entry, Error, ObjectRequest, ObjectResolver,
    manifest_io::{ManifestOptions, Spool},
    pack::*,
};

#[derive(Default)]
struct Memory {
    objects: Mutex<BTreeMap<String, Bytes>>,
    gets: AtomicUsize,
    puts: Mutex<Vec<String>>,
}
impl PackStore for Memory {
    async fn put(&self, key: String, mut data: Spool) -> zstar::Result<Snapshot> {
        let length = data.len();
        let bytes = Bytes::from(data.read(0, length)?);
        let etag = format!(
            "\"{}\"",
            zstar::manifest::object_address(zstar::Digest::sha256(&bytes).as_bytes())
        );
        self.objects.lock().unwrap().insert(key.clone(), bytes);
        self.puts.lock().unwrap().push(key.clone());
        Ok(Snapshot {
            key,
            length,
            etag,
            version: String::new(),
        })
    }
    async fn get_range(&self, snapshot: &Snapshot, range: Range<u64>) -> zstar::Result<Bytes> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let bytes = self
            .objects
            .lock()
            .unwrap()
            .get(&snapshot.key)
            .cloned()
            .unwrap();
        let etag = format!(
            "\"{}\"",
            zstar::manifest::object_address(zstar::Digest::sha256(&bytes).as_bytes())
        );
        if etag != snapshot.etag || bytes.len() as u64 != snapshot.length {
            return Err(Error::ChangedPack);
        }
        Ok(bytes.slice(range.start as usize..range.end as usize))
    }
}
fn requests(catalog: &Catalog) -> Vec<ObjectRequest> {
    catalog
        .chunks()
        .keys()
        .map(|address| ObjectRequest {
            address: *address,
            hint: 0,
            max_bytes: 17 << 20,
        })
        .collect()
}
#[test]
fn cancelling_queued_chunk_work_poisons_session() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (started, entered) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        started.send(()).unwrap();
        wait.recv().unwrap();
    });
    entered.recv().unwrap();
    runtime.block_on(async {
        let mut writer = PackWriter::new(
            Arc::new(Memory::default()),
            Catalog::default(),
            Default::default(),
        )
        .unwrap();
        let mut work = Box::pin(writer.put_chunk(Bytes::from_static(b"queued")));
        let pending = futures_util::poll!(work.as_mut()).is_pending();
        drop(work);
        let result = writer.finish().await;
        // Release even when an assertion below fails, so runtime drop can join.
        release.send(()).unwrap();
        assert!(pending);
        assert!(matches!(
            result,
            Err(Error::Unsupported("pack writer failed or closed"))
        ));
        assert_eq!(writer.completed_packs(), []);
        writer.abort();
        blocker.await.unwrap();
    });
}

#[tokio::test]
async fn reused_compressor_preserves_independent_frames_across_pack_flushes() {
    let chunks = [
        vec![17; 65_536],
        (0..131_072).map(|i| (i % 251) as u8).collect(),
        vec![17; 64],
        vec![29; 65_537],
    ];
    for level in [3, 9] {
        let store = Arc::new(Memory::default());
        let mut writer = PackWriter::new(
            store.clone(),
            Catalog::default(),
            PackWriterOptions {
                target_bytes: 1,
                compression_level: level,
                ..Default::default()
            },
        )
        .unwrap();
        for raw in &chunks {
            writer.put_chunk(Bytes::copy_from_slice(raw)).await.unwrap();
        }
        // Dedup must neither reset nor append the last encoded buffer.
        writer
            .put_chunk(Bytes::copy_from_slice(&chunks[0]))
            .await
            .unwrap();
        let catalog = writer.finish().await.unwrap();
        assert_eq!(catalog.packs().len(), chunks.len());
        let objects = store.objects.lock().unwrap();
        for raw in &chunks {
            let digest = *zstar::Digest::sha256(raw).as_bytes();
            let record = &catalog.chunks()[&digest];
            let key = &catalog.packs()[&record.pack].snapshot.key;
            assert_eq!(
                objects[key].as_ref(),
                zstar::encode_frame(raw, level).unwrap()
            );
        }
    }
}

#[tokio::test]
async fn packs_deduplicate_coalesce_and_reconstruct_unchanged_v1() {
    let store = Arc::new(Memory::default());
    let mut writer = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions::default(),
    )
    .unwrap();
    let mut extents = Vec::new();
    let mut expected = Vec::new();
    for chunk in [b"first".as_slice(), b"second", b"third", b"first"] {
        extents.push(
            writer
                .put_chunk(Bytes::copy_from_slice(chunk))
                .await
                .unwrap(),
        );
        expected.extend_from_slice(chunk);
    }
    assert_eq!(extents[0], extents[3]);
    let catalog = writer.finish().await.unwrap();
    assert_eq!(catalog.packs().len(), 1);
    assert_eq!(catalog.chunks().len(), 3);
    let encoded = catalog.encode(CatalogLimits::default()).unwrap();
    let catalog = Catalog::decode(&encoded, CatalogLimits::default()).unwrap();
    assert_eq!(catalog.encode(CatalogLimits::default()).unwrap(), encoded);
    let resolver = Arc::new(
        PackedObjectResolver::new(store.clone(), catalog.clone(), ResolverOptions::default())
            .unwrap(),
    );
    let objects = resolver
        .get_encoded_many(&requests(&catalog))
        .await
        .unwrap();
    assert_eq!(store.gets.load(Ordering::SeqCst), 1);
    for (request, bytes) in requests(&catalog).iter().zip(objects) {
        zstar::verify_sha256(
            &zstar::decode_frame(
                &bytes,
                catalog.chunks()[&request.address].decoded_length as usize,
            )
            .unwrap(),
            request.address,
        )
        .unwrap();
    }
    let mut builder = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    let mut entry = Entry::file("payload");
    entry.extents = extents;
    builder.add_metadata(entry).unwrap();
    let archive = builder.finish().unwrap();
    let options = zstar::ReaderOptions {
        resolver: Some(resolver),
        cache_bytes: 0,
        ..Default::default()
    };
    let reader = zstar::Archive::open_with_options(Arc::new(Bytes::from(archive)), options)
        .await
        .unwrap();
    let before = store.gets.load(Ordering::SeqCst);
    assert_eq!(&reader.read("payload").await.unwrap()[..], expected);
    assert_eq!(
        store.gets.load(Ordering::SeqCst) - before,
        1,
        "the archive miss plan must reach the packed resolver"
    );
    let mut cursor = reader.open_cursor("payload").await.unwrap();
    let before = store.gets.load(Ordering::SeqCst);
    assert_eq!(
        &cursor
            .read(&reader, 0, expected.len() as u64)
            .await
            .unwrap()[..],
        expected
    );
    assert_eq!(
        store.gets.load(Ordering::SeqCst) - before,
        1,
        "member reads must also communicate multiple-frame plans"
    );
    let mut reused =
        PackWriter::new(store.clone(), catalog.clone(), PackWriterOptions::default()).unwrap();
    reused
        .put_chunk(Bytes::from_static(b"first"))
        .await
        .unwrap();
    assert_eq!(reused.finish().await.unwrap().chunks().len(), 3);
    assert_eq!(reused.completed_packs(), []);
}
#[tokio::test]
async fn range_limits_gaps_and_changed_packs_are_checked() {
    let store = Arc::new(Memory::default());
    let mut writer = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions::default(),
    )
    .unwrap();
    for chunk in [b"a".as_slice(), b"gap", b"b"] {
        writer
            .put_chunk(Bytes::copy_from_slice(chunk))
            .await
            .unwrap();
    }
    let catalog = writer.finish().await.unwrap();
    let mut chunks: Vec<_> = catalog.chunks().values().cloned().collect();
    chunks.sort_by_key(|c| c.offset);
    chunks.remove(1);
    let sparse = Catalog::new(
        catalog.packs().values().cloned().collect(),
        chunks,
        CatalogLimits::default(),
    )
    .unwrap();
    let resolver =
        PackedObjectResolver::new(store.clone(), sparse.clone(), ResolverOptions::default())
            .unwrap();
    resolver.get_many(&requests(&sparse)).await.unwrap();
    assert_eq!(store.gets.load(Ordering::SeqCst), 2);
    let overfetch = PackedObjectResolver::new(
        store.clone(),
        sparse.clone(),
        ResolverOptions {
            max_overfetch_bytes: 64,
            ..Default::default()
        },
    )
    .unwrap();
    overfetch.get_many(&requests(&sparse)).await.unwrap();
    assert_eq!(store.gets.load(Ordering::SeqCst), 3);
    let mut request = requests(&sparse)[0];
    request.max_bytes = 1;
    assert!(matches!(
        resolver.get_many(&[request]).await,
        Err(Error::Budget(_))
    ));
    request.address = [0; 32];
    assert!(matches!(
        resolver.get_many(&[request]).await,
        Err(Error::MissingObject(_))
    ));
    let key = sparse.packs().values().next().unwrap().snapshot.key.clone();
    store
        .objects
        .lock()
        .unwrap()
        .insert(key, Bytes::from_static(b"changed"));
    assert!(matches!(
        resolver.get_many(&requests(&sparse)).await,
        Err(Error::ChangedPack)
    ));
}
#[test]
fn malformed_catalogs_and_budgets_are_rejected() {
    let snapshot = Snapshot {
        key: "pack".into(),
        length: 100,
        etag: "\"v1\"".into(),
        version: String::new(),
    };
    let pack = PackRecord {
        id: [1; 32],
        snapshot,
    };
    let chunk = ChunkRecord {
        digest: [2; 32],
        pack: pack.id,
        offset: 0,
        encoded_length: 50,
        decoded_length: 10,
    };
    let limits = CatalogLimits::default();
    assert!(Catalog::new(vec![pack.clone(), pack.clone()], vec![], limits).is_err());
    assert!(
        Catalog::new(
            vec![pack.clone()],
            vec![chunk.clone(), chunk.clone()],
            limits
        )
        .is_err()
    );
    let mut overlap = chunk.clone();
    overlap.digest = [3; 32];
    overlap.offset = 49;
    assert!(Catalog::new(vec![pack.clone()], vec![chunk.clone(), overlap], limits).is_err());
    let mut outside = chunk.clone();
    outside.offset = u64::MAX;
    assert!(Catalog::new(vec![pack.clone()], vec![outside], limits).is_err());
    let catalog = Catalog::new(vec![pack], vec![chunk], limits).unwrap();
    let encoded = catalog.encode(limits).unwrap();
    assert!(
        Catalog::decode(
            &encoded,
            CatalogLimits {
                max_encoded_bytes: 1,
                ..limits
            }
        )
        .is_err()
    );
    assert!(Catalog::decode(&encoded[..encoded.len() - 1], limits).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(Catalog::decode(&trailing, limits).is_err());
}
#[cfg(feature = "fs")]
#[tokio::test]
async fn local_publication_orders_dependencies_and_old_generations_remain_readable() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalPackStore::open(root.path()).unwrap());
    let mut writer = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions {
            target_bytes: 16,
            ..Default::default()
        },
    )
    .unwrap();
    let extent = writer
        .put_chunk(Bytes::from_static(b"local chunk"))
        .await
        .unwrap();
    let catalog = writer.finish().await.unwrap();
    let mut builder = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    let mut entry = Entry::file("file");
    entry.extents = vec![extent];
    builder.add_metadata(entry).unwrap();
    let archive = builder.finish().unwrap();
    let mut spool = Spool::new(ManifestOptions::default());
    spool.append(&archive).unwrap();
    let (descriptor, generation) = publish_generation(
        store.as_ref(),
        "run",
        spool,
        &catalog,
        zstar::chunking::Chunking::default(),
        CatalogLimits::default(),
    )
    .await
    .unwrap();
    let encoded = store
        .get_range(&descriptor, 0..descriptor.length)
        .await
        .unwrap();
    assert_eq!(
        Generation::decode(&encoded).unwrap().archive,
        generation.archive
    );
    let stored_catalog = store
        .get_range(&generation.catalog, 0..generation.catalog.length)
        .await
        .unwrap();
    let resolver = PackedObjectResolver::new(
        store.clone(),
        Catalog::decode(&stored_catalog, CatalogLimits::default()).unwrap(),
        ResolverOptions::default(),
    )
    .unwrap();
    let raw = resolver
        .get_encoded(
            catalog.chunks().keys().next().copied().unwrap(),
            u64::MAX,
            17 << 20,
        )
        .await
        .unwrap();
    assert_eq!(zstar::decode_frame(&raw, 11).unwrap(), b"local chunk");
    let key = catalog
        .packs()
        .values()
        .next()
        .unwrap()
        .snapshot
        .key
        .clone();
    std::fs::write(root.path().join(key), b"corrupt").unwrap();
    assert!(matches!(
        resolver.get_many(&requests(&catalog)).await,
        Err(Error::ChangedPack)
    ));
}

#[test]
fn negative_packed_catalog_fixtures() {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/packed-objects");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    for case in manifest["negative_catalogs"].as_array().unwrap() {
        let data = std::fs::read(root.join(case["file"].as_str().unwrap())).unwrap();
        assert_eq!(
            Catalog::decode(&data, CatalogLimits::default())
                .unwrap_err()
                .category(),
            case["category"].as_str().unwrap()
        );
    }
}

struct SlowStore {
    memory: Arc<Memory>,
    gate: Arc<tokio::sync::Semaphore>,
    started: AtomicUsize,
    active: Arc<AtomicUsize>,
    peak: AtomicUsize,
    notify: tokio::sync::Notify,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl PackStore for SlowStore {
    async fn put(&self, key: String, data: Spool) -> zstar::Result<Snapshot> {
        self.memory.put(key, data).await
    }
    async fn get_range(&self, s: &Snapshot, r: Range<u64>) -> zstar::Result<Bytes> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = Active(self.active.clone());
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.started.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
        self.gate.acquire().await.unwrap().forget();
        self.memory.get_range(s, r).await
    }
}
#[tokio::test]
async fn global_requests_are_bounded_and_cancellation_releases_shared_flights() {
    let memory = Arc::new(Memory::default());
    let mut writer = PackWriter::new(
        memory.clone(),
        Catalog::default(),
        PackWriterOptions {
            target_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap();
    for raw in [b"one".as_slice(), b"two", b"three", b"four"] {
        writer.put_chunk(Bytes::copy_from_slice(raw)).await.unwrap();
    }
    let catalog = writer.finish().await.unwrap();
    let requests = requests(&catalog);
    let store = Arc::new(SlowStore {
        memory,
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
        started: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
        peak: AtomicUsize::new(0),
        notify: tokio::sync::Notify::new(),
    });
    let resolver = Arc::new(
        PackedObjectResolver::new(
            store.clone(),
            catalog,
            ResolverOptions {
                concurrent_requests: 2,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let first = {
        let resolver = resolver.clone();
        let requests = requests[..2].to_vec();
        tokio::spawn(async move { resolver.get_many(&requests).await })
    };
    while store.started.load(Ordering::SeqCst) < 2 {
        let notified = store.notify.notified();
        if store.started.load(Ordering::SeqCst) < 2 {
            tokio::time::timeout(std::time::Duration::from_secs(5), notified)
                .await
                .unwrap();
        }
    }
    let second = {
        let resolver = resolver.clone();
        let requests = requests[2..].to_vec();
        tokio::spawn(async move { resolver.get_many(&requests).await })
    };
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    store.gate.add_permits(4);
    assert_eq!(second.await.unwrap().unwrap().len(), 2);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    assert!(store.peak.load(Ordering::SeqCst) <= 2);
    // Cancel a leader while another caller holds the same per-digest flight.
    let before = store.started.load(Ordering::SeqCst);
    let gate = store.gate.clone();
    while let Ok(p) = gate.try_acquire() {
        p.forget();
    }
    let request = requests[0];
    let leader = {
        let r = resolver.clone();
        tokio::spawn(async move { r.get_many(&[request]).await })
    };
    while store.started.load(Ordering::SeqCst) == before {
        let n = store.notify.notified();
        if store.started.load(Ordering::SeqCst) == before {
            tokio::time::timeout(std::time::Duration::from_secs(5), n)
                .await
                .unwrap();
        }
    }
    let waiter = {
        let r = resolver.clone();
        tokio::spawn(async move { r.get_many(&[request]).await })
    };
    tokio::task::yield_now().await;
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    gate.add_permits(1);
    assert_eq!(waiter.await.unwrap().unwrap().len(), 1);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn sequential_streams_coalesce_and_seeks_reset_read_ahead() {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let store = Arc::new(Memory::default());
    let mut packs = PackWriter::new(store.clone(), Catalog::default(), Default::default()).unwrap();
    let mut entry = Entry::file("stream");
    let mut expected = Vec::new();
    for n in 0..8 {
        let raw = vec![n; 4096];
        expected.extend_from_slice(&raw);
        entry
            .extents
            .push(packs.put_chunk(Bytes::from(raw)).await.unwrap());
    }
    let catalog = packs.finish().await.unwrap();
    let resolver =
        Arc::new(PackedObjectResolver::new(store.clone(), catalog, Default::default()).unwrap());
    let mut b = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    b.add_metadata(entry).unwrap();
    let archive = zstar::Archive::open_with_options(
        Arc::new(Bytes::from(b.finish().unwrap())),
        zstar::ReaderOptions {
            resolver: Some(resolver),
            cache_bytes: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut file = archive.open_file("stream").await.unwrap();
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        store.gets.load(Ordering::SeqCst),
        3,
        "first frame then bounded groups of four"
    );
    let before = store.gets.load(Ordering::SeqCst);
    file.seek(std::io::SeekFrom::Start(4 * 4096)).await.unwrap();
    let mut selected = [0; 1];
    file.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [4]);
    assert_eq!(store.gets.load(Ordering::SeqCst) - before, 1);
}

struct FailureStore {
    memory: Memory,
    fail_at: usize,
    puts: AtomicUsize,
}
impl PackStore for FailureStore {
    async fn put(&self, key: String, data: Spool) -> zstar::Result<Snapshot> {
        if self.puts.fetch_add(1, Ordering::SeqCst) == self.fail_at {
            return Err(Error::Io(std::io::Error::other("injected failure")));
        }
        self.memory.put(key, data).await
    }
    async fn get_range(&self, s: &Snapshot, r: Range<u64>) -> zstar::Result<Bytes> {
        self.memory.get_range(s, r).await
    }
}
#[tokio::test]
async fn publication_validates_before_upload_and_reports_completed_dependencies() {
    for fail_at in [0, 1, 2] {
        let store = FailureStore {
            memory: Memory::default(),
            fail_at,
            puts: AtomicUsize::new(0),
        };
        let mut builder = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
        builder
            .add_file(Entry::file("inline"), 4, &mut &b"data"[..])
            .unwrap();
        let archive = builder.finish().unwrap();
        let mut spool = Spool::new(ManifestOptions::default());
        spool.append(&archive).unwrap();
        let error = publish_generation(
            &store,
            "run",
            spool,
            &Catalog::default(),
            zstar::chunking::Chunking::default(),
            CatalogLimits::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.completed.len(), fail_at);
        assert_eq!(store.memory.objects.lock().unwrap().len(), fail_at);
    }
    let store = FailureStore {
        memory: Memory::default(),
        fail_at: usize::MAX,
        puts: AtomicUsize::new(0),
    };
    let mut spool = Spool::new(ManifestOptions::default());
    spool.append(b"not finalized").unwrap();
    let failure = publish_generation(
        &store,
        "run",
        spool,
        &Catalog::default(),
        zstar::chunking::Chunking::default(),
        CatalogLimits::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(failure.completed, []);
    assert_eq!(store.puts.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn repacking_changes_storage_without_changing_v1_references() {
    let store = Arc::new(Memory::default());
    let raw = Bytes::from(
        [
            b"abcdefghijklmnop".repeat(4096),
            b"slightly different tail".to_vec(),
        ]
        .concat(),
    );
    let mut first = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions {
            compression_level: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let extent = first.put_chunk(raw.clone()).await.unwrap();
    let old = first.finish().await.unwrap();
    let mut second = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions {
            compression_level: 19,
            ..Default::default()
        },
    )
    .unwrap();
    let recompressed = second.put_chunk(raw.clone()).await.unwrap();
    let new = second.finish().await.unwrap();
    assert_eq!(extent.source, recompressed.source);
    let mut builder = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    let mut entry = Entry::file("file");
    entry.extents = vec![extent];
    builder.add_metadata(entry).unwrap();
    let bytes = Bytes::from(builder.finish().unwrap());
    for catalog in [old, new] {
        let resolver = Arc::new(
            PackedObjectResolver::new(store.clone(), catalog, Default::default()).unwrap(),
        );
        let archive = zstar::Archive::open_with_options(
            Arc::new(bytes.clone()),
            zstar::ReaderOptions {
                resolver: Some(resolver),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(archive.read("file").await.unwrap(), raw);
    }
}

#[tokio::test]
async fn reader_windows_preserve_physical_pack_order() {
    let store = Arc::new(Memory::default());
    let mut writer =
        PackWriter::new(store.clone(), Catalog::default(), Default::default()).unwrap();
    let mut entry = Entry::file("ordered");
    let mut expected = Vec::new();
    for n in 0..32 {
        let data = vec![n; 1024];
        expected.extend_from_slice(&data);
        entry
            .extents
            .push(writer.put_chunk(Bytes::from(data)).await.unwrap());
    }
    let catalog = writer.finish().await.unwrap();
    let resolver =
        Arc::new(PackedObjectResolver::new(store.clone(), catalog, Default::default()).unwrap());
    let mut builder = zstar::backend::BlockingBuilder::new(Vec::new()).unwrap();
    builder.add_metadata(entry).unwrap();
    let archive = zstar::Archive::open_with_options(
        Arc::new(Bytes::from(builder.finish().unwrap())),
        zstar::ReaderOptions {
            resolver: Some(resolver),
            cache_bytes: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(archive.read("ordered").await.unwrap(), expected);
    assert_eq!(
        store.gets.load(Ordering::SeqCst),
        4,
        "32 adjacent frames in four bounded decode windows"
    );
}

#[cfg(feature = "fs")]
#[tokio::test]
async fn shared_packed_fixtures_and_generation_roundtrip() {
    for producer in ["", "go", "python"] {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/packed-objects")
            .join(producer);
        let encoded = std::fs::read(root.join("catalog.cbor")).unwrap();
        let catalog = Catalog::decode(&encoded, CatalogLimits::default()).unwrap();
        assert_eq!(catalog.encode(CatalogLimits::default()).unwrap(), encoded);
        let resolver = Arc::new(
            PackedObjectResolver::new(
                Arc::new(LocalPackStore::open(&root).unwrap()),
                catalog,
                Default::default(),
            )
            .unwrap(),
        );
        let archive = zstar::Archive::open_with_options(
            Arc::new(Bytes::from(std::fs::read(root.join("thin.zstar")).unwrap())),
            zstar::ReaderOptions {
                resolver: Some(resolver),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        archive
            .verify(zstar::VerificationScope::Archive)
            .await
            .unwrap();
        assert_eq!(
            archive.read("payload").await.unwrap(),
            std::fs::read(root.join("payload.bin")).unwrap()
        );
        let g = std::fs::read(root.join("generation.cbor")).unwrap();
        assert_eq!(Generation::decode(&g).unwrap().encode().unwrap(), g);
    }
}
