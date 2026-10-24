//! Archive I/O for the entry-oriented manifest. No persistent frame table.
use crate::decoded_cache::{DecodedBody, DecodedCacheKey, SharedDecodedCache};
use crate::manifest::{
    ENCODED_FRAME_MAX, EXTENTS_MAX, Entry, EntryKind, Extent, Source, decode_pointer,
    encode_pointer,
};
use crate::manifest_io::{ManifestEncoder, ManifestOptions, ManifestReader, SpillStore, Spool};
use crate::{Error, RangeReader, Result};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};

fn invalid(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}
fn uint(v: &ciborium::Value) -> Result<u64> {
    match v {
        ciborium::Value::Integer(n) => u64::try_from(*n).map_err(|_| invalid("spool integer")),
        _ => Err(invalid("spool integer")),
    }
}

pub trait EncodedObjectResolver {
    /// Return one complete encoded object under the supplied actual-byte limit.
    /// Encoded-size hints must not be interpreted as decoded sizes or limits.
    fn get_encoded(
        &mut self,
        address: [u8; 32],
        encoded_size_hint: u64,
        max_bytes: u64,
    ) -> Result<Vec<u8>>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ContentKey {
    Archive(u64, u64),
    Object([u8; 32]),
}
impl ContentKey {
    fn of(extent: &Extent) -> Self {
        match extent.source {
            Source::Archive(at) => Self::Archive(at, extent.encoded_size),
            Source::Object(hash) => Self::Object(hash),
        }
    }
}
struct CachedContent {
    raw: DecodedBody,
    verified: bool,
    stamp: u64,
}
#[derive(Default)]
struct ContentCache {
    entries: HashMap<ContentKey, CachedContent>,
    order: BTreeMap<u64, ContentKey>,
    capacity: usize,
    bytes: usize,
    clock: u64,
    shared: Option<SharedDecodedCache>,
    metadata: [u8; 32],
}
impl ContentCache {
    // Charge payload plus conservative map/node/Arc bookkeeping. Active streams
    // retain their selected frame independently of the cache capacity.
    const OVERHEAD: usize = 256;
    fn get(&mut self, key: &ContentKey, verify: bool) -> Result<Option<DecodedBody>> {
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        // Resolvers are supplied per operation and have no stable namespace.
        // Only a successful whole-object hash permits reuse across resolvers.
        if matches!(key, ContentKey::Object(_)) && !verify && !entry.verified {
            return Ok(None);
        }
        if let ContentKey::Object(hash) = key
            && verify
            && !entry.verified
        {
            // Never promote unchecked bytes without hashing, and never return
            // them to a caller requiring verification after a failed hash.
            crate::verify_sha256(&entry.raw, *hash)?;
            entry.verified = true;
        }
        self.order.remove(&entry.stamp);
        self.clock += 1;
        entry.stamp = self.clock;
        self.order.insert(entry.stamp, key.clone());
        Ok(Some(entry.raw.clone()))
    }
    fn insert(&mut self, key: ContentKey, raw: DecodedBody, verified: bool) {
        let charge = raw.allocated().saturating_add(Self::OVERHEAD);
        if charge > self.capacity {
            return;
        }
        if let Some(previous) = self.entries.remove(&key) {
            self.order.remove(&previous.stamp);
            self.bytes -= previous.raw.allocated() + Self::OVERHEAD;
        }
        while self.bytes > self.capacity - charge {
            let (stamp, oldest) = self.order.pop_first().unwrap();
            let entry = self.entries.remove(&oldest).unwrap();
            debug_assert_eq!(entry.stamp, stamp);
            self.bytes -= entry.raw.allocated() + Self::OVERHEAD;
        }
        self.clock += 1;
        self.bytes += charge;
        self.order.insert(self.clock, key.clone());
        self.entries.insert(
            key,
            CachedContent {
                raw,
                verified,
                stamp: self.clock,
            },
        );
    }
}
fn load_content<R: RangeReader>(
    source: &mut R,
    cache: &mut ContentCache,
    extent: &Extent,
    resolver: &mut Option<&mut dyn EncodedObjectResolver>,
    verify_external: bool,
) -> Result<DecodedBody> {
    let key = ContentKey::of(extent);
    if let Some(raw) = cache.get(&key, verify_external)? {
        return Ok(raw);
    }
    let shared_key = cache
        .shared
        .as_ref()
        .and_then(|shared| match extent.source {
            Source::Archive(at) => {
                Some(shared.archive_key(cache.metadata, at, extent.encoded_size))
            }
            Source::Object(hash) if verify_external => Some(DecodedCacheKey::Object(hash)),
            Source::Object(_) => None,
        });
    if let Some(shared_key) = shared_key
        && let Some(raw) = cache.shared.as_ref().unwrap().get(&shared_key)
    {
        cache.insert(key, raw.clone(), true);
        return Ok(raw);
    }
    let encoded = match extent.source {
        Source::Archive(at) => {
            let end = at.checked_add(extent.encoded_size).ok_or(Error::Range)?;
            let bytes = source.read_range(at..end)?;
            if bytes.len() as u64 != extent.encoded_size {
                return Err(Error::MalformedFrame("short archive frame".into()));
            }
            bytes
        }
        Source::Object(hash) => resolver
            .as_mut()
            .ok_or(Error::Unsupported("external read requires resolver"))?
            .get_encoded(hash, extent.encoded_size, ENCODED_FRAME_MAX)?,
    };
    let raw = DecodedBody::compact(match extent.source {
        Source::Archive(_) => decode_content(&encoded, true)?,
        Source::Object(_) => decode_object(&encoded)?,
    });
    if let Source::Object(hash) = extent.source
        && verify_external
    {
        crate::verify_sha256(&raw, hash)?;
    }
    if let Some(shared_key) = shared_key {
        cache.shared.as_ref().unwrap().put(shared_key, raw.clone());
    }
    cache.insert(key, raw.clone(), verify_external);
    Ok(raw)
}

/// A bounded logical file stream. Complete selected frames/objects are decoded
/// and validated before any bytes from them are returned. Only external objects
/// require SHA-256 verification; a zero-length read performs no content I/O.
pub struct ContentReader<'a, R: RangeReader> {
    source: &'a mut R,
    cache: &'a mut ContentCache,
    resolver: Option<&'a mut dyn EncodedObjectResolver>,
    verify_external: bool,
    extents: std::vec::IntoIter<Extent>,
    cached: Option<(Source, u64, DecodedBody)>,
    offset: usize,
    remaining: usize,
    failed: bool,
}
impl<R: RangeReader> ContentReader<'_, R> {
    fn read_content(&mut self, buffer: &mut [u8]) -> Result<usize> {
        if self.failed {
            return Err(Error::Unsupported("content reader failed"));
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        let result = (|| {
            if self.remaining == 0 {
                let Some(extent) = self.extents.next() else {
                    return Ok(0);
                };
                let hit = self.cached.as_ref().is_some_and(|(source, size, _)| {
                    source == &extent.source
                        && (matches!(source, Source::Object(_)) || *size == extent.encoded_size)
                });
                if !hit {
                    let raw = load_content(
                        self.source,
                        self.cache,
                        &extent,
                        &mut self.resolver,
                        self.verify_external,
                    )?;
                    self.cached = Some((extent.source, extent.encoded_size, raw));
                }
                let raw = &self.cached.as_ref().unwrap().2;
                crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
                self.offset = extent.offset as usize;
                self.remaining = extent.size as usize;
            }
            let count = buffer.len().min(self.remaining);
            buffer[..count].copy_from_slice(
                &self.cached.as_ref().unwrap().2[self.offset..self.offset + count],
            );
            self.offset += count;
            self.remaining -= count;
            Ok(count)
        })();
        self.failed = result.is_err();
        result
    }
    /// Copy with bounded scratch space, preserving codec/resolver error types.
    pub fn copy_to(&mut self, output: &mut impl Write) -> Result<u64> {
        let mut buffer = [0; 16 << 10];
        let mut size = 0_u64;
        loop {
            let count = self.read_content(&mut buffer)?;
            if count == 0 {
                return Ok(size);
            }
            output.write_all(&buffer[..count])?;
            size = size.checked_add(count as u64).ok_or(Error::Range)?;
        }
    }
}
impl<R: RangeReader> Read for ContentReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.read_content(buffer).map_err(std::io::Error::other)
    }
}

pub struct BlockingBuilder<W: Write> {
    output: W,
    pending: Vec<u8>,
    encoded: Vec<u8>,
    compressor: zstd::bulk::Compressor<'static>,
    frame_target: usize,
    raw_position: u64,
    encoded_position: u64,
    raw_emitted: u64,
    workers: Option<crate::parallel::ChecksummedFrameWorkers>,
    frames: Spool,
    records: Spool,
    options: ManifestOptions,
    level: i32,
    failed: bool,
    root_extensions: BTreeMap<String, ciborium::Value>,
}
impl<W: Write> BlockingBuilder<W> {
    #[cfg(feature = "async")]
    pub(crate) fn output(&self) -> &W {
        &self.output
    }
    pub fn new(output: W) -> Result<Self> {
        Self::with_options(output, 1 << 20, 3, Default::default())
    }
    pub fn with_options(
        output: W,
        frame_target: usize,
        level: i32,
        options: ManifestOptions,
    ) -> Result<Self> {
        if !(512..=crate::DATA_FRAME_MAX).contains(&frame_target) {
            return Err(Error::Unsupported("frame target outside format bounds"));
        }
        let mut compressor = zstd::bulk::Compressor::new(level)?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
        Ok(Self {
            output,
            pending: Vec::with_capacity(frame_target),
            encoded: Vec::with_capacity(zstd::zstd_safe::compress_bound(frame_target)),
            compressor,
            frame_target,
            raw_position: 0,
            encoded_position: 0,
            raw_emitted: 0,
            workers: None,
            frames: Spool::new(options.clone()),
            records: Spool::new(options.clone()),
            options,
            level,
            failed: false,
            root_extensions: BTreeMap::new(),
        })
    }
    fn flush_frame(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Some(workers) = &mut self.workers {
            let raw = std::mem::take(&mut self.pending);
            let encoded = std::mem::take(&mut self.encoded);
            let completed = workers.submit((raw, encoded))?;
            if let Some((raw, encoded)) = completed {
                let raw_size = raw.len();
                self.write_completed(raw_size, &encoded)?;
                self.recycle_buffers(raw, encoded);
            } else {
                self.pending = Vec::with_capacity(self.frame_target);
                self.encoded =
                    Vec::with_capacity(zstd::zstd_safe::compress_bound(self.frame_target));
            }
            return Ok(());
        }
        self.encoded.clear();
        self.compressor
            .compress_to_buffer(&self.pending, &mut self.encoded)?;
        if self.encoded.len() as u64 > ENCODED_FRAME_MAX {
            return Err(invalid("encoded frame maximum"));
        }
        self.output.write_all(&self.encoded)?;
        let mut record = [0; 32];
        for (slot, n) in record.as_chunks_mut::<8>().0.iter_mut().zip([
            self.raw_emitted,
            self.pending.len() as u64,
            self.encoded_position,
            self.encoded.len() as u64,
        ]) {
            slot.copy_from_slice(&n.to_le_bytes());
        }
        self.frames.append(&record)?;
        self.encoded_position = self
            .encoded_position
            .checked_add(self.encoded.len() as u64)
            .ok_or_else(|| invalid("encoded archive overflow"))?;
        self.raw_emitted = self
            .raw_emitted
            .checked_add(self.pending.len() as u64)
            .ok_or_else(|| invalid("decoded archive overflow"))?;
        self.pending.clear();
        Ok(())
    }
    fn recycle_buffers(&mut self, mut raw: Vec<u8>, mut encoded: Vec<u8>) {
        raw.clear();
        encoded.clear();
        self.pending = raw;
        self.encoded = encoded;
    }
    pub fn with_workers(mut self, count: usize) -> Result<Self> {
        if self.raw_position != 0 || count > 16 {
            return Err(Error::Unsupported(
                "workers require a fresh writer and count <= 16",
            ));
        }
        self.workers = if count == 0 {
            None
        } else {
            Some(crate::parallel::ChecksummedFrameWorkers::checksummed(
                count, self.level,
            )?)
        };
        Ok(self)
    }
    /// Supply empty caller-owned stores for the transient frame and entry
    /// records. The writer owns their cleanup after finishing or being dropped.
    pub fn with_spill_stores(
        mut self,
        frames: Box<dyn SpillStore>,
        records: Box<dyn SpillStore>,
    ) -> Result<Self> {
        if self.raw_position != 0 {
            return Err(Error::Unsupported("spill stores require a fresh writer"));
        }
        self.frames = Spool::with_store(self.options.clone(), frames)?;
        self.records = Spool::with_store(self.options.clone(), records)?;
        Ok(self)
    }
    pub fn with_root_extensions(
        mut self,
        extensions: BTreeMap<String, ciborium::Value>,
    ) -> Result<Self> {
        // Enforce the same restricted CBOR values before emitting payloads.
        let root = crate::manifest::ManifestRoot {
            required_features: vec![],
            entry_count: 0,
            extensions,
        };
        root.validate()?;
        self.root_extensions = root.extensions;
        Ok(self)
    }
    fn write_completed(&mut self, raw_size: usize, encoded: &[u8]) -> Result<()> {
        if encoded.len() as u64 > ENCODED_FRAME_MAX {
            return Err(invalid("encoded frame maximum"));
        }
        self.output.write_all(encoded)?;
        let mut record = [0; 32];
        for (slot, n) in record.as_chunks_mut::<8>().0.iter_mut().zip([
            self.raw_emitted,
            raw_size as u64,
            self.encoded_position,
            encoded.len() as u64,
        ]) {
            slot.copy_from_slice(&n.to_le_bytes());
        }
        self.frames.append(&record)?;
        self.raw_emitted = self
            .raw_emitted
            .checked_add(raw_size as u64)
            .ok_or_else(|| invalid("decoded archive overflow"))?;
        self.encoded_position = self
            .encoded_position
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| invalid("encoded archive overflow"))?;
        Ok(())
    }
    fn write_raw(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            if self.pending.len() == self.frame_target {
                self.flush_frame()?;
            }
            let count = (self.frame_target - self.pending.len()).min(bytes.len());
            self.pending.extend_from_slice(&bytes[..count]);
            self.raw_position = self
                .raw_position
                .checked_add(count as u64)
                .ok_or_else(|| invalid("decoded archive overflow"))?;
            bytes = &bytes[count..];
        }
        Ok(())
    }
    pub fn add_file(&mut self, entry: Entry, size: u64, payload: &mut impl Read) -> Result<()> {
        if entry.kind != EntryKind::File || !entry.extents.is_empty() {
            return Err(Error::Unsupported(
                "inline capture requires file metadata without source extents",
            ));
        }
        if self.failed {
            return Err(Error::Unsupported("archive writer failed"));
        }
        let result = self.add(entry, size, Some(payload));
        self.failed = result.is_err();
        result
    }
    pub fn add_metadata(&mut self, entry: Entry) -> Result<()> {
        if self.failed {
            return Err(Error::Unsupported("archive writer failed"));
        }
        if entry.kind == EntryKind::File && !entry.is_external() && !entry.extents.is_empty() {
            return Err(Error::Unsupported("inline file requires a payload reader"));
        }
        let result = if entry.is_external() {
            let text = encode_pointer(&entry.extents)?;
            self.add(entry, text.len() as u64, Some(&mut text.as_slice()))
        } else {
            self.add(entry, 0, None)
        };
        self.failed = result.is_err();
        result
    }
    fn add(&mut self, entry: Entry, size: u64, mut payload: Option<&mut dyn Read>) -> Result<()> {
        // Archive positions are resolved after compression; validate caller metadata separately.
        entry.validate(u64::MAX)?;
        if entry.kind != EntryKind::File && size != 0 {
            return Err(invalid("nonfile payload"));
        }
        let tar_path = if entry.kind == EntryKind::Directory {
            format!("{}/", entry.path)
        } else {
            entry.path.clone()
        };
        let flag = match entry.kind {
            EntryKind::File => b'0',
            EntryKind::Directory => b'5',
            EntryKind::Symlink => b'2',
        };
        let header = crate::tar_encode::member_header_full(
            &tar_path,
            size,
            entry.mode,
            flag,
            entry.linkname.as_deref().unwrap_or(""),
            entry.uid,
            entry.gid,
            entry.mtime_ns,
        )?;
        let padded = size
            .checked_add(511)
            .ok_or_else(|| invalid("payload padding overflow"))?
            / 512
            * 512;
        let total = padded
            .checked_add(header.len() as u64)
            .ok_or_else(|| invalid("member length overflow"))?;
        if total > (self.frame_target - self.pending.len()) as u64 {
            self.flush_frame()?;
        }
        self.write_raw(&header)?;
        let start = self.raw_position;
        let mut remaining = size;
        if let Some(input) = payload.as_mut() {
            while remaining != 0 {
                if self.pending.len() == self.frame_target {
                    self.flush_frame()?;
                }
                let at = self.pending.len();
                let count = remaining.min((self.frame_target - at) as u64) as usize;
                self.pending.resize(at + count, 0);
                let n = loop {
                    match input.read(&mut self.pending[at..]) {
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        result => break result?,
                    }
                };
                self.pending.truncate(at + n);
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "payload shortened",
                    )
                    .into());
                }
                remaining -= n as u64;
                self.raw_position = self
                    .raw_position
                    .checked_add(n as u64)
                    .ok_or_else(|| invalid("decoded archive overflow"))?;
            }
            let mut extra = [0];
            let n = loop {
                match input.read(&mut extra) {
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    result => break result?,
                }
            };
            if n != 0 {
                return Err(
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "payload grew").into(),
                );
            }
        } else if size != 0 {
            return Err(invalid("missing payload reader"));
        }
        self.write_raw(&[0; 512][..(padded - size) as usize])?;
        let mut record = Vec::new();
        crate::cbor::append_head(&mut record, 4, 3);
        entry.append_encoded(&mut record)?;
        crate::cbor::append_head(&mut record, 0, start);
        crate::cbor::append_head(&mut record, 0, size);
        self.records.append_encoded_record(&record)?;
        Ok(())
    }
    fn frame_record(frames: &mut Spool, index: u64) -> Result<[u64; 4]> {
        let bytes = frames.read(
            index
                .checked_mul(32)
                .ok_or_else(|| invalid("frame record offset"))?,
            32,
        )?;
        Ok(std::array::from_fn(|i| {
            u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap())
        }))
    }
    pub fn finish(mut self) -> Result<W> {
        if self.failed {
            return Err(Error::Unsupported("archive writer failed"));
        }
        self.write_raw(&[0; 1024])?;
        self.flush_frame()?;
        loop {
            let completed = match self.workers.as_mut() {
                Some(workers) => workers.receive()?,
                None => None,
            };
            let Some((raw, encoded)) = completed else {
                break;
            };
            let raw_size = raw.len();
            self.write_completed(raw_size, &encoded)?;
            self.recycle_buffers(raw, encoded);
        }
        let mut metadata =
            ManifestEncoder::new(self.output, self.encoded_position, self.level, self.options);
        let mut at = 0;
        let frame_count = self.frames.len() / 32;
        let mut frame_index = 0;
        let mut current_frame = None;
        while let Some(value) = self.records.read_record(&mut at)? {
            let ciborium::Value::Array(values) = value else {
                return Err(invalid("entry spool"));
            };
            let mut entry = Entry::from_value(&values[0], self.encoded_position)?;
            let start = uint(&values[1])?;
            let size = uint(&values[2])?;
            if entry.kind == EntryKind::File && !entry.is_external() && size != 0 {
                let end = start
                    .checked_add(size)
                    .ok_or_else(|| invalid("file length overflow"))?;
                // Capture records and frame records share physical order. Keep
                // the current frame across files, avoiding per-file binary disk
                // searches and repeated reads for shared small-file frames.
                while frame_index < frame_count {
                    let [raw, frame_size, encoded, encoded_size] = match current_frame {
                        Some(frame) => frame,
                        None => Self::frame_record(&mut self.frames, frame_index)?,
                    };
                    current_frame = Some([raw, frame_size, encoded, encoded_size]);
                    if raw >= end {
                        break;
                    }
                    let frame_end = raw
                        .checked_add(frame_size)
                        .ok_or_else(|| invalid("frame length overflow"))?;
                    let a = start.max(raw);
                    let b = end.min(frame_end);
                    if b > a {
                        entry.extents.push(Extent {
                            size: b - a,
                            source: Source::Archive(encoded),
                            encoded_size,
                            offset: a - raw,
                        });
                    }
                    if entry.extents.len() > EXTENTS_MAX {
                        return Err(invalid("entry extent maximum"));
                    }
                    if frame_end >= end {
                        break;
                    }
                    frame_index += 1;
                    current_frame = None;
                }
                if entry.size()? != size {
                    return Err(invalid("captured extent coverage"));
                }
            }
            metadata.add_entry(entry)?;
        }
        let (output, _) = metadata.finish(vec![], self.root_extensions)?;
        Ok(output)
    }
}

#[derive(Default)]
pub(crate) struct PhysicalReport {
    pub frames: u64,
    pub members: u64,
    pub pointers: u64,
}
pub struct BlockingArchive<R: RangeReader> {
    source: R,
    manifest: std::sync::Arc<ManifestReader>,
    data_end: u64,
    options: ManifestOptions,
    content_cache: ContentCache,
}
impl<R: RangeReader> BlockingArchive<R> {
    pub fn manifest(&self) -> &ManifestReader {
        &self.manifest
    }
    #[cfg(feature = "async")]
    pub(crate) fn from_manifest(
        source: R,
        manifest: std::sync::Arc<ManifestReader>,
        data_end: u64,
        options: ManifestOptions,
        metadata: [u8; 32],
    ) -> Self {
        Self {
            source,
            manifest,
            data_end,
            options,
            content_cache: ContentCache {
                metadata,
                ..Default::default()
            },
        }
    }

    pub fn open(mut source: R, options: ManifestOptions) -> Result<Self> {
        let len = source.len();
        if len < 80 {
            return Err(Error::NotZstar);
        }
        let footer = crate::parse_footer(&source.read_range(len - 80..len)?)?;
        let manifest = ManifestReader::open(&mut source, &footer, options.clone())?;
        Ok(Self {
            source,
            manifest: std::sync::Arc::new(manifest),
            data_end: footer.toc_offset,
            options,
            content_cache: ContentCache {
                metadata: footer.toc_sha256,
                ..Default::default()
            },
        })
    }
    /// Configure reader-local decoded content reuse. Zero (the default) disables
    /// retention across reads. Changing the budget clears all retained content.
    /// Archive bytes must remain immutable for the lifetime of this reader.
    pub fn set_content_cache_bytes(&mut self, capacity: usize) {
        self.content_cache = ContentCache {
            capacity,
            shared: self.content_cache.shared.clone(),
            metadata: self.content_cache.metadata,
            ..Default::default()
        };
    }
    /// Attach a caller-owned cache bound to this immutable archive snapshot.
    /// Backend failures fall back to the source. Unchecked objects bypass it.
    /// Replacing the binding clears reader-local retained content.
    pub fn set_shared_decoded_cache(&mut self, shared: Option<SharedDecodedCache>) {
        self.content_cache = ContentCache {
            capacity: self.content_cache.capacity,
            metadata: self.content_cache.metadata,
            shared,
            ..Default::default()
        };
    }
    /// Charged retained bytes; selected frames held by streams are additional.
    pub fn content_cache_bytes(&self) -> usize {
        self.content_cache.bytes
    }
    pub fn entry(&mut self, path: &str) -> Result<Option<Entry>> {
        self.manifest.entry(path)
    }
    pub fn open_file<'a>(
        &'a mut self,
        path: &str,
        resolver: Option<&'a mut dyn EncodedObjectResolver>,
        verify_external: bool,
    ) -> Result<ContentReader<'a, R>> {
        let entry = self
            .entry(path)?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        Ok(ContentReader {
            source: &mut self.source,
            cache: &mut self.content_cache,
            resolver,
            verify_external,
            extents: entry.extents.into_iter(),
            cached: None,
            offset: 0,
            remaining: 0,
            failed: false,
        })
    }
    pub fn read_link(&mut self, path: &str) -> Result<String> {
        let entry = self
            .entry(path)?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        entry
            .linkname
            .ok_or(Error::Unsupported("entry is not a symlink"))
    }
    pub fn read(
        &mut self,
        path: &str,
        offset: u64,
        size: u64,
        resolver: Option<&mut dyn EncodedObjectResolver>,
        verify_external: bool,
    ) -> Result<Vec<u8>> {
        let (entry, ends) = self
            .manifest
            .range_entry(path)
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        crate::checked_range(offset, size, ends.last().copied().unwrap_or(0))?;
        if size > self.options.max_read_bytes || usize::try_from(size).is_err() {
            return Err(Error::Budget("returned read size"));
        }
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut resolver = resolver;
        let mut out = Vec::with_capacity(size as usize);
        let end = offset.checked_add(size).ok_or(Error::Range)?;
        let first = ends.partition_point(|end| *end <= offset);
        let mut position = if first == 0 { 0 } else { ends[first - 1] };
        let mut cached: Option<(Source, u64, DecodedBody)> = None;
        for extent in &entry.extents[first..] {
            let next = position.checked_add(extent.size).ok_or(Error::Range)?;
            if position >= end {
                break;
            }
            if next > offset {
                let same = cached.as_ref().is_some_and(|(source, encoded, _)| {
                    source == &extent.source
                        && (matches!(source, Source::Object(_)) || *encoded == extent.encoded_size)
                });
                if !same {
                    let raw = load_content(
                        &mut self.source,
                        &mut self.content_cache,
                        extent,
                        &mut resolver,
                        verify_external,
                    )?;
                    cached = Some((extent.source.clone(), extent.encoded_size, raw));
                }
                let raw = &cached.as_ref().unwrap().2;
                // Check the complete declared selection even on partial reads.
                crate::checked_range(extent.offset, extent.size, raw.len() as u64)?;
                let a = offset.max(position);
                let b = end.min(next);
                let start = extent
                    .offset
                    .checked_add(a - position)
                    .ok_or(Error::Range)?;
                let stop = start.checked_add(b - a).ok_or(Error::Range)?;
                out.extend_from_slice(&raw[start as usize..stop as usize]);
            }
            position = next;
        }
        if out.len() as u64 != size {
            return Err(invalid("extent coverage"));
        }
        Ok(out)
    }
    /// Scan bounded encoded chunks and decode every physical data frame.
    pub fn visit_frames(
        &mut self,
        mut visitor: impl FnMut(u64, u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut at = 0_u64;
        let mut decoded = 0_u64;
        while at < self.data_end {
            let frame = fetch_frame(&mut self.source, at, self.data_end)?;
            let raw = decode_content(&frame, true)?;
            if raw.is_empty() {
                return Err(Error::MalformedFrame("empty data frame".into()));
            }
            visitor(at, decoded, &raw)?;
            at = at
                .checked_add(frame.len() as u64)
                .ok_or_else(|| invalid("encoded archive overflow"))?;
            decoded = decoded
                .checked_add(raw.len() as u64)
                .ok_or_else(|| invalid("decoded archive overflow"))?;
        }
        if at != self.data_end {
            return Err(invalid("data frame coverage"));
        }
        Ok(())
    }
    pub fn into_source(self) -> R {
        self.source
    }

    /// Validate and stream physical payloads. Output is provisional until this
    /// call succeeds; external payloads are pointer text, not original content.
    pub fn visit_payloads(
        &mut self,
        visitor: impl FnMut(&Entry, &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.visit_payloads_with_report(visitor).map(|_| ())
    }
    pub(crate) fn visit_payloads_with_report(
        &mut self,
        mut visitor: impl FnMut(&Entry, &[u8]) -> Result<()>,
    ) -> Result<PhysicalReport> {
        self.manifest.validate_metadata()?;
        struct Member {
            entry: Entry,
            remaining: u64,
            padding: u64,
            pointer: Vec<u8>,
            extents: Vec<Extent>,
        }
        let mut report = PhysicalReport::default();
        let mut headers = Vec::new();
        let mut member: Option<Member> = None;
        let mut zero_records = 0_u64;
        let mut observed = Spool::new(self.options.clone());
        let mut cursor = self.manifest.cursor();
        let mut ordered = true;
        let mut ordered_count = 0_u64;
        let mut encoded_at = 0_u64;
        let mut decoded_at = 0_u64;
        while encoded_at < self.data_end {
            let encoded = fetch_frame(&mut self.source, encoded_at, self.data_end)?;
            let raw = decode_content(&encoded, true)?;
            report.frames += 1;
            if raw.is_empty() {
                return Err(Error::MalformedFrame("empty data frame".into()));
            }
            let mut position = 0;
            while position < raw.len() {
                if zero_records != 0 {
                    if raw[position..].iter().any(|b| *b != 0) {
                        return Err(Error::MalformedTar("bytes after end marker"));
                    }
                    zero_records = zero_records
                        .checked_add((raw.len() - position) as u64)
                        .ok_or(Error::Range)?;
                    if zero_records > 1024 {
                        return Err(Error::MalformedTar("trailing decoded bytes"));
                    }
                    break;
                }
                if member.is_none() {
                    let needed = 512 - headers.len() % 512;
                    let count = needed.min(raw.len() - position);
                    headers.extend_from_slice(&raw[position..position + count]);
                    position += count;
                    if headers.len() == 512 && headers.iter().all(|b| *b == 0) {
                        zero_records = 512;
                        headers.clear();
                        continue;
                    }
                    if headers.len() > 1 << 20 {
                        return Err(Error::MalformedTar("member header maximum"));
                    }
                    let Some(parsed) = crate::tar::parse_header_prefix(&headers)? else {
                        continue;
                    };
                    let sequential = if ordered {
                        self.manifest.next_entry(&mut cursor)?
                    } else {
                        None
                    };
                    let entry = if let Some(entry) = sequential.filter(|e| e.path == parsed.path) {
                        ordered_count += 1;
                        entry
                    } else {
                        if ordered {
                            // Physical order is optional. Spool the already matched
                            // prefix only when the archive needs the general path.
                            let mut prefix = self.manifest.cursor();
                            for _ in 0..ordered_count {
                                let entry = self
                                    .manifest
                                    .next_entry(&mut prefix)?
                                    .ok_or(Error::TarTocMismatch("missing tar member"))?;
                                observed.append_record(&ciborium::Value::Text(entry.path))?;
                            }
                            ordered = false;
                        }
                        self.manifest
                            .entry(&parsed.path)?
                            .ok_or(Error::TarTocMismatch("unindexed member"))?
                    };
                    if entry.kind != parsed.kind
                        || entry.mode != parsed.mode
                        || entry.uid != parsed.uid
                        || entry.gid != parsed.gid
                        || entry.mtime_ns != parsed.mtime_ns
                        || entry.linkname != parsed.target
                    {
                        return Err(Error::TarTocMismatch("member metadata"));
                    }
                    report.members += 1;
                    if entry.is_external() {
                        report.pointers += 1;
                    }
                    let size = parsed.size.unwrap_or(0);
                    if !entry.is_external() && size != entry.size()? {
                        return Err(Error::TarTocMismatch("inline payload size"));
                    }
                    if entry.is_external() && size > 16 << 20 {
                        return Err(Error::MalformedPointer("pointer size maximum".into()));
                    }
                    let padded = size.checked_add(511).ok_or(Error::Range)? / 512 * 512;
                    headers.clear();
                    if size == 0 {
                        if entry.kind == EntryKind::File {
                            visitor(&entry, &[])?;
                        }
                        if !ordered {
                            observed.append_record(&ciborium::Value::Text(entry.path))?;
                        }
                    } else {
                        member = Some(Member {
                            entry,
                            remaining: size,
                            padding: padded - size,
                            pointer: Vec::new(),
                            extents: Vec::new(),
                        });
                    }
                    continue;
                }
                let current = member.as_mut().unwrap();
                if current.remaining != 0 {
                    let count = current.remaining.min((raw.len() - position) as u64) as usize;
                    let payload = &raw[position..position + count];
                    if current.entry.is_external() {
                        current.pointer.extend_from_slice(payload);
                    } else {
                        current.extents.push(Extent {
                            size: count as u64,
                            source: Source::Archive(encoded_at),
                            encoded_size: encoded.len() as u64,
                            offset: position as u64,
                        });
                    }
                    visitor(&current.entry, payload)?;
                    current.remaining -= count as u64;
                    position += count;
                } else if current.padding != 0 {
                    let count = current.padding.min((raw.len() - position) as u64) as usize;
                    if raw[position..position + count].iter().any(|b| *b != 0) {
                        return Err(Error::MalformedTar("payload padding"));
                    }
                    current.padding -= count as u64;
                    position += count;
                }
                if current.remaining == 0 && current.padding == 0 {
                    if current.entry.is_external() {
                        if decode_pointer(&current.pointer)? != current.entry.extents {
                            return Err(Error::TarTocMismatch("pointer text/reference"));
                        }
                    } else if !normalized_extents(&current.extents)
                        .eq(normalized_extents(&current.entry.extents))
                    {
                        return Err(Error::TarTocMismatch("inline payload selection"));
                    }
                    if !ordered {
                        observed
                            .append_record(&ciborium::Value::Text(current.entry.path.clone()))?;
                    }
                    member = None;
                }
            }
            encoded_at = encoded_at
                .checked_add(encoded.len() as u64)
                .ok_or(Error::Range)?;
            decoded_at = decoded_at
                .checked_add(raw.len() as u64)
                .ok_or(Error::Range)?;
        }
        if member.is_some() || !headers.is_empty() || zero_records != 1024 || decoded_at < 1024 {
            return Err(Error::MalformedTar("incomplete tar stream"));
        }
        if ordered {
            if self.manifest.next_entry(&mut cursor)?.is_some() {
                return Err(Error::TarTocMismatch("missing tar member"));
            }
            return Ok(report);
        }
        fn path_key(v: &ciborium::Value) -> Result<String> {
            match v {
                ciborium::Value::Text(s) => Ok(s.clone()),
                _ => Err(invalid("path spool")),
            }
        }
        let mut observed =
            crate::manifest_io::sort_records(observed, self.options.clone(), path_key)?;
        let mut at = 0;
        self.manifest.visit_entries(|entry| {
            let path = observed
                .read_record(&mut at)?
                .ok_or(Error::TarTocMismatch("missing tar member"))?;
            if path_key(&path)? != entry.path {
                return Err(Error::TarTocMismatch("duplicate or missing tar member"));
            }
            Ok(())
        })?;
        if at != observed.len() {
            return Err(Error::TarTocMismatch("unindexed or duplicate member"));
        }
        Ok(report)
    }
    pub fn validate(&mut self) -> Result<()> {
        self.visit_payloads(|_, _| Ok(()))
    }
}

fn normalized_extents(extents: &[Extent]) -> impl Iterator<Item = Extent> + '_ {
    let mut entries = extents.iter().peekable();
    std::iter::from_fn(move || {
        let mut extent = entries.next()?.clone();
        while let Some(next) = entries.peek() {
            if extent.source != next.source
                || extent.encoded_size != next.encoded_size
                || extent.offset.checked_add(extent.size) != Some(next.offset)
            {
                break;
            }
            extent.size = extent.size.saturating_add(next.size);
            entries.next();
        }
        Some(extent)
    })
}

pub fn decode_content(encoded: &[u8], require_checksum: bool) -> Result<Vec<u8>> {
    if encoded.len() < 5 || encoded.len() as u64 > ENCODED_FRAME_MAX {
        return Err(Error::MalformedFrame("encoded frame bounds".into()));
    }
    if require_checksum && encoded[4] & 4 == 0 {
        return Err(Error::MalformedFrame("missing data checksum".into()));
    }
    crate::decode_bounded(encoded, crate::DATA_FRAME_MAX)
}

pub(crate) fn decode_object(encoded: &[u8]) -> Result<Vec<u8>> {
    decode_content(encoded, false).map_err(|error| match error {
        Error::MalformedFrame(message) => Error::MalformedExternalContent(message),
        other => other,
    })
}

/// Determine one frame's boundary without assuming a content-size field exists.
pub fn fetch_frame<R: RangeReader>(source: &mut R, start: u64, end: u64) -> Result<Vec<u8>> {
    // Fetch a bounded read-ahead window once; block headers are scanned in memory.
    let header_end = start.saturating_add(256 << 10).min(end);
    let header = source.read_range(start..header_end)?;
    if header.len() < 6 || !header.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(Error::MalformedFrame("frame header".into()));
    }
    let descriptor = header[4];
    if descriptor & 0x08 != 0 {
        return Err(Error::MalformedFrame("reserved frame header bits".into()));
    }
    let single = descriptor & 0x20 != 0;
    let dictionary = match descriptor & 3 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    let content = match descriptor >> 6 {
        0 => usize::from(single),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let limit = start.saturating_add(ENCODED_FRAME_MAX).min(end);
    // Read ahead in large chunks and scan block headers in memory, so request
    // count is bounded by frame size / READ_AHEAD rather than block count.
    const READ_AHEAD: u64 = 256 << 10;
    // Zstandard Block_Maximum_Size.
    const BLOCK_MAX: u64 = 128 << 10;
    let mut buffer = header;
    let mut buffered_end = start + buffer.len() as u64;
    let mut at = start
        .checked_add(5 + u64::from(!single) + dictionary + content as u64)
        .ok_or_else(|| invalid("frame header overflow"))?;
    loop {
        if at.checked_add(3).is_none_or(|n| n > limit) {
            return Err(Error::MalformedFrame("block header bounds".into()));
        }
        if at + 3 > buffered_end {
            let to = at.saturating_add(READ_AHEAD).min(limit);
            let more = source.read_range(buffered_end..to)?;
            if more.is_empty() || more.len() as u64 != to - buffered_end {
                return Err(Error::MalformedFrame("short block header".into()));
            }
            buffer.extend_from_slice(&more);
            buffered_end = to;
            continue;
        }
        let i = (at - start) as usize;
        let block =
            u32::from(buffer[i]) | u32::from(buffer[i + 1]) << 8 | u32::from(buffer[i + 2]) << 16;
        let size = match (block >> 1) & 3 {
            0 | 2 => u64::from(block >> 3),
            1 => 1,
            _ => return Err(Error::MalformedFrame("reserved block type".into())),
        };
        if size > BLOCK_MAX || u64::from(block >> 3) > BLOCK_MAX {
            return Err(Error::MalformedFrame("block exceeds 128 KiB".into()));
        }
        at = at
            .checked_add(3 + size)
            .ok_or_else(|| invalid("block size overflow"))?;
        if block & 1 != 0 {
            break;
        }
    }
    if descriptor & 4 != 0 {
        at = at
            .checked_add(4)
            .ok_or_else(|| invalid("frame checksum overflow"))?;
    }
    if at > end || at - start > ENCODED_FRAME_MAX {
        return Err(Error::MalformedFrame("frame encoded limit".into()));
    }
    if at > buffered_end {
        let more = source.read_range(buffered_end..at)?;
        if more.len() as u64 != at - buffered_end {
            return Err(Error::MalformedFrame("short frame".into()));
        }
        buffer.extend_from_slice(&more);
    }
    buffer.truncate((at - start) as usize);
    let encoded = buffer;
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn root_extensions_validate_before_capture_and_roundtrip() {
        use ciborium::Value;
        for extensions in [
            BTreeMap::from([("unknown".into(), Value::Bool(true))]),
            BTreeMap::from([("x-null".into(), Value::Null)]),
            BTreeMap::from([(
                "x-duplicate".into(),
                Value::Map(vec![
                    (Value::Text("a".into()), Value::Bool(true)),
                    (Value::Text("a".into()), Value::Bool(false)),
                ]),
            )]),
        ] {
            let writer = BlockingBuilder::new(Vec::new()).unwrap();
            assert!(matches!(
                writer.with_root_extensions(extensions),
                Err(Error::MalformedToc(_))
            ));
        }
        let extensions = BTreeMap::from([(
            "x-data".into(),
            Value::Array(vec![Value::Bytes(vec![9; 1024]), Value::Bool(true)]),
        )]);
        let writer = BlockingBuilder::new(Vec::new())
            .unwrap()
            .with_root_extensions(extensions.clone())
            .unwrap();
        let bytes = writer.finish().unwrap();
        let reader =
            BlockingArchive::open(crate::SliceReader::new(&bytes), Default::default()).unwrap();
        assert_eq!(reader.manifest().root().extensions, extensions);
    }
    #[test]
    fn shared_and_split_frames_without_a_frame_table() {
        let mut writer =
            BlockingBuilder::with_options(Vec::new(), 2048, 3, Default::default()).unwrap();
        writer
            .add_file(Entry::file("a"), 5, &mut b"hello".as_slice())
            .unwrap();
        writer
            .add_file(Entry::file("b"), 5, &mut b"world".as_slice())
            .unwrap();
        let large = vec![7; 10_000];
        writer
            .add_file(Entry::file("c"), large.len() as u64, &mut large.as_slice())
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader =
            BlockingArchive::open(crate::SliceReader::new(&bytes), Default::default()).unwrap();
        assert_eq!(reader.read("a", 1, 3, None, true).unwrap(), b"ell");
        assert_eq!(reader.read("b", 0, 5, None, true).unwrap(), b"world");
        assert_eq!(reader.read("c", 0, 10_000, None, true).unwrap(), large);
        let a = reader.entry("a").unwrap().unwrap();
        let b = reader.entry("b").unwrap().unwrap();
        assert_eq!(a.extents[0].source, b.extents[0].source);
        let mut decoded = Vec::new();
        reader
            .visit_frames(|_, _, bytes| {
                decoded.extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(&decoded[decoded.len() - 1024..], &[0; 1024]);
        assert!(reader.read("c", 10_001, 0, None, true).is_err());
        reader.validate().unwrap();
        let mut streaming = Vec::new();
        reader
            .open_file("c", None, true)
            .unwrap()
            .read_to_end(&mut streaming)
            .unwrap();
        assert_eq!(streaming, large);
        assert!(matches!(
            reader.open_file("missing", None, true),
            Err(Error::MissingEntry(path)) if path == "missing"
        ));
        assert!(matches!(
            reader.read("missing", 0, 1, None, true),
            Err(Error::MissingEntry(_))
        ));
        assert!(matches!(
            reader.read_link("missing"),
            Err(Error::MissingEntry(_))
        ));
    }
    #[test]
    fn file_stream_reuses_adjacent_objects_and_failure_cannot_skip_an_extent() {
        let bytes = include_bytes!("../../../testdata/entry-manifest/thin.zstar");
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
        let mut reader =
            BlockingArchive::open(crate::SliceReader::new(bytes), Default::default()).unwrap();
        reader.validate().unwrap();
        let mut objects = Objects {
            encoded: crate::encode_frame(b"hello\nworld\n", 3).unwrap(),
            calls: 0,
        };
        let mut output = Vec::new();
        let copied = reader
            .open_file("repeat", Some(&mut objects), true)
            .unwrap()
            .copy_to(&mut output)
            .unwrap();
        assert_eq!(copied, 12);
        assert_eq!(output, b"hello\nhello\n");
        assert_eq!(objects.calls, 1);
        objects.encoded = b"bad codec".to_vec();
        let mut file = reader
            .open_file("repeat", Some(&mut objects), false)
            .unwrap();
        assert_eq!(file.read(&mut []).unwrap(), 0);
        let mut buffer = [0; 1];
        assert!(file.read(&mut buffer).is_err());
        assert!(file.read(&mut buffer).is_err());
        drop(file);
        assert_eq!(objects.calls, 2);
    }
    #[test]
    fn failed_capture_cannot_finalize_an_archive() {
        let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
        assert!(
            writer
                .add_file(Entry::file("a"), 10, &mut b"short".as_slice())
                .is_err()
        );
        assert!(
            writer
                .add_file(Entry::file("b"), 2, &mut b"ok".as_slice())
                .is_err()
        );
        assert!(writer.finish().is_err());
    }
    #[test]
    fn sequential_validation_falls_back_for_unordered_tar_and_rejects_bad_members() {
        fn archive(paths: &[&str]) -> Vec<u8> {
            let mut tar = Vec::new();
            for path in paths {
                let header =
                    crate::tar_encode::member_header_full(path, 0, 420, b'0', "", 0, 0, 0).unwrap();
                tar.extend(header);
            }
            tar.extend([0; 1024]);
            let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
            compressor
                .set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))
                .unwrap();
            let mut encoded = compressor.compress(&tar).unwrap();
            let mut manifest =
                ManifestEncoder::new(Vec::new(), encoded.len() as u64, 3, Default::default());
            for path in ["a", "b"] {
                manifest.add_entry(Entry::file(path)).unwrap();
            }
            let (metadata, _) = manifest.finish(vec![], Default::default()).unwrap();
            encoded.extend(metadata);
            encoded
        }
        for paths in [["a", "b"], ["b", "a"]] {
            let bytes = archive(&paths);
            let mut reader =
                BlockingArchive::open(crate::SliceReader::new(&bytes), Default::default()).unwrap();
            reader.validate().unwrap();
        }
        for paths in [&["a"][..], &["a", "a"], &["b", "b"], &["a", "b", "c"]] {
            let bytes = archive(paths);
            let mut reader =
                BlockingArchive::open(crate::SliceReader::new(&bytes), Default::default()).unwrap();
            assert!(reader.validate().is_err(), "accepted {paths:?}");
        }
    }
    #[test]
    fn external_hint_can_be_stale_and_only_objects_are_hashed() {
        let raw = b"some shared object";
        let encoded = crate::encode_frame(raw, 3).unwrap();
        let hash = *crate::Digest::sha256(raw).as_bytes();
        struct Objects(Vec<u8>);
        impl EncodedObjectResolver for Objects {
            fn get_encoded(&mut self, _: [u8; 32], hint: u64, _: u64) -> Result<Vec<u8>> {
                assert_eq!(hint, 1);
                Ok(self.0.clone())
            }
        }
        let mut entry = Entry::file("external");
        entry.extents = vec![Extent {
            size: 6,
            source: Source::Object(hash),
            encoded_size: 1,
            offset: 5,
        }];
        let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
        writer.add_metadata(entry.clone()).unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader =
            BlockingArchive::open(crate::SliceReader::new(&bytes), Default::default()).unwrap();
        assert_eq!(
            reader
                .read("external", 0, 6, Some(&mut Objects(encoded)), true)
                .unwrap(),
            &raw[5..11]
        );
        assert_eq!(
            decode_pointer(&encode_pointer(&entry.extents).unwrap()).unwrap(),
            entry.extents
        );
        reader.validate().unwrap();
    }

    #[test]
    fn parallel_frames_preserve_serial_bytes() {
        fn encode(workers: usize) -> Vec<u8> {
            let mut writer = BlockingBuilder::with_options(Vec::new(), 2048, 3, Default::default())
                .unwrap()
                .with_workers(workers)
                .unwrap();
            let bytes = vec![13; 20_000];
            for path in ["a", "b", "c"] {
                writer
                    .add_file(Entry::file(path), bytes.len() as u64, &mut bytes.as_slice())
                    .unwrap();
            }
            writer.finish().unwrap()
        }
        let serial = encode(0);
        for workers in [1, 2, 4] {
            assert_eq!(encode(workers), serial);
            let mut reader =
                BlockingArchive::open(crate::SliceReader::new(&serial), Default::default())
                    .unwrap();
            reader.validate().unwrap();
        }
    }

    struct Counting<'a> {
        bytes: &'a [u8],
        requests: usize,
    }
    impl RangeReader for Counting<'_> {
        fn len(&self) -> u64 {
            self.bytes.len() as u64
        }
        fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
            self.requests += 1;
            let end = (range.end as usize).min(self.bytes.len());
            Ok(self.bytes[range.start as usize..end].to_vec())
        }
    }
    #[test]
    fn fetch_frame_uses_constant_requests_per_frame() {
        // Incompressible data forces many blocks of up to 128 KiB.
        let mut data = vec![0_u8; 3 << 20];
        let mut x = 0x9e3779b97f4a7c15_u64;
        for b in &mut data {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let frame = crate::encode_frame(&data, 1).unwrap();
        let mut source = Counting {
            bytes: &frame,
            requests: 0,
        };
        let got = fetch_frame(&mut source, 0, frame.len() as u64).unwrap();
        assert_eq!(got, frame);
        // ~24 blocks; the old scanner needed one request per block.
        assert!(
            source.requests <= 3 + frame.len() / (256 << 10),
            "{}",
            source.requests
        );
        let small = crate::encode_frame(b"hello", 1).unwrap();
        let mut source = Counting {
            bytes: &small,
            requests: 0,
        };
        fetch_frame(&mut source, 0, small.len() as u64).unwrap();
        assert_eq!(source.requests, 1);
    }
    #[test]
    fn fetch_frame_rejects_hostile_rle_and_oversized_blocks() {
        // Single-segment-less header: descriptor 0 (no checksum), window byte.
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00];
        // Millions of 1-byte-payload RLE blocks (4 bytes each), never last.
        let block = [0x02_u8 | (1 << 3), 0, 0, 7];
        for _ in 0..(5 << 20) {
            frame.extend_from_slice(&block);
        }
        let started = std::time::Instant::now();
        let mut source = Counting {
            bytes: &frame,
            requests: 0,
        };
        let error = fetch_frame(&mut source, 0, frame.len() as u64).unwrap_err();
        assert!(matches!(error, Error::MalformedFrame(_)));
        assert!(source.requests <= 100, "{}", source.requests);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        // Raw block declaring 128 KiB + 1.
        let size = (128u32 << 10) + 1;
        let header = (size << 3) | 1;
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00];
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.resize(frame.len() + size as usize, 0);
        let mut source = Counting {
            bytes: &frame,
            requests: 0,
        };
        assert!(fetch_frame(&mut source, 0, frame.len() as u64).is_err());
    }
}
