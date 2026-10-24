//! One deterministic manifest sequence with bounded compression segments and disk spooling.
use crate::manifest::{
    BLOCK_TARGET, ENCODED_FRAME_MAX, Entry, METADATA_BLOCK_MAX, ManifestRoot, entry_allocation,
    valid_path,
};
use crate::{Error, Footer, RangeReader, Result, TOC_MAGIC};
use ciborium::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
#[cfg(test)]
use std::io::{Cursor, Read};
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

mod spool;
pub(crate) use spool::sort_records;
pub use spool::{SpillFactory, SpillStore, Spool, SpoolBudget};

const MEMORY_DEFAULT: u64 = 8 << 20;
const TOTAL_DEFAULT: u64 = 4 << 30;

fn invalid(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}

#[derive(Clone, Debug)]
pub struct ManifestOptions {
    pub memory_bytes: u64,
    pub max_encoded_bytes: u64,
    pub max_decoded_bytes: u64,
    /// Maximum bytes returned by one selective content read.
    pub max_read_bytes: u64,
    /// Estimated retained typed index, including vector capacity (default 1 GiB).
    pub max_retained_entry_bytes: u64,
    /// Separate entry-count limit; zero disables this additional limit.
    pub max_entries: u64,
    /// Target decoded bytes per compression segment, in [512, 1 MiB].
    pub block_target: usize,
    /// Native default spills to an automatically removed temporary file.
    pub allow_disk: bool,
    /// Shared quota for retained logical spool bytes. Cloned options share it.
    pub spool_budget: Arc<SpoolBudget>,
    /// Create an empty, exclusively used store when the memory threshold is crossed.
    pub create_spill_store: Option<SpillFactory>,
}
impl Default for ManifestOptions {
    fn default() -> Self {
        Self {
            memory_bytes: MEMORY_DEFAULT,
            max_encoded_bytes: TOTAL_DEFAULT,
            max_decoded_bytes: TOTAL_DEFAULT,
            max_read_bytes: 64 << 20,
            max_retained_entry_bytes: 1 << 30,
            max_entries: 0,
            block_target: BLOCK_TARGET,
            allow_disk: cfg!(feature = "disk"),
            spool_budget: Arc::new(SpoolBudget::new(TOTAL_DEFAULT)),
            create_spill_store: None,
        }
    }
}

/// Disk-spooled, unknown-count producer of a deterministic manifest sequence.
pub struct ManifestEncoder<W: Write> {
    output: W,
    start: u64,
    position: u64,
    decoded: u64,
    hash: Sha256,
    records: Spool,
    count: u64,
    external: bool,
    level: i32,
    compressor: Option<zstd::bulk::Compressor<'static>>,
    pending: Vec<u8>,
    entry_buffer: Vec<u8>,
    options: ManifestOptions,
}
impl<W: Write> ManifestEncoder<W> {
    pub fn new(output: W, data_end: u64, compression_level: i32, options: ManifestOptions) -> Self {
        let mut spool_options = options.clone();
        spool_options.max_encoded_bytes = options.max_decoded_bytes;
        Self {
            output,
            start: data_end,
            position: data_end,
            decoded: 0,
            hash: Sha256::new(),
            records: Spool::new(spool_options),
            count: 0,
            external: false,
            level: compression_level,
            compressor: None,
            pending: Vec::new(),
            entry_buffer: Vec::new(),
            options,
        }
    }
    fn validate_target(&self) -> Result<()> {
        if !(512..=1 << 20).contains(&self.options.block_target) {
            return Err(Error::Unsupported(
                "metadata segment target must be in [512, 1 MiB]",
            ));
        }
        Ok(())
    }
    pub fn add_entry(&mut self, entry: Entry) -> Result<()> {
        self.validate_target()?;
        entry.validate(self.start)?;
        self.entry_buffer.clear();
        entry.append_encoded(&mut self.entry_buffer)?;
        self.records.append_encoded_record(&self.entry_buffer)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("entry count overflow"))?;
        self.external |= entry.is_external();
        Ok(())
    }
    fn flush_segment(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.decoded = self
            .decoded
            .checked_add(self.pending.len() as u64)
            .ok_or_else(|| invalid("decoded metadata overflow"))?;
        if self.decoded > self.options.max_decoded_bytes {
            return Err(Error::Budget("decoded manifest size"));
        }
        if self.compressor.is_none() {
            self.compressor = Some(zstd::bulk::Compressor::new(self.level)?);
        }
        let encoded = self.compressor.as_mut().unwrap().compress(&self.pending)?;
        if encoded.len() as u64 > ENCODED_FRAME_MAX {
            return Err(invalid("encoded metadata segment maximum"));
        }
        let size = encoded.len() as u64 + 8;
        let end = self
            .position
            .checked_add(size)
            .ok_or_else(|| invalid("metadata offset overflow"))?;
        if end - self.start > self.options.max_encoded_bytes {
            return Err(Error::Budget("encoded manifest size"));
        }
        self.output.write_all(&TOC_MAGIC.to_le_bytes())?;
        self.output
            .write_all(&(encoded.len() as u32).to_le_bytes())?;
        self.output.write_all(&encoded)?;
        self.hash.update(&self.pending);
        self.position = end;
        self.pending.clear();
        Ok(())
    }
    fn write_document_bytes(&mut self, mut raw: &[u8]) -> Result<()> {
        while !raw.is_empty() {
            let n = raw
                .len()
                .min(self.options.block_target - self.pending.len());
            self.pending.extend_from_slice(&raw[..n]);
            raw = &raw[n..];
            if self.pending.len() == self.options.block_target {
                self.flush_segment()?;
            }
        }
        Ok(())
    }
    pub fn finish(
        mut self,
        mut required_features: Vec<String>,
        extensions: BTreeMap<String, Value>,
    ) -> Result<(W, Footer)> {
        self.validate_target()?;
        required_features.retain(|s| s != "external-content");
        if self.external {
            required_features.push("external-content".into());
        }
        required_features.sort();
        required_features.dedup();
        let root = ManifestRoot {
            required_features,
            entry_count: self.count,
            extensions,
        };
        root.validate()?;
        let placeholder = Spool::new(self.records.options.clone());
        let records = std::mem::replace(&mut self.records, placeholder);
        let mut records = sort_records(records, self.records.options.clone(), |v| {
            let Value::Array(fields) = v else {
                return Err(invalid("entry tuple"));
            };
            let Some(Value::Text(path)) = fields.first() else {
                return Err(invalid("entry path"));
            };
            Ok(path.clone())
        })?;
        let mut at = 0;
        let mut previous: Option<String> = None;
        let mut directories: Vec<String> = Vec::new();
        while let Some(value) = records.read_record(&mut at)? {
            let entry = Entry::from_value(&value, self.start)?;
            if previous
                .as_ref()
                .is_some_and(|p| p.as_bytes() >= entry.path.as_bytes())
            {
                return Err(invalid("duplicate entry path"));
            }
            while directories
                .last()
                .is_some_and(|dir| !crate::manifest::ancestor_retained(dir, &entry.path))
            {
                directories.pop();
            }
            if let Some((parent, _)) = entry.path.rsplit_once('/')
                && directories
                    .binary_search_by(|dir| dir.as_bytes().cmp(parent.as_bytes()))
                    .is_err()
            {
                return Err(invalid("missing ancestor directory"));
            }
            if entry.kind == crate::manifest::EntryKind::Directory {
                directories.push(entry.path.clone());
            }
            previous = Some(entry.path);
        }
        // Sort only the small field list. Entries stay in their bounded disk spool.
        enum RootField<'a> {
            Version,
            Features,
            Extension(&'a Value),
        }
        let mut fields: Vec<(&str, RootField<'_>)> = root
            .extensions
            .iter()
            .map(|(k, v)| (k.as_str(), RootField::Extension(v)))
            .collect();
        fields.extend([
            ("version", RootField::Version),
            ("required_features", RootField::Features),
        ]);
        fields.sort_unstable_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(b.0)));
        let mut raw = Vec::new();
        crate::cbor::append_head(&mut raw, 5, fields.len() as u64);
        self.write_document_bytes(&raw)?;
        for (key, value) in fields {
            raw.clear();
            crate::cbor::append_text(&mut raw, key);
            match value {
                RootField::Version => crate::cbor::append_head(&mut raw, 0, 1),
                RootField::Features => {
                    crate::cbor::append_head(&mut raw, 4, root.required_features.len() as u64);
                    for feature in &root.required_features {
                        crate::cbor::append_text(&mut raw, feature);
                    }
                }
                RootField::Extension(value) => crate::cbor::append_validated(&mut raw, value),
            }
            self.write_document_bytes(&raw)?;
        }
        at = 0;
        while let Some(value) = records.read_record(&mut at)? {
            // read_record has already validated restricted CBOR. Reuse the
            // output buffer instead of allocating for every nested scalar.
            raw.clear();
            crate::cbor::append_validated(&mut raw, &value);
            self.write_document_bytes(&raw)?;
        }
        self.flush_segment()?;
        let footer = Footer {
            toc_offset: self.start,
            toc_encoded_len: self.position - self.start,
            toc_decoded_len: self.decoded,
            toc_sha256: self.hash.finalize().into(),
        };
        self.output.write_all(&crate::encode_footer(&footer))?;
        Ok((self.output, footer))
    }
}

/// Independent sequential state over the authenticated entry vector.
/// Cloning a cursor checkpoints its position without retaining entry data.
#[derive(Clone)]
pub struct EntryCursor {
    owner: Arc<()>,
    item: usize,
    end: usize,
}

#[cfg(feature = "async")]
pub(crate) enum EntryLookup {
    NeedsWorker,
    Ready(Option<Entry>),
}

pub struct ManifestReader {
    root: ManifestRoot,
    entries: Vec<Entry>,
    extent_offsets: Vec<usize>,
    extent_ends: Vec<u64>,
    spilled: bool,
    cursor_owner: Arc<()>,
}
/// Segments are authenticated as bytes before the sequence is parsed.
pub(crate) struct ManifestIngest {
    footer: Footer,
    spool: Spool,
    at: u64,
    end: u64,
    decoded: u64,
    hash: Sha256,
}
impl ManifestIngest {
    pub(crate) fn new(footer: Footer, source_size: u64, mut spool: Spool) -> Result<Self> {
        if footer.toc_offset.checked_add(footer.toc_encoded_len)
            != source_size.checked_sub(crate::FOOTER_SIZE as u64)
        {
            return Err(Error::MalformedFooter("manifest coverage"));
        }
        if footer.toc_encoded_len > spool.options.max_encoded_bytes
            || footer.toc_decoded_len > spool.options.max_decoded_bytes
        {
            return Err(Error::Budget("manifest size"));
        }
        spool.options.max_encoded_bytes = spool.options.max_decoded_bytes;
        Ok(Self {
            at: footer.toc_offset,
            end: footer.toc_offset + footer.toc_encoded_len,
            footer,
            spool,
            decoded: 0,
            hash: Sha256::new(),
        })
    }
    pub(crate) fn complete(&self) -> bool {
        self.at == self.end
    }
    pub(crate) fn position(&self) -> u64 {
        self.at
    }
    pub(crate) fn header_range(&self) -> Result<std::ops::Range<u64>> {
        if self.end - self.at < 8 {
            return Err(invalid("truncated metadata wrapper"));
        }
        Ok(self.at..self.at + 8)
    }
    pub(crate) fn encoded_length(&self, header: &[u8]) -> Result<u64> {
        if self.end - self.at < 8 {
            return Err(invalid("truncated metadata wrapper"));
        }
        if header.len() != 8 || u32::from_le_bytes(header[..4].try_into().unwrap()) != TOC_MAGIC {
            return Err(invalid("metadata wrapper"));
        }
        let len = u64::from(u32::from_le_bytes(header[4..].try_into().unwrap()));
        if len == 0 || len > ENCODED_FRAME_MAX || len > self.end - self.at - 8 {
            return Err(invalid("encoded metadata bounds"));
        }
        Ok(len)
    }
    pub(crate) fn push(&mut self, header: &[u8], encoded: &[u8]) -> Result<()> {
        let len = self.encoded_length(header)?;
        if encoded.len() as u64 != len {
            return Err(invalid("short metadata frame"));
        }
        let raw = crate::decode_bounded(encoded, METADATA_BLOCK_MAX)
            .map_err(|e| invalid(&e.to_string()))?;
        if raw.is_empty() {
            return Err(invalid("empty metadata segment"));
        }
        self.decoded = self
            .decoded
            .checked_add(raw.len() as u64)
            .ok_or_else(|| invalid("decoded manifest overflow"))?;
        if self.decoded > self.spool.options.max_decoded_bytes {
            return Err(Error::Budget("decoded manifest size"));
        }
        if self.decoded > self.footer.toc_decoded_len {
            return Err(invalid("decoded manifest length"));
        }
        self.hash.update(&raw);
        self.spool.append(&raw)?;
        self.at += 8 + len;
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Result<ManifestReader> {
        if self.at != self.end || self.decoded != self.footer.toc_decoded_len {
            return Err(invalid("decoded manifest length"));
        }
        if self.hash.finalize().as_slice() != self.footer.toc_sha256 {
            return Err(Error::Integrity);
        }
        self.spool.store.seek(SeekFrom::Start(0))?;
        let reader = std::io::BufReader::new(&mut self.spool.store);
        // Retained entries are bounded by 4x the decoded budget, capped at the
        // default total; failures are Budget (local limit), not malformed.
        let retained = self
            .spool
            .options
            .max_decoded_bytes
            .saturating_mul(4)
            .min(self.spool.options.max_retained_entry_bytes);
        let (root, entries) = crate::manifest::decode_sequence_budgeted(
            reader,
            self.decoded,
            self.footer.toc_offset,
            retained,
            self.spool.options.max_entries,
        )?;
        let extent_count = entries.iter().try_fold(0_usize, |count, entry| {
            count
                .checked_add(entry.extents.len())
                .ok_or(Error::Budget("manifest range index"))
        })?;
        let mut extent_offsets = Vec::new();
        if !entries.is_empty() {
            extent_offsets
                .try_reserve_exact(
                    entries
                        .len()
                        .checked_add(1)
                        .ok_or(Error::Budget("manifest range index"))?,
                )
                .map_err(|_| Error::Budget("manifest range index"))?;
        }
        let mut extent_ends = Vec::new();
        extent_ends
            .try_reserve_exact(extent_count)
            .map_err(|_| Error::Budget("manifest range index"))?;
        for entry in &entries {
            extent_offsets.push(extent_ends.len());
            let mut end = 0_u64;
            for extent in &entry.extents {
                end = end.checked_add(extent.size).ok_or(Error::Range)?;
                extent_ends.push(end);
            }
        }
        if !entries.is_empty() {
            extent_offsets.push(extent_ends.len());
        }
        Ok(ManifestReader {
            root,
            entries,
            extent_offsets,
            extent_ends,
            spilled: self.spool.spilled(),
            cursor_owner: Arc::new(()),
        })
    }
}
impl ManifestReader {
    /// Immutable, authenticated root metadata.
    #[must_use]
    pub fn root(&self) -> &ManifestRoot {
        &self.root
    }
    /// Borrow immutable entries in path order using standard iterator adapters.
    pub fn iter(&self) -> std::slice::Iter<'_, Entry> {
        self.entries.iter()
    }
    pub fn open<R: RangeReader>(
        source: &mut R,
        footer: &Footer,
        options: ManifestOptions,
    ) -> Result<Self> {
        Self::open_with_spool(source, footer, Spool::new(options))
    }
    pub fn open_with_spool<R: RangeReader>(
        source: &mut R,
        footer: &Footer,
        spool: Spool,
    ) -> Result<Self> {
        let mut ingest = ManifestIngest::new(footer.clone(), source.len(), spool)?;
        while !ingest.complete() {
            let at = ingest.position();
            let header = source.read_range(ingest.header_range()?)?;
            let len = ingest.encoded_length(&header)?;
            let encoded = source.read_range(at + 8..at + 8 + len)?;
            ingest.push(&header, &encoded)?;
        }
        ingest.finish()
    }
    #[must_use]
    pub fn spilled(&self) -> bool {
        self.spilled
    }
    /// Retained decoded entries, including owned paths, extents and attributes.
    #[must_use]
    pub fn retained_lookup_bytes(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<Entry>()
            + self.entries.iter().map(entry_allocation).sum::<usize>()
            + self.extent_offsets.capacity() * std::mem::size_of::<usize>()
            + self.extent_ends.capacity() * std::mem::size_of::<u64>()
    }
    #[must_use]
    pub fn cursor(&self) -> EntryCursor {
        EntryCursor {
            owner: self.cursor_owner.clone(),
            item: 0,
            end: self.entries.len(),
        }
    }
    /// Creates a bounded cursor over paths beginning with `prefix` using the sorted index.
    #[must_use]
    pub fn cursor_prefix(&self, prefix: &str) -> EntryCursor {
        let item = self
            .entries
            .partition_point(|e| e.path.as_bytes() < prefix.as_bytes());
        let end = item + self.entries[item..].partition_point(|e| e.path.starts_with(prefix));
        EntryCursor {
            owner: self.cursor_owner.clone(),
            item,
            end,
        }
    }
    pub fn next_entry(&self, cursor: &mut EntryCursor) -> Result<Option<Entry>> {
        if !Arc::ptr_eq(&self.cursor_owner, &cursor.owner) {
            return Err(Error::Unsupported("cursor belongs to another manifest"));
        }
        let entry = (cursor.item < cursor.end)
            .then(|| self.entries.get(cursor.item).cloned())
            .flatten();
        if entry.is_some() {
            cursor.item += 1;
        }
        Ok(entry)
    }
    pub fn entry(&self, path: &str) -> Result<Option<Entry>> {
        if !valid_path(path) {
            return Ok(None);
        }
        Ok(self
            .entries
            .binary_search_by(|e| e.path.as_bytes().cmp(path.as_bytes()))
            .ok()
            .map(|i| self.entries[i].clone()))
    }
    pub(crate) fn range_entry(&self, path: &str) -> Option<(&Entry, &[u64])> {
        if !valid_path(path) {
            return None;
        }
        let index = self
            .entries
            .binary_search_by(|e| e.path.as_bytes().cmp(path.as_bytes()))
            .ok()?;
        Some((
            &self.entries[index],
            &self.extent_ends[self.extent_offsets[index]..self.extent_offsets[index + 1]],
        ))
    }
    #[cfg(feature = "async")]
    pub(crate) fn entry_cached(&self, path: &str) -> EntryLookup {
        if !valid_path(path) {
            return EntryLookup::Ready(None);
        }
        let selected = self
            .entries
            .binary_search_by(|e| e.path.as_bytes().cmp(path.as_bytes()))
            .ok()
            .map(|i| &self.entries[i]);
        if selected.is_some_and(|e| !e.attributes.is_empty() || e.extents.len() > 64) {
            return EntryLookup::NeedsWorker;
        }
        EntryLookup::Ready(selected.cloned())
    }
    pub fn visit_entries(&self, mut visitor: impl FnMut(Entry) -> Result<()>) -> Result<()> {
        for entry in &self.entries {
            visitor(entry.clone())?;
        }
        Ok(())
    }
    pub fn collect_entries(&self) -> Result<Vec<Entry>> {
        Ok(self.entries.clone())
    }
    /// Authentication, schema and namespace validation happen before open returns.
    #[doc(hidden)]
    pub fn validate_metadata(&self) -> Result<()> {
        Ok(())
    }
}

impl<'a> IntoIterator for &'a ManifestReader {
    type Item = &'a Entry;
    type IntoIter = std::slice::Iter<'a, Entry>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::EntryKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn typed_root_preserves_canonical_bytes_across_segments() {
        let extensions: BTreeMap<String, Value> = BTreeMap::from([
            ("x-a".into(), Value::Bool(true)),
            (
                "x-nested".into(),
                Value::Map(vec![
                    (Value::Text("longer".into()), Value::Bytes(vec![42; 1100])),
                    (Value::Text("a".into()), Value::Integer(u64::MAX.into())),
                ]),
            ),
            ("x-very-long-extension-key".into(), Value::Bool(false)),
        ]);
        for external in [false, true] {
            let entry = Entry {
                path: "f".into(),
                kind: EntryKind::File,
                mode: 420,
                uid: 0,
                gid: 0,
                mtime_ns: i64::MIN,
                extents: if external {
                    vec![crate::manifest::Extent {
                        size: 1,
                        source: crate::manifest::Source::Object([7; 32]),
                        encoded_size: 9,
                        offset: 0,
                    }]
                } else {
                    vec![]
                },
                linkname: None,
                attributes: BTreeMap::new(),
            };
            let features = if external {
                vec![Value::Text("external-content".into())]
            } else {
                vec![]
            };
            let mut fields: Vec<_> = extensions
                .iter()
                .map(|(key, value)| (Value::Text(key.clone()), value.clone()))
                .collect();
            fields.extend([
                (Value::Text("version".into()), Value::Integer(1.into())),
                (
                    Value::Text("required_features".into()),
                    Value::Array(features),
                ),
            ]);
            let mut expected = crate::cbor::encode(&Value::Map(fields)).unwrap();
            expected.extend(crate::cbor::encode(&entry.to_value()).unwrap());
            for block_target in [512, 1024, BLOCK_TARGET] {
                let options = ManifestOptions {
                    block_target,
                    ..Default::default()
                };
                let mut writer = ManifestEncoder::new(Vec::new(), 0, 3, options);
                writer.add_entry(entry.clone()).unwrap();
                // The declaration is derived from entries, including removal of stale declarations.
                let (bytes, footer) = writer
                    .finish(vec!["external-content".into(); 2], extensions.clone())
                    .unwrap();
                let mut decoded = Vec::new();
                let mut at = 0;
                while at < footer.toc_encoded_len as usize {
                    assert_eq!(
                        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()),
                        TOC_MAGIC
                    );
                    let len =
                        u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
                    let raw =
                        crate::decode_bounded(&bytes[at + 8..at + 8 + len], METADATA_BLOCK_MAX)
                            .unwrap();
                    assert!(raw.len() <= block_target);
                    decoded.extend(raw);
                    at += 8 + len;
                }
                assert_eq!(decoded, expected);
                assert_eq!(footer.toc_decoded_len, expected.len() as u64);
                assert_eq!(
                    footer.toc_sha256.as_slice(),
                    Sha256::digest(&expected).as_slice()
                );
                let (_, entries) = crate::manifest::decode_document(&decoded, 0).unwrap();
                assert_eq!(entries, vec![entry.clone()]);
            }
        }
    }

    #[test]
    fn concurrent_spools_cannot_overbook_shared_storage() {
        let budget = Arc::new(SpoolBudget::new(4));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|threads| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let options = ManifestOptions {
                        spool_budget: budget.clone(),
                        ..Default::default()
                    };
                    let barrier = barrier.clone();
                    threads.spawn(move || {
                        let mut spool = Spool::new(options);
                        barrier.wait();
                        let accepted = spool.append(b"x").is_ok();
                        // Keep successful reservations live until all attempts finish.
                        barrier.wait();
                        (spool, accepted)
                    })
                })
                .collect();
            let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            assert_eq!(results.iter().filter(|(_, accepted)| *accepted).count(), 4);
            assert_eq!(budget.used(), 4);
            drop(results);
            assert_eq!(budget.used(), 0);
        });
    }
    #[test]
    fn record_quota_rejection_does_not_append_a_length_prefix() {
        let budget = Arc::new(SpoolBudget::new(6));
        let mut spool = Spool::new(ManifestOptions {
            spool_budget: budget.clone(),
            ..Default::default()
        });
        spool.append(b"a").unwrap();
        assert!(matches!(
            spool.append_encoded_record(b"bc"),
            Err(Error::Budget(_))
        ));
        assert_eq!(spool.len(), 1);
        assert_eq!(spool.read(0, 1).unwrap(), b"a");
        assert_eq!(budget.used(), 1);
        spool.append_encoded_record(b"b").unwrap();
        assert_eq!(spool.read(1, 5).unwrap(), b"\x01\x00\x00\x00b");
    }
    #[test]
    fn shared_spool_quota_precedes_store_io_and_is_released_on_drop() {
        let opened = Arc::new(AtomicU64::new(0));
        let calls = opened.clone();
        let budget = Arc::new(SpoolBudget::new(5));
        let options = ManifestOptions {
            memory_bytes: 1,
            allow_disk: false,
            spool_budget: budget.clone(),
            create_spill_store: Some(SpillFactory::new(move || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new(Cursor::new(Vec::<u8>::new())))
            })),
            ..Default::default()
        };
        let mut first = Spool::new(options.clone());
        let mut second = Spool::new(options.clone());
        first.append(b"abc").unwrap();
        second.append(b"de").unwrap();
        assert_eq!(budget.used(), 5);
        assert_eq!(first.read(0, 3).unwrap(), b"abc");
        assert!(first.spilled() && second.spilled());
        let mut third = Spool::new(options);
        assert!(matches!(third.append(b"f"), Err(Error::Budget(_))));
        assert_eq!(opened.load(Ordering::Relaxed), 2);
        assert!(!third.spilled());
        drop(first);
        assert_eq!(budget.used(), 2);
        second.append(b"g").unwrap();
        assert_eq!(second.read(0, 3).unwrap(), b"deg");
        drop(second);
        drop(third);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn spill_creation_failure_preserves_existing_bytes_and_returns_reservation() {
        let budget = Arc::new(SpoolBudget::new(8));
        let mut spool = Spool::new(ManifestOptions {
            memory_bytes: 1,
            spool_budget: budget.clone(),
            create_spill_store: Some(SpillFactory::new(|| {
                Ok(Box::new(Cursor::new(b"nonempty".to_vec())))
            })),
            ..Default::default()
        });
        spool.append(b"a").unwrap();
        assert!(matches!(spool.append(b"bc"), Err(Error::Unsupported(_))));
        assert_eq!(spool.read(0, 1).unwrap(), b"a");
        assert_eq!(budget.used(), 1);
        spool.options.create_spill_store = Some(SpillFactory::new(|| panic!("factory panic")));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| spool.append(b"bc"))).is_err()
        );
        assert_eq!(budget.used(), 1);
        drop(spool);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn partial_write_keeps_a_conservative_charge_until_store_drop() {
        struct Partial(Cursor<Vec<u8>>);
        impl Read for Partial {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                self.0.read(out)
            }
        }
        impl Seek for Partial {
            fn seek(&mut self, at: SeekFrom) -> std::io::Result<u64> {
                self.0.seek(at)
            }
        }
        impl Write for Partial {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.0.get_ref().is_empty() {
                    self.0.write(&bytes[..1])
                } else {
                    Err(std::io::Error::other("store failed after a partial write"))
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let budget = Arc::new(SpoolBudget::new(4));
        let options = ManifestOptions {
            spool_budget: budget.clone(),
            ..Default::default()
        };
        let mut failed =
            Spool::with_store(options.clone(), Box::new(Partial(Cursor::new(vec![])))).unwrap();
        assert!(matches!(failed.append(b"abcd"), Err(Error::Io(_))));
        assert_eq!(failed.len(), 0);
        assert_eq!(budget.used(), 4);
        let mut other = Spool::new(options);
        assert!(matches!(other.append(b"e"), Err(Error::Budget(_))));
        drop(failed);
        other.append(b"e").unwrap();
        assert_eq!(budget.used(), 1);
        drop(other);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn document_lookup_and_cursor_keep_validation_and_ownership() {
        let options = ManifestOptions {
            block_target: 512,
            ..Default::default()
        };
        let mut writer = ManifestEncoder::new(Vec::new(), 1000, 3, options.clone());
        for i in 0..100 {
            writer
                .add_entry(Entry {
                    path: format!("f{i:04}"),
                    kind: EntryKind::File,
                    mode: 420,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: BTreeMap::new(),
                })
                .unwrap();
        }
        let (metadata, footer) = writer.finish(vec![], BTreeMap::new()).unwrap();
        let bytes = [vec![0; 1000], metadata].concat();
        let mut source = crate::SliceReader::new(&bytes);
        let reader = ManifestReader::open(&mut source, &footer, options.clone()).unwrap();
        let mut entry = reader.entry("f0020").unwrap().unwrap();
        entry.mode = 0;
        assert_eq!(reader.entry("f0020").unwrap().unwrap().mode, 420);
        let mut cursor = reader.cursor();
        let mut count = 0;
        while let Some(entry) = reader.next_entry(&mut cursor).unwrap() {
            assert_eq!(entry.path, format!("f{count:04}"));
            count += 1;
        }
        assert_eq!(count, 100);
        let mut selected = reader.cursor_prefix("f002");
        for i in 20..30 {
            assert_eq!(
                reader.next_entry(&mut selected).unwrap().unwrap().path,
                format!("f{i:04}")
            );
        }
        assert!(reader.next_entry(&mut selected).unwrap().is_none());
        assert!(
            reader
                .next_entry(&mut reader.cursor_prefix("missing"))
                .unwrap()
                .is_none()
        );
        for max_entries in [1, 99] {
            let mut limited = options.clone();
            limited.max_entries = max_entries;
            assert!(matches!(
                ManifestReader::open(&mut source, &footer, limited),
                Err(Error::Budget(_))
            ));
        }
        let mut limited = options.clone();
        limited.max_retained_entry_bytes = 1;
        assert!(matches!(
            ManifestReader::open(&mut source, &footer, limited),
            Err(Error::Budget(_))
        ));
        // This cap fits exactly 100 slots and their owned allocations. Geometric
        // vector growth must not either exceed it or prevent a compact fit.
        limited = options.clone();
        limited.max_retained_entry_bytes = (100 * std::mem::size_of::<Entry>()
            + reader.entries.iter().map(entry_allocation).sum::<usize>()
            + reader.extent_offsets.capacity() * std::mem::size_of::<usize>()
            + reader.extent_ends.capacity() * std::mem::size_of::<u64>())
            as u64;
        let compact = ManifestReader::open(&mut source, &footer, limited.clone()).unwrap();
        assert!(compact.retained_lookup_bytes() as u64 <= limited.max_retained_entry_bytes);

        let other = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert!(other.next_entry(&mut cursor).is_err());
    }
    #[test]
    fn external_sort_releases_all_stores_on_success_and_late_namespace_failure() {
        struct Tracked(Cursor<Vec<u8>>, Arc<AtomicU64>);
        impl Read for Tracked {
            fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
                self.0.read(b)
            }
        }
        impl Write for Tracked {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Seek for Tracked {
            fn seek(&mut self, p: SeekFrom) -> std::io::Result<u64> {
                self.0.seek(p)
            }
        }
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.1.fetch_sub(1, Ordering::Relaxed);
            }
        }
        for bad in ["", "duplicate", "ancestor", "quota"] {
            let live = Arc::new(AtomicU64::new(0));
            let calls = live.clone();
            let options = ManifestOptions {
                memory_bytes: 1,
                spool_budget: Arc::new(SpoolBudget::new(if bad == "quota" {
                    400_000
                } else {
                    8 << 20
                })),
                create_spill_store: Some(SpillFactory::new(move || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(Box::new(Tracked(Cursor::new(Vec::new()), calls.clone())))
                })),
                ..Default::default()
            };
            let budget = options.spool_budget.clone();
            let mut writer = ManifestEncoder::new(Vec::new(), 1, 3, options);
            for i in (0..300).rev() {
                writer
                    .add_entry(Entry {
                        path: format!("f{i:04}"),
                        kind: EntryKind::File,
                        mode: 420,
                        uid: 0,
                        gid: 0,
                        mtime_ns: 0,
                        extents: vec![],
                        linkname: None,
                        attributes: BTreeMap::from([(
                            "x-padding".into(),
                            Value::Bytes(vec![0; 1024]),
                        )]),
                    })
                    .unwrap();
            }
            if bad == "duplicate" || bad == "ancestor" {
                writer
                    .add_entry(Entry {
                        path: if bad == "duplicate" {
                            "f0000"
                        } else {
                            "missing/child"
                        }
                        .into(),
                        kind: EntryKind::File,
                        mode: 420,
                        uid: 0,
                        gid: 0,
                        mtime_ns: 0,
                        extents: vec![],
                        linkname: None,
                        attributes: BTreeMap::new(),
                    })
                    .unwrap();
            }
            assert_eq!(
                writer.finish(vec![], BTreeMap::new()).is_ok(),
                bad.is_empty()
            );
            assert_eq!(live.load(Ordering::Relaxed), 0);
            assert_eq!(budget.used(), 0);
        }
    }
    #[test]
    fn document_hash_spill_and_lookup() {
        let options = ManifestOptions {
            memory_bytes: 64,
            create_spill_store: Some(SpillFactory::new(|| {
                Ok(Box::new(Cursor::new(Vec::<u8>::new())))
            })),
            ..Default::default()
        };
        let mut writer = ManifestEncoder::new(Vec::new(), 1000, 3, options.clone());
        for i in (0..300).rev() {
            writer
                .add_entry(Entry {
                    path: format!("file-{i:04}"),
                    kind: EntryKind::File,
                    mode: 420,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: std::collections::BTreeMap::from([(
                        "x-padding".into(),
                        Value::Bytes(vec![0; 4096]),
                    )]),
                })
                .unwrap();
        }
        let (metadata, footer) = writer.finish(vec![], Default::default()).unwrap();
        let mut archive = vec![0; 1000];
        archive.extend(&metadata);
        let mut source = crate::SliceReader::new(&archive);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert!(reader.spilled());
        assert_eq!(
            reader.entry("file-0299").unwrap().unwrap().path,
            "file-0299"
        );
        assert!(reader.entry("file-0300").unwrap().is_none());
        assert_eq!(reader.collect_entries().unwrap().len(), 300);
        reader.validate_metadata().unwrap();
        let mut bad = footer;
        bad.toc_sha256[0] ^= 1;
        assert!(matches!(
            ManifestReader::open(&mut source, &bad, Default::default()),
            Err(Error::Integrity)
        ));
    }
    #[test]
    fn document_larger_than_segment_limit() {
        let options = ManifestOptions {
            memory_bytes: 1 << 20,
            create_spill_store: Some(SpillFactory::new(|| {
                Ok(Box::new(Cursor::new(Vec::<u8>::new())))
            })),
            ..Default::default()
        };
        let mut writer = ManifestEncoder::new(Vec::new(), 1, 3, options.clone());
        for i in 0..3 {
            writer
                .add_entry(Entry {
                    path: format!("file-{i}"),
                    kind: EntryKind::File,
                    mode: 420,
                    uid: 0,
                    gid: 0,
                    mtime_ns: 0,
                    extents: vec![],
                    linkname: None,
                    attributes: BTreeMap::from([(
                        "x-bytes".into(),
                        Value::Bytes(vec![i; 6 << 20]),
                    )]),
                })
                .unwrap();
        }
        let (metadata, footer) = writer.finish(vec![], Default::default()).unwrap();
        assert!(footer.toc_decoded_len > METADATA_BLOCK_MAX as u64);
        let mut archive = vec![0];
        archive.extend(metadata);
        let mut source = crate::SliceReader::new(&archive);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert_eq!(reader.root.entry_count, 3);
        assert_eq!(
            reader.entry("file-2").unwrap().unwrap().attributes["x-bytes"],
            Value::Bytes(vec![2; 6 << 20])
        );
    }
    #[test]
    fn header_item_can_exceed_segment_maximum() {
        let options = ManifestOptions {
            memory_bytes: 32 << 20,
            allow_disk: false,
            ..Default::default()
        };
        let extensions =
            BTreeMap::from([("x-large".into(), Value::Bytes(vec![7; (16 << 20) + 1]))]);
        let (bytes, footer) = ManifestEncoder::new(vec![0], 1, 3, options.clone())
            .finish(vec![], extensions.clone())
            .unwrap();
        let mut source = crate::SliceReader::new(&bytes);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert_eq!(reader.root.extensions, extensions);
        assert_eq!(reader.root.entry_count, 0);
    }
    #[test]
    fn empty_manifest_and_memory_budget() {
        let (metadata, footer) = ManifestEncoder::new(Vec::new(), 1, 3, Default::default())
            .finish(vec![], Default::default())
            .unwrap();
        let mut archive = vec![0];
        archive.extend(metadata);
        let mut source = crate::SliceReader::new(&archive);
        let reader = ManifestReader::open(&mut source, &footer, Default::default()).unwrap();
        assert!(reader.entry("anything").unwrap().is_none());
        assert_eq!(reader.collect_entries().unwrap(), Vec::<Entry>::new());
        let options = ManifestOptions {
            memory_bytes: 1,
            allow_disk: false,
            ..Default::default()
        };
        assert!(matches!(
            ManifestReader::open(&mut source, &footer, options),
            Err(Error::Budget(_))
        ));
    }
}
