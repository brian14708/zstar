//! Deterministic manifest pages with bounded in-memory metadata buffers.
use crate::manifest::{BLOCK_TARGET, Entry, ManifestRoot, valid_path};
use crate::{Error, Footer, RangeReader, Result, TOC_MAGIC};
use ciborium::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

mod paged;
#[cfg(feature = "async")]
pub use paged::ManifestDirectory;
mod buffer;
pub use buffer::MetadataBuffer;
pub use buffer::sort_records;

const PAGE_DEFAULT: u64 = 8 << 20;
const BUFFER_DEFAULT: u64 = 128 << 20;

pub const fn check_page_size(size: u64, maximum: u64) -> Result<()> {
    if size > maximum {
        return Err(Error::budget_exceeded(
            crate::BudgetResource::MetadataPageBytes,
            "metadata page size",
            maximum,
            size,
        ));
    }
    Ok(())
}

fn invalid(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}

#[derive(Clone, Debug)]
pub struct ManifestOptions {
    pub limits: crate::Limits,
    pub max_page_bytes: u64,
    pub max_buffer_bytes: u64,
    /// Maximum bytes returned by one selective content read.
    pub max_read_bytes: u64,
    /// Estimated retained typed index, including vector capacity (default 1 GiB).
    pub max_retained_entry_bytes: u64,
    /// Separate entry-count limit; zero disables this additional limit.
    pub max_entries: u64,
    /// Optional independently compressed extension-page groups.
    pub extension_pages: BTreeMap<String, Vec<Value>>,
    /// Target decoded bytes per entry page, at least 512 bytes.
    pub block_target: usize,
}
impl Default for ManifestOptions {
    fn default() -> Self {
        Self {
            limits: crate::Limits::default(),
            max_page_bytes: PAGE_DEFAULT,
            max_buffer_bytes: BUFFER_DEFAULT,
            max_read_bytes: 64 << 20,
            max_retained_entry_bytes: 1 << 30,
            max_entries: 0,
            block_target: BLOCK_TARGET,
            extension_pages: BTreeMap::new(),
        }
    }
}

/// In-memory producer of a deterministic manifest sequence.
/// Entries already in path order reuse their buffer; arbitrary input is sorted at finish.
pub struct ManifestEncoder<W: Write> {
    output: W,
    start: u64,
    position: u64,
    decoded: u64,
    records: MetadataBuffer,
    records_ordered: bool,
    previous_path: Option<String>,
    external: bool,
    level: i32,
    compressor: Option<zstd::bulk::Compressor<'static>>,
    pending: Vec<u8>,
    entry_buffer: Vec<u8>,
    options: ManifestOptions,
}
impl<W: Write> ManifestEncoder<W> {
    pub const fn new(
        output: W,
        data_end: u64,
        compression_level: i32,
        options: ManifestOptions,
    ) -> Self {
        Self {
            output,
            start: data_end,
            position: data_end,
            decoded: 0,
            records: MetadataBuffer::new(options.max_buffer_bytes),
            records_ordered: true,
            previous_path: None,
            external: false,
            level: compression_level,
            compressor: None,
            pending: Vec::new(),
            entry_buffer: Vec::new(),
            options,
        }
    }
    pub fn add_entry(&mut self, entry: Entry) -> Result<()> {
        entry.validate(self.start)?;
        // Page map, entries array, entry tuple, and attributes map precede values.
        for value in entry.attributes.values() {
            crate::cbor::encoded_len_at(value, 4)?;
        }
        self.options.limits.entry(&entry)?;
        self.entry_buffer.clear();
        entry.append_encoded(&mut self.entry_buffer)?;
        self.records.append_encoded_record(&self.entry_buffer)?;
        self.external |= entry.is_external();
        if self.records_ordered {
            self.records_ordered = self
                .previous_path
                .as_ref()
                .is_none_or(|path| path.as_bytes() <= entry.path.as_bytes());
            self.previous_path = self.records_ordered.then_some(entry.path);
        }
        Ok(())
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    fn flush_segment(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        check_page_size(self.pending.len() as u64, self.options.max_page_bytes)?;
        crate::Limits::check(
            self.pending.len() as u64,
            self.options.limits.max_frame_decoded_bytes,
            "decoded metadata frame size",
            crate::BudgetResource::DecodedFrameBytes,
        )?;
        self.decoded = self
            .decoded
            .checked_add(self.pending.len() as u64)
            .ok_or_else(|| invalid("decoded metadata overflow"))?;
        if self.compressor.is_none() {
            let mut compressor = zstd::bulk::Compressor::new(self.level)?;
            compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
            self.compressor = Some(compressor);
        }
        let encoded = self.compressor.as_mut().unwrap().compress(&self.pending)?;
        check_page_size(encoded.len() as u64, self.options.max_page_bytes)?;
        self.options.limits.frame(&encoded)?;
        if encoded.len() > u32::MAX as usize {
            return Err(invalid("metadata wrapper length"));
        }
        let size = encoded.len() as u64 + 8;
        let end = self
            .position
            .checked_add(size)
            .ok_or_else(|| invalid("metadata offset overflow"))?;
        self.output.write_all(&TOC_MAGIC.to_le_bytes())?;
        self.output
            .write_all(&(encoded.len() as u32).to_le_bytes())?;
        self.output.write_all(&encoded)?;
        self.position = end;
        self.pending.clear();
        Ok(())
    }
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the ordered validation or protocol operation together."
    )]
    pub fn finish(
        mut self,
        mut requires: Vec<String>,
        extensions: BTreeMap<String, Value>,
    ) -> Result<(W, Footer)> {
        for (group, values) in &self.options.extension_pages {
            if !group.starts_with("x-") || values.is_empty() {
                return Err(invalid("extension page group"));
            }
            for value in values {
                crate::cbor::encode_checked(value)?;
            }
        }
        requires.retain(|s| s != "external-content");
        if self.external {
            requires.push("external-content".into());
        }
        requires.sort();
        requires.dedup();
        let root = ManifestRoot {
            requires,
            entry_count: 0,
            extensions,
        };
        root.validate()?;
        let placeholder = MetadataBuffer::new(self.options.max_buffer_bytes);
        let records = std::mem::replace(&mut self.records, placeholder);
        let records = if self.records_ordered {
            // Keep the namespace validation below even when no sorting is needed.
            records
        } else {
            sort_records(records, |v| {
                let Value::Array(fields) = v else {
                    return Err(invalid("entry tuple"));
                };
                let Some(Value::Text(path)) = fields.first() else {
                    return Err(invalid("entry path"));
                };
                Ok(path.clone())
            })?
        };
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
        let mut pages = Vec::new();
        let mut page_entries = Vec::new();
        let mut raw = Vec::new();
        let mut page_count = 0_u64;
        let mut last_path = String::new();
        at = 0;
        while let Some(value) = records.read_record(&mut at)? {
            raw.clear();
            crate::cbor::append_validated(&mut raw, &value);
            let mut header = Vec::new();
            crate::cbor::append_head(&mut header, 4, page_count + 1);
            if page_count > 0
                && 9 + header.len() + page_entries.len() + raw.len() > self.options.block_target
            {
                pages.push(self.write_entry_page(
                    &page_entries,
                    page_count,
                    std::mem::take(&mut last_path),
                )?);
                page_entries.clear();
                page_count = 0;
            }
            let Value::Array(fields) = value else {
                return Err(invalid("entry tuple"));
            };
            fields[0]
                .as_text()
                .ok_or_else(|| invalid("entry path"))?
                .clone_into(&mut last_path);
            page_entries.extend_from_slice(&raw);
            page_count += 1;
        }
        if page_count > 0 {
            pages.push(self.write_entry_page(&page_entries, page_count, last_path)?);
        }
        let mut fields = vec![
            (Value::Text("version".into()), Value::Integer(1.into())),
            (
                Value::Text("requires".into()),
                Value::Array(root.requires.iter().cloned().map(Value::Text).collect()),
            ),
            (Value::Text("pages".into()), Value::Array(pages)),
        ];
        fields.extend(
            root.extensions
                .into_iter()
                .map(|(k, v)| (Value::Text(k), v)),
        );
        let mut groups: Vec<_> = std::mem::take(&mut self.options.extension_pages)
            .into_iter()
            .collect();
        groups.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        let mut extension_references = Vec::new();
        for (group, values) in groups {
            let mut references = Vec::new();
            for value in values {
                let start = self.position;
                self.pending = crate::manifest::encode_block(&value)?;
                let decoded = self.pending.len() as u64;
                self.flush_segment()?;
                references.push(Value::Array(vec![
                    Value::Integer((start - self.start).into()),
                    Value::Integer((self.position - start).into()),
                    Value::Integer(decoded.into()),
                ]));
            }
            extension_references.push((Value::Text(group), Value::Array(references)));
        }
        if !extension_references.is_empty() {
            fields.push((
                Value::Text("extension-pages".into()),
                Value::Map(extension_references),
            ));
        }
        let directory_start = self.position;
        self.pending = crate::manifest::encode_block(&Value::Map(fields))?;
        let directory_decoded_len = self.pending.len() as u64;
        self.flush_segment()?;
        let footer = Footer {
            toc_encoded_len: self.position - self.start,
            toc_decoded_len: self.decoded,
            directory_encoded_len: self.position - directory_start,
            directory_decoded_len,
        };
        self.output.write_all(&crate::encode_footer(&footer))?;
        Ok((self.output, footer))
    }
    fn write_entry_page(&mut self, entries: &[u8], count: u64, last_path: String) -> Result<Value> {
        let start = self.position;
        self.pending.clear();
        crate::cbor::append_head(&mut self.pending, 5, 1);
        crate::cbor::append_text(&mut self.pending, "entries");
        crate::cbor::append_head(&mut self.pending, 4, count);
        self.pending.extend_from_slice(entries);
        let decoded = self.pending.len() as u64;
        self.flush_segment()?;
        Ok(Value::Array(vec![
            Value::Text(last_path),
            Value::Integer((start - self.start).into()),
            Value::Integer((self.position - start).into()),
            Value::Integer(decoded.into()),
        ]))
    }
}

/// Independent sequential state over the validated entry vector.
/// Cloning a cursor checkpoints its position without retaining entry data.
#[derive(Clone)]
pub struct EntryCursor {
    owner: Arc<()>,
    item: usize,
    end: usize,
}

#[cfg(feature = "async")]
pub enum EntryLookup {
    NeedsWorker,
    Ready(Option<Entry>),
}

pub struct ManifestReader {
    root: ManifestRoot,
    entries: Vec<Entry>,
    extent_offsets: Vec<usize>,
    extent_ends: Vec<u64>,
    cursor_owner: Arc<()>,
}
/// Segments are validated as bytes before the sequence is parsed.
pub struct ManifestIngest {
    footer: Footer,
    start: u64,
    buffer: MetadataBuffer,
    options: ManifestOptions,
    at: u64,
    end: u64,
    decoded: u64,
    pages: Vec<(u64, u64, u64)>,
}
impl ManifestIngest {
    pub(crate) fn new(footer: Footer, source_size: u64, options: ManifestOptions) -> Result<Self> {
        let range = footer.metadata_range(source_size)?;
        Ok(Self {
            start: range.start,
            at: range.start,
            end: range.end,
            footer,
            buffer: MetadataBuffer::new(options.max_buffer_bytes),
            options,
            decoded: 0,
            pages: Vec::new(),
        })
    }
    pub(crate) const fn complete(&self) -> bool {
        self.at == self.end
    }
    pub(crate) const fn position(&self) -> u64 {
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
        if len == 0 || len > self.end - self.at - 8 {
            return Err(invalid("encoded metadata bounds"));
        }
        check_page_size(len, self.options.max_page_bytes)?;
        crate::Limits::check(
            len,
            self.options.limits.max_frame_encoded_bytes,
            "encoded metadata frame size",
            crate::BudgetResource::EncodedFrameBytes,
        )?;
        Ok(len)
    }
    pub(crate) fn push(&mut self, header: &[u8], encoded: &[u8]) -> Result<()> {
        let len = self.encoded_length(header)?;
        if encoded.len() as u64 != len {
            return Err(invalid("short metadata frame"));
        }
        if encoded.len() < 5 || encoded[4] & 4 == 0 {
            return Err(invalid("missing metadata checksum"));
        }
        let limits = self.options.limits;
        let limit = limits
            .decoded_limit()
            .min(usize::try_from(self.options.max_page_bytes).unwrap_or(usize::MAX));
        let raw =
            crate::decode_bounded_with_limits(encoded, limit, limits).map_err(
                |error| match error {
                    Error::Budget(crate::BudgetError {
                        resource: crate::BudgetResource::DecodedFrameBytes,
                        ..
                    }) if limits.max_frame_decoded_bytes == 0
                        || self.options.max_page_bytes <= limits.max_frame_decoded_bytes =>
                    {
                        Error::budget(
                            crate::BudgetResource::MetadataPageBytes,
                            "decoded metadata page size",
                        )
                    }
                    crate::Error::Budget(_) | crate::Error::Integrity => error,
                    other => invalid(&other.to_string()),
                },
            )?;
        if raw.is_empty() {
            return Err(invalid("empty metadata segment"));
        }
        self.decoded = self
            .decoded
            .checked_add(raw.len() as u64)
            .ok_or_else(|| invalid("decoded manifest overflow"))?;
        if self.decoded > self.footer.toc_decoded_len {
            return Err(invalid("decoded manifest length"));
        }
        self.pages
            .push((self.at - self.start, 8 + len, raw.len() as u64));
        self.buffer.append(&raw)?;
        self.at += 8 + len;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<ManifestReader> {
        if self.at != self.end || self.decoded != self.footer.toc_decoded_len {
            return Err(invalid("decoded manifest length"));
        }
        let (root, entries) = paged::decode_metadata(
            &self.buffer,
            &self.pages,
            &self.footer,
            self.start,
            &self.options,
        )?;
        ManifestReader::from_entries(root, entries)
    }
}
impl ManifestReader {
    pub(crate) fn from_entries(root: ManifestRoot, entries: Vec<Entry>) -> Result<Self> {
        let extent_count = entries.iter().try_fold(0_usize, |count, entry| {
            count
                .checked_add(entry.extents.len())
                .ok_or(crate::Error::budget(
                    crate::BudgetResource::IndexBytes,
                    "manifest range index",
                ))
        })?;
        let mut extent_offsets = Vec::new();
        if !entries.is_empty() {
            extent_offsets
                .try_reserve_exact(entries.len().checked_add(1).ok_or(crate::Error::budget(
                    crate::BudgetResource::IndexBytes,
                    "manifest range index",
                ))?)
                .map_err(|_| {
                    crate::Error::budget(crate::BudgetResource::IndexBytes, "manifest range index")
                })?;
        }
        let mut extent_ends = Vec::new();
        extent_ends.try_reserve_exact(extent_count).map_err(|_| {
            crate::Error::budget(crate::BudgetResource::IndexBytes, "manifest range index")
        })?;
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
        Ok(Self {
            root,
            entries,
            extent_offsets,
            extent_ends,
            cursor_owner: Arc::new(()),
        })
    }
}
impl ManifestReader {
    /// Immutable, validated root metadata.
    #[must_use]
    pub const fn root(&self) -> &ManifestRoot {
        &self.root
    }
    /// Borrow immutable entries in path order using standard iterator adapters.
    #[cfg(any(feature = "async", feature = "fs", test))]
    pub fn iter(&self) -> std::slice::Iter<'_, Entry> {
        self.entries.iter()
    }
    pub fn open<R: RangeReader>(
        source: &mut R,
        footer: &Footer,
        options: ManifestOptions,
    ) -> Result<Self> {
        let mut ingest = ManifestIngest::new(footer.clone(), source.len(), options)?;
        while !ingest.complete() {
            let at = ingest.position();
            let header = source.read_range(ingest.header_range()?)?;
            let len = ingest.encoded_length(&header)?;
            let encoded = source.read_range(at + 8..at + 8 + len)?;
            ingest.push(&header, &encoded)?;
        }
        ingest.finish()
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
    pub fn entry(&self, path: &str) -> Option<Entry> {
        self.range_entry(path).map(|(entry, _)| entry.clone())
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
        let selected = self.range_entry(path).map(|(entry, _)| entry);
        if selected.is_some_and(|e| !e.attributes.is_empty() || e.extents.len() > 64) {
            return EntryLookup::NeedsWorker;
        }
        EntryLookup::Ready(selected.cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{EntryKind, entry_allocation};

    #[test]
    fn writer_limits_entry_extension_and_directory_pages() {
        for kind in 0..3 {
            let mut options = ManifestOptions {
                block_target: 512,
                max_page_bytes: 512,
                ..Default::default()
            };
            let padding = Value::Bytes(vec![7; 1024]);
            if kind == 1 {
                options
                    .extension_pages
                    .insert("x-page".into(), vec![padding.clone()]);
            }
            let mut writer = ManifestEncoder::new(vec![0], 1, 3, options);
            if kind == 0 {
                let mut entry = Entry::file("file");
                entry.attributes.insert("x-padding".into(), padding.clone());
                writer.add_entry(entry).unwrap();
            }
            let extensions = if kind == 2 {
                BTreeMap::from([("x-padding".into(), padding)])
            } else {
                BTreeMap::new()
            };
            assert!(matches!(
                writer.finish(vec![], extensions),
                Err(Error::Budget(crate::BudgetError {
                    resource: crate::BudgetResource::MetadataPageBytes,
                    limit: Some(512),
                    ..
                }))
            ));
        }
    }

    #[test]
    fn entry_extensions_are_validated_at_page_depth() {
        for depth in [60, 61] {
            let mut value = Value::Bool(true);
            for _ in 0..depth {
                value = Value::Array(vec![value]);
            }
            let mut entry = Entry::file("file");
            entry.attributes.insert("x-depth".into(), value);
            let options = ManifestOptions::default();
            let mut writer = ManifestEncoder::new(vec![0], 1, 3, options.clone());
            let result = writer.add_entry(entry.clone());
            if depth == 61 {
                assert!(matches!(result, Err(Error::Budget(_))));
                continue;
            }
            result.unwrap();
            let (bytes, footer) = writer.finish(vec![], BTreeMap::new()).unwrap();
            let reader = ManifestReader::open(
                &mut crate::blocking::MemorySource::new(&bytes),
                &footer,
                options,
            )
            .unwrap();
            assert_eq!(reader.iter().as_slice(), &[entry]);
        }
    }

    #[test]
    fn manifest_bytes_match_for_sorted_reverse_and_late_unordered_entries() {
        let entries: Vec<_> = (0..300)
            .map(|i| Entry {
                path: format!("f{i:04}"),
                kind: EntryKind::File,
                mode: 420,
                uid: 0,
                gid: 0,
                mtime_ns: 0,
                extents: vec![],
                linkname: None,
                attributes: BTreeMap::from([("x-padding".into(), Value::Bytes(vec![7; 512]))]),
            })
            .collect();
        let mut expected = None;
        for max_buffer_bytes in [256 << 10, BUFFER_DEFAULT] {
            for order in 0..3 {
                let options = ManifestOptions {
                    max_buffer_bytes,
                    ..Default::default()
                };
                let mut writer = ManifestEncoder::new(vec![0], 1, 3, options);
                let mut input: Vec<_> = entries.iter().collect();
                match order {
                    1 => input.reverse(),
                    2 => input.swap(298, 299),
                    _ => {}
                }
                for entry in input {
                    writer.add_entry(entry.clone()).unwrap();
                }
                let (bytes, _) = writer.finish(vec![], BTreeMap::new()).unwrap();
                if let Some(expected) = &expected {
                    assert_eq!(&bytes, expected);
                } else {
                    expected = Some(bytes);
                }
            }
        }
    }

    #[test]
    fn sorted_manifest_still_rejects_duplicates_and_missing_ancestors() {
        for paths in [["a", "a"], ["a", "missing/child"], ["a", "a/child"]] {
            let mut writer = ManifestEncoder::new(Vec::new(), 1, 3, ManifestOptions::default());
            for path in paths {
                writer
                    .add_entry(Entry {
                        path: path.into(),
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
            assert!(matches!(
                writer.finish(vec![], BTreeMap::new()),
                Err(Error::MalformedToc(_))
            ));
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
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
                let first_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                let raw = crate::decode_bounded(&bytes[8..8 + first_len], usize::MAX).unwrap();
                let page = crate::cbor::decode(&raw).unwrap();
                assert_eq!(
                    page.as_map().unwrap()[0].1.as_array().unwrap(),
                    &[entry.to_value()]
                );
                let directory_start =
                    bytes.len() - crate::FOOTER_SIZE - footer.directory_encoded_len as usize;
                let directory = crate::decode_bounded(
                    &bytes[directory_start + 8..bytes.len() - crate::FOOTER_SIZE],
                    usize::MAX,
                )
                .unwrap();
                assert_eq!(directory.len() as u64, footer.directory_decoded_len);
                assert!(directory.len() > 1024);
                // The directory is one complete value even when larger than the page target.
                assert!(crate::cbor::decode(&directory).is_ok());
            }
        }
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
        let mut source = crate::blocking::MemorySource::new(&bytes);
        let reader = ManifestReader::open(&mut source, &footer, options.clone()).unwrap();
        let mut entry = reader.entry("f0020").unwrap();
        entry.mode = 0;
        assert_eq!(reader.entry("f0020").unwrap().mode, 420);
        let mut cursor = reader.cursor_prefix("");
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
        let compact = ManifestReader::open(&mut source, &footer, limited).unwrap();
        assert_eq!(compact.entries.capacity(), 100);

        let other = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert!(other.next_entry(&mut cursor).is_err());
    }
    #[test]
    fn document_checksum_and_lookup() {
        let options = ManifestOptions::default();
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
        let (metadata, footer) = writer.finish(vec![], BTreeMap::default()).unwrap();
        let mut archive = vec![0; 1000];
        archive.extend(&metadata);
        let mut source = crate::blocking::MemorySource::new(&archive);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert_eq!(reader.entry("file-0299").unwrap().path, "file-0299");
        assert!(reader.entry("file-0300").is_none());
        assert_eq!(reader.iter().len(), 300);
        let mut corrupted = archive;
        let checksum = corrupted.len() - crate::FOOTER_SIZE - 1;
        corrupted[checksum] ^= 1;
        let mut source = crate::blocking::MemorySource::new(&corrupted);
        assert!(matches!(
            ManifestReader::open(&mut source, &footer, ManifestOptions::default()),
            Err(Error::Integrity)
        ));
    }
    #[test]
    fn buffered_document_spans_many_segments() {
        let options = ManifestOptions::default();
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
        let (metadata, footer) = writer.finish(vec![], BTreeMap::default()).unwrap();
        assert!(footer.toc_decoded_len > options.block_target as u64);
        let mut archive = vec![0];
        archive.extend(metadata);
        let mut source = crate::blocking::MemorySource::new(&archive);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert_eq!(reader.root.entry_count, 3);
        assert_eq!(
            reader.entry("file-2").unwrap().attributes["x-bytes"],
            Value::Bytes(vec![2; 6 << 20])
        );
    }
    #[test]
    fn directory_can_exceed_page_target_within_page_limit() {
        let options = ManifestOptions {
            max_page_bytes: 32 << 20,
            max_buffer_bytes: 32 << 20,
            ..Default::default()
        };
        let extensions =
            BTreeMap::from([("x-large".into(), Value::Bytes(vec![7; (16 << 20) + 1]))]);
        let (bytes, footer) = ManifestEncoder::new(vec![0], 1, 3, options.clone())
            .finish(vec![], extensions.clone())
            .unwrap();
        let mut source = crate::blocking::MemorySource::new(&bytes);
        let reader = ManifestReader::open(&mut source, &footer, options).unwrap();
        assert_eq!(reader.root.extensions, extensions);
        assert_eq!(reader.root.entry_count, 0);
    }
    #[test]
    fn empty_manifest_and_memory_budget() {
        let (metadata, footer) = ManifestEncoder::new(Vec::new(), 1, 3, ManifestOptions::default())
            .finish(vec![], BTreeMap::default())
            .unwrap();
        let mut archive = vec![0];
        archive.extend(metadata);
        let mut source = crate::blocking::MemorySource::new(&archive);
        let reader =
            ManifestReader::open(&mut source, &footer, ManifestOptions::default()).unwrap();
        assert!(reader.entry("anything").is_none());
        assert_eq!(reader.iter().len(), 0);
        let options = ManifestOptions {
            max_buffer_bytes: 1,
            ..Default::default()
        };
        assert!(matches!(
            ManifestReader::open(&mut source, &footer, options),
            Err(Error::Budget(_))
        ));
    }
}
