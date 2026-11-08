//! Shared standard-I/O writer used directly and by the async pipeline.
use crate::manifest::{Entry, EntryKind, Extent, Source, encode_pointer};
use crate::manifest_io::{ManifestEncoder, ManifestOptions, MetadataBuffer};
use crate::{Error, Header, Limits, Result, WriterOptions};
use std::collections::BTreeMap;
use std::io::{Read, Write};

fn invalid(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}
fn uint(v: &ciborium::Value) -> Result<u64> {
    match v {
        ciborium::Value::Integer(n) => u64::try_from(*n).map_err(|_| invalid("buffer integer")),
        _ => Err(invalid("buffer integer")),
    }
}

/// Writes indexed tar bytes to a standard [`Write`] sink.
///
/// Include all ancestor directories. Finish sorts metadata, rejects duplicates,
/// writes the footer, flushes, and returns the sink. Discard output after failure.
pub struct Builder<W: Write> {
    output: W,
    pending: Vec<u8>,
    encoded: Vec<u8>,
    compressor: zstd::bulk::Compressor<'static>,
    frame_target: usize,
    raw_position: u64,
    encoded_position: u64,
    raw_emitted: u64,
    workers: Option<crate::parallel::ChecksummedFrameWorkers>,
    frames: MetadataBuffer,
    records: MetadataBuffer,
    options: ManifestOptions,
    level: i32,
    failed: bool,
    root_extensions: BTreeMap<String, ciborium::Value>,
}
impl<W: Write> Builder<W> {
    #[cfg(feature = "async")]
    pub(crate) const fn output(&self) -> &W {
        &self.output
    }
    /// Initializes a writer with default options.
    ///
    /// # Errors
    /// Returns configuration, compressor, worker, or temporary-storage errors.
    pub fn try_new(output: W) -> Result<Self> {
        Self::try_with_options(output, WriterOptions::default())
    }
    /// Validates all policy before initializing the shared encoding pipeline.
    ///
    /// # Errors
    /// Returns configuration, compressor, worker, or root-extension errors.
    pub fn try_with_options(output: W, policy: WriterOptions) -> Result<Self> {
        policy.validate()?;
        let frame_target = policy.frame_size;
        let level = policy.compression_level;
        let options = policy.manifest_options();
        let root = crate::manifest::ManifestRoot {
            requires: vec![],
            entry_count: 0,
            extensions: policy.root_extensions,
        };
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
            workers: if policy.workers == 0 {
                None
            } else {
                Some(crate::parallel::ChecksummedFrameWorkers::checksummed(
                    policy.workers,
                    level,
                )?)
            },
            frames: MetadataBuffer::new(options.max_buffer_bytes),
            records: MetadataBuffer::new(options.max_buffer_bytes),
            options,
            level,
            failed: false,
            root_extensions: root.extensions,
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
        let encoded = std::mem::take(&mut self.encoded);
        self.write_completed(self.pending.len(), &encoded)?;
        self.encoded = encoded;
        self.pending.clear();
        Ok(())
    }
    fn recycle_buffers(&mut self, mut raw: Vec<u8>, mut encoded: Vec<u8>) {
        raw.clear();
        encoded.clear();
        self.pending = raw;
        self.encoded = encoded;
    }
    fn write_completed(&mut self, raw_size: usize, encoded: &[u8]) -> Result<()> {
        self.options.limits.frame(encoded)?;
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
    /// Rejects a misuse of the public append contract and poisons the writer,
    /// matching every other failed mutation.
    const fn reject(&mut self, reason: &'static str) -> Result<()> {
        self.failed = true;
        Err(Error::Unsupported(reason))
    }
    /// Appends a file with default metadata.
    ///
    /// # Errors
    /// Returns metadata, encoding, budget, or output errors.
    pub fn append_bytes(&mut self, path: impl Into<String>, bytes: &[u8]) -> Result<()> {
        self.append_data(Header::file(path, bytes.len() as u64), bytes)
    }
    /// Appends a directory with default metadata.
    ///
    /// # Errors
    /// Returns metadata, budget, or output errors.
    pub fn append_dir(&mut self, path: impl Into<String>) -> Result<()> {
        self.append_header(Header::directory(path))
    }
    /// Appends a symlink with an exact target string.
    ///
    /// # Errors
    /// Returns metadata, budget, or output errors.
    pub fn append_symlink(
        &mut self,
        path: impl Into<String>,
        target: impl Into<String>,
    ) -> Result<()> {
        self.append_header(Header::symlink(path, target))
    }
    /// Appends a regular file whose payload is read from `input`.
    ///
    /// # Errors
    /// Returns header, input, output, format, or resource errors; failures poison the writer.
    pub fn append_data(&mut self, header: Header, mut input: impl Read) -> Result<()> {
        if header.entry.kind != EntryKind::File {
            return self.reject("append_data requires a file header");
        }
        self.add_file(header.entry, header.size, &mut input)
    }
    /// Appends a directory, symlink, or empty-file header.
    ///
    /// # Errors
    /// Returns an error for a nonempty file or invalid metadata/output; failures poison the writer.
    pub fn append_header(&mut self, header: Header) -> Result<()> {
        if header.size != 0 {
            return self.reject("nonempty file requires append_data");
        }
        self.add_metadata(header.entry)
    }
    /// Appends references to already stored external objects. An externalized
    /// empty file has no objects and is recorded as an empty regular file.
    ///
    /// # Errors
    /// Returns an error for inline metadata or invalid extents/output; failures poison the writer.
    pub fn append_external(&mut self, header: Header, extents: Vec<crate::Extent>) -> Result<()> {
        let entry = match header.external_entry(extents) {
            Ok(entry) => entry,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        let empty = entry.kind == EntryKind::File && entry.extents.is_empty();
        if !entry.is_external() && !empty {
            return self.reject("external entry required");
        }
        self.add_metadata(entry)
    }
    /// # Errors
    /// Returns input, output, format, or resource errors; mutation failures poison the writer.
    pub(crate) fn add_file(
        &mut self,
        entry: Entry,
        size: u64,
        payload: &mut impl Read,
    ) -> Result<()> {
        self.mutate(|writer| {
            if entry.kind != EntryKind::File || !entry.extents.is_empty() {
                return Err(Error::Unsupported(
                    "inline capture requires file metadata without source extents",
                ));
            }
            writer.add(entry, size, Some(payload))
        })
    }
    /// # Errors
    /// Returns metadata, encoding, output, or budget errors.
    pub(crate) fn add_metadata(&mut self, entry: Entry) -> Result<()> {
        self.mutate(|writer| {
            if entry.kind == EntryKind::File && !entry.is_external() && !entry.extents.is_empty() {
                return Err(Error::Unsupported("inline file requires a payload reader"));
            }
            if entry.is_external() {
                let text = encode_pointer(&entry.extents)?;
                Limits::check(
                    text.len() as u64,
                    writer.options.limits.max_pointer_bytes,
                    "pointer size",
                    crate::BudgetResource::PointerBytes,
                )?;
                writer.add(entry, text.len() as u64, Some(&mut text.as_slice()))
            } else {
                writer.add(entry, 0, None)
            }
        })
    }
    fn mutate(&mut self, operation: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.failed {
            return Err(Error::Unsupported("archive writer failed"));
        }
        let result = operation(self);
        self.failed = result.is_err();
        result
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Ownership matches the public API or asynchronous boundary."
    )]
    fn add(&mut self, entry: Entry, size: u64, mut payload: Option<&mut dyn Read>) -> Result<()> {
        // Archive positions are resolved after compression; validate caller metadata separately.
        entry.validate(u64::MAX)?;
        self.options.limits.entry(&entry)?;
        if entry.kind != EntryKind::File && size != 0 {
            return Err(invalid("nonfile payload"));
        }
        let tar_path = if entry.kind == EntryKind::Directory {
            format!("{}/", entry.path)
        } else {
            entry.path.clone()
        };
        let header = crate::tar_encode::member_header_full(
            &tar_path,
            size,
            entry.mode,
            match entry.kind {
                EntryKind::File => b'0',
                EntryKind::Directory => b'5',
                EntryKind::Symlink => b'2',
            },
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
        if entry.kind == EntryKind::File && !entry.is_external() && size != 0 {
            // Preflight minimum extent metadata before reading the payload.
            let pending = if total > (self.frame_target - self.pending.len()) as u64 {
                0
            } else {
                self.pending.len()
            };
            let offset = (pending + header.len()) % self.frame_target;
            self.options
                .limits
                .inline(size, self.frame_target, offset, entry.encoded_len()?)?;
        }
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
    fn frame_record(frames: &MetadataBuffer, index: u64) -> Result<[u64; 4]> {
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
    /// Writes metadata and footer, flushes, and returns the sink.
    ///
    /// # Errors
    /// Returns output, metadata validation, buffer, or budget errors.
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
        let limits = self.options.limits;
        let mut metadata =
            ManifestEncoder::new(self.output, self.encoded_position, self.level, self.options);
        let mut at = 0;
        let frame_count = self.frames.len() / 32;
        let mut frame_index = 0;
        let mut current_frame = None;
        while let Some(value) = self.records.read_record(&mut at)? {
            let ciborium::Value::Array(values) = value else {
                return Err(invalid("entry buffer"));
            };
            let mut entry = Entry::from_value(&values[0], self.encoded_position)?;
            let start = uint(&values[1])?;
            let size = uint(&values[2])?;
            if entry.kind == EntryKind::File && !entry.is_external() && size != 0 {
                let end = start
                    .checked_add(size)
                    .ok_or_else(|| invalid("file length overflow"))?;
                let mut extent_bytes = 0;
                let base_bytes = entry.encoded_len()? - 1;
                // Capture records and frame records share physical order. Keep
                // the current frame across files, avoiding per-file binary
                // searches and repeated reads for shared small-file frames.
                while frame_index < frame_count {
                    let [raw, frame_size, encoded, encoded_size] = match current_frame {
                        Some(frame) => frame,
                        None => Self::frame_record(&self.frames, frame_index)?,
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
                        let extent = Extent {
                            size: b - a,
                            source: Source::Archive(encoded),
                            encoded_size,
                            offset: a - raw,
                        };
                        extent_bytes += extent.encoded_len();
                        entry.extents.push(extent);
                    }
                    Limits::check(
                        entry.extents.len() as u64,
                        limits.max_extents,
                        "entry extent count",
                        crate::BudgetResource::Extents,
                    )?;
                    Limits::check(
                        (base_bytes
                            + crate::cbor::head_len(entry.extents.len() as u64)
                            + extent_bytes) as u64,
                        limits.max_entry_bytes,
                        "entry metadata size",
                        crate::BudgetResource::EntryMetadataBytes,
                    )?;
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
        let (mut output, _) = metadata.finish(vec![], self.root_extensions)?;
        output.flush()?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod regression;
