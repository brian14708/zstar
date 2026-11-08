//! Eager metadata loading, selective reads, and internal physical validation.
#[cfg(any(test, feature = "fs"))]
use crate::Limits;
use crate::decoded_body::DecodedBody;
#[cfg(any(test, feature = "fs"))]
use crate::frame::{decode_content_with_limits, fetch_frame_with_limits};
#[cfg(any(test, feature = "fs"))]
use crate::manifest::{Entry, EntryKind, Extent, Source, decode_pointer_bounded};
#[cfg(any(test, feature = "fs"))]
use crate::manifest_io::MetadataBuffer;
use crate::manifest_io::{ManifestOptions, ManifestReader};
use crate::primitives::RangeReader;
use crate::{Error, Result};

use crate::ReadReport;
use crate::blocking::ObjectResolver as EncodedObjectResolver;
use crate::read_plan::{Key, Plan, build_plan, decode, record, validate_copy};

#[cfg(any(test, feature = "fs"))]
fn invalid(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}

pub struct BlockingArchive<R: RangeReader> {
    source: R,
    manifest: ManifestReader,
    #[cfg(any(test, feature = "fs"))]
    data_end: u64,
    options: ManifestOptions,
}
impl<R: RangeReader> BlockingArchive<R> {
    pub const fn manifest(&self) -> &ManifestReader {
        &self.manifest
    }

    /// # Errors
    /// Returns source, footer, metadata validation, or budget errors.
    pub fn open(mut source: R, options: ManifestOptions) -> Result<Self> {
        let len = source.len();
        if len < crate::FOOTER_SIZE as u64 {
            return Err(Error::NotZstar);
        }
        let footer =
            crate::parse_footer(&source.read_range(len - crate::FOOTER_SIZE as u64..len)?)?;
        let manifest = ManifestReader::open(&mut source, &footer, options.clone())?;
        Ok(Self {
            source,
            manifest,
            #[cfg(any(test, feature = "fs"))]
            data_end: footer.metadata_range(len)?.start,
            options,
        })
    }
    pub(crate) fn plan(&self, requests: &[crate::ReadRequest<'_>]) -> Result<Plan> {
        if requests.len() > crate::MAX_BATCH_REQUESTS {
            return Err(crate::Error::budget(
                crate::BudgetResource::BatchRequests,
                "batch request count",
            ));
        }
        build_plan(
            requests.iter().map(|r| (r.path, r.offset, r.length)),
            self.options.max_read_bytes,
            |path| Ok(self.manifest.range_entry(path)),
        )
    }
    pub(crate) fn load(
        &mut self,
        key: &Key,
        hint: u64,
        resolver: &mut Option<&mut dyn EncodedObjectResolver>,
        verify_external: bool,
    ) -> Result<DecodedBody> {
        let encoded = match key {
            Key::Archive(at, size) => {
                let end = at.checked_add(*size).ok_or(Error::Range)?;
                let bytes = self.source.read_range(*at..end)?;
                if bytes.len() as u64 != *size {
                    return Err(Error::MalformedFrame("short archive frame".into()));
                }
                bytes
            }
            Key::Object(hash) => resolver
                .as_mut()
                .ok_or(Error::Unsupported("external read requires resolver"))?
                .get_encoded(*hash, hint, self.options.limits.encoded_limit())?,
        };
        decode(key, &encoded, verify_external, self.options.limits)
    }
    pub(crate) fn execute_plan(
        &mut self,
        plan: Plan,
        mut resolver: Option<&mut dyn EncodedObjectResolver>,
        verify_external: bool,
    ) -> Result<(Vec<Vec<u8>>, ReadReport)> {
        let mut report = ReadReport {
            metadata_validated: true,
            ..Default::default()
        };
        let mut outputs = Vec::new();
        for size in plan.sizes {
            let mut output = Vec::new();
            output.try_reserve_exact(size).map_err(|_| {
                crate::Error::budget(crate::BudgetResource::AllocationBytes, "read allocation")
            })?;
            output.resize(size, 0);
            outputs.push(output);
        }
        for (key, group) in plan.groups {
            let raw = self.load(&key, group.hint, &mut resolver, verify_external)?;
            for copy in &group.copies {
                validate_copy(copy, raw.len())?;
            }
            record(Some(&mut report), &key, verify_external);
            for copy in group.copies {
                outputs[copy.output][copy.destination].copy_from_slice(&raw[copy.source]);
            }
        }
        Ok((outputs, report))
    }
    /// Scan bounded encoded chunks and decode every physical data frame.
    /// # Errors
    /// Returns physical frame, source, budget, or visitor errors.
    #[cfg(test)]
    pub fn visit_frames(
        &mut self,
        mut visitor: impl FnMut(u64, u64, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut at = 0_u64;
        let mut decoded = 0_u64;
        while at < self.data_end {
            let frame =
                fetch_frame_with_limits(&mut self.source, at, self.data_end, self.options.limits)?;
            let raw = decode_content_with_limits(&frame, true, self.options.limits)?;
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
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    #[expect(
        clippy::items_after_statements,
        reason = "Keep local helpers next to the operation that uses them."
    )]
    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "The parsed tar link target corresponds to the manifest linkname field."
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "Keep the ordered validation or protocol operation together."
    )]
    /// Validates and streams physical payloads; output is provisional until success.
    /// External payloads are pointer JSON.
    /// # Errors
    /// Returns physical tar/manifest mismatch, frame, source, budget, or visitor errors.
    #[cfg(any(test, feature = "fs"))]
    pub fn visit_payloads(
        &mut self,
        mut visitor: impl FnMut(&Entry, &[u8]) -> Result<()>,
    ) -> Result<()> {
        struct Member {
            entry: Entry,
            remaining: u64,
            padding: u64,
            pointer: Vec<u8>,
            extents: Vec<Extent>,
        }
        let mut headers = Vec::new();
        let mut member: Option<Member> = None;
        let mut zero_records = 0_u64;
        let mut observed = MetadataBuffer::new(self.options.max_buffer_bytes);
        let mut cursor = self.manifest.cursor_prefix("");
        let mut ordered = true;
        let mut ordered_count = 0_u64;
        let mut encoded_at = 0_u64;
        let mut decoded_at = 0_u64;
        while encoded_at < self.data_end {
            let encoded = fetch_frame_with_limits(
                &mut self.source,
                encoded_at,
                self.data_end,
                self.options.limits,
            )?;
            let raw = decode_content_with_limits(&encoded, true, self.options.limits)?;
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
                            // Physical order is optional. Buffer the already matched
                            // prefix only when the archive needs the general path.
                            let mut prefix = self.manifest.cursor_prefix("");
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
                            .entry(&parsed.path)
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
                    let size = parsed.size.unwrap_or(0);
                    if !entry.is_external() && size != entry.size()? {
                        return Err(Error::TarTocMismatch("inline payload size"));
                    }
                    if entry.is_external() {
                        Limits::check(
                            size,
                            self.options.limits.max_pointer_bytes,
                            "pointer size",
                            crate::BudgetResource::PointerBytes,
                        )?;
                        if size == 0 {
                            return Err(Error::MalformedPointer("empty pointer payload".into()));
                        }
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
                        if decode_pointer_bounded(&current.pointer, current.entry.extents.len())?
                            != current.entry.extents
                        {
                            return Err(Error::TarTocMismatch("pointer JSON/reference"));
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
            return Ok(());
        }
        fn path_key(v: &ciborium::Value) -> Result<String> {
            match v {
                ciborium::Value::Text(s) => Ok(s.clone()),
                _ => Err(invalid("path buffer")),
            }
        }
        let observed = crate::manifest_io::sort_records(observed, path_key)?;
        let mut at = 0;
        for entry in self.manifest.iter() {
            let path = observed
                .read_record(&mut at)?
                .ok_or(Error::TarTocMismatch("missing tar member"))?;
            if path_key(&path)? != entry.path {
                return Err(Error::TarTocMismatch("duplicate or missing tar member"));
            }
        }
        if at != observed.len() {
            return Err(Error::TarTocMismatch("unindexed or duplicate member"));
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "fs"))]
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

#[cfg(test)]
mod tests;
