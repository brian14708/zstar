//! Synchronous content selection and copy validation shared by both execution modes.
use crate::{Entry, EntryKind, Error, Extent, ReadReport, Result};
use std::collections::BTreeMap;
use std::ops::Range;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Key {
    Archive(u64, u64),
    Object([u8; 32]),
}
impl Key {
    pub const fn of(extent: &Extent) -> Self {
        match extent.source {
            crate::manifest::Source::Archive(at) => Self::Archive(at, extent.encoded_size),
            crate::manifest::Source::Object(hash) => Self::Object(hash),
        }
    }
}
pub struct Copy {
    pub output: usize,
    pub selection: Range<u64>,
    pub source: Range<usize>,
    pub destination: Range<usize>,
}
pub struct Group {
    pub hint: u64,
    pub copies: Vec<Copy>,
}
pub struct Plan {
    pub sizes: Vec<usize>,
    pub groups: BTreeMap<Key, Group>,
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
pub fn build_plan<'a, 'm>(
    requests: impl Iterator<Item = (&'a str, u64, u64)>,
    budget: u64,
    mut lookup: impl FnMut(&str) -> Result<Option<(&'m Entry, &'m [u64])>>,
) -> Result<Plan> {
    let mut plan = Plan {
        sizes: Vec::new(),
        groups: BTreeMap::new(),
    };
    let mut total = 0_u64;
    let mut copies = 0_usize;
    for (output, (path, offset, length)) in requests.enumerate() {
        total = total.checked_add(length).ok_or(crate::Error::budget(
            crate::BudgetResource::ReadBytes,
            "batch output size",
        ))?;
        if total > budget || usize::try_from(total).is_err() {
            return Err(crate::Error::budget_exceeded(
                crate::BudgetResource::ReadBytes,
                "batch output size",
                budget.min(usize::MAX as u64),
                total,
            ));
        }
        let (entry, ends) = lookup(path)?.ok_or_else(|| Error::MissingEntry(path.into()))?;
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        let range = crate::checked_range(offset, length, ends.last().copied().unwrap_or(0))?;
        plan.sizes.push(length as usize);
        if length == 0 {
            continue;
        }
        let first = ends.partition_point(|end| *end <= offset);
        let mut position = if first == 0 { 0 } else { ends[first - 1] };
        for extent in &entry.extents[first..] {
            let next = position.checked_add(extent.size).ok_or(Error::Range)?;
            if position >= range.end {
                break;
            }
            if next > range.start {
                copies += 1;
                if copies > 65_536 {
                    return Err(crate::Error::budget(
                        crate::BudgetResource::BatchCopies,
                        "batch extent copies",
                    ));
                }
                let start = range.start.max(position);
                let end = range.end.min(next);
                let at = extent
                    .offset
                    .checked_add(start - position)
                    .ok_or(Error::Range)?;
                let selection_end = extent.offset.checked_add(extent.size).ok_or(Error::Range)?;
                let group = plan.groups.entry(Key::of(extent)).or_insert_with(|| Group {
                    hint: extent.encoded_size,
                    copies: Vec::new(),
                });
                group.copies.push(Copy {
                    output,
                    selection: extent.offset..selection_end,
                    source: usize::try_from(at).map_err(|_| Error::Range)?
                        ..usize::try_from(at.checked_add(end - start).ok_or(Error::Range)?)
                            .map_err(|_| Error::Range)?,
                    destination: (start - range.start) as usize..(end - range.start) as usize,
                });
            }
            position = next;
        }
    }
    Ok(plan)
}
pub fn validate_copy(copy: &Copy, decoded: usize) -> Result<()> {
    crate::checked_range(
        copy.selection.start,
        copy.selection.end - copy.selection.start,
        decoded as u64,
    )?;
    Ok(())
}
pub fn record(report: Option<&mut ReadReport>, key: &Key, verify: bool) {
    if let Some(report) = report {
        match key {
            Key::Archive(at, _) => report.data_frames_checked.push(*at),
            Key::Object(hash) if verify => report.external_objects_sha256_checked.push(*hash),
            Key::Object(hash) => report.external_objects_unchecked.push(*hash),
        }
    }
}

/// Decode complete content and apply the reader's external verification policy.
pub fn decode(
    key: &Key,
    encoded: &[u8],
    verify: bool,
    limits: crate::Limits,
) -> Result<crate::decoded_body::DecodedBody> {
    let raw = match key {
        Key::Archive(..) => crate::frame::decode_content_with_limits(encoded, true, limits)?,
        Key::Object(_) => crate::frame::decode_object(encoded, limits)?,
    };
    if let Key::Object(hash) = key
        && verify
    {
        crate::verify_sha256(&raw, *hash)?;
    }
    Ok(crate::decoded_body::DecodedBody::compact(raw))
}

/// Owned member metadata and the validated logical extent index.
pub struct Selection {
    pub entry: Entry,
    pub ends: Vec<u64>,
    pub size: u64,
}
impl Selection {
    pub fn new(entry: Entry) -> Result<Self> {
        if entry.kind != EntryKind::File {
            return Err(Error::Unsupported("entry is not a file"));
        }
        entry.validate(u64::MAX)?;
        let size = entry.size()?;
        let mut ends = Vec::new();
        ends.try_reserve_exact(entry.extents.len()).map_err(|_| {
            crate::Error::budget(crate::BudgetResource::IndexBytes, "file cursor index")
        })?;
        let mut position = 0_u64;
        for extent in &entry.extents {
            position = position.checked_add(extent.size).ok_or(Error::Range)?;
            ends.push(position);
        }
        Ok(Self { entry, ends, size })
    }
    pub fn extent_index(&self, offset: u64) -> usize {
        self.ends.partition_point(|end| *end <= offset)
    }
}
