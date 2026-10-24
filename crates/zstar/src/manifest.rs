//! Entry-oriented draft-v1 metadata. Wire tuples are deliberately separate from
//! the typed API: callers never need to manipulate positional CBOR values.
use crate::{Error, Result};
use ciborium::Value;
use std::collections::BTreeMap;

pub const METADATA_BLOCK_MAX: usize = 16 << 20;
pub const ENTRY_MAX: usize = 8 << 20;
pub const EXTENTS_MAX: usize = 65_536;
/// Target decoded bytes per independently compressed manifest segment.
pub const BLOCK_TARGET: usize = 64 << 10;
pub const ENCODED_FRAME_MAX: u64 = 17 << 20;

fn malformed(reason: &str) -> Error {
    Error::MalformedToc(reason.into())
}
fn number(n: u64) -> Value {
    Value::Integer(n.into())
}
fn text(s: &str) -> Value {
    Value::Text(s.into())
}
fn array(value: &Value) -> Result<&[Value]> {
    match value {
        Value::Array(v) => Ok(v),
        _ => Err(malformed("expected tuple")),
    }
}
fn unsigned(value: &Value) -> Result<u64> {
    match value {
        Value::Integer(n) => u64::try_from(*n).map_err(|_| malformed("uint64")),
        _ => Err(malformed("uint64")),
    }
}
fn string(value: &Value) -> Result<&str> {
    match value {
        Value::Text(s) => Ok(s),
        _ => Err(malformed("text")),
    }
}
fn map(value: &Value) -> Result<&[(Value, Value)]> {
    match value {
        Value::Map(m) => Ok(m),
        _ => Err(malformed("map")),
    }
}
fn field<'a>(m: &'a [(Value, Value)], key: &str) -> Result<&'a Value> {
    m.iter()
        .find_map(|(k, v)| matches!(k, Value::Text(name) if name == key).then_some(v))
        .ok_or_else(|| malformed(&format!("missing {key}")))
}

/// Codec-independent primitives used by native and WebAssembly SDKs.
pub fn decode_block(bytes: &[u8]) -> Result<Value> {
    crate::cbor::decode(bytes)
}
pub fn encode_block(value: &Value) -> Result<Vec<u8>> {
    crate::cbor::encode_checked(value)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    Archive(u64),
    Object([u8; 32]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extent {
    pub size: u64,
    pub source: Source,
    /// Exact for archive frames; only a hint for recompressible objects.
    pub encoded_size: u64,
    pub offset: u64,
}

#[must_use]
pub fn object_address(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("sha256:");
    for byte in digest {
        write!(out, "{byte:02x}").expect("String write");
    }
    out
}
pub fn parse_object_address(s: &str) -> Result<[u8; 32]> {
    let hash = s
        .strip_prefix("sha256:")
        .ok_or_else(|| malformed("object address"))?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(malformed("object address"));
    }
    let mut out = [0; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16)
            .map_err(|_| malformed("object address"))?;
    }
    Ok(out)
}
impl Extent {
    pub fn validate(&self, data_end: u64) -> Result<()> {
        if self.size == 0 || self.size > crate::DATA_FRAME_MAX as u64 || self.encoded_size == 0 {
            return Err(malformed("extent size"));
        }
        if self
            .offset
            .checked_add(self.size)
            .is_none_or(|end| end > crate::DATA_FRAME_MAX as u64)
        {
            return Err(malformed("extent selection"));
        }
        if let Source::Archive(start) = self.source
            && (self.encoded_size > ENCODED_FRAME_MAX
                || start
                    .checked_add(self.encoded_size)
                    .is_none_or(|end| end > data_end))
        {
            return Err(malformed("archive extent bounds"));
        }
        Ok(())
    }
    #[must_use]
    pub fn to_value(&self) -> Value {
        let source = match &self.source {
            Source::Archive(n) => number(*n),
            Source::Object(hash) => text(&object_address(hash)),
        };
        let mut values = vec![number(self.size), source, number(self.encoded_size)];
        if self.offset != 0 {
            values.push(number(self.offset));
        }
        Value::Array(values)
    }
    pub fn from_value(value: &Value, data_end: u64) -> Result<Self> {
        let v = array(value)?;
        if !(3..=4).contains(&v.len()) {
            return Err(malformed("extent tuple length"));
        }
        let source = match &v[1] {
            Value::Integer(_) => Source::Archive(unsigned(&v[1])?),
            Value::Text(s) => Source::Object(parse_object_address(s)?),
            _ => return Err(malformed("extent source")),
        };
        let offset = if v.len() == 4 {
            let n = unsigned(&v[3])?;
            if n == 0 {
                return Err(malformed("explicit zero extent offset"));
            }
            n
        } else {
            0
        };
        let extent = Self {
            size: unsigned(&v[0])?,
            source,
            encoded_size: unsigned(&v[2])?,
            offset,
        };
        extent.validate(data_end)?;
        Ok(extent)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Symlink,
    Directory,
}
impl EntryKind {
    #[must_use]
    pub fn code(self) -> u64 {
        match self {
            Self::File => 0,
            Self::Symlink => 2,
            Self::Directory => 5,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub path: String,
    pub kind: EntryKind,
    pub mode: u64,
    pub uid: u64,
    pub gid: u64,
    pub mtime_ns: i64,
    pub extents: Vec<Extent>,
    pub linkname: Option<String>,
    pub attributes: BTreeMap<String, Value>,
}
impl Entry {
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: EntryKind::File,
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime_ns: 0,
            extents: Vec::new(),
            linkname: None,
            attributes: BTreeMap::new(),
        }
    }
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            kind: EntryKind::Directory,
            mode: 0o755,
            ..Self::file(path)
        }
    }
    pub fn symlink(path: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            kind: EntryKind::Symlink,
            mode: 0o777,
            linkname: Some(target.into()),
            ..Self::file(path)
        }
    }

    pub fn size(&self) -> Result<u64> {
        self.extents.iter().try_fold(0_u64, |sum, e| {
            sum.checked_add(e.size)
                .ok_or_else(|| malformed("file size overflow"))
        })
    }
    #[must_use]
    pub fn is_external(&self) -> bool {
        self.extents
            .first()
            .is_some_and(|e| matches!(e.source, Source::Object(_)))
    }
    pub fn validate(&self, data_end: u64) -> Result<()> {
        if !valid_path(&self.path) || self.mode > 4095 || self.extents.len() > EXTENTS_MAX {
            return Err(malformed("entry metadata"));
        }
        if (self.kind == EntryKind::Symlink) != self.linkname.is_some() {
            return Err(malformed("inapplicable linkname"));
        }
        if let Some(target) = &self.linkname
            && (target.is_empty() || target.len() > 4096 || target.contains('\0'))
        {
            return Err(malformed("linkname"));
        }
        if self.kind != EntryKind::File && !self.extents.is_empty() {
            return Err(malformed("nonfile extents"));
        }
        for extent in &self.extents {
            extent.validate(data_end)?;
            if matches!(extent.source, Source::Object(_)) != self.is_external() {
                return Err(malformed("mixed sources"));
            }
        }
        self.size()?;
        for name in self.attributes.keys() {
            if !name.starts_with("x-") {
                return Err(malformed("unknown attribute"));
            }
        }
        if self.encoded_len()? > ENTRY_MAX {
            return Err(malformed("entry size maximum"));
        }
        Ok(())
    }
    /// Exact wire length without allocating an encoded tuple or object address.
    pub(crate) fn encoded_len(&self) -> Result<usize> {
        use crate::cbor::{encoded_len_at, head_len};
        let text_len = |s: &str| head_len(s.len() as u64) + s.len();
        let mut len = 1 + text_len(&self.path);
        for n in [self.kind.code(), self.mode, self.uid, self.gid] {
            len += head_len(n);
        }
        len += head_len(if self.mtime_ns >= 0 {
            self.mtime_ns as u64
        } else {
            (-1 - i128::from(self.mtime_ns)) as u64
        });
        len += head_len(self.extents.len() as u64);
        for extent in &self.extents {
            len += 1 + head_len(extent.size) + head_len(extent.encoded_size);
            len += match extent.source {
                Source::Archive(at) => head_len(at),
                Source::Object(_) => 2 + 71,
            };
            if extent.offset != 0 {
                len += head_len(extent.offset);
            }
        }
        if !self.attributes.is_empty() || self.linkname.is_some() {
            len += head_len((self.attributes.len() + usize::from(self.linkname.is_some())) as u64);
            if let Some(target) = &self.linkname {
                len += text_len("linkname") + text_len(target);
            }
            for (name, value) in &self.attributes {
                let value_len = encoded_len_at(value, 2)?;
                len = len
                    .checked_add(text_len(name))
                    .and_then(|n| n.checked_add(value_len))
                    .ok_or_else(|| malformed("entry size maximum"))?;
            }
        }
        Ok(len)
    }
    /// Serialize the entry tuple directly, borrowing attributes and extents.
    #[cfg(test)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.append_encoded(&mut out)?;
        Ok(out)
    }
    pub(crate) fn append_encoded(&self, out: &mut Vec<u8>) -> Result<()> {
        use crate::cbor::{append_head, append_text, append_validated};
        let attrs = !self.attributes.is_empty() || self.linkname.is_some();
        append_head(out, 4, if attrs { 8 } else { 7 });
        append_text(out, &self.path);
        for n in [self.kind.code(), self.mode, self.uid, self.gid] {
            append_head(out, 0, n);
        }
        if self.mtime_ns >= 0 {
            append_head(out, 0, self.mtime_ns as u64);
        } else {
            append_head(out, 1, (-1 - i128::from(self.mtime_ns)) as u64);
        }
        append_head(out, 4, self.extents.len() as u64);
        for extent in &self.extents {
            append_head(out, 4, if extent.offset == 0 { 3 } else { 4 });
            append_head(out, 0, extent.size);
            match &extent.source {
                Source::Archive(at) => append_head(out, 0, *at),
                Source::Object(hash) => append_text(out, &object_address(hash)),
            }
            append_head(out, 0, extent.encoded_size);
            if extent.offset != 0 {
                append_head(out, 0, extent.offset);
            }
        }
        if attrs {
            let linkname = self.linkname.as_ref().map(|s| Value::Text(s.clone()));
            let mut pairs: Vec<(&str, &Value)> = self
                .attributes
                .iter()
                .map(|(k, v)| (k.as_str(), v))
                .collect();
            if let Some(value) = &linkname {
                pairs.push(("linkname", value));
            }
            pairs.sort_unstable_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(b.0)));
            append_head(out, 5, pairs.len() as u64);
            for (key, value) in pairs {
                crate::cbor::encoded_len_at(value, 2)?;
                append_text(out, key);
                append_validated(out, value);
            }
        }
        Ok(())
    }
    pub fn to_value(&self) -> Value {
        let mut values = vec![
            text(&self.path),
            number(self.kind.code()),
            number(self.mode),
            number(self.uid),
            number(self.gid),
            Value::Integer(self.mtime_ns.into()),
            Value::Array(self.extents.iter().map(Extent::to_value).collect()),
        ];
        let mut attributes: Vec<(Value, Value)> = self
            .attributes
            .iter()
            .map(|(k, v)| (text(k), v.clone()))
            .collect();
        if let Some(target) = &self.linkname {
            attributes.push((text("linkname"), text(target)));
        }
        if !attributes.is_empty() {
            values.push(Value::Map(attributes));
        }
        Value::Array(values)
    }
    pub fn from_value(value: &Value, data_end: u64) -> Result<Self> {
        let v = array(value)?;
        if !(7..=8).contains(&v.len()) {
            return Err(malformed("entry tuple length"));
        }
        let kind = match unsigned(&v[1])? {
            0 => EntryKind::File,
            2 => EntryKind::Symlink,
            5 => EntryKind::Directory,
            _ => return Err(malformed("entry type")),
        };
        let mtime_ns = match v[5] {
            Value::Integer(n) => i64::try_from(n).map_err(|_| malformed("mtime_ns"))?,
            _ => return Err(malformed("mtime_ns")),
        };
        let values = array(&v[6])?;
        if values.len() > EXTENTS_MAX {
            return Err(malformed("extent count"));
        }
        let mut attributes = BTreeMap::new();
        let mut linkname = None;
        if v.len() == 8 {
            let m = map(&v[7])?;
            if m.is_empty() {
                return Err(malformed("explicit empty attributes"));
            }
            for (k, value) in m {
                let name = string(k)?;
                if name == "linkname" {
                    linkname = Some(string(value)?.into());
                } else if name.starts_with("x-") {
                    attributes.insert(name.into(), value.clone());
                } else {
                    return Err(malformed("unknown attribute"));
                }
            }
        }
        let entry = Self {
            path: string(&v[0])?.into(),
            kind,
            mode: unsigned(&v[2])?,
            uid: unsigned(&v[3])?,
            gid: unsigned(&v[4])?,
            mtime_ns,
            extents: values
                .iter()
                .map(|v| Extent::from_value(v, data_end))
                .collect::<Result<_>>()?,
            linkname,
            attributes,
        };
        entry.validate(data_end)?;
        Ok(entry)
    }
}

#[must_use]
pub fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 4096
        && !path.contains('\0')
        && path
            .split('/')
            .all(|c| !c.is_empty() && c != "." && c != ".." && c.len() <= 255)
}

/// Sequence header metadata. Entry count is derived, never stored on the wire.
#[derive(Clone, Debug, PartialEq)]
pub struct ManifestRoot {
    pub required_features: Vec<String>,
    pub entry_count: u64,
    pub extensions: BTreeMap<String, Value>,
}
impl ManifestRoot {
    pub(crate) fn validate(&self) -> Result<()> {
        for (i, name) in self.required_features.iter().enumerate() {
            if !valid_feature(name) || i > 0 && self.required_features[i - 1] >= *name {
                return Err(malformed("required feature syntax or order"));
            }
            if name != "external-content" {
                return Err(Error::UnsupportedFeature);
            }
        }
        for (key, value) in &self.extensions {
            if !key.starts_with("x-") {
                return Err(malformed("unknown manifest field"));
            }
            crate::cbor::encoded_len_at(value, 1)?;
        }
        Ok(())
    }
}

/// Decode and validate a complete manifest sequence. Storage segment
/// boundaries are deliberately absent from this codec-independent operation.
pub fn decode_document(bytes: &[u8], data_end: u64) -> Result<(ManifestRoot, Vec<Entry>)> {
    decode_sequence(bytes, bytes.len() as u64, data_end)
}

pub(crate) fn decode_sequence(
    reader: impl std::io::Read,
    len: u64,
    data_end: u64,
) -> Result<(ManifestRoot, Vec<Entry>)> {
    decode_sequence_budgeted(reader, len, data_end, RETAINED_ENTRIES_MAX, 0)
}

/// Header item cap: the format does not bound root extensions, and a header
/// item may span several metadata segments (see `manifest_io` tests), so this is
/// a deliberate local budget of four segments, far above `ENTRY_MAX`.
pub(crate) const HEADER_MAX: u64 = 4 * METADATA_BLOCK_MAX as u64;
/// Default ceiling on estimated heap retained by the decoded entry vector.
pub(crate) const RETAINED_ENTRIES_MAX: u64 = 4 << 30;

/// True when `entry` (sorted after directory `dir`) can still be inside or
/// adjacent to `dir`: it starts with `dir/` or sorts before `dir/` bytewise.
/// Allocation-free equivalent of the former `format!("{dir}/")` comparisons.
pub(crate) fn ancestor_retained(dir: &str, entry: &str) -> bool {
    let (d, e) = (dir.as_bytes(), entry.as_bytes());
    let common = d.len().min(e.len());
    match d[..common].cmp(&e[..common]) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => e.len() <= d.len() || e[d.len()] <= b'/',
    }
}

pub(crate) fn decode_sequence_budgeted(
    reader: impl std::io::Read,
    len: u64,
    data_end: u64,
    max_retained: u64,
    max_entries: u64,
) -> Result<(ManifestRoot, Vec<Entry>)> {
    let mut sequence = crate::cbor::SequenceDecoder::<_>::new(reader, len)
        .with_item_limits(HEADER_MAX, ENTRY_MAX as u64);
    let value = sequence
        .next()?
        .ok_or_else(|| malformed("missing manifest header"))?;
    let m = map(&value)?;
    if unsigned(field(m, "version")?)? != 1 {
        return Err(Error::UnsupportedVersion("manifest document"));
    }
    let required_features = array(field(m, "required_features")?)?
        .iter()
        .map(|v| string(v).map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    let mut root = ManifestRoot {
        required_features,
        entry_count: 0,
        extensions: BTreeMap::new(),
    };
    for (key, value) in m {
        let name = string(key)?;
        if name.starts_with("x-") {
            root.extensions.insert(name.into(), value.clone());
        } else if !["version", "required_features"].contains(&name) {
            return Err(malformed("unknown manifest field"));
        }
    }
    root.validate()?;
    let mut entries = Vec::new();
    // Sorted stack of directories that may still be ancestors. Entries arrive
    // sorted, so only the top can become stale: amortized O(1) per entry.
    let mut ancestors: Vec<String> = Vec::new();
    // Reader range indexes use one offset per entry, a terminal offset, and
    // one cumulative logical end per extent. Charge before allocating them.
    let mut retained = 0_u64;
    let mut external = false;
    while let Some(value) = sequence.next()? {
        if max_entries != 0 && entries.len() as u64 >= max_entries {
            return Err(Error::Budget("manifest entry count"));
        }
        let entry = Entry::from_value(&value, data_end)?;
        if entries
            .last()
            .is_some_and(|prev: &Entry| prev.path.as_bytes() >= entry.path.as_bytes())
        {
            return Err(malformed("entry order"));
        }
        while ancestors
            .last()
            .is_some_and(|dir| !ancestor_retained(dir, &entry.path))
        {
            ancestors.pop();
        }
        if let Some((parent, _)) = entry.path.rsplit_once('/')
            && ancestors
                .binary_search_by(|dir| dir.as_bytes().cmp(parent.as_bytes()))
                .is_err()
        {
            return Err(malformed("missing ancestor directory"));
        }
        if entry.kind == EntryKind::Directory {
            ancestors.push(entry.path.clone());
        }
        if entries.is_empty() {
            retained = retained.saturating_add(std::mem::size_of::<usize>() as u64);
        }
        retained = retained
            .saturating_add(entry_allocation(&entry) as u64)
            .saturating_add((entry.extents.len() * std::mem::size_of::<u64>()) as u64);
        let entry_bytes = (std::mem::size_of::<Entry>() + std::mem::size_of::<usize>()) as u64;
        let slots = max_retained.saturating_sub(retained) / entry_bytes;
        if retained > max_retained || slots <= entries.len() as u64 {
            return Err(Error::Budget("retained manifest entries"));
        }
        if entries.capacity() as u64 > slots {
            entries.shrink_to(entries.len());
        }
        if entries.len() == entries.capacity() {
            // Leave half the available spare-slot budget for future owned
            // paths/attributes, avoiding repeated compaction near a tight cap.
            let minimum = entries.len() as u64 + 1;
            let capacity_limit = minimum + (slots - minimum) / 2;
            let desired = (entries.capacity().max(8) as u64)
                .saturating_mul(2)
                .min(capacity_limit)
                .min(if max_entries == 0 {
                    u64::MAX
                } else {
                    max_entries
                });
            let desired = usize::try_from(desired)
                .map_err(|_| Error::Budget("manifest index address space"))?;
            entries
                .try_reserve_exact(desired - entries.len())
                .map_err(|_| Error::Budget("retained manifest allocation"))?;
        }
        if entries.capacity() as u64 > slots {
            return Err(Error::Budget("retained manifest entries"));
        }
        external |= entry.is_external();
        entries.push(entry);
    }
    if external
        != root
            .required_features
            .iter()
            .any(|s| s == "external-content")
    {
        return Err(malformed("external feature declaration"));
    }
    root.entry_count = entries.len() as u64;
    Ok((root, entries))
}

fn valid_feature(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 || !name.as_bytes()[0].is_ascii_lowercase() {
        return false;
    }
    let mut separator = false;
    for b in name.bytes() {
        if b == b'.' || b == b'-' {
            if separator {
                return false;
            }
            separator = true;
        } else {
            if !b.is_ascii_lowercase() && !b.is_ascii_digit() {
                return false;
            }
            separator = false;
        }
    }
    !separator
}

/// Serialize the portable external reference. No whole-file identity is stored.
pub fn encode_external_reference(extents: &[Extent]) -> Result<Vec<u8>> {
    if extents.is_empty()
        || extents.len() > EXTENTS_MAX
        || extents
            .iter()
            .any(|e| !matches!(e.source, Source::Object(_)))
    {
        return Err(malformed("external extents"));
    }
    for e in extents {
        e.validate(0)?;
    }
    crate::cbor::encode_checked(&Value::Map(vec![
        (text("version"), number(1)),
        (
            text("extents"),
            Value::Array(extents.iter().map(Extent::to_value).collect()),
        ),
    ]))
}

pub fn decode_external_reference(bytes: &[u8]) -> Result<Vec<Extent>> {
    let value = crate::cbor::decode(bytes)?;
    let m = map(&value)?;
    if m.len() != 2 || unsigned(field(m, "version")?)? != 1 {
        return Err(malformed("external reference schema"));
    }
    let extents = array(field(m, "extents")?)?
        .iter()
        .map(|v| Extent::from_value(v, 0))
        .collect::<Result<Vec<_>>>()?;
    encode_external_reference(&extents)?;
    Ok(extents)
}

pub fn encode_pointer(extents: &[Extent]) -> Result<Vec<u8>> {
    use std::fmt::Write as _;
    encode_external_reference(extents)?;
    let mut out = String::from("version zstar-pointer-v1\n");
    for e in extents {
        let Source::Object(hash) = &e.source else {
            return Err(malformed("pointer source"));
        };
        write!(
            out,
            "chunk {} {} {}",
            object_address(hash),
            e.size,
            e.encoded_size
        )
        .expect("String write");
        if e.offset != 0 {
            write!(out, " {}", e.offset).expect("String write");
        }
        out.push('\n');
    }
    if out.len() > 16 << 20 {
        return Err(Error::MalformedPointer("pointer size maximum".into()));
    }
    Ok(out.into_bytes())
}

pub fn decode_pointer(bytes: &[u8]) -> Result<Vec<Extent>> {
    let parse = || -> Result<Vec<Extent>> {
        if bytes.len() > 16 << 20 {
            return Err(malformed("pointer size maximum"));
        }
        let text = std::str::from_utf8(bytes).map_err(|_| malformed("pointer UTF-8"))?;
        let text = text
            .strip_suffix('\n')
            .ok_or_else(|| malformed("pointer final LF"))?;
        let mut lines = text.split('\n');
        if lines.next() != Some("version zstar-pointer-v1") {
            return Err(malformed("pointer version"));
        }
        let mut extents = Vec::new();
        for line in lines {
            let fields: Vec<_> = line.split(' ').collect();
            if !(4..=5).contains(&fields.len()) || fields[0] != "chunk" {
                return Err(malformed("pointer line"));
            }
            let decimal = |s: &str| -> Result<u64> {
                if s.is_empty()
                    || !s.bytes().all(|b| b.is_ascii_digit())
                    || s.len() > 1 && s.starts_with('0')
                {
                    return Err(malformed("pointer integer"));
                }
                s.parse().map_err(|_| malformed("pointer integer overflow"))
            };
            let e = Extent {
                size: decimal(fields[2])?,
                source: Source::Object(parse_object_address(fields[1])?),
                encoded_size: decimal(fields[3])?,
                offset: if fields.len() == 5 {
                    decimal(fields[4])?
                } else {
                    0
                },
            };
            if fields.len() == 5 && e.offset == 0 {
                return Err(malformed("explicit zero pointer offset"));
            }
            e.validate(0)?;
            extents.push(e);
            if extents.len() > EXTENTS_MAX {
                return Err(malformed("pointer extent count"));
            }
        }
        if encode_pointer(&extents)? != bytes {
            return Err(malformed("pointer canonical grammar"));
        }
        extents.iter().try_fold(0_u64, |n, e| {
            n.checked_add(e.size)
                .ok_or_else(|| malformed("pointer size overflow"))
        })?;
        Ok(extents)
    };
    parse().map_err(|e| Error::MalformedPointer(e.to_string()))
}

fn value_allocation(value: &Value) -> usize {
    match value {
        Value::Text(s) => s.capacity(),
        Value::Bytes(b) => b.capacity(),
        Value::Array(v) => {
            v.capacity() * std::mem::size_of::<Value>()
                + v.iter().map(value_allocation).sum::<usize>()
        }
        Value::Map(m) => {
            m.capacity() * std::mem::size_of::<(Value, Value)>()
                + m.iter()
                    .map(|(k, v)| value_allocation(k) + value_allocation(v))
                    .sum::<usize>()
        }
        _ => 0,
    }
}
pub(crate) fn entry_allocation(entry: &Entry) -> usize {
    entry.path.capacity()
        + entry.extents.capacity() * std::mem::size_of::<crate::manifest::Extent>()
        + entry.linkname.as_ref().map_or(0, String::capacity)
        + entry
            .attributes
            .iter()
            .map(|(key, value)| {
                2 * std::mem::size_of::<(String, Value)>()
                    + 6 * std::mem::size_of::<usize>()
                    + key.capacity()
                    + value_allocation(value)
            })
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn document_rejects_legacy_missing_fields_and_trailing_items() {
        let empty = Value::Map(vec![
            (Value::Text("version".into()), Value::Integer(1.into())),
            (
                Value::Text("required_features".into()),
                Value::Array(vec![]),
            ),
        ]);
        assert!(decode_document(&[], 1).is_err());
        let raw = crate::cbor::encode(&empty).unwrap();
        assert!(decode_document(&raw, 1).is_ok());
        let mut trailing = raw.clone();
        trailing.extend(&raw);
        assert!(decode_document(&trailing, 1).is_err());
        let Value::Map(mut fields) = empty else {
            unreachable!()
        };
        for name in ["version", "required_features"] {
            let mut missing = fields.clone();
            missing.retain(|(key, _)| key != &Value::Text(name.into()));
            assert!(
                decode_document(&crate::cbor::encode(&Value::Map(missing)).unwrap(), 1).is_err()
            );
        }
        fields.push((Value::Text("entry_count".into()), Value::Integer(0.into())));
        assert!(decode_document(&crate::cbor::encode(&Value::Map(fields)).unwrap(), 1).is_err());
        let legacy = Value::Map(vec![(Value::Text("root".into()), Value::Map(vec![]))]);
        assert!(decode_document(&crate::cbor::encode(&legacy).unwrap(), 1).is_err());
    }
    #[test]
    fn tuple_length_matches_independent_cbor_at_integer_and_container_boundaries() {
        for n in [
            0,
            23,
            24,
            255,
            256,
            65535,
            65536,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            u64::MAX,
        ] {
            for count in [0, 1, 23, 24, 255, 256] {
                let mut e = Entry {
                    path: "p".repeat(count + 1),
                    kind: EntryKind::File,
                    mode: 420,
                    uid: n,
                    gid: n,
                    mtime_ns: i64::MIN,
                    linkname: None,
                    extents: vec![
                        Extent {
                            size: n,
                            source: Source::Archive(n),
                            encoded_size: n,
                            offset: n
                        };
                        count
                    ],
                    attributes: BTreeMap::from([(
                        "x-value".into(),
                        Value::Array(vec![Value::Bytes(vec![0; count]), Value::Integer(n.into())]),
                    )]),
                };
                assert_eq!(
                    e.encoded_len().unwrap(),
                    crate::cbor::encode(&e.to_value()).unwrap().len()
                );
                for extent in &mut e.extents {
                    extent.source = Source::Object([0xab; 32]);
                }
                assert_eq!(
                    e.encoded_len().unwrap(),
                    crate::cbor::encode(&e.to_value()).unwrap().len()
                );
                e.extents.clear();
                e.kind = EntryKind::Symlink;
                e.linkname = Some("雪".repeat(count));
                assert_eq!(
                    e.encoded_len().unwrap(),
                    crate::cbor::encode(&e.to_value()).unwrap().len()
                );
            }
        }
    }
    #[test]
    fn tuple_round_trip_and_canonical_defaults() {
        let extent = Extent {
            size: 5,
            source: Source::Archive(8192),
            encoded_size: 800,
            offset: 512,
        };
        assert_eq!(
            Extent::from_value(&extent.to_value(), 8992).unwrap(),
            extent
        );
        let entry = Entry {
            path: "hello.txt".into(),
            kind: EntryKind::File,
            mode: 420,
            uid: u64::MAX,
            gid: 0,
            mtime_ns: i64::MIN,
            extents: vec![extent],
            linkname: None,
            attributes: BTreeMap::new(),
        };
        let bytes = crate::cbor::encode(&entry.to_value()).unwrap();
        assert_eq!(entry.encode().unwrap(), bytes);
        assert_eq!(entry.encoded_len().unwrap(), bytes.len());
        assert_eq!(
            Entry::from_value(&crate::cbor::decode(&bytes).unwrap(), 8992).unwrap(),
            entry
        );
        assert_eq!(array(&entry.to_value()).unwrap().len(), 7);
        let mut bad = entry.to_value();
        if let Value::Array(ref mut v) = bad {
            v.push(Value::Map(vec![]));
        }
        assert!(Entry::from_value(&bad, 8992).is_err());
        let mut bad = Extent {
            offset: 0,
            ..entry.extents[0].clone()
        }
        .to_value();
        if let Value::Array(ref mut v) = bad {
            v.push(number(0));
        }
        assert!(Extent::from_value(&bad, 8992).is_err());
    }
    #[test]
    fn typed_attributes_preserve_canonical_order_and_bound_nested_values() {
        let mut entry = Entry {
            path: "link".into(),
            kind: EntryKind::Symlink,
            mode: 511,
            uid: u64::MAX,
            gid: u64::MAX,
            mtime_ns: i64::MIN,
            extents: vec![],
            linkname: Some("../absolute//text".into()),
            attributes: BTreeMap::from([
                ("x-a".into(), Value::Bool(true)),
                (
                    "x-long".into(),
                    Value::Map(vec![(text("v"), Value::Bytes(vec![7]))]),
                ),
            ]),
        };
        entry.validate(0).unwrap();
        let encoded = entry.encode().unwrap();
        assert_eq!(entry.encoded_len().unwrap(), encoded.len());
        assert_eq!(encoded, crate::cbor::encode(&entry.to_value()).unwrap());
        assert_eq!(
            Entry::from_value(&decode_block(&encoded).unwrap(), 0).unwrap(),
            entry
        );
        let mut nested = Value::Bool(true);
        for _ in 0..62 {
            nested = Value::Array(vec![nested]);
        }
        entry.attributes.insert("x-nested".into(), nested.clone());
        entry.validate(0).unwrap();
        decode_block(&crate::cbor::encode(&entry.to_value()).unwrap()).unwrap();
        entry
            .attributes
            .insert("x-nested".into(), Value::Array(vec![nested]));
        assert!(entry.validate(0).is_err());
    }
    #[test]
    fn object_hint_does_not_bound_decoded_selection() {
        let extent = Extent {
            size: 5,
            source: Source::Object([7; 32]),
            encoded_size: 1,
            offset: 100,
        };
        assert!(extent.validate(0).is_ok());
        assert_eq!(
            parse_object_address(&object_address(&[7; 32])).unwrap(),
            [7; 32]
        );
        assert!(parse_object_address(&format!("sha256:{}", "A".repeat(64))).is_err());
    }
}
