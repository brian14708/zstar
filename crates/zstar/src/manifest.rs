//! Entry-oriented draft-v1 metadata. Wire tuples are deliberately separate from
//! the typed API: callers never need to manipulate positional CBOR values.
use crate::{BudgetResource, Error, Result};
use ciborium::Value;
use std::collections::BTreeMap;

/// Target decoded bytes per independently compressed manifest segment.
pub const BLOCK_TARGET: usize = 64 << 10;

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

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
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
#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
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
    pub(crate) fn encoded_len(&self) -> usize {
        use crate::cbor::head_len;
        1 + head_len(self.size)
            + head_len(self.encoded_size)
            + match self.source {
                Source::Archive(at) => head_len(at),
                Source::Object(_) => 73,
            }
            + if self.offset == 0 {
                0
            } else {
                head_len(self.offset)
            }
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub fn validate(&self, data_end: u64) -> Result<()> {
        if self.size == 0 || self.encoded_size == 0 {
            return Err(malformed("extent size"));
        }
        if self.offset.checked_add(self.size).is_none() {
            return Err(malformed("extent selection"));
        }
        if let Source::Archive(start) = self.source
            && (start
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
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
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
    pub const fn code(self) -> u64 {
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

    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
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
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
    pub fn validate(&self, data_end: u64) -> Result<()> {
        if !valid_path(&self.path) || self.mode > 4095 {
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
        self.encoded_len()?;
        Ok(())
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "The branch or bounded fixture establishes a nonnegative value."
    )]
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
            len = len
                .checked_add(extent.encoded_len())
                .ok_or_else(|| malformed("entry size maximum"))?;
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
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "The branch or bounded fixture establishes a nonnegative value."
    )]
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
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
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
    /// Mandatory extensions, sorted and unique. Unknown names are rejected.
    pub requires: Vec<String>,
    pub entry_count: u64,
    pub extensions: BTreeMap<String, Value>,
}
impl ManifestRoot {
    pub(crate) fn validate(&self) -> Result<()> {
        for (i, name) in self.requires.iter().enumerate() {
            if !valid_feature(name) || i > 0 && self.requires[i - 1] >= *name {
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

/// Default local parser budget for the manifest directory, whose extensions
/// have no format-defined size limit.
#[cfg(feature = "async")]
pub(crate) const HEADER_MAX: u64 = 64 << 20;

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

pub(crate) fn decode_values_budgeted(
    mut values: impl Iterator<Item = Result<Value>>,
    data_end: u64,
    max_retained: u64,
    max_entries: u64,
    limits: crate::Limits,
) -> Result<(ManifestRoot, Vec<Entry>)> {
    let value = values
        .next()
        .transpose()?
        .ok_or_else(|| malformed("missing manifest header"))?;
    let m = map(&value)?;
    if unsigned(field(m, "version")?)? != 1 {
        return Err(Error::UnsupportedVersion("manifest document"));
    }
    let requires = array(field(m, "requires")?)?
        .iter()
        .map(|v| string(v).map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    let mut root = ManifestRoot {
        requires,
        entry_count: 0,
        extensions: BTreeMap::new(),
    };
    for (key, value) in m {
        let name = string(key)?;
        if name.starts_with("x-") {
            root.extensions.insert(name.into(), value.clone());
        } else if !["version", "requires"].contains(&name) {
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
    while let Some(value) = values.next().transpose()? {
        if max_entries != 0 && entries.len() as u64 >= max_entries {
            return Err(Error::budget(
                BudgetResource::MetadataEntries,
                "manifest entry count",
            ));
        }
        let entry = Entry::from_value(&value, data_end)?;
        limits.entry(&entry)?;
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
            return Err(Error::budget(
                BudgetResource::MetadataRetainedBytes,
                "retained manifest entries",
            ));
        }
        reserve_entries(&mut entries, slots, max_entries)?;
        external |= entry.is_external();
        entries.push(entry);
    }
    if external != root.requires.iter().any(|s| s == "external-content") {
        return Err(malformed("external feature declaration"));
    }
    root.entry_count = entries.len() as u64;
    Ok((root, entries))
}

fn reserve_entries(entries: &mut Vec<Entry>, slots: u64, max_entries: u64) -> Result<()> {
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
        let desired = usize::try_from(desired).map_err(|_| {
            Error::budget(BudgetResource::IndexBytes, "manifest index address space")
        })?;
        entries
            .try_reserve_exact(desired - entries.len())
            .map_err(|_| {
                Error::budget(
                    BudgetResource::AllocationBytes,
                    "retained manifest allocation",
                )
            })?;
    }
    if entries.capacity() as u64 > slots {
        return Err(Error::budget(
            BudgetResource::MetadataRetainedBytes,
            "retained manifest entries",
        ));
    }
    Ok(())
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

fn validate_external_extents(extents: &[Extent]) -> Result<()> {
    if extents.is_empty()
        || extents
            .iter()
            .any(|e| !matches!(e.source, Source::Object(_)))
    {
        return Err(malformed("external extents"));
    }
    extents.iter().try_fold(0_u64, |total, e| {
        e.validate(0)?;
        total
            .checked_add(e.size)
            .ok_or_else(|| malformed("external reference size overflow"))
    })?;
    Ok(())
}

/// Serialize a standalone external reference with a logical size that fits uint64.
/// No whole-file identity is stored.
#[cfg(any(test, feature = "async"))]
pub(crate) fn encode_external_reference(extents: &[Extent]) -> Result<Vec<u8>> {
    validate_external_extents(extents)?;
    crate::cbor::encode_checked(&Value::Map(vec![
        (text("version"), number(1)),
        (
            text("extents"),
            Value::Array(extents.iter().map(Extent::to_value).collect()),
        ),
    ]))
}

#[cfg(test)]
pub(crate) fn decode_external_reference(bytes: &[u8]) -> Result<Vec<Extent>> {
    let value = crate::cbor::decode(bytes)?;
    let m = map(&value)?;
    if m.len() != 2 || unsigned(field(m, "version")?)? != 1 {
        return Err(malformed("external reference schema"));
    }
    let extents = array(field(m, "extents")?)?
        .iter()
        .map(|v| Extent::from_value(v, 0))
        .collect::<Result<Vec<_>>>()?;
    validate_external_extents(&extents)?;
    Ok(extents)
}

// Serde struct visitors also accept sequences; pointers require JSON objects.
#[derive(serde::Serialize)]
#[serde(transparent)]
struct JsonObject<T>(T);

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct ObjectVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for ObjectVisitor<T> {
            type Value = JsonObject<T>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }
        deserializer.deserialize_map(ObjectVisitor(std::marker::PhantomData))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentPointer<T> {
    version: String,
    chunks: T,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PointerChunk {
    source: String,
    size: u64,
    encoded_size: u64,
    #[serde(
        default,
        skip_serializing_if = "is_zero",
        deserialize_with = "positive_offset"
    )]
    offset: u64,
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde skip_serializing_if callbacks receive a reference."
)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn positive_offset<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u64, D::Error> {
    let value = <u64 as serde::Deserialize>::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom("explicit zero pointer offset"));
    }
    Ok(value)
}

/// Serialize a JSON content pointer, omitting zero offsets.
pub(crate) fn encode_pointer(extents: &[Extent]) -> Result<Vec<u8>> {
    validate_external_extents(extents)?;
    let chunks = extents
        .iter()
        .map(|e| {
            let Source::Object(hash) = &e.source else {
                return Err(malformed("pointer source"));
            };
            Ok(JsonObject(PointerChunk {
                source: object_address(hash),
                size: e.size,
                encoded_size: e.encoded_size,
                offset: e.offset,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    serde_json::to_vec(&ContentPointer {
        version: "zstar-pointer-v1".into(),
        chunks,
    })
    .map_err(|e| Error::MalformedPointer(e.to_string()))
}

/// Parse JSON pointer chunks with exact uint64 values and strict fields.
#[cfg(test)]
pub(crate) fn decode_pointer(bytes: &[u8]) -> Result<Vec<Extent>> {
    decode_pointer_bounded(bytes, usize::MAX)
}

#[cfg(any(test, all(feature = "fs", unix)))]
pub(crate) fn decode_pointer_bounded(bytes: &[u8], maximum_chunks: usize) -> Result<Vec<Extent>> {
    struct ChunkVisitor(usize);
    impl<'de> serde::de::Visitor<'de> for ChunkVisitor {
        type Value = Vec<Extent>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an array of pointer chunks")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut extents = Vec::new();
            while let Some(JsonObject(chunk)) =
                sequence.next_element::<JsonObject<PointerChunk>>()?
            {
                if extents.len() == self.0 {
                    return Err(serde::de::Error::custom(
                        "pointer chunk count exceeds manifest",
                    ));
                }
                extents.push(Extent {
                    source: Source::Object(
                        parse_object_address(&chunk.source).map_err(serde::de::Error::custom)?,
                    ),
                    size: chunk.size,
                    encoded_size: chunk.encoded_size,
                    offset: chunk.offset,
                });
            }
            Ok(extents)
        }
    }
    let parse = || -> Result<Vec<Extent>> {
        // Borrow the chunks' JSON bytes so parsing cannot allocate an unchecked
        // intermediate vector before the manifest's extent count is enforced.
        let JsonObject(pointer): JsonObject<ContentPointer<&serde_json::value::RawValue>> =
            serde_json::from_slice(bytes).map_err(|e| malformed(&e.to_string()))?;
        if pointer.version != "zstar-pointer-v1" {
            return Err(malformed("pointer version"));
        }
        let mut decoder = serde_json::Deserializer::from_str(pointer.chunks.get());
        let extents =
            serde::Deserializer::deserialize_seq(&mut decoder, ChunkVisitor(maximum_chunks))
                .map_err(|e| malformed(&e.to_string()))?;
        validate_external_extents(&extents)?;
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
mod hardening;

#[cfg(test)]
mod tests {
    use super::hardening::decode_document;
    use super::*;
    #[test]
    fn document_rejects_missing_unknown_fields_and_trailing_items() {
        let empty = Value::Map(vec![
            (Value::Text("version".into()), Value::Integer(1.into())),
            (Value::Text("requires".into()), Value::Array(vec![])),
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
        for name in ["version", "requires"] {
            let mut missing = fields.clone();
            missing.retain(|(key, _)| key != &Value::Text(name.into()));
            assert!(
                decode_document(&crate::cbor::encode(&Value::Map(missing)).unwrap(), 1).is_err()
            );
        }
        fields.push((Value::Text("unexpected".into()), Value::Integer(0.into())));
        assert!(decode_document(&crate::cbor::encode(&Value::Map(fields)).unwrap(), 1).is_err());
    }
    #[test]
    fn external_reference_and_pointer_sizes_fit_uint64() {
        for (first_size, valid) in [(u64::MAX - 1, true), (u64::MAX, false)] {
            let extents = [first_size, 1].map(|size| Extent {
                size,
                source: Source::Object([0; 32]),
                encoded_size: 1,
                offset: 0,
            });
            let reference = crate::cbor::encode(&Value::Map(vec![
                (text("version"), number(1)),
                (
                    text("extents"),
                    Value::Array(extents.iter().map(Extent::to_value).collect()),
                ),
            ]))
            .unwrap();
            let address = object_address(&[0; 32]);
            let pointer = format!(
                r#"{{"version":"zstar-pointer-v1","chunks":[{{"source":"{address}","size":{first_size},"encoded_size":1}},{{"source":"{address}","size":1,"encoded_size":1}}]}}"#
            );
            assert_eq!(encode_external_reference(&extents).is_ok(), valid);
            assert_eq!(decode_external_reference(&reference).is_ok(), valid);
            assert_eq!(encode_pointer(&extents).is_ok(), valid);
            assert_eq!(decode_pointer(pointer.as_bytes()).is_ok(), valid);
            assert!(decode_pointer_bounded(pointer.as_bytes(), 1).is_err());
        }
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
