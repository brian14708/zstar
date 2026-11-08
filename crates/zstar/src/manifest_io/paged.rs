//! Directory coverage and independent metadata page validation.
use super::{ManifestOptions, MetadataBuffer, invalid};
use crate::{Footer, Result};
use ciborium::Value;

fn map(value: &Value) -> Result<&[(Value, Value)]> {
    if let Value::Map(map) = value {
        Ok(map)
    } else {
        Err(invalid("metadata map"))
    }
}
fn array(value: &Value) -> Result<&[Value]> {
    if let Value::Array(array) = value {
        Ok(array)
    } else {
        Err(invalid("page reference array"))
    }
}
fn text(value: &Value) -> Result<&str> {
    if let Value::Text(text) = value {
        Ok(text)
    } else {
        Err(invalid("metadata text"))
    }
}
fn uint(value: &Value) -> Result<u64> {
    if let Value::Integer(n) = value {
        u64::try_from(*n).map_err(|_| invalid("metadata uint64"))
    } else {
        Err(invalid("metadata uint64"))
    }
}
fn field<'a>(fields: &'a [(Value, Value)], name: &str) -> Result<&'a Value> {
    fields
        .iter()
        .find_map(|(k, v)| (k.as_text() == Some(name)).then_some(v))
        .ok_or_else(|| invalid("missing metadata field"))
}
fn extensions(fields: &[(Value, Value)], known: &[&str]) -> Result<()> {
    for (key, _) in fields {
        let name = text(key)?;
        if !known.contains(&name) && !name.starts_with("x-") {
            return Err(invalid("unknown metadata field"));
        }
    }
    Ok(())
}
fn reference(value: &Value, entry: bool) -> Result<(Option<String>, (u64, u64, u64))> {
    let v = array(value)?;
    let base = usize::from(entry);
    if v.len() != base + 3 && v.len() != base + 4 {
        return Err(invalid("page reference tuple"));
    }
    let path = if entry {
        let path = text(&v[0])?;
        if !crate::manifest::valid_path(path) {
            return Err(invalid("page separator path"));
        }
        Some(path.to_owned())
    } else {
        None
    };
    if let Some(attrs) = v.get(base + 3) {
        let attrs = map(attrs)?;
        if attrs.is_empty() {
            return Err(invalid("empty reference attributes"));
        }
        extensions(attrs, &[])?;
    }
    let descriptor = (uint(&v[base])?, uint(&v[base + 1])?, uint(&v[base + 2])?);
    if descriptor.1 <= 8 || descriptor.2 == 0 {
        return Err(invalid("page reference length"));
    }
    Ok((path, descriptor))
}

pub(super) fn decode_metadata(
    buffer: &MetadataBuffer,
    physical: &[(u64, u64, u64)],
    footer: &Footer,
    data_end: u64,
    options: &ManifestOptions,
) -> Result<(crate::manifest::ManifestRoot, Vec<crate::manifest::Entry>)> {
    let directory = physical
        .last()
        .ok_or_else(|| invalid("missing directory"))?;
    if directory.1 != footer.directory_encoded_len || directory.2 != footer.directory_decoded_len {
        return Err(invalid("directory length"));
    }
    let raw = buffer.read(footer.toc_decoded_len - directory.2, directory.2)?;
    let archive_size = data_end + footer.toc_encoded_len + crate::FOOTER_SIZE as u64;
    let directory = ManifestDirectory::decode(raw, footer, archive_size)?;
    let references = directory.pages.iter().chain(&directory.extension_pages);
    if directory.pages.len() + directory.extension_pages.len() + 1 != physical.len() {
        return Err(invalid("metadata page coverage"));
    }
    let mut raw_offset = 0;
    for (i, (reference, actual)) in references.zip(physical).enumerate() {
        if (
            reference.offset,
            reference.encoded_len,
            reference.decoded_len,
        ) != *actual
        {
            return Err(invalid("page reference mismatch"));
        }
        if i >= directory.pages.len() {
            crate::cbor::decode(buffer.read(raw_offset, reference.decoded_len)?)?;
        }
        raw_offset += reference.decoded_len;
    }
    let root = directory.root_value();
    let mut offset = 0;
    let pages = directory
        .pages
        .iter()
        .enumerate()
        .map(|(index, reference)| {
            let raw = buffer.read(offset, reference.decoded_len)?;
            offset += reference.decoded_len;
            directory.entry_values(index, raw)
        });
    let values = std::iter::once(Ok(root)).chain(pages.flat_map(|page| match page {
        Ok(entries) => entries.into_iter().map(Ok).collect::<Vec<_>>(),
        Err(error) => vec![Err(error)],
    }));
    crate::manifest::decode_values_budgeted(
        values,
        directory.data_end,
        options.max_retained_entry_bytes,
        options.max_entries,
        options.limits,
    )
}

/// Validated routing information for an independently compressed metadata page.
#[derive(Clone, Debug)]
pub struct PageReference {
    pub offset: u64,
    pub encoded_len: u64,
    pub decoded_len: u64,
    pub last_path: Option<String>,
}

/// A complete validated directory, without fetching entry or extension pages.
#[derive(Clone, Debug)]
pub struct ManifestDirectory {
    pub(crate) root: crate::manifest::ManifestRoot,
    pub(crate) pages: Vec<PageReference>,
    pub(crate) data_end: u64,
    extension_pages: Vec<PageReference>,
}
impl ManifestDirectory {
    /// Validates a decoded directory and every declared page range.
    ///
    /// # Errors
    /// Rejects invalid CBOR, schema, required features, coverage, or decoded sums.
    pub fn decode(raw: &[u8], footer: &Footer, archive_size: u64) -> Result<Self> {
        let metadata = footer.metadata_range(archive_size)?;
        if raw.len() as u64 != footer.directory_decoded_len {
            return Err(invalid("directory decoded length"));
        }
        let value = crate::cbor::decode(raw)?;
        let fields = map(&value)?;
        extensions(fields, &["version", "requires", "pages", "extension-pages"])?;
        if uint(field(fields, "version")?)? != 1 {
            return Err(crate::Error::UnsupportedVersion("manifest directory"));
        }
        let requires = array(field(fields, "requires")?)?
            .iter()
            .map(|v| text(v).map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        let root = crate::manifest::ManifestRoot {
            requires,
            entry_count: 0,
            extensions: fields
                .iter()
                .filter_map(|(k, v)| {
                    k.as_text()
                        .filter(|s| s.starts_with("x-"))
                        .map(|s| (s.to_owned(), v.clone()))
                })
                .collect(),
        };
        root.validate()?;
        let convert = |v: &Value, entry: bool| -> Result<PageReference> {
            let (last_path, (offset, encoded_len, decoded_len)) = reference(v, entry)?;
            Ok(PageReference {
                offset,
                encoded_len,
                decoded_len,
                last_path,
            })
        };
        let pages = array(field(fields, "pages")?)?
            .iter()
            .map(|v| convert(v, true))
            .collect::<Result<Vec<_>>>()?;
        for pair in pages.windows(2) {
            if pair[0].last_path >= pair[1].last_path {
                return Err(invalid("page separator order"));
            }
        }
        let mut extension_pages = Vec::new();
        if let Some((_, v)) = fields
            .iter()
            .find(|(k, _)| k.as_text() == Some("extension-pages"))
        {
            let groups = map(v)?;
            if groups.is_empty() {
                return Err(invalid("empty extension groups"));
            }
            for (group, refs) in groups {
                if !text(group)?.starts_with("x-") {
                    return Err(invalid("extension page group"));
                }
                let refs = array(refs)?;
                if refs.is_empty() {
                    return Err(invalid("empty extension group"));
                }
                for v in refs {
                    extension_pages.push(convert(v, false)?);
                }
            }
        }
        let mut end = 0_u64;
        let mut decoded = footer.directory_decoded_len;
        for page in pages.iter().chain(&extension_pages) {
            if page.offset != end {
                return Err(invalid("page coverage"));
            }
            end = end
                .checked_add(page.encoded_len)
                .ok_or_else(|| invalid("page end overflow"))?;
            decoded = decoded
                .checked_add(page.decoded_len)
                .ok_or_else(|| invalid("decoded sum overflow"))?;
        }
        if end != footer.toc_encoded_len - footer.directory_encoded_len
            || decoded != footer.toc_decoded_len
        {
            return Err(invalid("directory coverage"));
        }
        Ok(Self {
            root,
            pages,
            extension_pages,
            data_end: metadata.start,
        })
    }
    /// Routes an exact path to its candidate entry page without fetching it.
    #[must_use]
    #[cfg(feature = "async")]
    pub fn find_page(&self, path: &str) -> Option<usize> {
        if !crate::manifest::valid_path(path) {
            return None;
        }
        let i = self.pages.partition_point(|page| {
            page.last_path
                .as_deref()
                .is_some_and(|s| s.as_bytes() < path.as_bytes())
        });
        (i < self.pages.len()).then_some(i)
    }
    /// Validates a complete selected entry page, without claiming global namespace validity.
    ///
    /// # Errors
    /// Rejects a page with incorrect length, schema, paths, extents, or feature declarations.
    #[cfg(feature = "async")]
    pub fn decode_entry_page(
        &self,
        index: usize,
        raw: &[u8],
        limits: crate::Limits,
    ) -> Result<Vec<crate::Entry>> {
        let values = self.entry_values(index, raw)?;
        let mut entries: Vec<crate::Entry> = Vec::new();
        for value in values {
            let entry = crate::Entry::from_value(&value, self.data_end)?;
            limits.entry(&entry)?;
            if entries
                .last()
                .is_some_and(|p| p.path.as_bytes() >= entry.path.as_bytes())
                || index > 0
                    && self.pages[index - 1]
                        .last_path
                        .as_ref()
                        .is_some_and(|p| p.as_bytes() >= entry.path.as_bytes())
            {
                return Err(invalid("entry page order"));
            }
            if entry.is_external() && !self.root.requires.iter().any(|s| s == "external-content") {
                return Err(invalid("undeclared external content"));
            }
            entries.push(entry);
        }
        Ok(entries)
    }
    fn root_value(&self) -> Value {
        let mut fields = vec![
            (Value::Text("version".into()), Value::Integer(1.into())),
            (
                Value::Text("requires".into()),
                Value::Array(
                    self.root
                        .requires
                        .iter()
                        .cloned()
                        .map(Value::Text)
                        .collect(),
                ),
            ),
        ];
        fields.extend(
            self.root
                .extensions
                .iter()
                .map(|(k, v)| (Value::Text(k.clone()), v.clone())),
        );
        Value::Map(fields)
    }
    fn entry_values(&self, index: usize, raw: &[u8]) -> Result<Vec<Value>> {
        let reference = self.pages.get(index).ok_or(crate::Error::Range)?;
        if raw.len() as u64 != reference.decoded_len {
            return Err(invalid("page decoded length"));
        }
        let value = crate::cbor::decode(raw)?;
        extensions(map(&value)?, &["entries"])?;
        let Value::Map(fields) = value else {
            unreachable!()
        };
        let entries = match fields
            .into_iter()
            .find_map(|(key, value)| (key.as_text() == Some("entries")).then_some(value))
        {
            Some(Value::Array(entries)) if !entries.is_empty() => entries,
            _ => return Err(invalid("missing or empty entry array")),
        };
        let first = array(&entries[0])?.first().and_then(Value::as_text);
        let last = array(entries.last().unwrap())?
            .first()
            .and_then(Value::as_text);
        if last != reference.last_path.as_deref()
            || index > 0
                && first.is_none_or(|path| Some(path) <= self.pages[index - 1].last_path.as_deref())
        {
            return Err(invalid("entry page path range"));
        }
        Ok(entries)
    }
}
