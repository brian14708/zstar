use crate::{Error, Result};
use ciborium::Value;
use std::collections::{BTreeMap, BTreeSet};

/// A relative object key and an immutable storage identity. Empty strings mean
/// absent; at least a non-null version or strong `ETag` must be supplied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub key: String,
    pub length: u64,
    pub version: String,
    pub etag: String,
}
impl Snapshot {
    pub fn validate(&self) -> Result<()> {
        if !crate::manifest::valid_path(&self.key)
            || self.key.len() > 4096
            || self.length == 0
            || self.version.len() > 4096
            || self.etag.len() > 4096
            || (self.version.is_empty() || self.version == "null") && !strong_etag(&self.etag)
            || !self.etag.is_empty() && !strong_etag(&self.etag)
        {
            return Err(invalid("invalid snapshot"));
        }
        Ok(())
    }
    pub(crate) fn value(&self) -> Value {
        Value::Array(vec![
            Value::Text(self.key.clone()),
            uint(self.length),
            Value::Text(self.version.clone()),
            Value::Text(self.etag.clone()),
        ])
    }
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let fields = tuple(value, 4)?;
        let snapshot = Self {
            key: text(&fields[0])?.into(),
            length: number(&fields[1])?,
            version: text(&fields[2])?.into(),
            etag: text(&fields[3])?.into(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}
fn strong_etag(tag: &str) -> bool {
    tag.len() >= 2
        && tag.starts_with('"')
        && tag.ends_with('"')
        && tag[1..tag.len() - 1]
            .bytes()
            .all(|b| b == 0x21 || b >= 0x23 && b != 0x7f)
}

/// Pack IDs are SHA-256 of the complete encoded pack, independent of chunk IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackRecord {
    pub id: [u8; 32],
    pub snapshot: Snapshot,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRecord {
    pub digest: [u8; 32],
    pub pack: [u8; 32],
    pub offset: u64,
    pub encoded_length: u64,
    pub decoded_length: u64,
}

/// MVP catalog loading is linear. Limits include conservative CBOR scratch
/// space as well as retained indexes; no bucket listing or pack probes occur.
#[derive(Clone, Copy, Debug)]
pub struct CatalogLimits {
    pub max_encoded_bytes: usize,
    pub max_packs: usize,
    pub max_chunks: usize,
    pub max_memory_bytes: usize,
}
impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 16 << 20,
            max_packs: 65_536,
            max_chunks: 262_144,
            max_memory_bytes: 512 << 20,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    packs: BTreeMap<[u8; 32], PackRecord>,
    chunks: BTreeMap<[u8; 32], ChunkRecord>,
}
impl Catalog {
    pub fn new(
        packs: Vec<PackRecord>,
        chunks: Vec<ChunkRecord>,
        limits: CatalogLimits,
    ) -> Result<Self> {
        if packs.len() > limits.max_packs || chunks.len() > limits.max_chunks {
            return Err(Error::Budget("catalog records"));
        }
        let mut catalog = Self::default();
        let mut keys = BTreeSet::new();
        let mut cost = chunks
            .len()
            .checked_mul(256)
            .ok_or(Error::Budget("catalog memory"))?;
        for pack in packs {
            pack.snapshot.validate()?;
            cost = cost
                .checked_add(
                    256 + pack.snapshot.key.len() * 2
                        + pack.snapshot.version.len()
                        + pack.snapshot.etag.len(),
                )
                .ok_or(Error::Budget("catalog memory"))?;
            if !keys.insert(pack.snapshot.key.clone())
                || catalog.packs.insert(pack.id, pack).is_some()
            {
                return Err(invalid("duplicate pack"));
            }
        }
        if cost > limits.max_memory_bytes / 2 {
            return Err(Error::Budget("catalog memory"));
        }
        let mut ranges: BTreeMap<[u8; 32], Vec<(u64, u64)>> = BTreeMap::new();
        for chunk in chunks {
            let pack = catalog
                .packs
                .get(&chunk.pack)
                .ok_or_else(|| invalid("unknown pack"))?;
            if chunk.encoded_length == 0
                || chunk.encoded_length > 17 << 20
                || chunk.decoded_length == 0
                || chunk.decoded_length > 16 << 20
                || chunk.offset > pack.snapshot.length
                || chunk.encoded_length > pack.snapshot.length - chunk.offset
            {
                return Err(invalid("chunk bounds"));
            }
            ranges
                .entry(chunk.pack)
                .or_default()
                .push((chunk.offset, chunk.offset + chunk.encoded_length));
            if catalog.chunks.insert(chunk.digest, chunk).is_some() {
                return Err(invalid("duplicate chunk"));
            }
        }
        for offsets in ranges.values_mut() {
            offsets.sort_unstable();
            if offsets.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return Err(invalid("overlapping frames"));
            }
        }
        Ok(catalog)
    }
    #[must_use]
    pub fn packs(&self) -> &BTreeMap<[u8; 32], PackRecord> {
        &self.packs
    }
    #[must_use]
    pub fn chunks(&self) -> &BTreeMap<[u8; 32], ChunkRecord> {
        &self.chunks
    }
    pub fn encode(&self, limits: CatalogLimits) -> Result<Vec<u8>> {
        Self::new(
            self.packs.values().cloned().collect(),
            self.chunks.values().cloned().collect(),
            limits,
        )?;
        let indices: BTreeMap<_, _> = self
            .packs
            .keys()
            .enumerate()
            .map(|(index, id)| (*id, index as u64))
            .collect();
        let packs = self
            .packs
            .values()
            .map(|pack| Value::Array(vec![Value::Bytes(pack.id.to_vec()), pack.snapshot.value()]))
            .collect();
        let chunks = self
            .chunks
            .values()
            .map(|chunk| {
                Value::Array(vec![
                    Value::Bytes(chunk.digest.to_vec()),
                    uint(indices[&chunk.pack]),
                    uint(chunk.offset),
                    uint(chunk.encoded_length),
                    uint(chunk.decoded_length),
                ])
            })
            .collect();
        let encoded = crate::manifest::encode_block(&Value::Array(vec![
            uint(1),
            Value::Array(packs),
            Value::Array(chunks),
        ]))?;
        check_encoded(encoded.len(), limits)?;
        Ok(encoded)
    }
    pub fn decode(encoded: &[u8], limits: CatalogLimits) -> Result<Self> {
        check_encoded(encoded.len(), limits)?;
        let value = crate::manifest::decode_block(encoded).map_err(|error| match error {
            Error::Budget(_) => error,
            _ => invalid("invalid deterministic CBOR"),
        })?;
        let fields = tuple(&value, 3)?;
        if number(&fields[0])? != 1 {
            return Err(Error::UnsupportedVersion("packed catalog"));
        }
        let pack_values = array(&fields[1])?;
        let chunk_values = array(&fields[2])?;
        if pack_values.len() > limits.max_packs || chunk_values.len() > limits.max_chunks {
            return Err(Error::Budget("catalog records"));
        }
        let mut packs = Vec::with_capacity(pack_values.len());
        let mut last = None;
        for value in pack_values {
            let pair = tuple(value, 2)?;
            let id = digest(&pair[0])?;
            if last.is_some_and(|previous| previous >= id) {
                return Err(invalid("pack ordering"));
            }
            last = Some(id);
            packs.push(PackRecord {
                id,
                snapshot: Snapshot::parse(&pair[1])?,
            });
        }
        let mut chunks = Vec::with_capacity(chunk_values.len());
        last = None;
        for value in chunk_values {
            let fields = tuple(value, 5)?;
            let hash = digest(&fields[0])?;
            if last.is_some_and(|previous| previous >= hash) {
                return Err(invalid("chunk ordering"));
            }
            last = Some(hash);
            let index = usize::try_from(number(&fields[1])?).map_err(|_| invalid("pack index"))?;
            chunks.push(ChunkRecord {
                digest: hash,
                pack: packs.get(index).ok_or_else(|| invalid("pack index"))?.id,
                offset: number(&fields[2])?,
                encoded_length: number(&fields[3])?,
                decoded_length: number(&fields[4])?,
            });
        }
        Self::new(packs, chunks, limits)
    }
}
fn check_encoded(size: usize, limits: CatalogLimits) -> Result<()> {
    if size > limits.max_encoded_bytes || size > limits.max_memory_bytes / 128 {
        return Err(Error::Budget("catalog encoded bytes or parsing memory"));
    }
    Ok(())
}
pub(crate) fn invalid(reason: &str) -> Error {
    Error::MalformedCatalog(reason.into())
}
pub(crate) fn uint(value: u64) -> Value {
    Value::Integer(value.into())
}
pub(crate) fn number(value: &Value) -> Result<u64> {
    match value {
        Value::Integer(n) => u64::try_from(*n).map_err(|_| invalid("uint64")),
        _ => Err(invalid("uint64")),
    }
}
pub(crate) fn text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(s) => Ok(s),
        _ => Err(invalid("text")),
    }
}
pub(crate) fn array(value: &Value) -> Result<&[Value]> {
    match value {
        Value::Array(values) => Ok(values),
        _ => Err(invalid("array")),
    }
}
pub(crate) fn tuple(value: &Value, count: usize) -> Result<&[Value]> {
    let values = array(value)?;
    if values.len() != count {
        return Err(invalid("tuple length"));
    }
    Ok(values)
}
fn digest(value: &Value) -> Result<[u8; 32]> {
    match value {
        Value::Bytes(bytes) => bytes
            .as_slice()
            .try_into()
            .map_err(|_| invalid("digest length")),
        _ => Err(invalid("digest bytes")),
    }
}
