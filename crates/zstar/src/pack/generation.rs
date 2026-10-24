use super::catalog::{invalid, number, tuple};
use super::{Catalog, CatalogLimits, PackStore, Snapshot};
use crate::{
    Error, Result,
    chunking::{Chunking, FASTCDC_PROFILE},
    manifest_io::{ManifestOptions, Spool},
};
use ciborium::Value;

/// One retained immutable generation. Provenance is descriptive, never needed
/// for archive reconstruction or algorithm dispatch.
#[derive(Clone, Debug)]
pub struct Generation {
    pub archive: Snapshot,
    pub catalog: Snapshot,
    pub provenance: crate::Extensions,
}
impl Generation {
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.archive.validate()?;
        self.catalog.validate()?;
        let provenance = Value::Map(
            self.provenance
                .iter()
                .map(|(key, value)| (Value::Text(key.clone()), value.clone()))
                .collect(),
        );
        let bytes = crate::manifest::encode_block(&Value::Array(vec![
            Value::Integer(1.into()),
            self.archive.value(),
            self.catalog.value(),
            provenance,
        ]))?;
        if bytes.len() > 64 << 10 {
            return Err(Error::Budget("generation descriptor"));
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 64 << 10 {
            return Err(Error::Budget("generation descriptor"));
        }
        let value = crate::manifest::decode_block(bytes).map_err(|_| invalid("generation CBOR"))?;
        let fields = tuple(&value, 4)?;
        if number(&fields[0])? != 1 {
            return Err(Error::UnsupportedVersion("packed generation"));
        }
        let Value::Map(values) = &fields[3] else {
            return Err(invalid("provenance map"));
        };
        let mut provenance = crate::Extensions::new();
        for (key, value) in values {
            let Value::Text(key) = key else {
                return Err(invalid("provenance keys"));
            };
            provenance.insert(key.clone(), value.clone());
        }
        Ok(Self {
            archive: Snapshot::parse(&fields[1])?,
            catalog: Snapshot::parse(&fields[2])?,
            provenance,
        })
    }
}
/// Failed publication with completed immutable artifacts for orphan accounting.
#[derive(Debug, thiserror::Error)]
#[error("generation publication failed: {error}")]
pub struct PublicationFailure {
    #[source]
    pub error: Error,
    pub completed: Vec<Snapshot>,
}
impl From<Error> for PublicationFailure {
    fn from(error: Error) -> Self {
        Self {
            error,
            completed: Vec::new(),
        }
    }
}
struct SpoolBorrow<'a>(&'a mut Spool);
impl crate::RangeReader for SpoolBorrow<'_> {
    fn len(&self) -> u64 {
        self.0.len()
    }
    fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        self.0.read(range.start, range.end - range.start)
    }
}
/// Publish dependencies before the final descriptor. A later failure leaves
/// completed immutable artifacts orphaned; shared objects are never deleted.
pub async fn publish_generation<S: PackStore>(
    store: &S,
    prefix: &str,
    archive: Spool,
    catalog: &Catalog,
    strategy: Chunking,
    limits: CatalogLimits,
) -> std::result::Result<(Snapshot, Generation), PublicationFailure> {
    strategy.validate()?;
    if !crate::manifest::valid_path(prefix) {
        return Err(Error::Unsupported("invalid generation prefix").into());
    }
    // Validate the finalized physical v1 archive before publishing dependencies.
    let archive = tokio::task::spawn_blocking(move || {
        let mut archive = archive;
        let mut reader = crate::backend::BlockingArchive::open(
            SpoolBorrow(&mut archive),
            ManifestOptions::default(),
        )?;
        reader.validate()?;
        drop(reader);
        Ok::<_, Error>(archive)
    })
    .await
    .map_err(|_| Error::Unsupported("archive validation worker failed"))??;
    let encoded = catalog.encode(limits)?;
    let mut spool = Spool::new(ManifestOptions::default());
    spool.append(&encoded)?;
    let catalog = put_artifact(store, prefix, "catalogs", "cbor", spool).await?;
    let archive = put_artifact(store, prefix, "archives", "zstar", archive)
        .await
        .map_err(|error| PublicationFailure {
            error,
            completed: vec![catalog.clone()],
        })?;
    let (profile, min, target, max) = match strategy {
        Chunking::Fixed { size } => ("fixed-v1", size, size, size),
        Chunking::FastCdc { min, target, max } => (FASTCDC_PROFILE, min, target, max),
    };
    let provenance = [
        ("chunking".into(), Value::Text(profile.into())),
        ("min".into(), Value::Integer((min as u64).into())),
        ("target".into(), Value::Integer((target as u64).into())),
        ("max".into(), Value::Integer((max as u64).into())),
    ]
    .into_iter()
    .collect();
    let generation = Generation {
        archive,
        catalog,
        provenance,
    };
    let mut spool = Spool::new(ManifestOptions::default());
    let completed = vec![generation.catalog.clone(), generation.archive.clone()];
    let encoded = generation.encode().map_err(|error| PublicationFailure {
        error,
        completed: completed.clone(),
    })?;
    spool.append(&encoded).map_err(|error| PublicationFailure {
        error,
        completed: completed.clone(),
    })?;
    let descriptor = put_artifact(store, prefix, "generations", "cbor", spool)
        .await
        .map_err(|error| PublicationFailure { error, completed })?;
    Ok((descriptor, generation))
}
async fn put_artifact<S: PackStore>(
    store: &S,
    prefix: &str,
    directory: &str,
    suffix: &str,
    data: Spool,
) -> Result<Snapshot> {
    if !crate::manifest::valid_path(prefix)
        || !crate::manifest::valid_path(directory)
        || !crate::manifest::valid_path(suffix)
    {
        return Err(Error::Unsupported("invalid artifact path"));
    }
    let length = data.len();
    let (data, digest) = tokio::task::spawn_blocking(move || {
        use sha2::{Digest as _, Sha256};
        let mut data = data;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < length {
            let size = (length - offset).min(64 << 10);
            hash.update(data.read(offset, size)?);
            offset += size;
        }
        Ok::<_, Error>((data, hash.finalize().into()))
    })
    .await
    .map_err(|_| Error::Unsupported("artifact hash worker failed"))??;
    let digest: [u8; 32] = digest;
    let address = crate::manifest::object_address(&digest);
    let key = format!("{prefix}/{directory}/{}.{suffix}", &address[7..]);
    let snapshot = store.put(key.clone(), data).await?;
    snapshot.validate()?;
    if snapshot.key != key || snapshot.length != length {
        return Err(Error::ChangedPack);
    }
    Ok(snapshot)
}
