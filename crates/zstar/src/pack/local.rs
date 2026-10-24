//! Local immutable artifacts with confined paths, atomic no-clobber publication,
//! lazy full-pack identity checks and exact positional ranges.
use super::{PackStore, Snapshot};
use crate::{Error, Result, fs::LocalFileSource, manifest_io::Spool};
use bytes::Bytes;
use cap_std::fs::{Dir, OpenOptions};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Write,
    ops::Range,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};
use tokio::sync::{OnceCell, Semaphore};

struct LocalSnapshot {
    source: LocalFileSource,
    file: std::fs::File,
    modified: SystemTime,
}
type Discovered = Arc<OnceCell<Arc<LocalSnapshot>>>;
pub struct LocalPackStore {
    root: Arc<Dir>,
    discovered: Mutex<BTreeMap<(String, String), Discovered>>,
    permits: Arc<Semaphore>,
}
impl LocalPackStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            root: Arc::new(Dir::open_ambient_dir(root, cap_std::ambient_authority())?),
            discovered: Mutex::new(BTreeMap::new()),
            permits: Arc::new(Semaphore::new(8)),
        })
    }
}
fn etag(hash: &[u8; 32]) -> String {
    format!("\"{}\"", crate::manifest::object_address(hash))
}
fn authenticate(source: &LocalFileSource) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    let mut offset = 0;
    while offset < source.len() {
        let end = (offset + (64 << 10)).min(source.len());
        hash.update(source.read_range(offset..end)?);
        offset = end;
    }
    Ok(hash.finalize().into())
}
struct RemoveTemporary {
    root: Arc<Dir>,
    key: String,
}
impl Drop for RemoveTemporary {
    fn drop(&mut self) {
        let _ = self.root.remove_file(&self.key);
    }
}
impl PackStore for LocalPackStore {
    async fn put(&self, key: String, mut data: Spool) -> Result<Snapshot> {
        if !crate::manifest::valid_path(&key) || data.is_empty() {
            return Err(Error::Unsupported("invalid artifact key or length"));
        }
        let root = self.root.clone();
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("local pack store closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if let Some(parent) = Path::new(&key).parent()
                && !parent.as_os_str().is_empty()
            {
                root.create_dir_all(parent)?;
            }
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let (temporary, mut output) = loop {
                let candidate = format!(
                    ".zstar-pack-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                );
                match root.open_with(&candidate, OpenOptions::new().write(true).create_new(true)) {
                    Ok(output) => break (candidate, output),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
            };
            let cleanup = RemoveTemporary {
                root: root.clone(),
                key: temporary,
            };
            let mut hash = Sha256::new();
            let mut offset = 0;
            let length = data.len();
            while offset < length {
                let size = (length - offset).min(64 << 10);
                let bytes = data.read(offset, size)?;
                hash.update(&bytes);
                output.write_all(&bytes)?;
                offset += size;
            }
            output.flush()?;
            output.sync_all()?;
            drop(output);
            let digest: [u8; 32] = hash.finalize().into();
            match root.hard_link(&cleanup.key, &root, &key) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let existing = LocalFileSource::from_file(root.open(&key)?.into_std())?;
                    if existing.len() != length || authenticate(&existing)? != digest {
                        return Err(Error::ChangedPack);
                    }
                }
                Err(error) => return Err(error.into()),
            }
            Ok(Snapshot {
                key,
                length,
                version: String::new(),
                etag: etag(&digest),
            })
        })
        .await
        .map_err(|_| Error::Unsupported("local pack worker failed"))?
    }
    async fn get_range(&self, snapshot: &Snapshot, range: Range<u64>) -> Result<Bytes> {
        snapshot.validate()?;
        let length = range.end.checked_sub(range.start).ok_or(Error::Range)?;
        crate::checked_range(range.start, length, snapshot.length)?;
        if length > 256 << 20 {
            return Err(Error::Budget("local packed range"));
        }
        let discovered = {
            let mut discovered = self.discovered.lock().unwrap();
            let key = (snapshot.key.clone(), snapshot.etag.clone());
            if let Some(value) = discovered.get(&key) {
                value.clone()
            } else {
                if discovered.len() >= 128 {
                    discovered.pop_first();
                }
                let value = Arc::new(OnceCell::new());
                discovered.insert(key, value.clone());
                value
            }
        };
        let pinned = discovered
            .get_or_try_init(|| async {
                let root = self.root.clone();
                let snapshot = snapshot.clone();
                let permit = self
                    .permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Unsupported("local pack store closed"))?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let file = root.open(&snapshot.key)?.into_std();
                    let metadata = file.metadata()?;
                    let source = LocalFileSource::from_file(file.try_clone()?)?
                        .with_max_range_bytes(256 << 20)?;
                    let modified = metadata.modified()?;
                    if !snapshot.version.is_empty()
                        || source.len() != snapshot.length
                        || etag(&authenticate(&source)?) != snapshot.etag
                        || file.metadata()?.modified()? != modified
                    {
                        return Err(Error::ChangedPack);
                    }
                    Ok(Arc::new(LocalSnapshot {
                        source,
                        file,
                        modified,
                    }))
                })
                .await
                .map_err(|_| Error::Unsupported("local pack worker failed"))?
            })
            .await?
            .clone();
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Unsupported("local pack store closed"))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let before = pinned.file.metadata()?;
            if before.len() != pinned.source.len() || before.modified()? != pinned.modified {
                return Err(Error::ChangedPack);
            }
            let bytes = pinned.source.read_range(range)?;
            let after = pinned.file.metadata()?;
            if after.len() != pinned.source.len() || after.modified()? != pinned.modified {
                return Err(Error::ChangedPack);
            }
            Ok(Bytes::from(bytes))
        })
        .await
        .map_err(|_| Error::Unsupported("local pack worker failed"))?
    }
}
