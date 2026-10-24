//! Filesystem capture, restoration, and materialization without following links.
//! Paths are exact UTF-8 identities; filesystem authority is held by directory
//! handles. Ownership is captured but is never applied by restoration; use
//! [`extract_with_report`](crate::fs::extract_with_report) to learn exactly which metadata was not applied.
//!
//! `pack` and `pack_to_with_options` capture portable archives. `extract` and
//! `extract_file_with_options` refuse collisions, preserve symlink target text,
//! and stage a new destination before publishing it. `LocalFileSource` provides
//! bounded positional reads; `AsyncLocalFileSource` runs them on blocking
//! workers. `LocalObjects` resolves content-addressed external objects.

use crate::backend::{BlockingArchive, BlockingBuilder, EncodedObjectResolver};
use crate::capture::CaptureSpool;
use crate::manifest_io::ManifestOptions;
#[cfg(unix)]
use crate::manifest_io::ManifestReader;
use crate::{Entry, EntryKind};
use crate::{Error, RangeReader, Result};
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};
use std::fs;
#[cfg(unix)]
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use std::{
    io::{Read, Write},
    path::Path,
};

mod file_source;
pub use file_source::AsyncLocalFileSource;
pub use file_source::LocalFileSource;

pub type AnyResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// External-object verification and destination resource policy. Metadata,
/// codec checksums, tar structure and no-follow restoration are always checked.
#[derive(Clone, Debug)]
pub struct ExtractOptions {
    /// Options for filesystem restoration. Metadata and codec checks remain
    /// mandatory even when external hash verification is disabled.
    pub allow_special_bits: bool,
    pub verify_external: bool,
    /// Open directory handles retained at once, including the root handle; a
    /// local resource limit, not a wire entry cap. Directories beyond the limit
    /// are evicted least-recently-used and transparently reopened without
    /// following links, so any tree extracts with a limit of two or more.
    pub max_directory_handles: usize,
    pub manifest: ManifestOptions,
}
impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            allow_special_bits: false,
            verify_external: true,
            max_directory_handles: 64,
            manifest: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PackOptions {
    /// Options for bounded filesystem capture and frame encoding.
    /// Preserve source modes, ownership and modification times.
    pub preserve_metadata: bool,
    pub frame_size: usize,
    pub compression_level: i32,
    pub workers: usize,
    pub manifest: ManifestOptions,
}
impl Default for PackOptions {
    fn default() -> Self {
        Self {
            preserve_metadata: false,
            frame_size: 1 << 20,
            compression_level: 3,
            workers: std::thread::available_parallelism()
                .map_or(1, std::num::NonZero::get)
                .min(2),
            manifest: Default::default(),
        }
    }
}

/// Encoded local objects named `<lowercase SHA-256>.zst`. This transport never
/// hashes implicitly; the reader establishes verified/unchecked provenance.
pub struct LocalObjects {
    dir: Dir,
}
impl LocalObjects {
    pub fn open(path: &Path) -> AnyResult<Self> {
        Ok(Self {
            dir: Dir::open_ambient_dir(path, cap_std::ambient_authority())?,
        })
    }
}
impl EncodedObjectResolver for LocalObjects {
    fn get_encoded(&mut self, digest: [u8; 32], _: u64, max_bytes: u64) -> Result<Vec<u8>> {
        let name = crate::manifest::object_address(&digest);
        let limit = max_bytes.min(17 << 20);
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = match self.dir.open_with(format!("{}.zst", &name[7..]), &options) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::MissingObject(digest));
            }
            result => result?,
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > limit {
            return Err(Error::MalformedExternalContent(
                "encoded object bounds".into(),
            ));
        }
        let mut encoded = Vec::new();
        file.take(limit + 1).read_to_end(&mut encoded)?;
        if encoded.len() as u64 > limit {
            return Err(Error::MalformedExternalContent(
                "encoded object grew".into(),
            ));
        }
        Ok(encoded)
    }
}
#[cfg(unix)]
fn time(entry: &Entry) -> AnyResult<std::time::SystemTime> {
    let value = i128::from(entry.mtime_ns);
    let duration = Duration::from_nanos(value.unsigned_abs() as u64);
    if value < 0 {
        UNIX_EPOCH.checked_sub(duration)
    } else {
        UNIX_EPOCH.checked_add(duration)
    }
    .ok_or_else(|| "filesystem timestamp out of range".into())
}
/// Which standard metadata an extraction could not apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[non_exhaustive]
pub enum OmittedField {
    /// Recorded uid/gid differ from the owner of the restored entry. Restoration
    /// never changes ownership.
    Ownership,
    /// Setuid/setgid (and any bits above `0o7777`) were masked because
    /// `allow_special_bits` is false; with it true, only bits above `0o7777`.
    SpecialModeBits,
    /// A symlink mode other than `0o777` that the platform cannot apply
    /// (Linux symlinks always report `0o777`).
    SymlinkMode,
    /// The platform rejected setting a symlink's own modification time.
    SymlinkTimes,
}
/// One class of omitted metadata: how many entries and a few example paths.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OmittedMetadata {
    pub field: OmittedField,
    pub count: u64,
    /// At most [`ExtractReport::EXAMPLES`] paths, in first-seen order.
    pub examples: Vec<String>,
}
/// Outcome of a successful extraction.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExtractReport {
    pub files: u64,
    pub directories: u64,
    pub symlinks: u64,
    /// Metadata recorded in the archive but not applied; sorted by field and
    /// empty when restoration was lossless.
    pub omitted: Vec<OmittedMetadata>,
}
impl ExtractReport {
    /// Example paths retained per omitted-metadata class.
    pub const EXAMPLES: usize = 8;
    /// True when every recorded standard metadata field was applied.
    #[must_use]
    pub fn is_lossless(&self) -> bool {
        self.omitted.is_empty()
    }
    /// Count of entries affected by `field`.
    #[must_use]
    pub fn omitted_count(&self, field: OmittedField) -> u64 {
        self.omitted
            .iter()
            .find(|o| o.field == field)
            .map_or(0, |o| o.count)
    }
    #[cfg(unix)]
    fn omit(&mut self, field: OmittedField, path: &str) {
        let at = if let Some(at) = self.omitted.iter().position(|o| o.field == field) {
            at
        } else {
            self.omitted.push(OmittedMetadata {
                field,
                count: 0,
                examples: Vec::new(),
            });
            self.omitted.sort_by_key(|o| o.field);
            self.omitted.iter().position(|o| o.field == field).unwrap()
        };
        let item = &mut self.omitted[at];
        item.count += 1;
        if item.examples.len() < Self::EXAMPLES {
            item.examples.push(path.to_owned());
        }
    }
}
#[cfg(unix)]
fn special_mask(allow_special_bits: bool) -> u64 {
    if allow_special_bits { 0o7777 } else { 0o1777 }
}
#[cfg(unix)]
fn metadata(
    file: &std::fs::File,
    entry: &Entry,
    allow_special_bits: bool,
    report: &mut ExtractReport,
) -> AnyResult<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let mask = special_mask(allow_special_bits);
    file.set_permissions(std::fs::Permissions::from_mode((entry.mode & mask) as u32))?;
    file.set_times(std::fs::FileTimes::new().set_modified(time(entry)?))?;
    if entry.mode & !mask != 0 {
        report.omit(OmittedField::SpecialModeBits, &entry.path);
    }
    let owner = file.metadata()?;
    if u64::from(owner.uid()) != entry.uid || u64::from(owner.gid()) != entry.gid {
        report.omit(OmittedField::Ownership, &entry.path);
    }
    Ok(())
}
#[cfg(unix)]
type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// Prefix an error with the archive path it concerns, keeping `io::ErrorKind`.
#[cfg(unix)]
fn at(path: &str, error: impl Into<BoxError>) -> BoxError {
    let error = error.into();
    // Typed archive errors carry a category callers match on; keep them intact.
    if error.is::<crate::Error>() {
        return error;
    }
    let kind = error
        .downcast_ref::<std::io::Error>()
        .map(std::io::Error::kind);
    let message = format!("{path}: {error}");
    match kind {
        Some(kind) => std::io::Error::new(kind, message).into(),
        None => message.into(),
    }
}

/// Least-recently-used set of open directory handles. The root is pinned. A
/// miss reopens the directory one component at a time from the nearest cached
/// ancestor with no-follow opens, so evicting a handle never lets a symlink
/// redirect later writes outside the destination.
#[cfg(unix)]
struct DirCache {
    root: Arc<Dir>,
    capacity: usize,
    entries: HashMap<String, (Arc<Dir>, u64)>,
    order: BTreeMap<u64, String>,
    clock: u64,
}
#[cfg(unix)]
impl DirCache {
    fn new(root: Dir, max_handles: usize) -> Self {
        Self {
            root: Arc::new(root),
            capacity: max_handles.saturating_sub(1),
            entries: HashMap::new(),
            order: BTreeMap::new(),
            clock: 0,
        }
    }
    fn insert(&mut self, path: &str, dir: Arc<Dir>) {
        if self.capacity == 0 {
            return;
        }
        if let Some((_, stamp)) = self.entries.remove(path) {
            self.order.remove(&stamp);
        }
        while self.entries.len() >= self.capacity {
            let (_, oldest) = self.order.pop_first().expect("order tracks entries");
            self.entries.remove(&oldest);
        }
        self.clock += 1;
        self.order.insert(self.clock, path.to_owned());
        self.entries.insert(path.to_owned(), (dir, self.clock));
    }
    fn touch(&mut self, path: &str) -> Option<Arc<Dir>> {
        let (dir, stamp) = self.entries.get_mut(path)?;
        self.order.remove(stamp);
        self.clock += 1;
        *stamp = self.clock;
        self.order.insert(self.clock, path.to_owned());
        Some(dir.clone())
    }
    fn dir(&mut self, path: &str) -> AnyResult<Arc<Dir>> {
        if path.is_empty() {
            return Ok(self.root.clone());
        }
        if let Some(dir) = self.touch(path) {
            return Ok(dir);
        }
        let mut start = 0;
        let mut current = self.root.clone();
        let mut cut = path.len();
        while let Some(slash) = path[..cut].rfind('/') {
            cut = slash;
            if let Some(dir) = self.touch(&path[..cut]) {
                current = dir;
                start = cut + 1;
                break;
            }
        }
        let mut end = start;
        for (index, part) in path[start..].split('/').enumerate() {
            if index != 0 {
                end += 1;
            }
            end += part.len();
            let next = Arc::new(current.open_dir_nofollow(part)?);
            self.insert(&path[..end], next.clone());
            current = next;
        }
        Ok(current)
    }
    fn parent<'a>(&mut self, path: &'a str) -> AnyResult<(Arc<Dir>, &'a str)> {
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        Ok((self.dir(parent).map_err(|e| at(path, e))?, name))
    }
}

/// Every descendant operation uses a captured directory plus one leaf name.
/// Failure leaves no new entries; see [`extract_with_report`].
pub fn extract<R: RangeReader>(
    archive: &mut BlockingArchive<R>,
    destination: &Path,
    objects: Option<LocalObjects>,
    allow_special_bits: bool,
) -> AnyResult<()> {
    extract_with_options(
        archive,
        destination,
        objects,
        ExtractOptions {
            allow_special_bits,
            ..Default::default()
        },
    )
}

/// Where one extraction writes. A destination that does not exist is built in a
/// fresh sibling directory and renamed into place only after full success, so
/// a failed extraction leaves nothing behind. An existing destination (or one
/// with no file name, such as `.` or `/`) is extracted into in place; on
/// failure the entries this call created are removed, and pre-existing content
/// is never touched.
#[cfg(unix)]
struct Target {
    root: Dir,
    staged: Option<Staged>,
}
#[cfg(unix)]
struct Staged {
    parent: Dir,
    temp: std::ffi::OsString,
    name: std::ffi::OsString,
    armed: bool,
}
#[cfg(unix)]
impl Drop for Staged {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.parent.remove_dir_all(&self.temp);
        }
    }
}
#[cfg(unix)]
impl Target {
    fn open(destination: &Path) -> AnyResult<Self> {
        let Some(name) = destination.file_name() else {
            let path = if destination.as_os_str().is_empty() {
                Path::new(".")
            } else {
                destination
            };
            return Ok(Self {
                root: Dir::open_ambient_dir(path, cap_std::ambient_authority())?,
                staged: None,
            });
        };
        let parent_path = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = Dir::open_ambient_dir(parent_path, cap_std::ambient_authority())?;
        match parent.symlink_metadata(name) {
            // Includes symlinks and files: opening without following rejects them.
            Ok(_) => {
                let root = parent.open_dir_nofollow(name)?;
                Ok(Self { root, staged: None })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let nanos = std::time::SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.subsec_nanos());
                for attempt in 0..32_u32 {
                    let temp = std::ffi::OsString::from(format!(
                        ".zstar-tmp-{}-{nanos}-{attempt}",
                        std::process::id()
                    ));
                    match parent.create_dir(&temp) {
                        Ok(()) => {
                            let staged = Staged {
                                parent,
                                temp,
                                name: name.to_owned(),
                                armed: true,
                            };
                            let root = staged.parent.open_dir_nofollow(&staged.temp)?;
                            return Ok(Self {
                                root,
                                staged: Some(staged),
                            });
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Err("could not create a temporary extraction directory".into())
            }
            Err(e) => Err(e.into()),
        }
    }
    fn commit(&mut self) -> AnyResult<()> {
        if let Some(staged) = &mut self.staged {
            staged
                .parent
                .rename(&staged.temp, &staged.parent, &staged.name)
                .map_err(|e| {
                    at(
                        &staged.name.to_string_lossy(),
                        format!("could not move extracted tree into place: {e}"),
                    )
                })?;
            staged.armed = false;
        }
        Ok(())
    }
}

/// What a failed extraction must undo. Directories are always listed (they are
/// needed anyway); leaves are only tracked when extracting in place.
#[cfg(unix)]
#[derive(Default)]
struct Progress {
    dirs: Vec<String>,
    leaves: Vec<String>,
    track_leaves: bool,
}
#[cfg(unix)]
impl Progress {
    fn leaf(&mut self, path: &str) {
        if self.track_leaves {
            self.leaves.push(path.to_owned());
        }
    }
}
#[cfg(unix)]
fn rollback(target: &Target, progress: &Progress) {
    use std::os::unix::fs::PermissionsExt;
    let Ok(root) = target.root.try_clone() else {
        return;
    };
    let mut cache = DirCache::new(root, 64);
    // Directory metadata may already have been applied (for example 0o555).
    for path in &progress.dirs {
        if let Ok(dir) = cache.dir(path) {
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            if let Ok(file) = dir.open_with(".", &options) {
                let _ = file
                    .into_std()
                    .set_permissions(std::fs::Permissions::from_mode(0o700));
            }
        }
    }
    if target.staged.is_some() {
        return; // the staging directory is removed recursively on drop
    }
    for path in progress.leaves.iter().rev() {
        if let Ok((parent, name)) = cache.parent(path) {
            let _ = parent.remove_file(name);
        }
    }
    for path in progress.dirs.iter().rev() {
        if let Ok((parent, name)) = cache.parent(path) {
            let _ = parent.remove_dir(name);
        }
    }
}

#[cfg(unix)]
fn create_directories(
    manifest: &ManifestReader,
    cache: &mut DirCache,
    max_handles: usize,
    progress: &mut Progress,
    report: &mut ExtractReport,
) -> AnyResult<BTreeMap<String, Entry>> {
    let mut metadata = BTreeMap::new();
    let mut cursor = manifest.cursor();
    while let Some(mut entry) = manifest.next_entry(&mut cursor)? {
        if entry.kind != EntryKind::Directory {
            continue;
        }
        // The root plus one working handle are the minimum to build a tree.
        if max_handles < 2 {
            return Err(Error::Budget("destination directory handles").into());
        }
        let (parent, name) = cache.parent(&entry.path)?;
        parent.create_dir(name).map_err(|e| at(&entry.path, e))?;
        progress.dirs.push(entry.path.clone());
        let dir = parent
            .open_dir_nofollow(name)
            .map_err(|e| at(&entry.path, e))?;
        cache.insert(&entry.path, Arc::new(dir));
        report.directories += 1;
        // Restoration only applies standard metadata. Do not retain potentially
        // large extension values for every directory.
        entry.attributes.clear();
        metadata.insert(entry.path.clone(), entry);
    }
    Ok(metadata)
}
#[cfg(unix)]
fn finish_destination(
    manifest: &ManifestReader,
    cache: &mut DirCache,
    directories: &BTreeMap<String, Entry>,
    allow_special_bits: bool,
    progress: &mut Progress,
    report: &mut ExtractReport,
) -> AnyResult<()> {
    use cap_std::fs::{MetadataExt, PermissionsExt};
    let mask = special_mask(allow_special_bits);
    let mut cursor = manifest.cursor();
    while let Some(entry) = manifest.next_entry(&mut cursor)? {
        if entry.kind != EntryKind::Symlink {
            continue;
        }
        let (parent, name) = cache.parent(&entry.path)?;
        let target = entry.linkname.as_deref().ok_or("missing linkname")?;
        parent
            .symlink_contents(target, name)
            .map_err(|e| at(&entry.path, e))?;
        progress.leaf(&entry.path);
        report.symlinks += 1;
        if entry.mode & !mask != 0 {
            report.omit(OmittedField::SpecialModeBits, &entry.path);
        }
        // Unlike Linux, macOS applies the umask when creating symlinks, so even
        // the default archived mode of 0o777 may need to be restored.
        let created_mode = parent
            .symlink_metadata(name)
            .map_err(|e| at(&entry.path, e))?
            .permissions()
            .mode()
            & 0o7777;
        if u64::from(created_mode) != entry.mode & mask {
            // Linux cannot change a symlink's own mode; other platforms may.
            #[cfg(target_os = "linux")]
            report.omit(OmittedField::SymlinkMode, &entry.path);
            #[cfg(not(target_os = "linux"))]
            {
                match parent.set_symlink_permissions(
                    name,
                    cap_std::fs::Permissions::from_mode((entry.mode & mask) as u32),
                ) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                        report.omit(OmittedField::SymlinkMode, &entry.path);
                    }
                    Err(e) => return Err(at(&entry.path, e)),
                }
            }
        }
        match parent.set_symlink_times(
            name,
            None,
            Some(cap_std::time::SystemTime::from_std(time(&entry)?).into()),
        ) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                report.omit(OmittedField::SymlinkTimes, &entry.path);
            }
            Err(e) => return Err(at(&entry.path, e)),
        }
        let owner = parent
            .symlink_metadata(name)
            .map_err(|e| at(&entry.path, e))?;
        if u64::from(owner.uid()) != entry.uid || u64::from(owner.gid()) != entry.gid {
            report.omit(OmittedField::Ownership, &entry.path);
        }
    }
    for entry in directories.values().rev() {
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = cache
            .dir(&entry.path)?
            .open_with(".", &options)
            .map_err(|e| at(&entry.path, e))?
            .into_std();
        metadata(&file, entry, allow_special_bits, report).map_err(|e| at(&entry.path, e))?;
    }
    Ok(())
}

pub fn extract_file(
    archive: &Path,
    destination: &Path,
    objects: Option<LocalObjects>,
    allow_special_bits: bool,
) -> AnyResult<()> {
    extract_file_with_options(
        archive,
        destination,
        objects,
        ExtractOptions {
            allow_special_bits,
            ..Default::default()
        },
    )
}
pub fn extract_file_with_options(
    archive: &Path,
    destination: &Path,
    objects: Option<LocalObjects>,
    options: ExtractOptions,
) -> AnyResult<()> {
    let mut reader =
        BlockingArchive::open(LocalFileSource::open(archive)?, options.manifest.clone())?;
    extract_with_options(&mut reader, destination, objects, options)
}

/// Extract and report which recorded metadata could not be applied.
///
/// A destination that does not yet exist is created atomically: the tree is
/// built in a sibling temporary directory and renamed on success, and a failure
/// removes it. An existing destination, or one without a file name (`.`, `/`),
/// is extracted into in place; pre-existing entries collide as errors and are
/// never modified, and on failure the entries this call created are removed.
/// Errors name the archive path involved.
#[cfg(unix)]
pub fn extract_with_report<R: RangeReader>(
    reader: &mut BlockingArchive<R>,
    destination: &Path,
    objects: Option<LocalObjects>,
    options: ExtractOptions,
) -> AnyResult<ExtractReport> {
    reader.manifest().validate_metadata()?;
    if reader
        .manifest()
        .root()
        .required_features
        .iter()
        .any(|s| s == "external-content")
        && objects.is_none()
    {
        return Err(Error::Unsupported("external content requires --objects").into());
    }
    if options.max_directory_handles == 0 {
        return Err(Error::Budget("destination directory handles").into());
    }
    let mut target = Target::open(destination)?;
    let mut progress = Progress {
        track_leaves: target.staged.is_none(),
        ..Default::default()
    };
    match extract_into(reader, &target.root, objects, &options, &mut progress) {
        Ok(report) => {
            target.commit()?;
            Ok(report)
        }
        Err(error) => {
            rollback(&target, &progress);
            Err(error)
        }
    }
}
#[cfg(unix)]
fn extract_into<R: RangeReader>(
    reader: &mut BlockingArchive<R>,
    root: &Dir,
    mut objects: Option<LocalObjects>,
    options: &ExtractOptions,
    progress: &mut Progress,
) -> AnyResult<ExtractReport> {
    let mut report = ExtractReport::default();
    let mut cache = DirCache::new(root.try_clone()?, options.max_directory_handles);
    let directories = create_directories(
        reader.manifest(),
        &mut cache,
        options.max_directory_handles,
        progress,
        &mut report,
    )?;
    let mut current: Option<(Entry, fs::File)> = None;
    reader.visit_payloads(|entry, bytes| {
        let mut write = || -> AnyResult<()> {
            if entry.is_external() {
                return Ok(());
            }
            if current
                .as_ref()
                .is_none_or(|(previous, _)| previous.path != entry.path)
            {
                if let Some((previous, file)) = current.take() {
                    metadata(&file, &previous, options.allow_special_bits, &mut report)
                        .map_err(|e| at(&previous.path, e))?;
                    report.files += 1;
                }
                let (parent, name) = cache.parent(&entry.path)?;
                let mut open = OpenOptions::new();
                open.write(true).create_new(true).follow(FollowSymlinks::No);
                let file = parent
                    .open_with(name, &open)
                    .map_err(|e| at(&entry.path, e))?
                    .into_std();
                progress.leaf(&entry.path);
                current = Some((entry.clone(), file));
            }
            current
                .as_mut()
                .ok_or("missing output file")?
                .1
                .write_all(bytes)
                .map_err(|e| at(&entry.path, e))?;
            Ok(())
        };
        write().map_err(|error| Error::Io(std::io::Error::other(error)))
    })?;
    if let Some((entry, file)) = current {
        metadata(&file, &entry, options.allow_special_bits, &mut report)
            .map_err(|e| at(&entry.path, e))?;
        report.files += 1;
    }
    let mut cursor = reader.manifest().cursor();
    while let Some(entry) = reader.manifest().next_entry(&mut cursor)? {
        if !entry.is_external() {
            continue;
        }
        let resolver = objects
            .as_mut()
            .ok_or(Error::Unsupported("external content requires --objects"))?;
        let (parent, name) = cache.parent(&entry.path)?;
        let mut open = OpenOptions::new();
        open.write(true).create_new(true).follow(FollowSymlinks::No);
        let mut file = parent
            .open_with(name, &open)
            .map_err(|e| at(&entry.path, e))?
            .into_std();
        progress.leaf(&entry.path);
        let copied = reader
            .open_file(&entry.path, Some(resolver), options.verify_external)
            .and_then(|mut f| f.copy_to(&mut file))
            .map_err(|e| at(&entry.path, e))?;
        if copied != entry.size()? {
            return Err(at(&entry.path, Error::Range));
        }
        metadata(&file, &entry, options.allow_special_bits, &mut report)
            .map_err(|e| at(&entry.path, e))?;
        report.files += 1;
    }
    finish_destination(
        reader.manifest(),
        &mut cache,
        &directories,
        options.allow_special_bits,
        progress,
        &mut report,
    )?;
    Ok(report)
}
#[cfg(not(unix))]
pub fn extract_with_report<R: RangeReader>(
    _: &mut BlockingArchive<R>,
    _: &Path,
    _: Option<LocalObjects>,
    _: ExtractOptions,
) -> AnyResult<ExtractReport> {
    Err("extraction requires a Unix filesystem".into())
}
pub fn extract_with_options<R: RangeReader>(
    reader: &mut BlockingArchive<R>,
    destination: &Path,
    objects: Option<LocalObjects>,
    options: ExtractOptions,
) -> AnyResult<()> {
    extract_with_report(reader, destination, objects, options).map(|_| ())
}
/// Open a local archive file and [`extract_with_report`] it.
pub fn extract_file_with_report(
    archive: &Path,
    destination: &Path,
    objects: Option<LocalObjects>,
    options: ExtractOptions,
) -> AnyResult<ExtractReport> {
    let mut reader =
        BlockingArchive::open(LocalFileSource::open(archive)?, options.manifest.clone())?;
    extract_with_report(&mut reader, destination, objects, options)
}

/// Explicit in-memory convenience. Use `materialize_to` for large output archives.
pub fn materialize<R: RangeReader>(
    archive: &mut BlockingArchive<R>,
    objects: Option<LocalObjects>,
) -> AnyResult<Vec<u8>> {
    let mut output = Vec::new();
    materialize_to(archive, &mut output, objects, true, Default::default())?;
    Ok(output)
}
/// Validate the physical archive, resolve external objects once per adjacent
/// extent group, and stream a portable archive preserving entry/root extensions.
pub fn materialize_to<R: RangeReader>(
    archive: &mut BlockingArchive<R>,
    output: &mut impl Write,
    mut objects: Option<LocalObjects>,
    verify_external: bool,
    options: PackOptions,
) -> AnyResult<()> {
    archive.validate()?;
    let mut writer = BlockingBuilder::with_options(
        output,
        options.frame_size,
        options.compression_level,
        options.manifest,
    )?
    .with_workers(options.workers)?
    .with_root_extensions(archive.manifest().root().extensions.clone())?;
    let mut cursor = archive.manifest().cursor();
    while let Some(mut entry) = archive.manifest().next_entry(&mut cursor)? {
        if entry.kind == EntryKind::File {
            let size = entry.size()?;
            let mut body = archive.open_file(
                &entry.path,
                objects
                    .as_mut()
                    .map(|r| r as &mut dyn EncodedObjectResolver),
                verify_external,
            )?;
            entry.extents.clear();
            writer.add_file(entry, size, &mut body)?;
        } else {
            writer.add_metadata(entry)?;
        }
    }
    writer.finish()?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn replaced_parent_name_cannot_redirect_captured_directory_writes() {
        let temp = tempfile::tempdir().unwrap();
        let root_path = temp.path().join("root");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let root = Dir::open_ambient_dir(&root_path, cap_std::ambient_authority()).unwrap();
        root.create_dir("dir").unwrap();
        let captured = root.open_dir_nofollow("dir").unwrap();
        let mut dirs = DirCache::new(root, 8);
        dirs.insert("dir", Arc::new(captured));
        // Model a concurrent replacement between capturing the directory and
        // creating its member: the replacement name now points outside root.
        std::fs::rename(root_path.join("dir"), root_path.join("parked")).unwrap();
        std::os::unix::fs::symlink(&outside, root_path.join("dir")).unwrap();
        let (parent, name) = dirs.parent("dir/file").unwrap();
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .follow(FollowSymlinks::No);
        parent
            .open_with(name, &options)
            .unwrap()
            .write_all(b"safe")
            .unwrap();
        assert!(!outside.join("file").exists());
        assert_eq!(
            std::fs::read(root_path.join("parked/file")).unwrap(),
            b"safe"
        );
    }
}

/// Capture a directory tree and each link's own metadata. The root itself must
/// be a real directory. Only the encoded archive is retained in memory.
pub fn pack(input: &Path) -> AnyResult<Vec<u8>> {
    let mut archive = Vec::new();
    pack_to(input, &mut archive)?;
    Ok(archive)
}

/// Pack with disk-spooled capture records; no whole namespace or archive buffer.
pub fn pack_to(input: &Path, output: &mut impl Write) -> AnyResult<()> {
    pack_to_with_options(input, output, Default::default())
}
pub fn pack_to_with_options(
    input: &Path,
    output: &mut impl Write,
    options: PackOptions,
) -> AnyResult<()> {
    if !fs::symlink_metadata(input)?.is_dir() {
        return Err("pack input must be a directory without following a root symlink".into());
    }
    let root = if let Some(name) = input.file_name() {
        let parent = input
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        Dir::open_ambient_dir(parent, cap_std::ambient_authority())?.open_dir_nofollow(name)?
    } else {
        Dir::open_ambient_dir(input, cap_std::ambient_authority())?
    };
    let mut entries = CaptureSpool::new(options.manifest.clone());
    collect_entries("", &root, &mut entries, options.preserve_metadata)?;
    let mut writer = BlockingBuilder::with_options(
        output,
        options.frame_size,
        options.compression_level,
        options.manifest,
    )?
    .with_workers(options.workers)?;
    append_captured(&root, "", entries, &mut writer, options.preserve_metadata)?;
    writer.finish()?;
    Ok(())
}

/// Append a captured directory, including its root entry, to an existing
/// archive builder. Filesystem traversal and reopening stay relative to
/// captured no-follow directory handles.
pub fn append_directory<W: Write>(
    input: &Path,
    archive_name: &str,
    writer: &mut BlockingBuilder<W>,
    preserve_metadata: bool,
) -> AnyResult<()> {
    if !crate::manifest::valid_path(archive_name) {
        return Err("invalid archive directory name".into());
    }
    if !fs::symlink_metadata(input)?.is_dir() {
        return Err("pack input must be a directory without following a root symlink".into());
    }
    let root = if let Some(name) = input.file_name() {
        let parent = input
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        Dir::open_ambient_dir(parent, cap_std::ambient_authority())?.open_dir_nofollow(name)?
    } else {
        Dir::open_ambient_dir(input, cap_std::ambient_authority())?
    };
    let meta = root.dir_metadata()?;
    let mut entries = CaptureSpool::new(Default::default());
    let mut root_entry = Entry::directory(archive_name);
    root_entry.mode = if preserve_metadata {
        permissions(&meta)
    } else {
        normalized_mode(EntryKind::Directory, permissions(&meta))
    };
    if preserve_metadata {
        (root_entry.uid, root_entry.gid, root_entry.mtime_ns) = owner_and_time(&meta)?;
    }
    entries.add(root_entry, 0)?;
    collect_entries(archive_name, &root, &mut entries, preserve_metadata)?;
    append_captured(&root, archive_name, entries, writer, preserve_metadata)
}

fn append_captured<W: Write>(
    root: &Dir,
    prefix: &str,
    entries: CaptureSpool,
    writer: &mut BlockingBuilder<W>,
    preserve_metadata: bool,
) -> AnyResult<()> {
    for captured in entries.sorted()? {
        let (mut entry, size) = captured?;
        if entry.kind == EntryKind::File {
            let path = if prefix.is_empty() {
                entry.path.as_str()
            } else {
                entry
                    .path
                    .strip_prefix(prefix)
                    .and_then(|path| path.strip_prefix('/'))
                    .ok_or("captured path prefix")?
            };
            let mut file = open_captured_file(root, path)?;
            let meta = cap_std::fs::Metadata::from_just_metadata(file.metadata()?);
            if !meta.is_file() || meta.len() != size {
                return Err("input file changed size while packing".into());
            }
            capture_metadata(&mut entry, &meta, preserve_metadata)?;
            writer.add_file(entry, size, &mut file)?;
        } else {
            writer.add_metadata(entry)?;
        }
    }
    Ok(())
}

fn capture_metadata(
    entry: &mut Entry,
    meta: &cap_std::fs::Metadata,
    preserve: bool,
) -> AnyResult<()> {
    entry.mode = if preserve {
        permissions(meta)
    } else {
        normalized_mode(entry.kind, permissions(meta))
    };
    if preserve {
        (entry.uid, entry.gid, entry.mtime_ns) = owner_and_time(meta)?;
    }
    Ok(())
}

/// Append one filesystem path, without following the path's final symlink.
pub fn append_path<W: Write>(
    input: &Path,
    archive_name: &str,
    writer: &mut BlockingBuilder<W>,
    preserve_metadata: bool,
    recursive: bool,
) -> AnyResult<()> {
    if !crate::manifest::valid_path(archive_name) {
        return Err("invalid archive path".into());
    }
    let meta = cap_std::fs::Metadata::from_just_metadata(fs::symlink_metadata(input)?);
    if meta.is_dir() {
        if !recursive {
            let mut entry = Entry::directory(archive_name);
            let mode = permissions(&meta);
            entry.mode = if preserve_metadata {
                mode
            } else {
                normalized_mode(EntryKind::Directory, mode)
            };
            if preserve_metadata {
                (entry.uid, entry.gid, entry.mtime_ns) = owner_and_time(&meta)?;
            }
            writer.add_metadata(entry)?;
            return Ok(());
        }
        return append_directory(input, archive_name, writer, preserve_metadata);
    }
    let parent = input
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = input.file_name().ok_or("input filename required")?;
    let dir = Dir::open_ambient_dir(parent, cap_std::ambient_authority())?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    if meta.file_type().is_symlink() {
        let mut entry = Entry::symlink(
            archive_name,
            dir.read_link_contents(name)?
                .to_str()
                .ok_or("symlink target must be UTF-8")?,
        );
        if preserve_metadata {
            entry.mode = permissions(&meta);
            (entry.uid, entry.gid, entry.mtime_ns) = owner_and_time(&meta)?;
        }
        writer.add_metadata(entry)?;
    } else if meta.is_file() {
        let mut file = dir.open_with(name, &options)?.into_std();
        let current = cap_std::fs::Metadata::from_just_metadata(file.metadata()?);
        if !current.is_file() || current.len() != meta.len() {
            return Err("input file changed while packing".into());
        }
        let mut entry = Entry::file(archive_name);
        entry.mode = if preserve_metadata {
            permissions(&current)
        } else {
            normalized_mode(EntryKind::File, permissions(&current))
        };
        if preserve_metadata {
            (entry.uid, entry.gid, entry.mtime_ns) = owner_and_time(&current)?;
        }
        writer.add_file(entry, current.len(), &mut file)?;
    } else {
        return Err("only regular files, directories, and symlinks are supported".into());
    }
    Ok(())
}

fn open_captured_file(root: &Dir, path: &str) -> AnyResult<std::fs::File> {
    let mut parts = path.split('/').peekable();
    let mut current: Option<Dir> = None;
    while let Some(part) = parts.next() {
        if parts.peek().is_none() {
            let parent = current.as_ref().unwrap_or(root);
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            return Ok(parent.open_with(part, &options)?.into_std());
        }
        let parent = current.as_ref().unwrap_or(root);
        current = Some(parent.open_dir_nofollow(part)?);
    }
    Err("empty archive path".into())
}

fn collect_entries(
    prefix: &str,
    dir: &Dir,
    entries: &mut CaptureSpool,
    preserve_metadata: bool,
) -> AnyResult<()> {
    for child in dir.entries()? {
        let child = child?;
        let name = child.file_name();
        let name_text = name.to_str().ok_or("archive paths must be UTF-8")?;
        let relative = if prefix.is_empty() {
            name_text.to_string()
        } else {
            format!("{prefix}/{name_text}")
        };
        // `file_type` is normally supplied by the directory enumeration and
        // avoids a second stat for regular files. The sorted packing pass still
        // reopens files without following symlinks and checks their size.
        let file_type = child.file_type()?;
        let meta;
        let mut opened_dir = None;
        if file_type.is_dir() {
            let child = dir.open_dir_nofollow(&name)?;
            meta = child.dir_metadata()?;
            opened_dir = Some(child);
        } else if file_type.is_symlink() {
            meta = dir.symlink_metadata(&name)?;
        } else if file_type.is_file() {
            meta = child.metadata()?;
        } else {
            // Preserve the existing behavior for unusual filesystem entry
            // types while keeping them out of the regular-file fast path.
            meta = dir.symlink_metadata(&name)?;
        }
        let mode = permissions(&meta);
        let (uid, gid, mtime_ns) = if preserve_metadata {
            owner_and_time(&meta)?
        } else {
            (0, 0, 0)
        };
        let mut entry = Entry {
            attributes: Default::default(),
            path: relative.clone(),
            kind: EntryKind::File,
            mode,
            uid,
            gid,
            mtime_ns,
            extents: vec![],
            linkname: None,
        };
        if meta.file_type().is_symlink() {
            entry.kind = EntryKind::Symlink;
            entry.linkname = Some(
                dir.read_link_contents(&name)?
                    .to_str()
                    .ok_or("symlink target is not UTF-8")?
                    .to_string(),
            );
        } else if meta.is_dir() {
            entry.kind = EntryKind::Directory;
        } else if meta.is_file() {
            // Read during the sorted streaming pass below.
        } else {
            return Err(format!("unsupported filesystem entry: {relative}").into());
        }
        if !preserve_metadata {
            entry.mode = normalized_mode(entry.kind, mode);
        }
        let is_dir = entry.kind == EntryKind::Directory;
        entries.add(entry, if meta.is_file() { meta.len() } else { 0 })?;
        if is_dir {
            collect_entries(
                &relative,
                opened_dir.as_ref().ok_or("input directory disappeared")?,
                entries,
                preserve_metadata,
            )?;
        }
    }
    Ok(())
}
fn normalized_mode(kind: EntryKind, source: u64) -> u64 {
    match kind {
        EntryKind::Symlink => 0o777,
        EntryKind::Directory => 0o755,
        EntryKind::File if source & 0o111 != 0 => 0o755,
        EntryKind::File => 0o644,
    }
}

#[cfg(unix)]
fn permissions(meta: &cap_std::fs::Metadata) -> u64 {
    use cap_std::fs::PermissionsExt;
    u64::from(meta.permissions().mode() & 0o7777)
}
#[cfg(not(unix))]
fn permissions(_: &cap_std::fs::Metadata) -> u64 {
    0o644
}

#[cfg(unix)]
fn owner_and_time(meta: &cap_std::fs::Metadata) -> AnyResult<(u64, u64, i64)> {
    use cap_std::fs::MetadataExt;
    let nanos = i128::from(meta.mtime()) * 1_000_000_000 + i128::from(meta.mtime_nsec());
    Ok((
        u64::from(meta.uid()),
        u64::from(meta.gid()),
        i64::try_from(nanos)?,
    ))
}
#[cfg(not(unix))]
fn owner_and_time(_: &cap_std::fs::Metadata) -> AnyResult<(u64, u64, i64)> {
    Err("packing requires Unix filesystem metadata".into())
}
