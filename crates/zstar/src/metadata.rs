//! Validated logical metadata, independent of the archive's physical layout.
use crate::{Entry, EntryKind, Extensions, Result};

/// An immutable snapshot of a validated archive member.
///
/// Sizes are checked when metadata is loaded, so [`Self::size`] is infallible.
/// Physical frame offsets and extents belong to [`crate::layout`].
#[derive(Clone, Debug, PartialEq)]
pub struct EntryMetadata {
    path: String,
    kind: EntryKind,
    size: u64,
    mode: u64,
    uid: u64,
    gid: u64,
    mtime_ns: i64,
    linkname: Option<String>,
    attributes: Extensions,
    external: bool,
}

impl EntryMetadata {
    pub(crate) fn from_entry(entry: &Entry) -> Result<Self> {
        Ok(Self {
            path: entry.path.clone(),
            kind: entry.kind,
            size: entry.size()?,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            mtime_ns: entry.mtime_ns,
            linkname: entry.linkname.clone(),
            attributes: entry.attributes.clone(),
            external: entry.is_external(),
        })
    }

    /// Archive-relative UTF-8 path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
    /// Member type; symlinks are never followed implicitly.
    #[must_use]
    pub const fn kind(&self) -> EntryKind {
        self.kind
    }
    /// Logical payload length in bytes; zero for directories and symlinks.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
    /// Recorded Unix permission bits.
    #[must_use]
    pub const fn mode(&self) -> u64 {
        self.mode
    }
    /// Recorded user ID.
    #[must_use]
    pub const fn uid(&self) -> u64 {
        self.uid
    }
    /// Recorded group ID.
    #[must_use]
    pub const fn gid(&self) -> u64 {
        self.gid
    }
    /// Modification time in signed nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn mtime_ns(&self) -> i64 {
        self.mtime_ns
    }
    /// Exact symlink target, if this member is a symlink.
    #[must_use]
    pub fn linkname(&self) -> Option<&str> {
        self.linkname.as_deref()
    }
    /// Entry-level `x-*` extension values.
    #[must_use]
    pub const fn attributes(&self) -> &Extensions {
        &self.attributes
    }
    /// Whether nonempty content refers to external objects.
    #[must_use]
    pub const fn is_external(&self) -> bool {
        self.external
    }
}
