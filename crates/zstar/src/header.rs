//! Shared writer input metadata.
use crate::{Entry, Extensions};

/// Metadata for an entry to append with [`crate::blocking::Builder`].
///
/// The encoder assigns extents; this header describes the input file and its
/// declared size. Directory and symlink headers have size zero.
#[derive(Clone, Debug)]
pub struct Header {
    pub(crate) entry: Entry,
    pub(crate) size: u64,
}
impl Header {
    pub(crate) fn external_entry(mut self, extents: Vec<crate::Extent>) -> crate::Result<Entry> {
        if self.entry.kind != crate::EntryKind::File {
            return Err(crate::Error::Unsupported(
                "external content requires a file header",
            ));
        }
        self.entry.extents = extents;
        if self.entry.size()? != self.size {
            return Err(crate::Error::MalformedExternalContent(
                "external content length differs from header".into(),
            ));
        }
        Ok(self.entry)
    }
    /// Creates a regular-file header with a declared byte length.
    pub fn file(path: impl Into<String>, size: u64) -> Self {
        Self {
            entry: Entry::file(path),
            size,
        }
    }
    /// Creates a directory header with mode `0o755`.
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            entry: Entry::directory(path),
            size: 0,
        }
    }
    /// Creates a symlink header with mode `0o777` and an exact target string.
    pub fn symlink(path: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            entry: Entry::symlink(path, target),
            size: 0,
        }
    }
    /// Returns the archive-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.entry.path
    }
    /// Returns the declared payload length in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }
    /// Sets the Unix permission bits.
    pub const fn set_mode(&mut self, mode: u64) {
        self.entry.mode = mode;
    }
    /// Sets the user ID recorded in the archive.
    pub const fn set_uid(&mut self, uid: u64) {
        self.entry.uid = uid;
    }
    /// Sets the group ID recorded in the archive.
    pub const fn set_gid(&mut self, gid: u64) {
        self.entry.gid = gid;
    }
    /// Sets modification time as signed nanoseconds since the Unix epoch.
    pub const fn set_mtime_ns(&mut self, timestamp: i64) {
        self.entry.mtime_ns = timestamp;
    }
    /// Replaces the entry's `x-*` CBOR extension values.
    pub fn set_attributes(&mut self, attributes: Extensions) {
        self.entry.attributes = attributes;
    }
    crate::options::with_fields! { const
        /// Sets recorded Unix permissions.
        with_mode(entry.mode: u64);
        /// Sets the recorded user ID.
        with_uid(entry.uid: u64);
        /// Sets the recorded group ID.
        with_gid(entry.gid: u64);
        /// Sets signed nanoseconds since the Unix epoch.
        with_mtime_ns(entry.mtime_ns: i64);
    }
    crate::options::with_fields! {
        /// Sets entry-level extension values.
        with_attributes(entry.attributes: Extensions);
    }
}
