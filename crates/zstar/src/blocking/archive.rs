//! Supported eager blocking archive facade.
use super::engine::BlockingArchive;
use super::{File, MemorySource, ObjectResolver, RangeReader};
use crate::manifest_io::{EntryCursor, ManifestReader};
use crate::{
    Entry, EntryMetadata, Error, Extensions, Limits, MetadataReaderOptions, ReadReport,
    ReadRequest, Result,
};
use std::ops::Range;

/// Blocking metadata, allocation, and verification policy.
///
/// Metadata is validated eagerly on open. Sources and resolvers are used
/// sequentially, and have no `Send`/`Sync` requirement.
pub struct ReaderOptions {
    /// Metadata parsing, retained index, and temporary storage budgets.
    pub metadata: MetadataReaderOptions,
    /// Frame, window, entry, extent, and pointer limits. Zero disables a limit.
    pub limits: Limits,
    /// Maximum returned bytes per allocating read or batch; defaults to 64 MiB.
    pub max_read_bytes: u64,
    /// Verify decoded external objects against their SHA-256 addresses.
    /// Frame checksums and structural validation are always mandatory.
    pub verify_external: bool,
    /// Reader-scoped external object resolver.
    pub resolver: Option<Box<dyn ObjectResolver>>,
}
impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            metadata: MetadataReaderOptions::default(),
            limits: Limits::default(),
            max_read_bytes: crate::options::DEFAULT_MAX_READ_BYTES,
            verify_external: false,
            resolver: None,
        }
    }
}
impl ReaderOptions {
    /// Finite frame, entry, metadata, and buffer policy with an 8 MiB returned-read budget.
    #[must_use]
    pub fn bounded() -> Self {
        Self {
            metadata: MetadataReaderOptions::bounded(),
            limits: Limits::bounded(),
            max_read_bytes: 8 << 20,
            ..Self::default()
        }
    }
    /// Checks configuration before reading source bytes.
    ///
    /// # Errors
    /// Returns an invalid-options error for zero read budgets or invalid metadata page limits.
    pub const fn validate(&self) -> Result<()> {
        if self.max_read_bytes == 0 {
            return Err(Error::InvalidOptions {
                option: "max_read_bytes",
                reason: "must be positive",
            });
        }
        self.metadata.validate()
    }
    pub(super) fn resolver(&mut self) -> Option<&mut dyn ObjectResolver> {
        self.resolver
            .as_mut()
            .map(|r| r.as_mut() as &mut dyn ObjectResolver)
    }
    crate::options::with_fields! { const
        /// Sets metadata parsing and storage policy.
        with_metadata(metadata: crate::MetadataReaderOptions);
    }
    crate::options::with_fields! { const
        /// Sets per-frame and per-entry resource limits.
        with_limits(limits: crate::Limits);
        /// Sets the maximum returned bytes per allocating read or batch.
        with_max_read_bytes(max_read_bytes: u64);
        /// Enables SHA-256 verification of decoded external objects.
        with_verify_external(verify_external: bool);
    }
    /// Sets the reader-wide external object resolver.
    #[must_use]
    pub fn with_resolver(mut self, value: Box<dyn ObjectResolver>) -> Self {
        self.resolver = Some(value);
        self
    }
}
impl std::fmt::Debug for ReaderOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderOptions")
            .field("metadata", &self.metadata)
            .field("limits", &self.limits)
            .field("max_read_bytes", &self.max_read_bytes)
            .field("verify_external", &self.verify_external)
            .field("resolver", &self.resolver.as_ref().map(|_| "configured"))
            .finish()
    }
}

/// An indexed archive using sequential standard I/O.
///
/// Open validates all metadata eagerly, unlike the root async reader's lazy
/// entry pages. Content checks are performed only when that content is read.
/// A member [`File`] exclusively borrows the archive until the handle is dropped.
pub struct Archive<R: RangeReader> {
    pub(crate) engine: BlockingArchive<R>,
    pub(crate) options: ReaderOptions,
}
impl<R: RangeReader> Archive<R> {
    /// Opens an immutable source and validates all metadata.
    ///
    /// # Errors
    /// Returns source, format, metadata validation, or budget errors.
    pub fn open(source: R) -> Result<Self> {
        Self::open_with_options(source, ReaderOptions::default())
    }
    /// Opens and validates a source using the selected policies.
    ///
    /// # Errors
    /// Returns source, format, metadata validation, or budget errors.
    pub fn open_with_options(source: R, options: ReaderOptions) -> Result<Self> {
        options.validate()?;
        let manifest = crate::options::reader_manifest_options(
            &options.metadata,
            options.limits,
            options.max_read_bytes,
        );
        Ok(Self {
            engine: BlockingArchive::open(source, manifest)?,
            options,
        })
    }
    /// Returns archive-level extension values.
    #[must_use]
    pub const fn root_extensions(&self) -> &Extensions {
        &self.engine.manifest().root().extensions
    }
    /// Returns the number of indexed entries.
    #[must_use]
    pub const fn entry_count(&self) -> u64 {
        self.engine.manifest().root().entry_count
    }
    /// Returns an owned metadata snapshot, or `None` for a missing path.
    ///
    /// # Errors
    /// Returns metadata access errors.
    pub fn entry(&self, path: &str) -> Result<Option<EntryMetadata>> {
        self.entry_layout(path)?
            .as_ref()
            .map(EntryMetadata::from_entry)
            .transpose()
    }
    /// Returns a validated physical layout for advanced format tooling.
    ///
    /// # Errors
    /// Returns metadata access errors.
    pub fn entry_layout(&self, path: &str) -> Result<Option<Entry>> {
        Ok(self.engine.manifest().entry(path))
    }
    /// All metadata was validated by [`Self::open`]. Content is checked when read.
    ///
    /// # Errors
    /// This eager reader has no remaining metadata validation work.
    pub const fn validate_all_metadata(&self) -> Result<()> {
        Ok(())
    }
    /// Enumerates owned snapshots in UTF-8 path order with bounded iteration.
    #[must_use]
    pub fn entries(&self) -> Entries<'_> {
        self.entries_with_prefix("")
    }
    /// Enumerates paths beginning with the exact string prefix.
    #[must_use]
    pub fn entries_with_prefix(&self, prefix: &str) -> Entries<'_> {
        Entries {
            manifest: self.engine.manifest(),
            cursor: self.engine.manifest().cursor_prefix(prefix),
            failed: false,
        }
    }
    /// Reads a regular file within the allocating-read budget.
    ///
    /// # Errors
    /// Returns missing-entry, wrong-kind, budget, source, or validation errors.
    pub fn read(&mut self, path: &str) -> Result<Vec<u8>> {
        self.read_with_report(path).map(|(bytes, _)| bytes)
    }
    /// Reads a complete regular file and reports performed checks.
    ///
    /// # Errors
    /// Returns missing-entry, wrong-kind, budget, source, or validation errors.
    pub fn read_with_report(&mut self, path: &str) -> Result<(Vec<u8>, ReadReport)> {
        let size = self
            .entry(path)?
            .ok_or_else(|| Error::MissingEntry(path.into()))?
            .size();
        self.read_range_with_report(path, 0..size)
    }
    /// Reads an exact logical range. Empty ranges still validate path and kind.
    ///
    /// # Errors
    /// Returns missing-entry, wrong-kind, range, budget, source, or validation errors.
    pub fn read_range(&mut self, path: &str, range: Range<u64>) -> Result<Vec<u8>> {
        self.read_range_with_report(path, range)
            .map(|(bytes, _)| bytes)
    }
    /// Reads a range and reports the checks performed on selected content.
    ///
    /// # Errors
    /// Returns the same errors as [`Self::read_range`].
    pub fn read_range_with_report(
        &mut self,
        path: &str,
        range: Range<u64>,
    ) -> Result<(Vec<u8>, ReadReport)> {
        let offset = range.start;
        let length = range.end.checked_sub(offset).ok_or(Error::Range)?;
        let (mut bytes, report) = self.execute(&[ReadRequest {
            path,
            offset,
            length,
        }])?;
        Ok((bytes.remove(0), report))
    }
    /// Reads ranges in request order, fetching each selected source once.
    /// All metadata selections and aggregate output budgets precede content I/O.
    ///
    /// # Errors
    /// Returns request, metadata, budget, source, or content-validation errors.
    pub fn read_many(&mut self, requests: &[ReadRequest<'_>]) -> Result<Vec<Vec<u8>>> {
        self.execute(requests).map(|(bytes, _)| bytes)
    }
    fn execute(&mut self, requests: &[ReadRequest<'_>]) -> Result<(Vec<Vec<u8>>, ReadReport)> {
        let plan = self.engine.plan(requests)?;
        let verify_external = self.options.verify_external;
        self.engine
            .execute_plan(plan, self.options.resolver(), verify_external)
    }
    /// Returns the exact target of a symlink without following it.
    ///
    /// # Errors
    /// Returns an error for a missing entry or a non-symlink.
    pub fn read_link(&self, path: &str) -> Result<String> {
        self.entry_layout(path)?
            .ok_or_else(|| Error::MissingEntry(path.into()))?
            .linkname
            .ok_or(Error::Unsupported("entry is not a symlink"))
    }
    /// Borrows a regular member for bounded standard `Read + Seek` operations.
    ///
    /// # Errors
    /// Returns an error for a missing/non-file member or cursor allocation.
    pub fn open_file(&mut self, path: &str) -> Result<File<'_, R>> {
        let entry = self
            .entry_layout(path)?
            .ok_or_else(|| Error::MissingEntry(path.into()))?;
        File::new(self, entry)
    }
}
impl<B: AsRef<[u8]>> Archive<MemorySource<B>> {
    /// Opens immutable memory bytes using default reader options.
    ///
    /// # Errors
    /// Returns metadata format, validation, or budget errors.
    pub fn from_bytes(bytes: B) -> Result<Self> {
        Self::open(MemorySource::new(bytes))
    }
}

impl<R: std::io::Read + std::io::Seek> Archive<super::SeekSource<R>> {
    /// Opens a standard seekable reader, recording its immutable size once.
    ///
    /// # Errors
    /// Returns seek, format, validation, or resource-limit errors.
    pub fn from_reader(reader: R) -> Result<Self> {
        Self::from_reader_with_options(reader, ReaderOptions::default())
    }
    /// Opens a seekable reader with explicit reader policy.
    ///
    /// # Errors
    /// Returns seek, format, validation, or resource-limit errors.
    pub fn from_reader_with_options(reader: R, options: ReaderOptions) -> Result<Self> {
        Self::open_with_options(super::SeekSource::new(reader)?, options)
    }
}

#[cfg(feature = "fs")]
impl Archive<crate::fs::LocalFileSource> {
    /// Opens an immutable local archive path.
    ///
    /// # Errors
    /// Returns filesystem, format, validation, or resource-limit errors.
    pub fn open_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::open_path_with_options(path, ReaderOptions::default())
    }
    /// Opens an immutable local path with explicit reader policy.
    ///
    /// # Errors
    /// Returns filesystem, format, validation, or resource-limit errors.
    pub fn open_path_with_options(
        path: impl AsRef<std::path::Path>,
        options: ReaderOptions,
    ) -> Result<Self> {
        Self::open_with_options(crate::fs::LocalFileSource::open(path)?, options)
    }
}

/// A fallible iterator over owned metadata snapshots.
pub struct Entries<'a> {
    manifest: &'a ManifestReader,
    cursor: EntryCursor,
    failed: bool,
}
impl Iterator for Entries<'_> {
    type Item = Result<EntryMetadata>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.manifest.next_entry(&mut self.cursor) {
            Ok(Some(entry)) => Some(EntryMetadata::from_entry(&entry)),
            Ok(None) => {
                self.failed = true;
                None
            }
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}
impl std::iter::FusedIterator for Entries<'_> {}
