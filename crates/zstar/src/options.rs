//! Focused metadata configuration for the ordinary reader and writer interfaces.
use crate::{Extensions, ManifestOptions};

// Keep fluent field setters consistent without repeating their implementation.
macro_rules! with_fields {
    (const $(
        $(#[$doc:meta])* $method:ident($($field:ident).+: $ty:ty);
    )*) => {
        $(
            $(#[$doc])*
            #[must_use]
            pub const fn $method(mut self, value: $ty) -> Self {
                self.$($field).+ = value;
                self
            }
        )*
    };
    ($(
        $(#[$doc:meta])* $method:ident($($field:ident).+: $ty:ty);
    )*) => {
        $(
            $(#[$doc])*
            #[must_use]
            pub fn $method(mut self, value: $ty) -> Self {
                self.$($field).+ = value;
                self
            }
        )*
    };
}
pub(crate) use with_fields;

/// Metadata page and retained-index budgets. Content limits belong to `Limits`.
#[derive(Clone, Debug)]
pub struct MetadataReaderOptions {
    /// Maximum encoded or decoded bytes in one metadata page, including the directory.
    pub max_page_bytes: u64,
    /// Maximum retained directory/cache/index bytes, and temporary full-scan buffer bytes.
    pub max_retained_entry_bytes: u64,
    /// Maximum entry count; zero disables this limit.
    pub max_entries: u64,
}
impl Default for MetadataReaderOptions {
    fn default() -> Self {
        let options = ManifestOptions::default();
        Self {
            max_page_bytes: options.max_page_bytes,
            max_retained_entry_bytes: options.max_retained_entry_bytes,
            max_entries: options.max_entries,
        }
    }
}
impl MetadataReaderOptions {
    /// Limits pages to 8 MiB, retained metadata to 128 MiB, and entries to one million.
    #[must_use]
    pub fn bounded() -> Self {
        Self {
            max_retained_entry_bytes: 128 << 20,
            max_entries: 1_000_000,
            ..Self::default()
        }
    }
    /// Checks configuration without reading the source.
    ///
    /// # Errors
    /// Returns an invalid-options error for a zero page limit.
    pub const fn validate(&self) -> crate::Result<()> {
        validate_page_limit(self.max_page_bytes)
    }
    pub(crate) const fn apply(self, options: &mut ManifestOptions) {
        options.max_page_bytes = self.max_page_bytes;
        options.max_buffer_bytes = self.max_retained_entry_bytes;
        options.max_retained_entry_bytes = self.max_retained_entry_bytes;
        options.max_entries = self.max_entries;
    }
    with_fields! { const
        /// Sets the maximum encoded or decoded size of one metadata page.
        with_max_page_bytes(max_page_bytes: u64);
        /// Sets the retained directory/cache/index and full-scan buffer budget.
        with_max_retained_entry_bytes(max_retained_entry_bytes: u64);
        /// Sets the maximum entry count; zero disables this limit.
        with_max_entries(max_entries: u64);
    }
}

/// Metadata page encoding and in-memory working buffers for writers.
#[derive(Clone, Debug)]
pub struct MetadataWriterOptions {
    /// Maximum encoded or decoded bytes in one metadata page, including the directory.
    pub max_page_bytes: u64,
    /// Maximum bytes in each temporary metadata buffer or sorting index.
    pub max_buffer_bytes: u64,
    /// Target decoded page size, at least 512 bytes and at most `max_page_bytes`.
    pub block_target: usize,
    pub extension_pages: std::collections::BTreeMap<String, Vec<ciborium::Value>>,
}
impl Default for MetadataWriterOptions {
    fn default() -> Self {
        let options = ManifestOptions::default();
        Self {
            max_page_bytes: options.max_page_bytes,
            max_buffer_bytes: options.max_buffer_bytes,
            block_target: options.block_target,
            extension_pages: options.extension_pages,
        }
    }
}
impl MetadataWriterOptions {
    /// Limits pages to 8 MiB and each temporary buffer to 64 MiB.
    #[must_use]
    pub fn bounded() -> Self {
        Self {
            max_buffer_bytes: 64 << 20,
            ..Self::default()
        }
    }
    /// Checks metadata page encoding configuration.
    ///
    /// # Errors
    /// Returns an invalid-options error for invalid page targets or limits.
    pub const fn validate(&self) -> crate::Result<()> {
        if self.block_target < 512 || self.block_target as u64 > self.max_page_bytes {
            return Err(crate::Error::InvalidOptions {
                option: "metadata.block_target",
                reason: "must be at least 512 bytes and at most max_page_bytes",
            });
        }
        validate_page_limit(self.max_page_bytes)
    }
    pub(crate) fn apply(self, options: &mut ManifestOptions) {
        options.max_page_bytes = self.max_page_bytes;
        options.max_buffer_bytes = self.max_buffer_bytes;
        options.block_target = self.block_target;
        options.extension_pages = self.extension_pages;
    }
    with_fields! { const
        /// Sets the maximum encoded or decoded size of one metadata page.
        with_max_page_bytes(max_page_bytes: u64);
        /// Sets the byte limit for each temporary metadata buffer or sorting index.
        with_max_buffer_bytes(max_buffer_bytes: u64);
        /// Sets the target decoded metadata page size, at least 512 bytes.
        with_block_target(block_target: usize);
    }
}

const fn validate_page_limit(max_page_bytes: u64) -> crate::Result<()> {
    if max_page_bytes == 0 {
        return Err(crate::Error::InvalidOptions {
            option: "metadata.max_page_bytes",
            reason: "must be positive",
        });
    }
    Ok(())
}

/// Compression, metadata, and storage options for a [`crate::blocking::Builder`].
///
/// Reader-only metadata settings are unavailable on writers:
/// ```compile_fail
/// let options = zstar::WriterOptions::default();
/// let _ = options.metadata.max_entries;
/// ```
#[derive(Clone, Debug)]
pub struct WriterOptions {
    /// Decoded data-frame target: at least 512 bytes, subject to manifest limits.
    pub frame_size: usize,
    /// Zstandard compression level; defaults to 3.
    pub compression_level: i32,
    /// Compression worker count, from 0 (serial) to 16.
    pub workers: usize,
    /// Metadata encoding and temporary storage policy.
    pub metadata: crate::MetadataWriterOptions,
    /// Per-source frame/window and entry/extent/pointer limits. Zero disables each limit.
    pub limits: crate::Limits,
    /// Archive-level `x-*` CBOR values.
    pub root_extensions: Extensions,
}
impl WriterOptions {
    /// Finite frame, entry, metadata page, and buffer budgets with default compression.
    #[must_use]
    pub fn bounded() -> Self {
        Self {
            limits: crate::Limits::bounded(),
            metadata: MetadataWriterOptions::bounded(),
            ..Self::default()
        }
    }
    /// Checks configuration before allocating codec state or writing output.
    ///
    /// # Errors
    /// Returns invalid-options, resource-limit, or root-extension validation errors.
    pub fn validate(&self) -> crate::Result<()> {
        if self.frame_size < 512 {
            return Err(crate::Error::InvalidOptions {
                option: "frame_size",
                reason: "must be at least 512 bytes",
            });
        }
        if self.workers > 16 {
            return Err(crate::Error::InvalidOptions {
                option: "workers",
                reason: "must be between 0 and 16",
            });
        }
        if !zstd::compression_level_range().contains(&self.compression_level) {
            return Err(crate::Error::InvalidOptions {
                option: "compression_level",
                reason: "outside the supported Zstandard range",
            });
        }
        self.metadata.validate()?;
        crate::Limits::check(
            self.frame_size as u64,
            self.limits.max_frame_decoded_bytes,
            "writer frame size",
            crate::BudgetResource::DecodedFrameBytes,
        )?;
        crate::manifest::ManifestRoot {
            requires: Vec::new(),
            entry_count: 0,
            extensions: self.root_extensions.clone(),
        }
        .validate()
    }
    pub(crate) fn manifest_options(&self) -> ManifestOptions {
        let mut options = ManifestOptions {
            limits: self.limits,
            ..Default::default()
        };
        self.metadata.clone().apply(&mut options);
        options
    }
    with_fields! { const
        /// Sets the decoded data-frame target, at least 512 bytes.
        with_frame_size(frame_size: usize);
        /// Sets the Zstandard compression level.
        with_compression_level(compression_level: i32);
        /// Sets compression workers, from zero (serial) through sixteen.
        with_workers(workers: usize);
    }
    with_fields! {
        /// Sets metadata encoding and storage policy.
        with_metadata(metadata: MetadataWriterOptions);
    }
    with_fields! { const
        /// Sets per-frame and per-entry resource limits.
        with_limits(limits: crate::Limits);
    }
    with_fields! {
        /// Sets archive-level extension values.
        with_root_extensions(root_extensions: Extensions);
    }
}
impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            frame_size: 1 << 20,
            compression_level: 3,
            workers: 0,
            metadata: crate::MetadataWriterOptions::default(),
            limits: crate::Limits::default(),
            root_extensions: Extensions::default(),
        }
    }
}

/// Default maximum output of an allocating read or batch.
pub const DEFAULT_MAX_READ_BYTES: u64 = 64 << 20;

pub fn reader_manifest_options(
    metadata: &MetadataReaderOptions,
    limits: crate::Limits,
    max_read_bytes: u64,
) -> ManifestOptions {
    let mut options = ManifestOptions {
        limits,
        max_read_bytes,
        ..Default::default()
    };
    metadata.clone().apply(&mut options);
    options
}
