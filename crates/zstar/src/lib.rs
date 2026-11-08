//! Format primitives, blocking I/O, and async I/O for indexed tar archives.
//!
//! Storage sources and external object resolvers keep transport policy separate
//! from the archive format.

#![doc = include_str!("../README.md")]

/// Local archive sources, capture, and restoration. Requires the `fs` feature.
#[cfg(feature = "fs")]
pub mod fs;
/// HTTP range sources and external-object resolution. Requires the `http` feature.
#[cfg(feature = "http")]
pub mod http;
/// S3 storage and presigned requests. Requires the `s3` feature.
#[cfg(feature = "s3")]
pub mod s3;

use thiserror::Error;
/// Internal support for bindings and CLI operations. This module is public for
/// workspace consumers and is not part of the supported SDK interface.
#[doc(hidden)]
pub mod backend;
#[cfg(any(feature = "fs", test))]
mod capture;
mod cbor;
pub mod chunking;
mod decoded_body;
pub(crate) mod frame;
mod writer_engine;
#[cfg(test)]
pub(crate) use frame::decode_bounded;
pub(crate) use frame::decode_bounded_with_limits;
/// Blocking archives, builders, and storage interfaces; available without Tokio.
pub mod blocking;
mod budget;
mod header;
mod limits;
mod metadata;
pub(crate) mod options;
pub(crate) mod read_plan;
mod report;
mod request;
pub use budget::{BudgetError, BudgetResource};
pub use header::Header;
pub use limits::Limits;
pub use metadata::EntryMetadata;
pub use options::{MetadataReaderOptions, MetadataWriterOptions, WriterOptions};
pub use report::ReadReport;
pub use request::{MAX_BATCH_REQUESTS, ReadRequest};
#[expect(
    clippy::redundant_pub_crate,
    reason = "Crate-only helpers must stay out of backend's public manifest reexports."
)]
mod manifest;
mod manifest_io;
pub use manifest::EntryKind;
/// Advanced physical member layouts for external content and format tooling.
pub mod layout {
    pub use crate::manifest::{Entry, Extent, Source};
}
pub(crate) use layout::{Entry, Extent, Source};
pub type Extensions = std::collections::BTreeMap<String, ciborium::Value>;
#[cfg(feature = "async")]
mod archive;
#[cfg(feature = "async")]
mod builder;
#[cfg(feature = "async")]
mod file;
#[cfg(feature = "async")]
mod io;
#[cfg(feature = "async")]
mod range_cache;
#[cfg(feature = "async")]
pub use archive::{Archive, ObjectResolver, ReaderOptions};
#[cfg(feature = "async")]
pub use builder::Builder;
#[cfg(feature = "async")]
pub use file::File;
#[cfg(feature = "async")]
pub use io::AsyncRangeReader;
#[cfg(feature = "async")]
pub use range_cache::{CachedRangeReader, RangeCacheOptions, RangeCacheStats};
mod parallel;
mod primitives;
#[cfg(any(test, feature = "s3"))]
pub(crate) use primitives::{Digest, encode_frame};
mod tar;
mod tar_encode;
pub(crate) use manifest_io::ManifestOptions;
#[cfg(feature = "async")]
pub(crate) use manifest_io::ManifestReader;
pub(crate) use primitives::{
    FOOTER_SIZE, Footer, RangeReader, TOC_MAGIC, checked_range, encode_footer, parse_footer,
    verify_sha256,
};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("local resource budget exceeded: {0}")]
    Budget(#[source] BudgetError),
    #[error("invalid option {option}: {reason}")]
    InvalidOptions {
        option: &'static str,
        reason: &'static str,
    },
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),
    #[error("{path}: {source}")]
    Filesystem {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("archive is not a Zstar file")]
    NotZstar,
    #[error("malformed footer: {0}")]
    MalformedFooter(&'static str),
    #[error("malformed TOC: {0}")]
    MalformedToc(String),
    #[error("malformed frame: {0}")]
    MalformedFrame(String),
    #[error("malformed tar: {0}")]
    MalformedTar(&'static str),
    #[error("tar and TOC disagree: {0}")]
    TarTocMismatch(&'static str),
    #[error("integrity check failed")]
    Integrity,
    #[error("archive entry is missing: {0}")]
    MissingEntry(String),
    #[error("requested range is outside the file")]
    Range,
    #[error("external object is missing")]
    MissingObject([u8; 32]),
    #[error("unsupported version: {0}")]
    UnsupportedVersion(&'static str),
    #[error("unknown required feature")]
    UnsupportedFeature,
    #[error("malformed pointer: {0}")]
    MalformedPointer(String),
    #[error("malformed external content: {0}")]
    MalformedExternalContent(String),
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid UTF-8 or CBOR: {0}")]
    Encoding(String),
}

impl Error {
    /// Creates a typed resource rejection with operation context.
    #[must_use]
    pub const fn budget(resource: BudgetResource, context: &'static str) -> Self {
        Self::Budget(BudgetError::new(resource, context))
    }
    /// Creates a typed resource rejection with numeric accounting.
    #[must_use]
    pub const fn budget_exceeded(
        resource: BudgetResource,
        context: &'static str,
        limit: u64,
        requested: u64,
    ) -> Self {
        Self::Budget(BudgetError::exceeded(resource, context, limit, requested))
    }
    /// Structural copy for sharing one failure among waiters. I/O errors keep
    /// their kind and message but not their source chain.
    #[cfg(feature = "async")]
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            Self::Budget(m) => Self::Budget(*m),
            Self::InvalidOptions { option, reason } => Self::InvalidOptions { option, reason },
            Self::InvalidInput(m) => Self::InvalidInput(m),
            Self::Filesystem { path, source } => Self::Filesystem {
                path: path.clone(),
                source: std::io::Error::new(source.kind(), source.to_string()),
            },
            Self::NotZstar => Self::NotZstar,
            Self::MalformedFooter(m) => Self::MalformedFooter(m),
            Self::MalformedToc(m) => Self::MalformedToc(m.clone()),
            Self::MalformedFrame(m) => Self::MalformedFrame(m.clone()),
            Self::MalformedTar(m) => Self::MalformedTar(m),
            Self::TarTocMismatch(m) => Self::TarTocMismatch(m),
            Self::Integrity => Self::Integrity,
            Self::MissingEntry(m) => Self::MissingEntry(m.clone()),
            Self::Range => Self::Range,
            Self::MissingObject(a) => Self::MissingObject(*a),
            Self::UnsupportedVersion(m) => Self::UnsupportedVersion(m),
            Self::UnsupportedFeature => Self::UnsupportedFeature,
            Self::MalformedPointer(m) => Self::MalformedPointer(m.clone()),
            Self::MalformedExternalContent(m) => Self::MalformedExternalContent(m.clone()),
            Self::Unsupported(m) => Self::Unsupported(m),
            Self::Io(e) => Self::Io(std::io::Error::new(e.kind(), e.to_string())),
            Self::Encoding(m) => Self::Encoding(m.clone()),
        }
    }
    /// Stable machine-readable rejection category, shared with the corpus.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::Budget(_) => "resource-limit",
            Self::InvalidOptions { .. } => "invalid-options",
            Self::InvalidInput(_) => "invalid-input",
            Self::NotZstar => "not-zstar",
            Self::MalformedFooter(_) => "malformed-footer",
            Self::MalformedToc(_) | Self::Encoding(_) => "malformed-toc",
            Self::MalformedFrame(_) => "malformed-frame",
            Self::MalformedTar(_) => "malformed-tar",
            Self::TarTocMismatch(_) => "tar-toc-mismatch",
            Self::Integrity => "integrity-failure",
            Self::UnsupportedVersion(_) => "unsupported-version",
            Self::UnsupportedFeature => "unsupported-feature",
            Self::MalformedPointer(_) => "malformed-pointer",
            Self::MalformedExternalContent(_) => "malformed-external-content",
            Self::MissingObject(_) => "missing-external-content",
            Self::Range => "invalid-range",
            Self::MissingEntry(_) => "missing-entry",
            Self::Unsupported(_) => "unsupported-operation",
            Self::Io(_) | Self::Filesystem { .. } => "io",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for std::io::Error {
    fn from(error: Error) -> Self {
        use std::io::ErrorKind;
        if let Error::Io(error) = error {
            return error;
        }
        let kind = match &error {
            Error::Filesystem { source, .. } => source.kind(),
            Error::InvalidOptions { .. } | Error::InvalidInput(_) | Error::Range => {
                ErrorKind::InvalidInput
            }
            Error::MissingEntry(_) | Error::MissingObject(_) => ErrorKind::NotFound,
            Error::Unsupported(_) => ErrorKind::Unsupported,
            Error::Budget(_) => ErrorKind::Other,
            _ => ErrorKind::InvalidData,
        };
        Self::new(kind, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_checks_are_overflow_safe() {
        assert_eq!(checked_range(2, 3, 5).unwrap(), 2..5);
        assert!(checked_range(6, 0, 5).is_err());
        assert!(checked_range(u64::MAX, 1, u64::MAX).is_err());
    }

    #[test]
    fn digest_verification() {
        let digest = *Digest::sha256(b"zstar").as_bytes();
        assert!(verify_sha256(b"zstar", digest).is_ok());
        assert!(verify_sha256(b"other", digest).is_err());
    }

    #[test]
    fn decoded_frames_are_compact_and_still_check_bounds_and_integrity() {
        for size in [65_536, 1_048_576] {
            let raw = vec![7; size];
            let encoded = encode_frame(&raw, 3).unwrap();
            assert_ne!(encoded[4] & 4, 0);
            let decoded = decode_bounded(&encoded, 16 << 20).unwrap();
            assert_eq!(decoded, raw);
            assert_eq!(decoded.capacity(), size);
            assert!(decode_bounded(&encoded, size - 1).is_err());
            let mut corrupt = encoded;
            *corrupt.last_mut().unwrap() ^= 1;
            assert!(decode_bounded(&corrupt, 16 << 20).is_err());
        }
        let raw = vec![7; 65_536];
        let encoded = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&encoded).unwrap(),
            None
        );
        let decoded = decode_bounded(&encoded, 16 << 20).unwrap();
        assert_eq!(decoded, raw);
        assert_eq!(decoded.capacity(), raw.len());
        assert!(decode_bounded(&encoded, raw.len() - 1).is_err());
    }
}

#[cfg(feature = "async")]
pub(crate) use manifest_io::ManifestDirectory;

#[cfg(feature = "async")]
mod paged_archive;
