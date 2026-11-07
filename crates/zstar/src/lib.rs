//! Format primitives and async I/O for indexed tar archives.
//!
//! Storage sources and external object resolvers keep transport policy separate
//! from the archive format.

#![cfg_attr(feature = "async", doc = include_str!("../README.md"))]

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
#[cfg(any(feature = "fs", all(test, feature = "disk")))]
mod capture;
mod cbor;
pub mod chunking;
mod decoded_body;
mod limits;
mod options;
pub use limits::Limits;
pub use options::{MetadataReaderOptions, MetadataWriterOptions, SpoolOptions};
#[expect(
    clippy::redundant_pub_crate,
    reason = "Crate-only helpers must stay out of backend's public manifest reexports."
)]
mod manifest;
mod manifest_io;
pub use manifest::{Entry, EntryKind, Extent, Source};
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
pub use archive::{Archive, ObjectResolver, ReadReport, ReaderOptions};
#[cfg(feature = "async")]
pub use builder::{Builder, Header, WriterOptions};
#[cfg(feature = "async")]
pub use file::{File, FileCursor};
#[cfg(feature = "async")]
pub use io::{AsyncRangeReader, MAX_BATCH_REQUESTS, ReadRequest};
#[cfg(feature = "async")]
pub use range_cache::{CachedRangeReader, RangeCacheOptions, RangeCacheStats};
mod parallel;
mod primitives;
#[cfg(test)]
use primitives::SliceReader;
mod tar;
mod tar_encode;
pub(crate) use manifest_io::{ManifestOptions, ManifestReader};
pub use manifest_io::{SpillFactory, SpillStore, SpoolBudget};
pub(crate) use primitives::{
    Digest, FOOTER_SIZE, Footer, RangeReader, TOC_MAGIC, checked_range, encode_footer,
    encode_frame, parse_footer, verify_sha256,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("local resource budget exceeded: {0}")]
    Budget(&'static str),
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
    /// Structural copy for sharing one failure among waiters. I/O errors keep
    /// their kind and message but not their source chain.
    #[cfg(feature = "async")]
    pub(crate) fn duplicate(&self) -> Self {
        match self {
            Self::Budget(m) => Self::Budget(m),
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
            Self::Io(_) => "io",
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
            Error::MissingEntry(_) | Error::MissingObject(_) => ErrorKind::NotFound,
            Error::Range => ErrorKind::InvalidInput,
            Error::Unsupported(_) => ErrorKind::Unsupported,
            Error::Budget(_) => ErrorKind::Other,
            _ => ErrorKind::InvalidData,
        };
        Self::new(kind, error)
    }
}

pub(crate) fn decode_frame_into(
    encoded: &[u8],
    expected_size: usize,
    decoded: &mut Vec<u8>,
    context: &mut zstd::zstd_safe::DCtx<'static>,
) -> Result<()> {
    if expected_size == 0 {
        return Err(Error::MalformedFrame("frame size limit".into()));
    }
    let start = decoded.len();
    decode_bounded_into(encoded, expected_size, decoded, context, Limits::default())?;
    if decoded.len() - start != expected_size {
        return Err(Error::MalformedFrame("decoded size mismatch".into()));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn decode_bounded(encoded: &[u8], limit: usize) -> Result<Vec<u8>> {
    decode_bounded_with_limits(encoded, limit, Limits::default())
}
pub(crate) fn decode_bounded_with_limits(
    encoded: &[u8],
    limit: usize,
    limits: Limits,
) -> Result<Vec<u8>> {
    limits.frame(encoded)?;
    let mut decoded = Vec::new();
    let mut context = zstd::zstd_safe::DCtx::create();
    decode_bounded_into(encoded, limit, &mut decoded, &mut context, limits)?;
    decoded.shrink_to_fit();
    Ok(decoded)
}

// The streaming codec exposes its stable Zstandard error name through io::Error.
fn frame_decode_error(error: &std::io::Error) -> Error {
    if error.to_string() == "Restored data doesn't match checksum" {
        Error::Integrity
    } else {
        Error::MalformedFrame(error.to_string())
    }
}

fn decode_bounded_into(
    encoded: &[u8],
    limit: usize,
    decoded: &mut Vec<u8>,
    context: &mut zstd::zstd_safe::DCtx<'static>,
    limits: Limits,
) -> Result<()> {
    use std::io::Read;
    if !encoded.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
        || zstd::zstd_safe::find_frame_compressed_size(encoded)
            .map_err(|_| Error::MalformedFrame("invalid frame".into()))?
            != encoded.len()
    {
        return Err(Error::MalformedFrame(
            "expected exactly one Zstandard frame".into(),
        ));
    }
    if zstd::zstd_safe::get_dict_id_from_frame(encoded).is_some() {
        return Err(Error::MalformedFrame("dictionary identifier".into()));
    }
    let declared = zstd::zstd_safe::get_frame_content_size(encoded)
        .map_err(|_| Error::MalformedFrame("invalid content size".into()))?;
    if declared.is_some_and(|size| size > limit as u64) {
        return Err(Error::Budget("decoded frame size"));
    }
    context
        .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
        .map_err(|e| Error::MalformedFrame(zstd::zstd_safe::get_error_name(e).into()))?;
    let mut frame_decoder =
        zstd::stream::read::Decoder::with_context(encoded, context).single_frame();
    let window_log = if limits.max_window_bytes == 0 {
        31
    } else {
        // Zstandard accepts powers of two; inspect the exact declared window
        // separately so a non-power-of-two budget is not rounded up.
        (limits.max_window_bytes.max(1024) - 1).bit_width()
    };
    frame_decoder
        .window_log_max(window_log.min(31))
        .map_err(|error| frame_decode_error(&error))?;
    let start = decoded.len();
    if let Some(size) = declared {
        let size = usize::try_from(size).map_err(|_| Error::Budget("decoded frame size"))?;
        decoded
            .try_reserve_exact(size)
            .map_err(|_| Error::Budget("decoded frame allocation"))?;
        decoded.resize(start + size, 0);
        frame_decoder
            .read_exact(&mut decoded[start..])
            .map_err(|error| frame_decode_error(&error))?;
        // Read through EOF to verify the checksum without growing the output
        // allocation just to discover that a full frame has ended.
        let mut extra = [0];
        if frame_decoder
            .read(&mut extra)
            .map_err(|error| frame_decode_error(&error))?
            != 0
        {
            return Err(Error::MalformedFrame("decoded size mismatch".into()));
        }
    } else {
        frame_decoder
            .take(limit as u64 + 1)
            .read_to_end(decoded)
            .map_err(|error| frame_decode_error(&error))?;
    }
    if decoded.len() - start > limit {
        return Err(Error::Budget("decoded frame size"));
    }
    Ok(())
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
