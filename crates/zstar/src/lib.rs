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

use sha2::{Digest as Sha2Digest, Sha256};
use std::ops::Range;
use thiserror::Error;
#[doc(hidden)]
pub mod backend;
pub mod capture;
mod cbor;
mod decoded_cache;
pub use decoded_cache::{
    CachedChunk, DecodedCache, DecodedCacheKey, MemoryDecodedCache, SharedDecodedCache,
};
pub mod chunking;
pub mod manifest;
pub mod manifest_io;
pub mod pack;
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
pub use archive::{
    Archive, ObjectRequest, ObjectResolver, ReadReport, ReaderOptions, VerificationReport,
    VerificationScope,
};
#[cfg(feature = "async")]
pub use builder::{Builder, Header, WriterOptions};
#[cfg(feature = "async")]
pub use file::{File, FileCursor};
#[cfg(feature = "async")]
pub use io::{AsyncRangeReader, MAX_BATCH_REQUESTS, ReadRequest};
mod parallel;
mod tar;
mod tar_encode;
pub use manifest_io::{
    EntryCursor, ManifestEncoder, ManifestOptions, ManifestReader, SpillFactory, SpillStore,
    SpoolBudget,
};
pub use tar::{TarHeader, parse_header_prefix as parse_member_header};

pub const FOOTER_SIZE: usize = 80;
pub const DATA_FRAME_MAX: usize = 16 * 1024 * 1024;
pub const FOOTER_MAGIC: u32 = 0x184D_2A5F;
pub const TOC_MAGIC: u32 = 0x184D_2A5E;

#[derive(Debug, Error)]
pub enum Error {
    #[error("malformed packed-object catalog: {0}")]
    MalformedCatalog(String),
    #[error("packed object snapshot changed")]
    ChangedPack,
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
            Self::MalformedCatalog(m) => Self::MalformedCatalog(m.clone()),
            Self::ChangedPack => Self::ChangedPack,
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
    pub fn category(&self) -> &'static str {
        match self {
            Self::MalformedCatalog(_) => "malformed-catalog",
            Self::ChangedPack => "changed-pack",
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

pub trait RangeReader {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Digest([u8; 32]);

impl Digest {
    #[must_use]
    pub fn sha256(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Verify a complete byte sequence against a SHA-256 digest.
pub fn verify_sha256(bytes: &[u8], expected: [u8; 32]) -> Result<()> {
    if Digest::sha256(bytes).0 == expected {
        Ok(())
    } else {
        Err(Error::Integrity)
    }
}

/// Validate a bounded half-open range without permitting integer wraparound.
pub fn checked_range(offset: u64, length: u64, size: u64) -> Result<Range<u64>> {
    if offset > size || length > size - offset {
        return Err(Error::Range);
    }
    Ok(offset..offset + length)
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Footer {
    pub toc_offset: u64,
    pub toc_encoded_len: u64,
    pub toc_decoded_len: u64,
    pub toc_sha256: [u8; 32],
}

/// Parse and validate the fixed 80-byte terminal footer.
pub fn parse_footer(bytes: &[u8]) -> Result<Footer> {
    if bytes.len() != FOOTER_SIZE {
        return Err(Error::MalformedFooter("footer must be 80 bytes"));
    }
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != FOOTER_MAGIC
        || u32::from_le_bytes(bytes[4..8].try_into().unwrap()) != 72
    {
        return Err(Error::NotZstar);
    }
    if &bytes[8..16] != b"ZSTAR001" {
        return Err(Error::NotZstar);
    }
    if u32::from_le_bytes(bytes[16..20].try_into().unwrap()) != 1 {
        return Err(Error::UnsupportedVersion("footer"));
    }
    if bytes[20..24] != [0; 4] {
        return Err(Error::MalformedFooter("reserved field"));
    }
    let toc_offset = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let toc_encoded_len = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
    let toc_decoded_len = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
    if toc_offset == 0 || toc_encoded_len <= 8 || toc_decoded_len == 0 {
        return Err(Error::MalformedFooter("invalid TOC bounds"));
    }
    let mut digest = [0; 32];
    digest.copy_from_slice(&bytes[48..80]);
    Ok(Footer {
        toc_offset,
        toc_encoded_len,
        toc_decoded_len,
        toc_sha256: digest,
    })
}

#[must_use]
pub fn encode_footer(footer: &Footer) -> [u8; FOOTER_SIZE] {
    let mut out = [0u8; FOOTER_SIZE];
    out[0..4].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
    out[4..8].copy_from_slice(&72u32.to_le_bytes());
    out[8..16].copy_from_slice(b"ZSTAR001");
    out[16..20].copy_from_slice(&1u32.to_le_bytes());
    out[24..32].copy_from_slice(&footer.toc_offset.to_le_bytes());
    out[32..40].copy_from_slice(&footer.toc_encoded_len.to_le_bytes());
    out[40..48].copy_from_slice(&footer.toc_decoded_len.to_le_bytes());
    out[48..80].copy_from_slice(&footer.toc_sha256);
    out
}

pub fn encode_frame(data: &[u8], level: i32) -> Result<Vec<u8>> {
    if data.is_empty() || data.len() > DATA_FRAME_MAX {
        return Err(Error::MalformedFrame("invalid decoded frame size".into()));
    }
    let mut encoder = zstd::bulk::Compressor::new(level)?;
    encoder.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
    encoder.compress(data).map_err(Error::Io)
}

pub fn decode_frame(encoded: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut context = zstd::zstd_safe::DCtx::create();
    decode_frame_into(encoded, expected_size, &mut decoded, &mut context)?;
    Ok(decoded)
}

pub(crate) fn decode_frame_into(
    encoded: &[u8],
    expected_size: usize,
    decoded: &mut Vec<u8>,
    context: &mut zstd::zstd_safe::DCtx<'static>,
) -> Result<()> {
    if expected_size == 0 || expected_size > DATA_FRAME_MAX || encoded.len() > 17 * 1024 * 1024 {
        return Err(Error::MalformedFrame("frame size limit".into()));
    }
    let start = decoded.len();
    decode_bounded_into(encoded, expected_size, decoded, context)?;
    if decoded.len() - start != expected_size {
        return Err(Error::MalformedFrame("decoded size mismatch".into()));
    }
    Ok(())
}

pub(crate) fn decode_bounded(encoded: &[u8], limit: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut context = zstd::zstd_safe::DCtx::create();
    decode_bounded_into(encoded, limit, &mut decoded, &mut context)?;
    decoded.shrink_to_fit();
    Ok(decoded)
}

fn decode_bounded_into(
    encoded: &[u8],
    limit: usize,
    decoded: &mut Vec<u8>,
    context: &mut zstd::zstd_safe::DCtx<'static>,
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
        return Err(Error::MalformedFrame("decoded output limit".into()));
    }
    context
        .reset(zstd::zstd_safe::ResetDirective::SessionOnly)
        .map_err(|e| Error::MalformedFrame(zstd::zstd_safe::get_error_name(e).into()))?;
    let mut frame_decoder =
        zstd::stream::read::Decoder::with_context(encoded, context).single_frame();
    frame_decoder
        .window_log_max(24)
        .map_err(|e| Error::MalformedFrame(e.to_string()))?;
    let start = decoded.len();
    if let Some(size) = declared {
        let size = usize::try_from(size).map_err(|_| Error::Budget("decoded frame size"))?;
        decoded
            .try_reserve_exact(size)
            .map_err(|_| Error::Budget("decoded frame allocation"))?;
        decoded.resize(start + size, 0);
        frame_decoder
            .read_exact(&mut decoded[start..])
            .map_err(|e| Error::MalformedFrame(e.to_string()))?;
        // Read through EOF to verify the checksum without growing the output
        // allocation just to discover that a full frame has ended.
        let mut extra = [0];
        if frame_decoder
            .read(&mut extra)
            .map_err(|e| Error::MalformedFrame(e.to_string()))?
            != 0
        {
            return Err(Error::MalformedFrame("decoded size mismatch".into()));
        }
    } else {
        frame_decoder
            .take(limit as u64 + 1)
            .read_to_end(decoded)
            .map_err(|e| Error::MalformedFrame(e.to_string()))?;
    }
    if decoded.len() - start > limit {
        return Err(Error::MalformedFrame("decoded output limit".into()));
    }
    Ok(())
}

pub struct SliceReader<'a> {
    bytes: &'a [u8],
}
impl<'a> SliceReader<'a> {
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}
impl RangeReader for SliceReader<'_> {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn read_range(&mut self, range: Range<u64>) -> Result<Vec<u8>> {
        checked_range(
            range.start,
            range.end.checked_sub(range.start).ok_or(Error::Range)?,
            self.len(),
        )?;
        Ok(self.bytes[range.start as usize..range.end as usize].to_vec())
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
            let decoded = decode_bounded(&encoded, DATA_FRAME_MAX).unwrap();
            assert_eq!(decoded, raw);
            assert_eq!(decoded.capacity(), size);
            assert!(decode_bounded(&encoded, size - 1).is_err());
            let mut corrupt = encoded;
            *corrupt.last_mut().unwrap() ^= 1;
            assert!(decode_bounded(&corrupt, DATA_FRAME_MAX).is_err());
        }
        let raw = vec![7; 65_536];
        let encoded = zstd::stream::encode_all(raw.as_slice(), 3).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&encoded).unwrap(),
            None
        );
        let decoded = decode_bounded(&encoded, DATA_FRAME_MAX).unwrap();
        assert_eq!(decoded, raw);
        assert_eq!(decoded.capacity(), raw.len());
        assert!(decode_bounded(&encoded, raw.len() - 1).is_err());
    }
}
