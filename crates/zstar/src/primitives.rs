//! Internal wire-format and codec helpers.
use crate::{Error, Result, decode_frame_into};
use sha2::{Digest as Sha2Digest, Sha256};
use std::ops::Range;

pub const FOOTER_SIZE: usize = 28;
pub const FOOTER_MAGIC: u32 = 0x184D_2A5F;
pub const TOC_MAGIC: u32 = 0x184D_2A5E;

pub trait RangeReader {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[expect(
        clippy::missing_errors_doc,
        reason = "Errors follow the documented operation contract and crate error types."
    )]
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
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
/// Verify a complete byte sequence against a SHA-256 digest.
pub fn verify_sha256(bytes: &[u8], expected: [u8; 32]) -> Result<()> {
    if Digest::sha256(bytes).0 == expected {
        Ok(())
    } else {
        Err(Error::Integrity)
    }
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
/// Validate a bounded half-open range without permitting integer wraparound.
pub const fn checked_range(offset: u64, length: u64, size: u64) -> Result<Range<u64>> {
    if offset > size || length > size - offset {
        return Err(Error::Range);
    }
    Ok(offset..offset + length)
}

/// Lengths stored in the fixed terminal footer. Metadata positions are derived from archive size.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Footer {
    pub toc_encoded_len: u64,
    pub toc_decoded_len: u64,
}

impl Footer {
    /// Derive and validate the metadata region for the complete encoded archive size.
    ///
    /// # Errors
    /// Returns an error for empty metadata, underflow, or an empty data region.
    pub fn metadata_range(&self, source_size: u64) -> Result<Range<u64>> {
        let end = source_size
            .checked_sub(FOOTER_SIZE as u64)
            .ok_or(Error::MalformedFooter("manifest coverage"))?;
        let start = end
            .checked_sub(self.toc_encoded_len)
            .filter(|start| *start > 0)
            .ok_or(Error::MalformedFooter("manifest coverage"))?;
        if self.toc_encoded_len <= 8 || self.toc_decoded_len == 0 {
            return Err(Error::MalformedFooter("invalid TOC bounds"));
        }
        Ok(start..end)
    }
}

/// Parse and validate the fixed 28-byte terminal footer.
///
/// # Errors
/// Returns an error for an invalid signature, payload size, or metadata lengths.
#[expect(
    clippy::missing_panics_doc,
    reason = "Fixed-length slices are established by the footer size check."
)]
pub fn parse_footer(bytes: &[u8]) -> Result<Footer> {
    if bytes.len() != FOOTER_SIZE {
        return Err(Error::MalformedFooter("footer must be 28 bytes"));
    }
    if bytes[..4] != FOOTER_MAGIC.to_le_bytes() || &bytes[24..] != b"Z*01" {
        return Err(Error::NotZstar);
    }
    if bytes[4..8] != 20_u32.to_le_bytes() {
        return Err(Error::MalformedFooter("footer payload length"));
    }
    let footer = Footer {
        toc_encoded_len: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        toc_decoded_len: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
    };
    if footer.toc_encoded_len <= 8 || footer.toc_decoded_len == 0 {
        return Err(Error::MalformedFooter("invalid TOC bounds"));
    }
    Ok(footer)
}

/// Encode the fixed 28-byte terminal footer. Validate coverage with [`Footer::metadata_range`].
#[must_use]
pub fn encode_footer(footer: &Footer) -> [u8; FOOTER_SIZE] {
    let mut out = [0u8; FOOTER_SIZE];
    out[..4].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
    out[4..8].copy_from_slice(&20_u32.to_le_bytes());
    out[8..16].copy_from_slice(&footer.toc_encoded_len.to_le_bytes());
    out[16..24].copy_from_slice(&footer.toc_decoded_len.to_le_bytes());
    out[24..].copy_from_slice(b"Z*01");
    out
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
pub fn encode_frame(data: &[u8], level: i32) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(Error::MalformedFrame("invalid decoded frame size".into()));
    }
    let mut encoder = zstd::bulk::Compressor::new(level)?;
    encoder.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
    encoder.compress(data).map_err(Error::Io)
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
pub fn decode_frame(encoded: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut context = zstd::zstd_safe::DCtx::create();
    decode_frame_into(encoded, expected_size, &mut decoded, &mut context)?;
    Ok(decoded)
}

pub struct SliceReader<'a> {
    bytes: &'a [u8],
}
impl<'a> SliceReader<'a> {
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}
impl RangeReader for SliceReader<'_> {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
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
mod footer_tests {
    use super::*;

    #[test]
    fn compact_footer_preserves_u64_lengths_and_derives_bounds() {
        let footer = Footer {
            toc_encoded_len: (1_u64 << 32) + 9,
            toc_decoded_len: (1_u64 << 40) + 1,
        };
        let encoded = encode_footer(&footer);
        assert_eq!(encoded.len(), 28);
        assert_eq!(&encoded[..8], &[0x5f, 0x2a, 0x4d, 0x18, 20, 0, 0, 0]);
        assert_eq!(&encoded[24..], b"Z*01");
        assert_eq!(parse_footer(&encoded).unwrap(), footer);
        let size = footer.toc_encoded_len + FOOTER_SIZE as u64 + 123;
        assert_eq!(footer.metadata_range(size).unwrap(), 123..size - 28);
        for size in [0, 27, 28, footer.toc_encoded_len + 28, u64::MAX] {
            let result = footer.metadata_range(size);
            assert_eq!(result.is_ok(), size == u64::MAX);
        }
        let overflow = Footer {
            toc_encoded_len: u64::MAX,
            toc_decoded_len: 1,
        };
        assert!(overflow.metadata_range(u64::MAX).is_err());
    }
}
