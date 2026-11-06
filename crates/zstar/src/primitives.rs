//! Internal wire-format and codec helpers.
use crate::{Error, Result, decode_frame_into};
use sha2::{Digest as Sha2Digest, Sha256};
use std::ops::Range;

pub const FOOTER_SIZE: usize = 80;
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Footer {
    pub toc_offset: u64,
    pub toc_encoded_len: u64,
    pub toc_decoded_len: u64,
    pub toc_sha256: [u8; 32],
}

#[expect(
    clippy::missing_errors_doc,
    reason = "Errors follow the documented operation contract and crate error types."
)]
#[expect(
    clippy::missing_panics_doc,
    reason = "Unwraps enforce internal invariants established before this operation."
)]
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
