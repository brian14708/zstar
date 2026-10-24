//! Bounded, segmentation-independent fixed and content-defined chunking.
use crate::{DATA_FRAME_MAX, Error, Result};
use std::io::Read;

/// Implementation provenance: `FastCDC` 2020, level 1, unseeded gear table.
#[cfg(feature = "async")]
pub(crate) const FASTCDC_PROFILE: &str = "zstar-fastcdc-2020-level1-v1";

/// Chunk boundaries are reset for each file; empty files have no chunks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Chunking {
    Fixed {
        size: usize,
    },
    FastCdc {
        min: usize,
        target: usize,
        max: usize,
    },
}
impl Default for Chunking {
    fn default() -> Self {
        Self::Fixed { size: 1 << 20 }
    }
}
impl Chunking {
    /// Provisional evaluation profile, not a change to portable writer defaults.
    #[must_use]
    pub fn fastcdc() -> Self {
        Self::FastCdc {
            min: 64 << 10,
            target: 256 << 10,
            max: 1 << 20,
        }
    }
    pub fn validate(self) -> Result<()> {
        match self {
            Self::Fixed { size } if (1..=DATA_FRAME_MAX).contains(&size) => Ok(()),
            Self::FastCdc { min, target, max }
                if min >= 64
                    && min <= target
                    && target <= max
                    && max <= DATA_FRAME_MAX
                    && min.is_multiple_of(2)
                    && target.is_power_of_two()
                    && max.is_multiple_of(2) =>
            {
                Ok(())
            }
            _ => Err(Error::Unsupported("invalid chunking settings")),
        }
    }
    #[must_use]
    pub fn max_size(self) -> usize {
        match self {
            Self::Fixed { size } => size,
            Self::FastCdc { max, .. } => max,
        }
    }
}

/// One maximum-sized window plus one returned owned chunk. Fill the whole
/// window before cutting; caller read segmentation cannot change boundaries.
pub struct Chunker {
    strategy: Chunking,
    buffer: Vec<u8>,
    filled: usize,
    eof: bool,
    masks: (u64, u64),
}
impl Chunker {
    pub fn new(strategy: Chunking) -> Result<Self> {
        strategy.validate()?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(strategy.max_size())
            .map_err(|_| Error::Budget("chunk buffer"))?;
        buffer.resize(strategy.max_size(), 0);
        let masks = match strategy {
            Chunking::Fixed { .. } => (0, 0),
            Chunking::FastCdc { target, .. } => {
                fastcdc::v2020::select_masks(target, fastcdc::v2020::Normalization::Level1)
            }
        };
        Ok(Self {
            strategy,
            buffer,
            filled: 0,
            eof: false,
            masks,
        })
    }
    fn take_chunk(&mut self) -> Option<Vec<u8>> {
        if self.filled == 0 {
            return None;
        }
        let source = &self.buffer[..self.filled];
        let length = match self.strategy {
            Chunking::Fixed { size } => source.len().min(size),
            Chunking::FastCdc { min, target, max } => {
                fastcdc::v2020::cut(
                    source,
                    min,
                    target,
                    max,
                    self.masks.0,
                    self.masks.1,
                    self.masks.0 << 1,
                    self.masks.1 << 1,
                )
                .1
            }
        };
        let chunk = self.buffer[..length].to_vec();
        self.buffer.copy_within(length..self.filled, 0);
        self.filled -= length;
        Some(chunk)
    }
    pub fn next(&mut self, input: &mut impl Read) -> Result<Option<Vec<u8>>> {
        while !self.eof && self.filled < self.buffer.len() {
            match input.read(&mut self.buffer[self.filled..]) {
                Ok(0) => self.eof = true,
                Ok(count) => self.filled += count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(self.take_chunk())
    }
    /// Cancellation retains already received input in the bounded window.
    #[cfg(feature = "async")]
    pub async fn next_async<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        input: &mut R,
    ) -> Result<Option<bytes::Bytes>> {
        use tokio::io::AsyncReadExt;
        while !self.eof && self.filled < self.buffer.len() {
            match input.read(&mut self.buffer[self.filled..]).await {
                Ok(0) => self.eof = true,
                Ok(count) => self.filled += count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(self.take_chunk().map(bytes::Bytes::from))
    }
}

/// Storage is independent of boundary selection and physical packing.
#[cfg(feature = "async")]
pub trait ObjectWriter: Send {
    fn put_chunk(
        &mut self,
        raw: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<crate::Extent>> + Send;
}

/// Externalize one declared file into ordered v1 extents. Returned metadata is
/// bounded by the v1 extent and pointer limits; failures never publish an archive.
#[cfg(feature = "async")]
pub async fn external_file<R: tokio::io::AsyncRead + Unpin>(
    input: R,
    size: u64,
    strategy: Chunking,
    writer: &mut impl ObjectWriter,
) -> Result<Vec<crate::Extent>> {
    use tokio::io::AsyncReadExt;
    let mut chunker = Chunker::new(strategy)?;
    if size > (strategy.max_size() as u64) * crate::manifest::EXTENTS_MAX as u64 {
        return Err(Error::Budget("external file extent count"));
    }
    let mut input = input.take(size.checked_add(1).ok_or(Error::Range)?);
    let mut total = 0_u64;
    let mut extents = Vec::new();
    while let Some(raw) = chunker.next_async(&mut input).await? {
        total = total.checked_add(raw.len() as u64).ok_or(Error::Range)?;
        if total > size || extents.len() == crate::manifest::EXTENTS_MAX {
            return Err(Error::Budget("external file size or extent count"));
        }
        let digest = *crate::Digest::sha256(&raw).as_bytes();
        let length = raw.len() as u64;
        let extent = writer.put_chunk(raw).await?;
        if extent.size != length
            || extent.offset != 0
            || extent.source != crate::Source::Object(digest)
        {
            return Err(Error::MalformedExternalContent(
                "object writer changed chunk identity".into(),
            ));
        }
        extent.validate(u64::MAX)?;
        extents.push(extent);
    }
    if total != size {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
    }
    if !extents.is_empty() {
        crate::manifest::encode_external_reference(&extents)?;
        crate::manifest::encode_pointer(&extents)?;
    }
    Ok(extents)
}
