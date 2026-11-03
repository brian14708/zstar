//! Caller-selected resource limits, independent of archive conformance.
use crate::{Entry, Error, Result};

/// Optional per-source and per-entry budgets. Zero disables that additional
/// limit. Aggregate metadata, index, spool and concurrency budgets still apply.
#[derive(Clone, Copy, Debug, Default)]
pub struct Limits {
    pub max_frame_encoded_bytes: u64,
    pub max_frame_decoded_bytes: u64,
    pub max_window_bytes: u64,
    pub max_entry_bytes: u64,
    pub max_extents: u64,
    pub max_pointer_bytes: u64,
}
impl Limits {
    pub(crate) const fn check(value: u64, maximum: u64, name: &'static str) -> Result<()> {
        if maximum != 0 && value > maximum {
            Err(Error::Budget(name))
        } else {
            Ok(())
        }
    }
    pub(crate) fn encoded_limit(self) -> u64 {
        if self.max_frame_encoded_bytes == 0 {
            isize::MAX as u64 - 1
        } else {
            self.max_frame_encoded_bytes.min(isize::MAX as u64 - 1)
        }
    }
    pub(crate) fn decoded_limit(self) -> usize {
        if self.max_frame_decoded_bytes == 0 {
            isize::MAX as usize - 1
        } else {
            usize::try_from(self.max_frame_decoded_bytes)
                .unwrap_or(isize::MAX as usize - 1)
                .min(isize::MAX as usize - 1)
        }
    }
    pub(crate) fn frame(self, encoded: &[u8]) -> Result<()> {
        Self::check(
            encoded.len() as u64,
            self.max_frame_encoded_bytes,
            "encoded frame size",
        )?;
        if encoded.len() < 6 {
            return Ok(());
        }
        let declared = zstd::zstd_safe::get_frame_content_size(encoded)
            .ok()
            .flatten();
        if let Some(size) = declared {
            Self::check(size, self.max_frame_decoded_bytes, "decoded frame size")?;
        }
        let window = if encoded[4] & 32 != 0 {
            declared.unwrap_or(0)
        } else {
            let descriptor = encoded[5];
            let base = 1_u64 << (10 + (descriptor >> 3));
            base + (base / 8) * u64::from(descriptor & 7)
        };
        Self::check(window, self.max_window_bytes, "Zstandard window")
    }
    pub(crate) fn inline(
        self,
        size: u64,
        frame: usize,
        offset: usize,
        metadata: usize,
    ) -> Result<()> {
        let count = (size - 1) / frame as u64
            + 1
            + u64::from((size - 1) % frame as u64 + offset as u64 >= frame as u64);
        Self::check(count, self.max_extents, "entry extent count")?;
        if self.max_entry_bytes != 0
            && count > self.max_entry_bytes.saturating_sub(metadata as u64) / 4
        {
            return Err(Error::Budget("inline entry metadata size"));
        }
        Ok(())
    }
    pub(crate) fn entry(self, entry: &Entry) -> Result<()> {
        Self::check(
            entry.extents.len() as u64,
            self.max_extents,
            "entry extent count",
        )?;
        Self::check(
            entry.encoded_len()? as u64,
            self.max_entry_bytes,
            "entry metadata size",
        )?;
        for extent in &entry.extents {
            Self::check(
                extent.offset + extent.size,
                self.max_frame_decoded_bytes,
                "extent decoded selection",
            )?;
            if matches!(extent.source, crate::Source::Archive(_)) {
                Self::check(
                    extent.encoded_size,
                    self.max_frame_encoded_bytes,
                    "encoded frame size",
                )?;
            }
        }
        Ok(())
    }
}
