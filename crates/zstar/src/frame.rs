//! Shared bounded frame/object decoding and physical frame scanning.
#[cfg(any(test, feature = "fs"))]
use crate::primitives::RangeReader;
use crate::{Error, Limits, Result};

#[cfg(test)]
pub fn decode_bounded(encoded: &[u8], limit: usize) -> Result<Vec<u8>> {
    decode_bounded_with_limits(encoded, limit, Limits::default())
}
pub fn decode_bounded_with_limits(encoded: &[u8], limit: usize, limits: Limits) -> Result<Vec<u8>> {
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
        return Err(crate::Error::budget(
            crate::BudgetResource::DecodedFrameBytes,
            "decoded frame size",
        ));
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
        let size = usize::try_from(size).map_err(|_| {
            crate::Error::budget(
                crate::BudgetResource::DecodedFrameBytes,
                "decoded frame size",
            )
        })?;
        decoded.try_reserve_exact(size).map_err(|_| {
            crate::Error::budget(
                crate::BudgetResource::AllocationBytes,
                "decoded frame allocation",
            )
        })?;
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
        return Err(crate::Error::budget(
            crate::BudgetResource::DecodedFrameBytes,
            "decoded frame size",
        ));
    }
    Ok(())
}

/// Encode an external chunk and apply the same source limits used by readers.
///
/// # Errors
/// Returns compression errors or rejects encoded/decoded source budgets.
pub fn encode_object(raw: &[u8], level: i32, limits: Limits) -> Result<([u8; 32], Vec<u8>)> {
    Limits::check(
        raw.len() as u64,
        limits.max_frame_decoded_bytes,
        "decoded object size",
        crate::BudgetResource::DecodedFrameBytes,
    )?;
    let digest = *crate::primitives::Digest::sha256(raw).as_bytes();
    let encoded = crate::primitives::encode_frame(raw, level)?;
    limits.frame(&encoded)?;
    Ok((digest, encoded))
}
pub fn decode_content_with_limits(
    encoded: &[u8],
    require_checksum: bool,
    limits: Limits,
) -> Result<Vec<u8>> {
    if encoded.len() < 5 {
        return Err(Error::MalformedFrame("encoded frame bounds".into()));
    }
    if require_checksum && encoded[4] & 4 == 0 {
        return Err(Error::MalformedFrame("missing data checksum".into()));
    }
    crate::decode_bounded_with_limits(encoded, limits.decoded_limit(), limits)
}

pub fn decode_object(encoded: &[u8], limits: Limits) -> Result<Vec<u8>> {
    decode_content_with_limits(encoded, false, limits).map_err(|error| match error {
        Error::MalformedFrame(message) => Error::MalformedExternalContent(message),
        other => other,
    })
}

/// Determine one frame's boundary without assuming a content-size field exists.
///
/// # Errors
/// Returns source, structural frame, or budget errors.
#[cfg(test)]
pub fn fetch_frame<R: RangeReader>(source: &mut R, start: u64, end: u64) -> Result<Vec<u8>> {
    fetch_frame_with_limits(source, start, end, Limits::default())
}
#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
#[expect(
    clippy::items_after_statements,
    reason = "Keep local helpers next to the operation that uses them."
)]
#[cfg(any(test, feature = "fs"))]
pub fn fetch_frame_with_limits<R: RangeReader>(
    source: &mut R,
    start: u64,
    end: u64,
    limits: Limits,
) -> Result<Vec<u8>> {
    // Fetch a bounded read-ahead window once; block headers are scanned in memory.
    let header_end = start
        .saturating_add((256 << 10).min(limits.encoded_limit()))
        .min(end);
    let header = source.read_range(start..header_end)?;
    if header.len() < 6 || !header.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(Error::MalformedFrame("frame header".into()));
    }
    let descriptor = header[4];
    if descriptor & 0x08 != 0 {
        return Err(Error::MalformedFrame("reserved frame header bits".into()));
    }
    let single = descriptor & 0x20 != 0;
    let dictionary = match descriptor & 3 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    let content = match descriptor >> 6 {
        0 => usize::from(single),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let limit = start.saturating_add(limits.encoded_limit()).min(end);
    // Read ahead in large chunks and scan block headers in memory, so request
    // count is bounded by frame size / READ_AHEAD rather than block count.
    const READ_AHEAD: u64 = 256 << 10;
    // Zstandard Block_Maximum_Size.
    const BLOCK_MAX: u64 = 128 << 10;
    let mut buffer = header;
    let mut buffered_end = start + buffer.len() as u64;
    let mut at = start
        .checked_add(5 + u64::from(!single) + dictionary + content as u64)
        .ok_or_else(|| Error::MalformedFrame("frame header overflow".into()))?;
    loop {
        if at.checked_add(3).is_none_or(|n| n > limit) {
            if limit < end {
                return Err(crate::Error::budget(
                    crate::BudgetResource::EncodedFrameBytes,
                    "encoded frame size",
                ));
            }
            return Err(Error::MalformedFrame("block header bounds".into()));
        }
        if at + 3 > buffered_end {
            let to = at.saturating_add(READ_AHEAD).min(limit);
            let more = source.read_range(buffered_end..to)?;
            if more.is_empty() || more.len() as u64 != to - buffered_end {
                return Err(Error::MalformedFrame("short block header".into()));
            }
            buffer.extend_from_slice(&more);
            buffered_end = to;
            continue;
        }
        let i = (at - start) as usize;
        let block =
            u32::from(buffer[i]) | u32::from(buffer[i + 1]) << 8 | u32::from(buffer[i + 2]) << 16;
        let size = match (block >> 1) & 3 {
            0 | 2 => u64::from(block >> 3),
            1 => 1,
            _ => return Err(Error::MalformedFrame("reserved block type".into())),
        };
        if size > BLOCK_MAX || u64::from(block >> 3) > BLOCK_MAX {
            return Err(Error::MalformedFrame("block exceeds 128 KiB".into()));
        }
        at = at
            .checked_add(3 + size)
            .ok_or_else(|| Error::MalformedFrame("block size overflow".into()))?;
        if block & 1 != 0 {
            break;
        }
    }
    if descriptor & 4 != 0 {
        at = at
            .checked_add(4)
            .ok_or_else(|| Error::MalformedFrame("frame checksum overflow".into()))?;
    }
    if at > end {
        return Err(Error::MalformedFrame("truncated frame".into()));
    }
    Limits::check(
        at - start,
        limits.max_frame_encoded_bytes,
        "encoded frame size",
        crate::BudgetResource::EncodedFrameBytes,
    )?;
    if at > buffered_end {
        let more = source.read_range(buffered_end..at)?;
        if more.len() as u64 != at - buffered_end {
            return Err(Error::MalformedFrame("short frame".into()));
        }
        buffer.extend_from_slice(&more);
    }
    buffer.truncate((at - start) as usize);
    Ok(buffer)
}

#[cfg(test)]
mod tests;
