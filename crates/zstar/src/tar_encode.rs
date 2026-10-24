//! Bounded standard ustar/local-PAX header encoding.
use crate::{Error, Result};

const USTAR_SIZE_MAX: u64 = 0o77_777_777_777;
const USTAR_SECONDS_MAX: i64 = 0o77_777_777_777;
const USTAR_ID_MAX: u64 = 0o7_777_777;
fn decimal_bytes(mut value: u128) -> ([u8; 39], usize) {
    let mut bytes = [0; 39];
    let mut at = bytes.len();
    loop {
        at -= 1;
        bytes[at] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    (bytes, at)
}

fn append_pax_u64(output: &mut Vec<u8>, key: &str, value: u64) {
    let (digits, at) = decimal_bytes(u128::from(value));
    append_pax_record(output, key, &digits[at..]);
}

fn append_pax_mtime(output: &mut Vec<u8>, value: i64) {
    let negative = value < 0;
    let absolute = u128::from(value.unsigned_abs());
    let seconds = absolute / 1_000_000_000;
    let fraction = absolute % 1_000_000_000;
    let (digits, at) = decimal_bytes(seconds);
    // An i64 nanosecond timestamp needs at most a sign, ten second digits,
    // a decimal point, and nine fractional digits.
    let mut number = [b'0'; 21];
    let mut end = usize::from(negative);
    if negative {
        number[0] = b'-';
    }
    number[end..end + digits.len() - at].copy_from_slice(&digits[at..]);
    end += digits.len() - at;
    number[end] = b'.';
    end += 10;
    let (fraction_digits, fraction_at) = decimal_bytes(fraction);
    let fraction_len = fraction_digits.len() - fraction_at;
    number[end - fraction_len..end].copy_from_slice(&fraction_digits[fraction_at..]);
    append_pax_record(output, "mtime", &number[..end]);
}

fn append_pax_record(output: &mut Vec<u8>, key: &str, value: &[u8]) {
    let body_len = 1 + key.len() + 1 + value.len() + 1;
    let mut length = body_len + 1;
    loop {
        let (digits, at) = decimal_bytes(length as u128);
        let next = digits.len() - at + body_len;
        if next == length {
            output.reserve(length);
            output.extend_from_slice(&digits[at..]);
            output.push(b' ');
            output.extend_from_slice(key.as_bytes());
            output.push(b'=');
            output.extend_from_slice(value);
            output.push(b'\n');
            return;
        }
        length = next;
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn member_header_full(
    path: &str,
    size: u64,
    mode: u64,
    kind: u8,
    target: &str,
    uid: u64,
    gid: u64,
    mtime_ns: i64,
) -> Result<Vec<u8>> {
    if mode > 4095 {
        return Err(Error::MalformedToc("mode".into()));
    }
    if path.contains('\0') || target.contains('\0') || target.len() > 4096 {
        return Err(Error::MalformedToc("path or target".into()));
    }
    let mut records = Vec::new();
    if path.len() > 100 {
        append_pax_record(&mut records, "path", path.as_bytes());
    }
    if target.len() > 100 {
        append_pax_record(&mut records, "linkpath", target.as_bytes());
    }
    // Ustar's size field has eleven octal digits; larger values use local PAX.
    if size > USTAR_SIZE_MAX {
        append_pax_u64(&mut records, "size", size);
    }
    for (key, value) in [("uid", uid), ("gid", gid)] {
        if value > USTAR_ID_MAX {
            append_pax_u64(&mut records, key, value);
        }
    }
    let seconds = mtime_ns.div_euclid(1_000_000_000);
    let use_pax_time = mtime_ns < 0 || mtime_ns % 1_000_000_000 != 0 || seconds > USTAR_SECONDS_MAX;
    if use_pax_time {
        append_pax_mtime(&mut records, mtime_ns);
    }
    let physical = tar_header(
        if path.len() > 100 { "PaxEntry" } else { path },
        if size > USTAR_SIZE_MAX { 0 } else { size },
        mode,
        kind,
        if target.len() > 100 { "" } else { target },
        if uid > USTAR_ID_MAX { 0 } else { uid },
        if gid > USTAR_ID_MAX { 0 } else { gid },
        if use_pax_time { 0 } else { seconds as u64 },
    )?;
    if records.is_empty() {
        return Ok(physical);
    }
    let mut out = tar_header(
        "PaxHeaders/entry",
        records.len() as u64,
        mode,
        b'x',
        "",
        0,
        0,
        0,
    )?;
    let padded_records_len = records.len().div_ceil(512) * 512;
    out.reserve(padded_records_len + 512);
    out.extend_from_slice(&records);
    out.resize(512 + padded_records_len, 0);
    out.extend_from_slice(&physical);
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn tar_header(
    path: &str,
    size: u64,
    mode: u64,
    kind: u8,
    target: &str,
    uid: u64,
    gid: u64,
    mtime: u64,
) -> Result<Vec<u8>> {
    if path.len() > 100 || target.len() > 100 {
        return Err(Error::Unsupported("long paths require PAX"));
    }
    let mut h = [0u8; 512];
    h[0..path.len()].copy_from_slice(path.as_bytes());
    write_octal(&mut h[100..108], mode)?;
    write_octal(&mut h[108..116], uid)?;
    write_octal(&mut h[116..124], gid)?;
    write_octal(&mut h[124..136], size)?;
    write_octal(&mut h[136..148], mtime)?;
    h[148..156].fill(b' ');
    h[156] = kind;
    h[157..157 + target.len()].copy_from_slice(target.as_bytes());
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    let sum: u32 = h.iter().map(|b| u32::from(*b)).sum();
    write_octal(&mut h[148..156], u64::from(sum))?;
    Ok(h.to_vec())
}
fn write_octal(dst: &mut [u8], mut value: u64) -> Result<()> {
    if dst.is_empty() || (dst.len() <= 22 && value >> (3 * (dst.len() - 1)) != 0) {
        return Err(Error::Unsupported("ustar integer overflow"));
    }
    let (digits, terminator) = dst.split_at_mut(dst.len() - 1);
    terminator[0] = 0;
    digits.fill(b'0');
    for byte in digits.iter_mut().rev() {
        *byte += (value & 7) as u8;
        value >>= 3;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pax_integer_and_time_formatting_is_canonical_at_full_width() {
        let mut output = Vec::new();
        append_pax_u64(&mut output, "uid", u64::MAX);
        for value in [i64::MIN, i64::MAX, -1, 0, 1, -1_000_000_000] {
            append_pax_mtime(&mut output, value);
        }
        assert_eq!(
            output,
            concat!(
                "28 uid=18446744073709551615\n",
                "31 mtime=-9223372036.854775808\n",
                "30 mtime=9223372036.854775807\n",
                "22 mtime=-0.000000001\n",
                "21 mtime=0.000000000\n",
                "21 mtime=0.000000001\n",
                "22 mtime=-1.000000000\n",
            )
            .as_bytes()
        );
    }

    #[test]
    fn pax_record_lengths_include_their_own_decimal_digits() {
        let mut output = Vec::new();
        for (value_len, expected_len) in [
            (1, 9),
            (2, 11),
            (89, 98),
            (90, 99),
            (91, 101),
            (988, 998),
            (989, 999),
            (990, 1001),
        ] {
            let start = output.len();
            append_pax_record(&mut output, "path", &vec![b'a'; value_len]);
            let record = &output[start..];
            assert_eq!(record.len(), expected_len);
            assert!(record.starts_with(format!("{expected_len} path=").as_bytes()));
            assert_eq!(record.last(), Some(&b'\n'));
        }
    }
}
