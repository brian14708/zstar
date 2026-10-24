//! Strict POSIX ustar/local PAX validation. Parsing is bounded by the validated
//! format header limit; all lengths are checked before indexing hostile input.
use crate::{EntryKind, Error, Result};
use std::collections::BTreeMap;

fn malformed(reason: &'static str) -> Error {
    Error::MalformedTar(reason)
}
struct Header<'a> {
    name: String,
    link: &'a str,
    kind: u8,
    mode: u64,
    uid: u64,
    gid: u64,
    size: u64,
    mtime: u64,
}
fn text(bytes: &[u8]) -> Result<&str> {
    let at = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    if bytes[at..].iter().any(|&b| b != 0) {
        return Err(malformed("nonzero text padding"));
    }
    std::str::from_utf8(&bytes[..at]).map_err(|_| malformed("invalid header UTF-8"))
}
fn octal(bytes: &[u8]) -> Result<u64> {
    // Spec v1: optional leading spaces and octal digits, optionally followed
    // by NUL/space padding. Digits may fill the entire field.
    let start = bytes.iter().take_while(|&&b| b == b' ').count();
    let digits = bytes[start..]
        .iter()
        .take_while(|b| (b'0'..=b'7').contains(b))
        .count();
    let end = start + digits;
    let rest = &bytes[end..];
    if !rest.iter().all(|&b| b == 0 || b == b' ') {
        return Err(malformed("octal syntax"));
    }
    if digits == 0 {
        return Ok(0);
    }
    bytes[start..end].iter().try_fold(0_u64, |acc, &b| {
        acc.checked_mul(8)
            .and_then(|v| v.checked_add(u64::from(b - b'0')))
            .ok_or_else(|| malformed("octal overflow"))
    })
}
fn decimal(s: &str) -> Result<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed("decimal syntax"));
    }
    s.parse().map_err(|_| malformed("decimal overflow"))
}
fn header(bytes: &[u8]) -> Result<Header<'_>> {
    if bytes.len() != 512 || &bytes[257..265] != b"ustar\x0000" {
        return Err(malformed("ustar header signature"));
    }
    let sum: u64 = bytes
        .iter()
        .enumerate()
        .map(|(i, &b)| {
            if (148..156).contains(&i) {
                32
            } else {
                u64::from(b)
            }
        })
        .sum();
    if octal(&bytes[148..156])? != sum {
        return Err(malformed("header checksum"));
    }
    if bytes[265..329].iter().any(|&b| b != 0) || bytes[500..].iter().any(|&b| b != 0) {
        return Err(malformed("owner/group names or header padding"));
    }
    if octal(&bytes[329..337])? != 0 || octal(&bytes[337..345])? != 0 {
        return Err(malformed("device fields"));
    }
    let name = text(&bytes[..100])?;
    let prefix = text(&bytes[345..500])?;
    let name = if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    };
    Ok(Header {
        name,
        link: text(&bytes[157..257])?,
        kind: bytes[156],
        mode: octal(&bytes[100..108])?,
        uid: octal(&bytes[108..116])?,
        gid: octal(&bytes[116..124])?,
        size: octal(&bytes[124..136])?,
        mtime: octal(&bytes[136..148])?,
    })
}
fn pax(records: &[u8]) -> Result<BTreeMap<&str, &str>> {
    if records.is_empty() {
        return Err(malformed("empty PAX payload"));
    }
    let mut fields = BTreeMap::new();
    let mut position = 0;
    while position < records.len() {
        let rest = &records[position..];
        let space = rest
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| malformed("PAX length separator"))?;
        let digits = std::str::from_utf8(&rest[..space]).map_err(|_| malformed("PAX length"))?;
        let n = usize::try_from(decimal(digits)?).map_err(|_| malformed("PAX length overflow"))?;
        if n < space + 4 || n > rest.len() || rest[n - 1] != b'\n' {
            return Err(malformed("PAX record bounds"));
        }
        let body =
            std::str::from_utf8(&rest[space + 1..n - 1]).map_err(|_| malformed("PAX UTF-8"))?;
        let (key, value) = body
            .split_once('=')
            .ok_or_else(|| malformed("PAX key/value"))?;
        if !["path", "linkpath", "size", "uid", "gid", "mtime"].contains(&key)
            || fields.insert(key, value).is_some()
        {
            return Err(malformed("unsupported or duplicate PAX key"));
        }
        position += n;
    }
    Ok(fields)
}
fn mtime(text: &str) -> Result<i64> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(w, f)| (w, Some(f)));
    let seconds = decimal(whole)?;
    let mut nanos = i128::from(seconds) * 1_000_000_000;
    if let Some(fraction) = fraction {
        // Additional fractional zeros are exactly representable without rounding.
        if fraction.is_empty()
            || !fraction.bytes().all(|b| b.is_ascii_digit())
            || fraction
                .get(9..)
                .is_some_and(|tail| tail.bytes().any(|b| b != b'0'))
        {
            return Err(malformed("PAX mtime precision"));
        }
        let significant = &fraction[..fraction.len().min(9)];
        nanos += i128::from(decimal(significant)?) * 10i128.pow(9 - significant.len() as u32);
    }
    if negative {
        nanos = -nanos;
    }
    i64::try_from(nanos).map_err(|_| malformed("PAX mtime range"))
}

/// Parse a header sequence without relying on indexed physical offsets.
/// `None` means that more decoded bytes are needed, never malformed acceptance.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct TarHeader {
    pub path: String,
    pub kind: EntryKind,
    pub mode: u64,
    pub uid: u64,
    pub gid: u64,
    pub mtime_ns: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}
/// Parse complete effective standard tar metadata from a bounded header prefix.
pub fn parse_header_prefix(bytes: &[u8]) -> Result<Option<TarHeader>> {
    if bytes.len() < 512 {
        return Ok(None);
    }
    let first = header(&bytes[..512])?;
    let mut fields = BTreeMap::new();
    let header_size = if first.kind == b'x' {
        if !first.link.is_empty() {
            return Err(malformed("local PAX link"));
        }
        let padded = first
            .size
            .checked_add(511)
            .ok_or_else(|| malformed("PAX size overflow"))?
            / 512
            * 512;
        let total = padded
            .checked_add(1024)
            .ok_or_else(|| malformed("PAX header overflow"))?;
        if total > 1 << 20 || first.size == 0 {
            return Err(malformed("PAX header size"));
        }
        if bytes.len() < total as usize {
            return Ok(None);
        }
        fields = pax(&bytes[512..512 + first.size as usize])?;
        if bytes[512 + first.size as usize..512 + padded as usize]
            .iter()
            .any(|b| *b != 0)
        {
            return Err(malformed("PAX padding"));
        }
        total as usize
    } else {
        512
    };
    let raw = if header_size == 512 {
        first
    } else {
        header(&bytes[header_size - 512..header_size])?
    };
    let kind = match raw.kind {
        0 | b'0' => EntryKind::File,
        b'2' => EntryKind::Symlink,
        b'5' => EntryKind::Directory,
        _ => return Err(malformed("unsupported member type")),
    };
    if fields.contains_key("linkpath") && kind != EntryKind::Symlink {
        return Err(malformed("linkpath on nonsymlink"));
    }
    let path = fields.get("path").copied().unwrap_or(&raw.name);
    let path = if kind == EntryKind::Directory {
        path.strip_suffix('/')
            .ok_or_else(|| malformed("directory slash"))?
    } else {
        path
    };
    if !crate::manifest::valid_path(path) || raw.mode > 4095 {
        return Err(malformed("member path or mode"));
    }
    let size = fields
        .get("size")
        .map(|s| decimal(s))
        .transpose()?
        .unwrap_or(raw.size);
    if kind != EntryKind::File && size != 0 {
        return Err(malformed("nonfile payload"));
    }
    let time = fields
        .get("mtime")
        .map(|s| mtime(s))
        .transpose()?
        .map_or(i128::from(raw.mtime) * 1_000_000_000, i128::from);
    let target = fields.get("linkpath").copied().unwrap_or(raw.link);
    if kind != EntryKind::Symlink && !raw.link.is_empty() {
        return Err(malformed("link on nonsymlink"));
    }
    if target.contains('\0') || target.len() > 4096 {
        return Err(malformed("linkname"));
    }
    Ok(Some(TarHeader {
        path: path.into(),
        kind,
        mode: raw.mode,
        uid: fields
            .get("uid")
            .map(|s| decimal(s))
            .transpose()?
            .unwrap_or(raw.uid),
        gid: fields
            .get("gid")
            .map(|s| decimal(s))
            .transpose()?
            .unwrap_or(raw.gid),
        mtime_ns: i64::try_from(time).map_err(|_| malformed("mtime range"))?,
        size: (kind == EntryKind::File).then_some(size),
        target: (kind == EntryKind::Symlink).then(|| target.into()),
    }))
}

#[cfg(all(test, feature = "compression"))]
mod tests {
    use super::*;
    #[test]
    fn malformed_pax_lengths_never_panic() {
        for input in [
            b"1 a=\n".as_slice(),
            b"0 a=b\n",
            b"+9 a=b\n",
            b"18446744073709551615 a=b\n",
            b"3 a=b\n",
            b"99 a=b\n",
        ] {
            assert!(pax(input).is_err(), "{input:?}");
        }
        assert!(pax(b"6 a=b\n").is_err(), "unknown key");
        assert!(pax(b"10 path=a\n10 path=b\n").is_err());
    }
    fn checksum(bytes: &mut [u8]) {
        bytes[148..156].fill(b' ');
        let sum: u64 = bytes.iter().map(|&b| u64::from(b)).sum();
        bytes[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    }
    #[test]
    fn validates_unused_fields_and_pax_header_itself() {
        let headers =
            crate::tar_encode::member_header_full("long", 0, 420, b'0', "", u64::MAX, 0, -1)
                .unwrap();
        assert!(parse_header_prefix(&headers).unwrap().is_some());
        for (offset, value) in [(265, b'x'), (297, b'x'), (500, 1), (257, b'X'), (108, 0x80)] {
            let mut bad = headers.clone();
            bad[offset] = value;
            checksum(&mut bad[..512]);
            assert!(parse_header_prefix(&bad).is_err());
        }
        let length = headers.len();
        let mut bad = headers.clone();
        bad[length - 512 + 108] = 0x80;
        checksum(&mut bad[length - 512..]);
        assert!(parse_header_prefix(&bad).is_err());
        let payload_size = header(&headers[..512]).unwrap().size as usize;
        let mut bad = headers.clone();
        bad[512 + payload_size] = 1;
        assert!(parse_header_prefix(&bad).is_err());
    }
    #[test]
    fn honors_ustar_prefix_and_requires_exact_directory_slash() {
        let mut headers =
            crate::tar_encode::member_header_full("file", 0, 420, b'0', "", 0, 0, 0).unwrap();
        headers[345..348].copy_from_slice(b"dir");
        checksum(&mut headers);
        assert_eq!(
            parse_header_prefix(&headers).unwrap().unwrap().path,
            "dir/file"
        );
        let headers =
            crate::tar_encode::member_header_full("dir", 0, 493, b'5', "", 0, 0, 0).unwrap();
        assert!(parse_header_prefix(&headers).is_err());
    }
    #[test]
    fn octal_grammar_rejects_signs_base256_and_hidden_digits() {
        for value in [b"+1".as_slice(), b"-1", b"8", b"0\x001", b"\x001", b"\x80"] {
            assert!(octal(value).is_err(), "{value:?}");
        }
        assert_eq!(octal(b" 0017\0 ").unwrap(), 15);
    }
    #[test]
    fn octal_accepts_full_width_and_zero_fields() {
        // Full-width 12-digit size field has no terminator.
        assert_eq!(octal(b"777777777777").unwrap(), 0o777777777777);
        assert!(octal(b"00000000000\0").is_ok());
        assert_eq!(octal(b"77777777777\0").unwrap(), 0o77777777777);
        assert_eq!(octal(b"77777777777 ").unwrap(), 0o77777777777);
        assert_eq!(octal(b"0000644").unwrap(), 0o644);
        assert_eq!(octal(b"0000644\0").unwrap(), 0o644);
        assert_eq!(octal(&[0; 8]).unwrap(), 0);
        assert_eq!(octal(b"        ").unwrap(), 0);
        assert_eq!(octal(b"").unwrap(), 0);
        assert!(octal(b"17 1\0").is_err());
        // 22 digits overflow u64 even with a terminator.
        assert!(octal(b"7777777777777777777777\0").is_err());
        assert!(octal(b"7777777777777777777777").is_err());
    }
    #[test]
    fn time_grammar_and_signed_nanosecond_limits() {
        for invalid in [
            "+1",
            "--1",
            "-+1",
            "1.",
            ".1",
            "1.0000000001",
            "9223372036.854775808",
            "-9223372036.854775809",
            "18446744073709551616",
        ] {
            assert!(mtime(invalid).is_err(), "{invalid}");
        }
        for (input, expected) in [
            ("-0.000000001", -1),
            ("1.0000000000", 1_000_000_000),
            ("9223372036.854775807", i64::MAX),
            ("-9223372036.854775808", i64::MIN),
        ] {
            assert_eq!(mtime(input).unwrap(), expected);
        }
    }
}
