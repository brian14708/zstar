//! Hostile-input hardening: size caps, ancestor tracking, tar header fuzzing.
use proptest::prelude::*;
use std::time::{Duration, Instant};
use zstar::manifest::{Entry, decode_document, encode_block};

fn header() -> Vec<u8> {
    let mut out = vec![0xa2, 0x67];
    out.extend(b"version");
    out.push(1);
    out.push(0x71);
    out.extend(b"required_features");
    out.push(0x80);
    out
}

fn document(entries: &[Entry]) -> Vec<u8> {
    let mut out = header();
    for entry in entries {
        out.extend(encode_block(&entry.to_value()).unwrap());
    }
    out
}

#[test]
fn oversized_item_is_rejected_without_allocating() {
    // Entry tuple whose path claims a 3 GiB text string; only 20 bytes exist.
    let mut doc = header();
    doc.extend([0x86, 0x7a]);
    doc.extend((3u32 << 30).to_be_bytes());
    doc.extend([b'a'; 16]);
    assert!(decode_document(&doc, 0).is_err());
    // Same through a header extension claiming a huge byte string.
    let mut doc = vec![0xa3, 0x67];
    doc.extend(b"version");
    doc.push(1);
    doc.extend([0x63]);
    doc.extend(b"x-a");
    doc.extend([0x5b]);
    doc.extend((1u64 << 40).to_be_bytes());
    assert!(decode_document(&doc, 0).is_err());
}

#[test]
fn entry_above_cap_is_rejected_while_decoding() {
    let mut doc = header();
    // Array head then a text string longer than ENTRY_MAX that really exists.
    doc.extend([0x86, 0x7a]);
    let big = zstar::manifest::ENTRY_MAX + 1;
    doc.extend((big as u32).to_be_bytes());
    doc.extend(vec![b'a'; big]);
    assert!(decode_document(&doc, 0).is_err());
}

#[test]
fn many_retained_ancestors_validate_quickly() {
    // Chain of directories "d", "d-", "d--", ... each remains a candidate
    // ancestor, followed by many files; must stay near-linear.
    let mut entries = Vec::new();
    let mut names = Vec::new();
    for n in 1..=250 {
        names.push(format!("d{}", "-".repeat(n - 1)));
    }
    for name in &names {
        entries.push(Entry::directory(name.clone()));
    }
    for i in 0..50_000 {
        entries.push(Entry::file(format!("{}/f{i:06}", names[0])));
    }
    entries.sort_by(|a, b| a.path.as_bytes().cmp(b.path.as_bytes()));
    let doc = document(&entries);
    let start = Instant::now();
    let (_, decoded) = decode_document(&doc, 0).unwrap();
    assert_eq!(decoded.len(), entries.len());
    assert!(start.elapsed() < Duration::from_secs(20));
    // Missing ancestor still rejected.
    let bad = document(&[Entry::file("nodir/x")]);
    assert!(decode_document(&bad, 0).is_err());
    // A sibling whose name merely sorts between "d" and "d/" is not a parent.
    let bad = document(&[Entry::directory("d"), Entry::file("d-/x")]);
    assert!(decode_document(&bad, 0).is_err());
}

proptest! {
    #[test]
    fn parse_header_prefix_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = zstar::parse_member_header(&bytes);
    }
    #[test]
    fn parse_header_prefix_never_panics_on_ustar_shaped_input(
        mut bytes in proptest::collection::vec(any::<u8>(), 512..1600),
        kind in prop_oneof![Just(b'0'), Just(b'x'), Just(b'5'), Just(b'2'), any::<u8>()],
    ) {
        bytes[257..265].copy_from_slice(b"ustar\x0000");
        bytes[156] = kind;
        let _ = zstar::parse_member_header(&bytes);
    }
}
