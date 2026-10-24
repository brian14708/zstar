//! Public tar/CBOR validation remains available without compression.
#[test]
fn arbitrary_header_prefixes_are_bounded_and_never_panic() {
    for length in [0, 1, 511, 512, 513, 1024, 1535, 1536, 1_049_088] {
        for byte in [0, 1, 32, 255] {
            let input = vec![byte; length];
            let result = zstar::parse_member_header(&input);
            if byte != 0 && length >= 512 {
                assert_eq!(result.unwrap_err().category(), "malformed-tar");
            }
        }
    }
}
#[test]
fn oversized_pax_declaration_is_rejected_before_allocating_payload() {
    let mut header = [0u8; 512];
    header[0] = b'x';
    header[124..136].copy_from_slice(b"77777777777\0");
    header[148..156].fill(b' ');
    header[156] = b'x';
    header[257..265].copy_from_slice(b"ustar\x0000");
    let checksum: u64 = header.iter().map(|b| u64::from(*b)).sum();
    header[148..156].copy_from_slice(format!("{checksum:07o}\0").as_bytes());
    assert_eq!(
        zstar::parse_member_header(&header).unwrap_err().category(),
        "malformed-tar"
    );
}
