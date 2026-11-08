use super::*;

struct Counting<'a> {
    bytes: &'a [u8],
    requests: usize,
}
impl RangeReader for Counting<'_> {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
    )]
    fn read_range(&mut self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        self.requests += 1;
        let end = (range.end as usize).min(self.bytes.len());
        Ok(self.bytes[range.start as usize..end].to_vec())
    }
}
#[expect(
    clippy::cast_possible_truncation,
    reason = "Lengths and offsets are bounded by validated buffers or wire-field limits."
)]
#[test]
fn fetch_frame_uses_constant_requests_per_frame() {
    // Incompressible data forces many blocks of up to 128 KiB.
    let mut data = vec![0_u8; 3 << 20];
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    for b in &mut data {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    let frame = crate::encode_frame(&data, 1).unwrap();
    let mut source = Counting {
        bytes: &frame,
        requests: 0,
    };
    let got = fetch_frame(&mut source, 0, frame.len() as u64).unwrap();
    assert_eq!(got, frame);
    // ~24 blocks; the old scanner needed one request per block.
    assert!(
        source.requests <= 3 + frame.len() / (256 << 10),
        "{}",
        source.requests
    );
    let small = crate::encode_frame(b"hello", 1).unwrap();
    let mut source = Counting {
        bytes: &small,
        requests: 0,
    };
    fetch_frame(&mut source, 0, small.len() as u64).unwrap();
    assert_eq!(source.requests, 1);
}
#[test]
fn fetch_frame_rejects_hostile_rle_and_oversized_blocks() {
    // Single-segment-less header: descriptor 0 (no checksum), window byte.
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00];
    // Millions of 1-byte-payload RLE blocks (4 bytes each), never last.
    let block = [0x02_u8 | (1 << 3), 0, 0, 7];
    for _ in 0..(5 << 20) {
        frame.extend_from_slice(&block);
    }
    let started = std::time::Instant::now();
    let mut source = Counting {
        bytes: &frame,
        requests: 0,
    };
    let error = fetch_frame(&mut source, 0, frame.len() as u64).unwrap_err();
    assert!(matches!(error, Error::MalformedFrame(_)));
    assert!(source.requests <= 100, "{}", source.requests);
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // Raw block declaring 128 KiB + 1.
    let size = (128u32 << 10) + 1;
    let header = (size << 3) | 1;
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00];
    frame.extend_from_slice(&header.to_le_bytes()[..3]);
    frame.resize(frame.len() + size as usize, 0);
    let mut source = Counting {
        bytes: &frame,
        requests: 0,
    };
    assert!(fetch_frame(&mut source, 0, frame.len() as u64).is_err());
}
