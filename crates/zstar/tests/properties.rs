//! Generated logical ranges are checked against source bytes; hostile inputs
//! exercise bounded metadata and physical validation with shrinking failures.
#![cfg(feature = "compression")]
use proptest::prelude::*;
use zstar::backend::{BlockingArchive, BlockingBuilder};
use zstar::{Entry, EntryKind, ManifestOptions, SliceReader};

fn entry(path: String) -> Entry {
    Entry {
        path,
        kind: EntryKind::File,
        mode: 0o640,
        uid: 0,
        gid: 0,
        mtime_ns: 0,
        extents: vec![],
        linkname: None,
        attributes: Default::default(),
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn generated_metadata_and_ranges_agree_with_original_payloads(
        files in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4097), 0..12),
        frame_size in 512usize..8193,
        picks in prop::collection::vec((any::<usize>(), any::<usize>(), any::<usize>()), 1..20),
        timestamp in any::<i64>(), uid in any::<u64>(), gid in any::<u64>(),
    ) {
        let mut writer = BlockingBuilder::with_options(Vec::new(), frame_size, 3, Default::default()).unwrap();
        for (index, bytes) in files.iter().enumerate() {
            let mut e = entry(format!("file-{index:02}-雪"));
            e.uid = uid; e.gid = gid; e.mtime_ns = timestamp;
            writer.add_file(e, bytes.len() as u64, &mut bytes.as_slice()).unwrap();
        }
        let encoded = writer.finish().unwrap();
        let mut reader = BlockingArchive::open(SliceReader::new(&encoded), Default::default()).unwrap();
        reader.validate().unwrap();
        for (choice, offset, length) in picks {
            if files.is_empty() { break; }
            let index = choice % files.len();
            let content = &files[index];
            let offset = offset % (content.len() + 1);
            let length = length % (content.len() - offset + 1);
            let name = format!("file-{index:02}-雪");
            prop_assert_eq!(reader.read(&name, offset as u64, length as u64, None, true).unwrap(), &content[offset..offset+length]);
            let e = reader.entry(&name).unwrap().unwrap();
            prop_assert_eq!((e.uid, e.gid, e.mtime_ns), (uid, gid, timestamp));
        }
    }
    #[test]
    fn exact_long_symlink_targets_round_trip(target in "[a-zA-Z0-9./雪_-]{1,500}", frame_size in 512usize..4097) {
        let mut writer = BlockingBuilder::with_options(Vec::new(), frame_size, 3, Default::default()).unwrap();
        let mut e = entry("link".into()); e.kind = EntryKind::Symlink; e.linkname = Some(target.clone());
        writer.add_metadata(e).unwrap();
        let encoded = writer.finish().unwrap();
        let mut reader = BlockingArchive::open(SliceReader::new(&encoded), Default::default()).unwrap();
        reader.validate().unwrap();
        prop_assert_eq!(reader.read_link("link").unwrap(), target);
    }
    #[test]
    fn untrusted_metadata_and_footer_never_panic(raw in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = zstar::parse_footer(&raw);
        let _ = zstar::manifest::decode_block(&raw);
        let _ = zstar::parse_member_header(&raw);
        let _ = BlockingArchive::open(SliceReader::new(&raw), ManifestOptions { max_decoded_bytes: 65536, ..Default::default() });
    }
    #[test]
    fn mutated_valid_archives_never_panic(changes in prop::collection::vec((any::<usize>(), any::<u8>()), 1..12), truncate in prop::option::of(any::<usize>())) {
        let mut writer = BlockingBuilder::with_options(Vec::new(), 1024, 3, Default::default()).unwrap();
        writer.add_file(entry("payload".into()), 255, &mut (0..255u8).collect::<Vec<_>>().as_slice()).unwrap();
        let mut encoded = writer.finish().unwrap(); let len = encoded.len();
        for (at, mask) in changes { encoded[at % len] ^= mask; }
        if let Some(at) = truncate { encoded.truncate(at % (len + 1)); }
        if let Ok(mut reader) = BlockingArchive::open(SliceReader::new(&encoded), ManifestOptions { max_decoded_bytes: 65536, ..Default::default() }) {
            let _ = reader.read("payload", 0, 255, None, true);
            let _ = reader.validate();
        }
    }
}
