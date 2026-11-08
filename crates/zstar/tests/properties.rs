//! Generated logical ranges are checked against source bytes; hostile inputs
//! exercise bounded metadata and physical validation with shrinking failures.
use proptest::prelude::*;
use zstar::blocking::{Archive, Builder, MemorySource, ReaderOptions};
use zstar::{Header, MetadataReaderOptions, WriterOptions};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn generated_metadata_and_ranges_agree_with_original_payloads(
        files in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..4097), 0..12),
        frame_size in 512usize..8193,
        picks in prop::collection::vec((any::<usize>(), any::<usize>(), any::<usize>()), 1..20),
        timestamp in any::<i64>(), uid in any::<u64>(), gid in any::<u64>(),
    ) {
        let mut writer = Builder::try_with_options(Vec::new(), WriterOptions { frame_size, ..Default::default() }).unwrap();
        for (index, bytes) in files.iter().enumerate() {
            let mut e = Header::file(format!("file-{index:02}-雪"), bytes.len() as u64);
            e.set_uid(uid); e.set_gid(gid); e.set_mtime_ns(timestamp);
            writer.append_data(e, bytes.as_slice()).unwrap();
        }
        let encoded = writer.finish().unwrap();
        let mut reader = Archive::from_bytes(&encoded).unwrap();

        for (choice, offset, length) in picks {
            if files.is_empty() { break; }
            let index = choice % files.len();
            let content = &files[index];
            let offset = offset % (content.len() + 1);
            let length = length % (content.len() - offset + 1);
            let name = format!("file-{index:02}-雪");
            prop_assert_eq!(reader.read_range(&name, offset as u64..(offset+length) as u64).unwrap(), &content[offset..offset+length]);
            let e = reader.entry(&name).unwrap().unwrap();
            prop_assert_eq!((e.uid(), e.gid(), e.mtime_ns()), (uid, gid, timestamp));
        }
    }
    #[test]
    fn exact_long_symlink_targets_round_trip(target in "[a-zA-Z0-9./雪_-]{1,500}", frame_size in 512usize..4097) {
        let mut writer = Builder::try_with_options(Vec::new(), WriterOptions { frame_size, ..Default::default() }).unwrap();
        writer.append_symlink("link", target.clone()).unwrap();
        let encoded = writer.finish().unwrap();
        let reader = Archive::from_bytes(&encoded).unwrap();

        prop_assert_eq!(reader.read_link("link").unwrap(), target);
    }
    #[test]
    fn untrusted_metadata_and_footer_never_panic(raw in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = zstar::backend::parse_footer(&raw);
        let _ = zstar::backend::manifest::decode_block(&raw);
        let _ = zstar::backend::parse_member_header(&raw);
        let _ = Archive::open_with_options(MemorySource::new(&raw), ReaderOptions { metadata: MetadataReaderOptions { max_page_bytes: 65536, ..Default::default() }, ..Default::default() });
    }
    #[test]
    fn mutated_valid_archives_never_panic(changes in prop::collection::vec((any::<usize>(), any::<u8>()), 1..12), truncate in prop::option::of(any::<usize>())) {
        let mut writer = Builder::try_with_options(Vec::new(), WriterOptions { frame_size: 1024, ..Default::default() }).unwrap();
        writer.append_bytes("payload", &(0..255u8).collect::<Vec<_>>()).unwrap();
        let mut encoded = writer.finish().unwrap(); let len = encoded.len();
        for (at, mask) in changes { encoded[at % len] ^= mask; }
        if let Some(at) = truncate { encoded.truncate(at % (len + 1)); }
        if let Ok(mut reader) = Archive::open_with_options(MemorySource::new(&encoded), ReaderOptions { metadata: MetadataReaderOptions { max_page_bytes: 65536, ..Default::default() }, ..Default::default() }) {
            let _ = reader.read_range("payload", 0..255);

        }
    }
}
