//! Temporary sorted capture metadata. Payload lengths never become entry fields.
use crate::manifest::Entry;
use crate::manifest_io::{ManifestOptions, Spool, sort_records};
use crate::{Error, Result};
use ciborium::Value;

fn invalid() -> Error {
    Error::MalformedToc("capture record".into())
}
fn key(value: &Value) -> Result<String> {
    let Value::Array(record) = value else {
        return Err(invalid());
    };
    let Some(Value::Array(entry)) = record.first() else {
        return Err(invalid());
    };
    let Some(Value::Text(path)) = entry.first() else {
        return Err(invalid());
    };
    Ok(path.clone())
}

/// An unsorted capture stream with bounded in-memory sorting runs and native
/// temporary-file spill. Input entries contain no inline extents yet.
pub struct CaptureSpool {
    records: Spool,
    options: ManifestOptions,
    encoded: Vec<u8>,
}
impl CaptureSpool {
    #[must_use]
    pub fn new(options: ManifestOptions) -> Self {
        Self {
            records: Spool::new(options.clone()),
            options,
            encoded: Vec::new(),
        }
    }
    pub fn add(&mut self, entry: Entry, payload_size: u64) -> Result<()> {
        entry.validate(u64::MAX)?;
        if !entry.extents.is_empty()
            || entry.kind != crate::manifest::EntryKind::File && payload_size != 0
        {
            return Err(Error::Unsupported(
                "capture requires metadata without extents",
            ));
        }
        self.encoded.clear();
        crate::cbor::append_head(&mut self.encoded, 4, 2);
        entry.append_encoded(&mut self.encoded)?;
        crate::cbor::append_head(&mut self.encoded, 0, payload_size);
        self.records.append_encoded_record(&self.encoded)
    }
    pub fn sorted(self) -> Result<CapturedEntries> {
        Ok(CapturedEntries {
            records: sort_records(self.records, self.options, key)?,
            at: 0,
            failed: false,
        })
    }
}

/// Sorted, owned entries, each paired with its temporary capture length.
pub struct CapturedEntries {
    records: Spool,
    at: u64,
    failed: bool,
}
impl Iterator for CapturedEntries {
    type Item = Result<(Entry, u64)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let result = (|| {
            let Some(value) = self.records.read_record(&mut self.at)? else {
                return Ok(None);
            };
            let Value::Array(record) = value else {
                return Err(invalid());
            };
            if record.len() != 2 {
                return Err(invalid());
            }
            let Value::Integer(size) = record[1] else {
                return Err(invalid());
            };
            let size = u64::try_from(size).map_err(|_| invalid())?;
            Ok(Some((Entry::from_value(&record[0], u64::MAX)?, size)))
        })();
        match result {
            Ok(value) => value.map(Ok),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

impl std::iter::FusedIterator for CapturedEntries {}

#[cfg(all(test, feature = "spooling"))]
mod tests {
    use super::*;
    use crate::manifest::EntryKind;
    #[test]
    fn typed_capture_records_preserve_canonical_bytes_and_optional_metadata() {
        let mut spool = CaptureSpool::new(Default::default());
        let mut at = 0;
        for (kind, size) in [
            (EntryKind::File, u64::MAX),
            (EntryKind::Symlink, 0),
            (EntryKind::Directory, 0),
            (EntryKind::File, 24),
        ] {
            let entry = Entry {
                path: "entry".into(),
                kind,
                mode: 0o6754,
                uid: u64::MAX,
                gid: 24,
                mtime_ns: i64::MIN,
                extents: vec![],
                linkname: (kind == EntryKind::Symlink).then(|| "../target".into()),
                attributes: [("x-test".into(), Value::Bytes(vec![0, 1, 255]))].into(),
            };
            let expected = crate::cbor::encode_checked(&Value::Array(vec![
                entry.to_value(),
                Value::Integer(size.into()),
            ]))
            .unwrap();
            spool.add(entry, size).unwrap();
            let length = u32::from_le_bytes(spool.records.read(at, 4).unwrap().try_into().unwrap());
            assert_eq!(length as usize, expected.len());
            assert_eq!(
                spool.records.read(at + 4, u64::from(length)).unwrap(),
                expected
            );
            at += 4 + u64::from(length);
        }
    }
    #[test]
    fn spilled_capture_sorts_utf8_paths_across_runs_and_keeps_payload_lengths() {
        let mut spool = CaptureSpool::new(ManifestOptions {
            memory_bytes: 1,
            ..Default::default()
        });
        let mut expected = Vec::new();
        for (index, path) in (0..5000).rev().map(|i| (i, format!("f{i:04}"))).chain(
            ["é", "a/file", "a-b", "a", "z"]
                .into_iter()
                .map(|p| (0, p.into())),
        ) {
            let kind = if path == "a" {
                EntryKind::Directory
            } else {
                EntryKind::File
            };
            let entry = Entry {
                path: path.clone(),
                kind,
                mode: 420,
                uid: 0,
                gid: 0,
                mtime_ns: 0,
                extents: vec![],
                linkname: None,
                attributes: Default::default(),
            };
            spool.add(entry, index).unwrap();
            expected.push((path, index));
        }
        assert!(spool.records.spilled());
        expected.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let actual: Vec<_> = spool
            .sorted()
            .unwrap()
            .map(|v| {
                let (entry, size) = v.unwrap();
                (entry.path, size)
            })
            .collect();
        assert_eq!(actual, expected);
    }
    #[test]
    fn corrupt_last_capture_record_errors_instead_of_ending_iteration() {
        let mut spool = CaptureSpool::new(Default::default());
        spool
            .records
            .append_record(&Value::Text("bad record".into()))
            .unwrap();
        let mut entries = CapturedEntries {
            records: spool.records,
            at: 0,
            failed: false,
        };
        assert!(entries.next().unwrap().is_err());
        assert!(entries.next().is_none());
    }
}
