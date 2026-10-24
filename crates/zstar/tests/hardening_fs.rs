#![cfg(all(feature = "fs", unix))]
use std::{collections::BTreeMap, fs, os::unix::fs::PermissionsExt, path::Path};
use zstar::backend::{BlockingArchive, BlockingBuilder};
use zstar::fs::{ExtractOptions, OmittedField, extract_with_report};
use zstar::{Entry, EntryKind, SliceReader};

fn entry(path: &str, kind: EntryKind, mode: u64) -> Entry {
    Entry {
        path: path.into(),
        kind,
        mode,
        uid: 0,
        gid: 0,
        mtime_ns: 0,
        extents: vec![],
        linkname: None,
        attributes: BTreeMap::new(),
    }
}
fn build(items: Vec<(Entry, Vec<u8>)>) -> Vec<u8> {
    let mut writer = BlockingBuilder::new(Vec::new()).unwrap();
    for (entry, bytes) in items {
        if entry.kind == EntryKind::File {
            writer
                .add_file(entry, bytes.len() as u64, &mut bytes.as_slice())
                .unwrap();
        } else {
            writer.add_metadata(entry).unwrap();
        }
    }
    writer.finish().unwrap()
}
fn run(
    bytes: &[u8],
    dest: &Path,
    options: ExtractOptions,
) -> zstar::fs::AnyResult<zstar::fs::ExtractReport> {
    let mut archive = BlockingArchive::open(SliceReader::new(bytes), Default::default()).unwrap();
    extract_with_report(&mut archive, dest, None, options)
}

#[test]
fn deep_and_wide_trees_exceed_the_default_handle_limit() {
    let mut items = vec![];
    for i in 0..1200 {
        items.push((
            entry(&format!("d{i:04}"), EntryKind::Directory, 0o755),
            vec![],
        ));
        items.push((
            entry(&format!("d{i:04}/f"), EntryKind::File, 0o644),
            vec![i as u8],
        ));
    }
    // A deep chain as well.
    let mut path = String::new();
    for i in 0..40 {
        path = if path.is_empty() {
            format!("z{i}")
        } else {
            format!("{path}/z{i}")
        };
        items.push((entry(&path, EntryKind::Directory, 0o755), vec![]));
    }
    items.push((
        entry(&format!("{path}/leaf"), EntryKind::File, 0o644),
        b"deep".to_vec(),
    ));
    let bytes = build(items);
    let temp = tempfile::tempdir().unwrap();
    let dest = temp.path().join("out");
    let report = run(&bytes, &dest, ExtractOptions::default()).unwrap();
    assert_eq!(report.directories, 1240);
    assert_eq!(fs::read(dest.join("d1199/f")).unwrap(), [1199_u32 as u8]);
    assert_eq!(
        fs::read(dest.join(format!("{path}/leaf"))).unwrap(),
        b"deep"
    );
    // Still works with a tiny limit.
    let tiny = temp.path().join("tiny");
    let options = ExtractOptions {
        max_directory_handles: 2,
        ..Default::default()
    };
    run(&bytes, &tiny, options).unwrap();
    assert_eq!(
        fs::read(tiny.join("d0500/f")).unwrap(),
        [(500_u32 % 256) as u8]
    );
}

#[test]
fn failed_extract_leaves_no_partial_tree() {
    let mut bytes = build(vec![
        (entry("a", EntryKind::Directory, 0o555), vec![]),
        (entry("a/one", EntryKind::File, 0o600), vec![1; 100_000]),
        (entry("z", EntryKind::File, 0o644), vec![2; 100_000]),
    ]);
    // Damage the data frame so failure happens after files were written.
    let len = bytes.len();
    bytes[len / 4] ^= 0xff;
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("new");
    assert!(run(&bytes, &missing, ExtractOptions::default()).is_err());
    assert!(!missing.exists());
    assert_eq!(
        fs::read_dir(temp.path()).unwrap().count(),
        0,
        "temp dir left"
    );
    // Pre-existing destination: created entries are removed, others kept.
    let existing = temp.path().join("existing");
    fs::create_dir(&existing).unwrap();
    fs::write(existing.join("keep"), b"keep").unwrap();
    let error = run(&bytes, &existing, ExtractOptions::default());
    assert!(error.is_err());
    let names: Vec<_> = fs::read_dir(&existing)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["keep"]);
}

#[test]
fn collision_error_names_the_archive_path() {
    let bytes = build(vec![(entry("file", EntryKind::File, 0o644), vec![1])]);
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("file"), b"mine").unwrap();
    let error = run(&bytes, temp.path(), ExtractOptions::default()).unwrap_err();
    assert!(error.to_string().starts_with("file:"), "{error}");
    assert_eq!(fs::read(temp.path().join("file")).unwrap(), b"mine");
}

#[test]
fn dot_destination_extracts_into_the_current_directory_handle() {
    let bytes = build(vec![(entry("file", EntryKind::File, 0o644), b"x".to_vec())]);
    let temp = tempfile::tempdir().unwrap();
    // "<dir>/." has no file name; no process-wide chdir is needed.
    let dest = temp.path().join(".");
    run(&bytes, &dest, ExtractOptions::default()).unwrap();
    assert_eq!(fs::read(temp.path().join("file")).unwrap(), b"x");
    let dotdot = temp.path().join("sub");
    fs::create_dir(&dotdot).unwrap();
    run(&bytes, &dotdot.join(".."), ExtractOptions::default()).unwrap_err();
}

#[test]
fn default_symlink_mode_is_restored() {
    let mut link = entry("link", EntryKind::Symlink, 0o777);
    link.linkname = Some(".././missing".into());
    let bytes = build(vec![(link, vec![])]);
    let temp = tempfile::tempdir().unwrap();
    let dest = temp.path().join("out");
    let report = run(&bytes, &dest, ExtractOptions::default()).unwrap();
    assert_eq!(
        fs::symlink_metadata(dest.join("link"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o777
    );
    assert_eq!(
        fs::read_link(dest.join("link")).unwrap(),
        Path::new(".././missing")
    );
    assert_eq!(report.omitted_count(OmittedField::SymlinkMode), 0);
}

#[test]
fn metadata_not_applied_is_reported_and_symlink_modes_do_not_abort() {
    let mut link = entry("link", EntryKind::Symlink, 0o640);
    link.linkname = Some("target".into());
    let mut owned = entry("owned", EntryKind::File, 0o4755);
    owned.uid = u64::from(u32::MAX - 1);
    let bytes = build(vec![
        (link, vec![]),
        (owned, b"x".to_vec()),
        (entry("plain", EntryKind::File, 0o644), b"y".to_vec()),
    ]);
    let temp = tempfile::tempdir().unwrap();
    let dest = temp.path().join("out");
    let report = run(&bytes, &dest, ExtractOptions::default()).unwrap();
    assert_eq!(
        fs::read_link(dest.join("link")).unwrap(),
        Path::new("target")
    );
    assert_eq!(
        fs::metadata(dest.join("owned"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o755
    );
    assert!(!report.is_lossless());
    assert_eq!(report.omitted_count(OmittedField::SpecialModeBits), 1);
    // Every entry recorded uid 0 or a huge uid; unless run as root, all differ.
    assert!(report.omitted_count(OmittedField::Ownership) >= 1);
    #[cfg(target_os = "linux")]
    assert_eq!(report.omitted_count(OmittedField::SymlinkMode), 1);
    assert_eq!(report.files, 2);
    assert_eq!(report.symlinks, 1);
    let ownership = report
        .omitted
        .iter()
        .find(|o| o.field == OmittedField::Ownership)
        .unwrap();
    assert!(ownership.examples.iter().any(|p| p == "owned"));
    // Allowing special bits removes that class from the report.
    let again = temp.path().join("again");
    let options = ExtractOptions {
        allow_special_bits: true,
        ..Default::default()
    };
    let report = run(&bytes, &again, options).unwrap();
    assert_eq!(report.omitted_count(OmittedField::SpecialModeBits), 0);
}
