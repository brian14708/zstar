#![cfg(unix)]
use std::collections::BTreeMap;
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Command, Output},
};
use zstar::SliceReader;
use zstar::backend::{BlockingArchive, BlockingBuilder};
use zstar::manifest::{Entry, EntryKind};

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
struct FixtureArchive(BTreeMap<String, (Entry, Vec<u8>)>);
impl FixtureArchive {
    fn new() -> Self {
        Self(BTreeMap::new())
    }
    fn add_directory(&mut self, path: &str, mode: u64) {
        self.0.insert(
            path.into(),
            (entry(path, EntryKind::Directory, mode), vec![]),
        );
    }
    fn add_file(&mut self, path: &str, bytes: Vec<u8>, mode: u64) {
        self.0
            .insert(path.into(), (entry(path, EntryKind::File, mode), bytes));
    }
    fn add_symlink(&mut self, path: &str, target: &str, mode: u64) {
        let mut metadata = entry(path, EntryKind::Symlink, mode);
        metadata.linkname = Some(target.into());
        self.0.insert(path.into(), (metadata, vec![]));
    }
    fn finish(self) -> zstar::Result<Vec<u8>> {
        let mut writer = BlockingBuilder::new(Vec::new())?;
        for (_, (entry, bytes)) in self.0 {
            if entry.kind == EntryKind::File {
                writer.add_file(entry, bytes.len() as u64, &mut bytes.as_slice())?;
            } else {
                writer.add_metadata(entry)?;
            }
        }
        writer.finish()
    }
}
fn corpus() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/entry-manifest")
}

fn run(args: &[&Path]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zstar"))
        .args(args)
        .output()
        .unwrap()
}
fn extract(archive: &Path, dest: &Path) -> Output {
    run(&[Path::new("extract"), archive, dest])
}

#[test]
fn pack_rejects_output_inside_source_and_preserves_existing_output_on_failure() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input");
    fs::create_dir(&input).unwrap();
    let inside = input.join("archive.zstar");
    assert!(!run(&[Path::new("pack"), &input, &inside]).status.success());
    assert!(!inside.exists());

    fs::write(input.join("non-utf8"), b"contents").unwrap();
    use std::os::unix::ffi::OsStringExt;
    let unsupported_entry =
        match fs::write(input.join(std::ffi::OsString::from_vec(vec![0xff])), b"bad") {
            Ok(()) => None,
            // Some Unix filesystems (including the GitHub Actions runner's) reject
            // non-UTF-8 names. Use another unsupported entry to exercise the same
            // pack failure path while still checking that the output is preserved.
            Err(_) => Some(
                std::os::unix::net::UnixListener::bind(input.join("unsupported-socket")).unwrap(),
            ),
        };
    let output = temp.path().join("existing.zstar");
    fs::write(&output, b"preserved").unwrap();
    assert!(
        !run(&[Path::new("pack"), &input, &output, Path::new("--force")])
            .status
            .success()
    );
    assert_eq!(fs::read(output).unwrap(), b"preserved");
    drop(unsupported_entry);
}
fn archive(root: &Path) -> std::path::PathBuf {
    let mut builder = FixtureArchive::new();
    builder.add_directory("dir", 0o555);
    builder.add_file("dir/data", b"hello".to_vec(), 0o4640);
    for (name, target) in [
        ("absolute", "/outside/missing"),
        ("dangling", "./dir/../absent"),
        ("cycle", "cycle"),
        ("outside", "../../escape"),
    ] {
        builder.add_symlink(name, target, 0o777);
    }
    let archive = root.join("input.zstar");
    fs::write(&archive, builder.finish().unwrap()).unwrap();
    archive
}
#[test]
fn preserves_exact_links_and_restores_permissions_without_implicit_setid() {
    let root = tempfile::tempdir().unwrap();
    let input = archive(root.path());
    let dest = root.path().join("out");
    let result = extract(&input, &dest);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read(dest.join("dir/data")).unwrap(), b"hello");
    assert_eq!(
        fs::metadata(dest.join("dir/data"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o640
    );
    assert_eq!(
        fs::metadata(dest.join("dir")).unwrap().permissions().mode() & 0o7777,
        0o555
    );
    assert_eq!(
        fs::read_link(dest.join("absolute")).unwrap(),
        Path::new("/outside/missing")
    );
    assert_eq!(
        fs::read_link(dest.join("dangling")).unwrap(),
        Path::new("./dir/../absent")
    );
    assert_eq!(
        fs::read_link(dest.join("outside")).unwrap(),
        Path::new("../../escape")
    );
    assert_eq!(
        fs::read_link(dest.join("cycle")).unwrap(),
        Path::new("cycle")
    );
    let second = root.path().join("authorized");
    let result = run(&[
        Path::new("extract"),
        &input,
        &second,
        Path::new("--allow-special-bits"),
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::metadata(second.join("dir/data"))
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o4640
    );
}
#[test]
fn refuses_root_and_ancestor_symlinks_and_file_collisions() {
    let root = tempfile::tempdir().unwrap();
    let input = archive(root.path());
    let outside = root.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let dest = root.path().join("out");
    symlink(&outside, &dest).unwrap();
    assert!(!extract(&input, &dest).status.success());
    assert!(fs::read_dir(&outside).unwrap().next().is_none());
    fs::remove_file(&dest).unwrap();
    fs::create_dir(&dest).unwrap();
    symlink(&outside, dest.join("dir")).unwrap();
    assert!(!extract(&input, &dest).status.success());
    assert!(fs::read_dir(&outside).unwrap().next().is_none());
    fs::remove_file(dest.join("dir")).unwrap();
    // Files are opened with create_new; preexisting regular content survives.
    let mut builder = FixtureArchive::new();
    builder.add_file("file", vec![1], 0o644);
    fs::write(&input, builder.finish().unwrap()).unwrap();
    fs::write(dest.join("file"), b"keep").unwrap();
    assert!(!extract(&input, &dest).status.success());
    assert_eq!(fs::read(dest.join("file")).unwrap(), b"keep");
    fs::remove_file(dest.join("file")).unwrap();
    let target = outside.join("target");
    fs::write(&target, b"target").unwrap();
    symlink(&target, dest.join("file")).unwrap();
    assert!(!extract(&input, &dest).status.success());
    assert_eq!(fs::read(target).unwrap(), b"target");
}
#[test]
fn extracts_thin_objects_without_pointer_leakage() {
    let root = tempfile::tempdir().unwrap();
    let corpus = corpus();
    let input = corpus.join("thin.zstar");
    let objects = root.path().join("objects");
    fs::create_dir(&objects).unwrap();
    for object in fs::read_dir(corpus.join("objects")).unwrap() {
        let object = object.unwrap();
        fs::copy(object.path(), objects.join(object.file_name())).unwrap();
    }
    let missing = root.path().join("missing-output");
    assert!(!extract(&input, &missing).status.success());
    assert!(!missing.join("hello.txt").exists());
    let dest = root.path().join("restored");
    let result = run(&[
        Path::new("extract"),
        &input,
        &dest,
        Path::new("--objects"),
        &objects,
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read(dest.join("hello.txt")).unwrap(), b"hello\n");
}

#[test]
fn external_hash_skip_keeps_metadata_codec_tar_pointer_and_path_checks() {
    for fixture in [
        "data-checksum",
        "metadata-hash",
        "tar-header",
        "tar-end",
        "pointer-mismatch",
    ] {
        let root = tempfile::tempdir().unwrap();
        let input = corpus().join("invalid").join(format!("{fixture}.zstar"));
        for skip in [false, true] {
            let out = root.path().join(if skip { "unchecked" } else { "checked" });
            let objects = corpus().join("objects");
            let mut args = vec![
                Path::new("extract"),
                &input,
                &out,
                Path::new("--objects"),
                &objects,
            ];
            if skip {
                args.push(Path::new("--skip-external-hash"));
            }
            assert!(
                !run(&args).status.success(),
                "accepted {fixture}, skip={skip}"
            );
        }
    }
    for malformed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let objects = root.path().join("objects");
        fs::create_dir(&objects).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let replacement = if malformed {
            b"not zstd".to_vec()
        } else {
            compressor.compress(&[0; 12]).unwrap()
        };
        for object in fs::read_dir(corpus().join("objects")).unwrap() {
            fs::write(objects.join(object.unwrap().file_name()), &replacement).unwrap();
        }
        let input = corpus().join("thin.zstar");
        for skip in [false, true] {
            let destination = root.path().join(if skip { "unchecked" } else { "checked" });
            let mut args = vec![
                Path::new("extract"),
                &input,
                &destination,
                Path::new("--objects"),
                &objects,
            ];
            if skip {
                args.push(Path::new("--skip-external-hash"));
            }
            let result = run(&args);
            assert_eq!(
                result.status.success(),
                skip && !malformed,
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            if result.status.success() {
                assert_eq!(fs::read(destination.join("hello.txt")).unwrap(), [0; 6]);
            }
        }
    }
    let root = tempfile::tempdir().unwrap();
    let input = archive(root.path());
    let outside = root.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let destination = root.path().join("destination");
    symlink(&outside, &destination).unwrap();
    assert!(
        !run(&[
            Path::new("extract"),
            &input,
            &destination,
            Path::new("--skip-external-hash")
        ])
        .status
        .success()
    );
    assert!(fs::read_dir(outside).unwrap().next().is_none());
    assert!(
        !run(&[
            Path::new("extract"),
            &input,
            &root.path().join("legacy-flag"),
            Path::new("--skip-content-hash")
        ])
        .status
        .success()
    );
}

#[test]
fn pack_captures_link_metadata_and_preserves_unix_filename_identity() {
    use std::os::unix::fs::MetadataExt;
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("back\\slash"), b"literal backslash").unwrap();
    symlink("/missing/.././target", input.join("link")).unwrap();
    let link_meta = fs::symlink_metadata(input.join("link")).unwrap();
    let output = root.path().join("packed.zstar");
    let result = run(&[
        Path::new("pack"),
        &input,
        &output,
        Path::new("--preserve-metadata"),
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let bytes = fs::read(output).unwrap();
    let mut archive = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    archive.validate().unwrap();
    let link = archive.entry("link").unwrap().unwrap();
    assert_eq!(link.linkname.as_deref(), Some("/missing/.././target"));
    assert_eq!(link.uid, u64::from(link_meta.uid()));
    assert_eq!(link.gid, u64::from(link_meta.gid()));
    assert_eq!(
        link.mtime_ns,
        link_meta.mtime() * 1_000_000_000 + link_meta.mtime_nsec()
    );
    assert_eq!(
        archive.read("back\\slash", 0, 17, None, true).unwrap(),
        b"literal backslash"
    );
}

#[test]
fn metadata_inspection_accepts_large_sources_without_claiming_unread_tar_validity() {
    use std::io::{Seek, SeekFrom, Write};
    let temp = tempfile::tempdir().unwrap();
    let bytes = BlockingBuilder::new(Vec::new()).unwrap().finish().unwrap();
    let mut footer = zstar::parse_footer(&bytes[bytes.len() - 80..]).unwrap();
    let old_start = footer.toc_offset;
    footer.toc_offset = (2 << 30) + old_start;
    let input = temp.path().join("large.zstar");
    let mut file = fs::File::create(&input).unwrap();
    file.write_all(&bytes[..old_start as usize]).unwrap();
    file.seek(SeekFrom::Start(footer.toc_offset)).unwrap();
    file.write_all(&bytes[old_start as usize..bytes.len() - 80])
        .unwrap();
    file.write_all(&zstar::encode_footer(&footer)).unwrap();
    let inspection = run(&[Path::new("inspect"), &input]);
    assert!(
        inspection.status.success(),
        "{}",
        String::from_utf8_lossy(&inspection.stderr)
    );
    assert!(String::from_utf8_lossy(&inspection.stdout).contains("entries=0"));
    let mut archive = BlockingArchive::open(
        zstar::fs::LocalFileSource::open(&input).unwrap(),
        Default::default(),
    )
    .unwrap();
    assert!(archive.validate().is_err());
}

#[test]
fn capture_controls_and_destination_handle_limits_are_local_policies() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::create_dir(input.join("a")).unwrap();
    fs::write(input.join("a/file"), [7; 5000]).unwrap();
    fs::write(input.join("a-b"), b"sibling sorts before slash").unwrap();
    let archive = root.path().join("archive.zstar");
    let packed = run(&[
        Path::new("pack"),
        &input,
        &archive,
        Path::new("--metadata-memory-bytes"),
        Path::new("1"),
        Path::new("--metadata-block-size"),
        Path::new("512"),
        Path::new("--frame-size"),
        Path::new("2048"),
        Path::new("--compression-level"),
        Path::new("1"),
        Path::new("--workers"),
        Path::new("1"),
    ]);
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stderr)
    );
    let bytes = fs::read(&archive).unwrap();
    let mut parsed = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    parsed.validate().unwrap();
    let limited = root.path().join("limited");
    let result = run(&[
        Path::new("extract"),
        &archive,
        &limited,
        Path::new("--max-directory-handles"),
        Path::new("1"),
    ]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("directory handles"));
    assert!(!limited.join("a/file").exists());
    let restored = root.path().join("restored");
    assert!(
        run(&[
            Path::new("extract"),
            &archive,
            &restored,
            Path::new("--max-directory-handles"),
            Path::new("2")
        ])
        .status
        .success()
    );
    assert_eq!(fs::read(restored.join("a/file")).unwrap(), [7; 5000]);
    assert_eq!(
        fs::read(restored.join("a-b")).unwrap(),
        b"sibling sorts before slash"
    );
}

#[test]
fn packed_capture_materialize_generation_and_standalone_export() {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("input");
    let objects = root.path().join("objects");
    let exported = root.path().join("standalone");
    fs::create_dir(&input).unwrap();
    fs::create_dir(&objects).unwrap();
    fs::create_dir(&exported).unwrap();
    let expected = vec![42; 3 << 20];
    fs::write(input.join("file"), &expected).unwrap();
    let thin = root.path().join("thin.zstar");
    let catalog = root.path().join("catalog.cbor");
    let result = run(&[
        Path::new("pack-objects"),
        &input,
        &thin,
        &catalog,
        Path::new("--store"),
        &objects,
        Path::new("--chunking"),
        Path::new("fastcdc"),
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let generation = String::from_utf8(result.stdout).unwrap();
    let portable = root.path().join("portable.zstar");
    let result = run(&[
        Path::new("materialize-objects"),
        &thin,
        &catalog,
        &portable,
        Path::new("--store"),
        &objects,
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let from_generation = root.path().join("generation.zstar");
    let result = run(&[
        Path::new("materialize-generation"),
        Path::new(generation.trim()),
        &from_generation,
        Path::new("--store"),
        &objects,
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(&portable).unwrap(),
        fs::read(from_generation).unwrap()
    );
    let bytes = fs::read(&portable).unwrap();
    let mut archive = BlockingArchive::open(SliceReader::new(&bytes), Default::default()).unwrap();
    archive.validate().unwrap();
    assert_eq!(
        archive
            .read("file", 0, expected.len() as u64, None, true)
            .unwrap(),
        expected
    );
    let result = run(&[
        Path::new("export-objects"),
        &catalog,
        Path::new("--store"),
        &objects,
        Path::new("--destination"),
        &exported,
    ]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(exported.join("objects").is_dir());
    let result = run(&[
        Path::new("pack-objects"),
        &input,
        &thin,
        &catalog,
        Path::new("--store"),
        &objects,
    ]);
    assert!(!result.status.success());
    assert_eq!(fs::read(portable).unwrap(), bytes);
}
