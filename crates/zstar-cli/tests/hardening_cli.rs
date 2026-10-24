#![cfg(unix)]
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::{Command, Output},
};

fn run(args: &[&Path]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zstar"))
        .args(args)
        .output()
        .unwrap()
}
fn run_str(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_zstar"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn pack_refuses_existing_output_without_force_and_honors_umask() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("f"), b"data").unwrap();
    let output = temp.path().join("a.zstar");
    let result = run(&[Path::new("pack"), &input, &output]);
    assert!(result.status.success());
    // Not the 0600 of a bare temp file: group/other read bits follow the umask.
    let mode = fs::metadata(&output).unwrap().permissions().mode() & 0o777;
    let umask_probe = temp.path().join("probe");
    fs::write(&umask_probe, b"").unwrap();
    let expected = fs::metadata(&umask_probe).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, expected & 0o666);
    let first = fs::read(&output).unwrap();
    let again = run(&[Path::new("pack"), &input, &output]);
    assert!(!again.status.success());
    assert!(String::from_utf8_lossy(&again.stderr).contains("--force"));
    assert_eq!(fs::read(&output).unwrap(), first);
    fs::write(&output, b"old").unwrap();
    assert!(
        run(&[Path::new("pack"), &input, &output, Path::new("--force")])
            .status
            .success()
    );
    assert_ne!(fs::read(&output).unwrap(), b"old");
}

#[test]
fn pack_rejects_output_symlinked_into_input() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input");
    fs::create_dir(&input).unwrap();
    let inside = input.join("target");
    fs::write(&inside, b"keep").unwrap();
    let link = temp.path().join("link.zstar");
    symlink(&inside, &link).unwrap();
    let result = run(&[Path::new("pack"), &input, &link, Path::new("--force")]);
    assert!(!result.status.success());
    assert_eq!(fs::read(&inside).unwrap(), b"keep");
}

#[test]
fn argument_ranges_are_validated() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("in");
    fs::create_dir(&input).unwrap();
    let out = temp.path().join("o.zstar");
    let (i, o) = (input.to_str().unwrap(), out.to_str().unwrap());
    for bad in [
        ["--workers", "0"],
        ["--workers", "17"],
        ["--compression-level", "23"],
        ["--compression-level", "-131073"],
    ] {
        let result = run_str(&["pack", i, o, bad[0], bad[1]]);
        assert!(!result.status.success(), "{bad:?}");
        assert!(!out.exists());
    }
    for good in [["--compression-level", "22"], ["--compression-level", "-5"]] {
        let result = run_str(&["pack", i, o, "--force", good[0], good[1]]);
        assert!(result.status.success(), "{good:?}");
    }
}

#[test]
fn extract_reports_unapplied_metadata_on_stderr() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("in");
    fs::create_dir(&input).unwrap();
    fs::write(input.join("f"), b"x").unwrap();
    fs::set_permissions(input.join("f"), fs::Permissions::from_mode(0o4755)).unwrap();
    let archive = temp.path().join("a.zstar");
    let packed = run(&[
        Path::new("pack"),
        &input,
        &archive,
        Path::new("--preserve-metadata"),
    ]);
    assert!(packed.status.success());
    let out = temp.path().join("out");
    let result = run(&[Path::new("extract"), &archive, &out]);
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("SpecialModeBits"));
}
