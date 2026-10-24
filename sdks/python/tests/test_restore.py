import os

import pytest
import zstar


@pytest.mark.skipif(os.name != "posix", reason="Unix filesystem restoration")
def test_pack_restore_preserves_exact_symlinks_and_metadata(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data").write_bytes(b"hello")
    (source / "data").chmod(0o640)
    (source / "absolute").symlink_to("/missing/.././target")
    (source / "dangling").symlink_to(".././outside")
    data = zstar.pack_directory(source, preserve_metadata=True)
    assert isinstance(data, bytes)
    archive = zstar.Archive(data)
    assert archive.read("data") == b"hello"
    destination = tmp_path / "destination"
    zstar.extract(data, destination)
    assert (destination / "data").read_bytes() == b"hello"
    assert os.readlink(destination / "absolute") == "/missing/.././target"
    assert os.readlink(destination / "dangling") == ".././outside"
    assert (destination / "data").stat().st_mode & 0o7777 == 0o640
    assert (destination / "absolute").lstat().st_mtime_ns == (
        source / "absolute"
    ).lstat().st_mtime_ns
    with pytest.raises(ValueError):
        zstar.extract(data, destination)  # no silent overwrite


@pytest.mark.skipif(os.name != "posix", reason="Unix filesystem restoration")
def test_extract_refuses_destination_symlink(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    destination = tmp_path / "dest"
    destination.symlink_to(outside)
    with pytest.raises(ValueError):
        zstar.extract(bytes(zstar.pack_file("file", b"hello")), destination)
    assert list(outside.iterdir()) == []


def test_materialize_external_objects(thin_data, corpus_dir):
    with pytest.raises(ValueError, match="requires"):
        zstar.materialize(thin_data)
    result = zstar.materialize(thin_data, objects=corpus_dir / "objects")
    assert isinstance(result, bytes)
    with zstar.Archive(result) as archive:
        assert archive.read("hello.txt") == b"hello\n"


@pytest.mark.parametrize("operation", ["materialize", "extract"])
@pytest.mark.parametrize("verify", [True, False])
@pytest.mark.parametrize("malformed", [True, False])
def test_external_hash_policy_keeps_codec_validation(
    tmp_path, operation, verify, malformed, corpus_dir, thin_data
):
    if operation == "extract" and os.name != "posix":
        pytest.skip("Unix filesystem restoration")
    objects = tmp_path / "objects"
    objects.mkdir()
    replacement = (
        b"not zstd" if malformed else (corpus_dir / "invalid/object-wrong-hash.zst").read_bytes()
    )
    for path in (corpus_dir / "objects").glob("*.zst"):
        (objects / path.name).write_bytes(replacement)

    def restore():
        if operation == "materialize":
            encoded = zstar.materialize(thin_data, objects=objects, verify_external=verify)
            with zstar.Archive(encoded) as archive:
                assert archive.verify()["full_archive_verified"]
                return archive.read("hello.txt")
        destination = tmp_path / "destination"
        zstar.extract(thin_data, destination, objects=objects, verify_external=verify)
        return (destination / "hello.txt").read_bytes()

    if verify or malformed:
        with pytest.raises(zstar.ZstarError) as caught:
            restore()
        assert caught.value.category == (
            "malformed-external-content" if malformed else "integrity-failure"
        )
    else:
        assert restore() == b"wrong\n"


@pytest.mark.parametrize(
    "fixture", ["metadata-hash", "data-checksum", "tar-header", "tar-end", "pointer-mismatch"]
)
def test_filesystem_hash_opt_out_keeps_physical_and_metadata_validation(
    tmp_path, fixture, corpus_dir
):
    source = (corpus_dir / "invalid" / f"{fixture}.zstar").read_bytes()
    with pytest.raises(ValueError):
        zstar.materialize(source, objects=corpus_dir / "objects", verify_external=False)
    if os.name == "posix":
        with pytest.raises(ValueError):
            zstar.extract(
                source,
                tmp_path / "destination",
                objects=corpus_dir / "objects",
                verify_external=False,
            )


@pytest.mark.skipif(os.name == "posix", reason="Non-Unix filesystem contract")
def test_non_unix_extraction_reports_unsupported(tmp_path):
    encoded = zstar.pack_file("file", b"hello")
    with pytest.raises(ValueError, match="Unix filesystem"):
        zstar.extract(encoded, tmp_path / "destination")


@pytest.mark.skipif(os.name != "posix", reason="Unix capture metadata")
def test_default_capture_normalizes_metadata_and_is_reproducible(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "dir").mkdir()
    for name, mode in [("file", 0o6640), ("exec", 0o6751)]:
        (source / name).write_bytes(name.encode())
        (source / name).chmod(mode)
    (source / "link").symlink_to(".././missing")
    first = zstar.pack_directory(source)
    (source / "file").chmod(0o600)
    (source / "exec").chmod(0o711)
    for path in [source, *source.rglob("*")]:
        os.utime(path, ns=(1234567890, 9876543210), follow_symlinks=False)
    assert first == zstar.pack_directory(source)
    destination = tmp_path / "out"
    zstar.extract(first, destination)
    for name, mode in [("file", 0o644), ("exec", 0o755), ("dir", 0o755), ("link", 0o777)]:
        metadata = (destination / name).lstat()
        assert metadata.st_mode & 0o7777 == mode
        assert metadata.st_mtime_ns == 0
    assert first != zstar.pack_directory(source, preserve_metadata=True)
