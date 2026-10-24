"""Exercise the public API using standard tarfile metadata and binary streams."""

import asyncio
import io
import os
import stat
import subprocess
import sys
import tarfile
from decimal import localcontext
from pathlib import Path

import pytest
import zstar
import zstar.format


@pytest.fixture
def encoded_archive():
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as archive:
        directory = tarfile.TarInfo("dir")
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        info = zstar.TarInfo("dir/file")
        info.size, info.uid, info.gid = 12, 123, 456
        info.mtime_ns = -1234567891
        info.extensions = {"x-label": {"value": b"owned"}}
        archive.addfile(info, io.BytesIO(b"hello\nworld\n"))
        link = tarfile.TarInfo("dir/link")
        link.type, link.linkname = tarfile.SYMTYPE, "file"
        archive.addfile(link)
    assert archive.closed and not output.closed
    return output.getvalue()


def test_tar_info_lazy_members_file_streams_and_owned_metadata(encoded_archive):
    data = encoded_archive
    with zstar.Archive(data) as archive:
        members = archive.getmembers()
        assert iter(members) is members
        assert next(members).name == "dir"
        assert [member.name for member in members] == ["dir/file", "dir/link"]
        assert list(members) == []
        assert list(archive.getnames()) == [member.name for member in archive]
        info = archive.getmember("dir/file")
        assert isinstance(info, tarfile.TarInfo) and info.isfile()
        assert (info.size, info.uid, info.gid, info.mtime_ns) == (12, 123, 456, -1234567891)
        info.extensions["x-label"]["value"] = b"changed"
        assert archive.getmember(info.name).extensions["x-label"]["value"] == b"owned"
        assert archive.extractfile("dir") is None
        with archive.extractfile(info) as member:
            assert isinstance(member, io.RawIOBase)
            assert member.name == "dir/file" and member.mode == "rb"
            assert member.readable() and member.seekable()
            buffer = bytearray(5)
            assert member.readinto(buffer) == 5 and buffer == b"hello"
            assert member.tell() == 5
            assert member.seek(-6, os.SEEK_END) == 6
            assert member.readline() == b"world\n"
            member.seek(6)
            buffer = bytearray(b"?" * 10)
            assert member.readinto(memoryview(buffer)[1:]) == 6
            assert buffer == b"?world\n???" and member.tell() == 12
            assert member.readinto(buffer) == 0
            with pytest.raises(TypeError, match="writable"):
                member.readinto(b"readonly")
            buffer.extend(b"?")  # No exported native buffer remains after a read.
            assert member.seek(100) == 100 and member.read() == b""
            with pytest.raises(ValueError):
                member.seek(-1)
        assert member.closed and not archive.closed
        with pytest.raises(ValueError):
            member.read()
        with archive.extractfile("dir/link") as linked:
            assert linked.read() == b"hello\nworld\n"
        with pytest.raises(KeyError):
            archive.getmember("missing")
        with pytest.raises(KeyError):
            archive.read("missing")
        report = archive.verify()
        assert report["tar_members"] == 3
        assert report["external_objects_sha256_checked"] == []
        assert report["data_checksums_checked"] and report["full_archive_verified"]
        assert not report["external_closure_validated"]
        assert "whole_file_sha256_checked" not in report
        assert archive.read("dir/file", 6) == b"world\n"
        assert archive.read_with_report("dir/file", 6)[0] == b"world\n"
        assert archive.read_with_report("dir/file", 12)[0] == b""
        for length in (None, 1):
            with pytest.raises(KeyError):
                archive.read_with_report("missing", length=length)
            with pytest.raises(zstar.ZstarError) as caught:
                archive.read_with_report("dir/file", 13, length)
            assert caught.value.category == "invalid-range"


def test_sync_local_open_and_borrowed_file_preserve_ownership_and_cursor(tmp_path, encoded_archive):
    data = encoded_archive
    path = tmp_path / "archive.zstar"
    path.write_bytes(data)
    with zstar.Archive.open(path) as archive:
        assert archive.read("dir/file") == b"hello\nworld\n"
        member = archive.extractfile("dir/file")
    with pytest.raises(ValueError, match="closed"):
        member.read()
    borrowed = io.BytesIO(data)
    borrowed.seek(7)
    with zstar.Archive.open(fileobj=borrowed) as archive:
        assert archive.read("dir/file", 0, 5) == b"hello"
        assert borrowed.tell() == 7
    assert not borrowed.closed and borrowed.tell() == 7
    with pytest.raises(TypeError):
        zstar.Archive.open(path, fileobj=borrowed)


def test_sync_and_async_verification_share_one_report_schema(object_resolver, thin_data):
    data = thin_data

    with zstar.Archive(data, resolver=object_resolver) as archive:
        synchronous = archive.verify(external=True)

    async def exercise():
        async with await zstar.AsyncArchive.from_bytes(data, resolver=object_resolver) as archive:
            assert await archive.verify(external=True) == synchronous

    asyncio.run(exercise())


def test_async_local_open_lazy_iteration_and_cursor_operations(tmp_path, encoded_archive):
    path = tmp_path / "archive.zstar"
    data = encoded_archive
    path.write_bytes(data)
    borrowed = io.BytesIO(data)
    borrowed.seek(11)

    async def exercise():
        for constructor in (
            lambda: zstar.AsyncArchive.open(path),
            lambda: zstar.AsyncArchive.open(fileobj=borrowed),
        ):
            async with await constructor() as archive:
                members = archive.getmembers()
                assert aiter(members) is members
                assert (await anext(members)).isdir()
                assert [member.name async for member in members] == ["dir/file", "dir/link"]
                assert [name async for name in archive.getnames()] == [
                    "dir",
                    "dir/file",
                    "dir/link",
                ]
                assert (await archive.read_with_report("dir/file", 6))[0] == b"world\n"
                assert (await archive.read_with_report("dir/file", 12))[0] == b""
                for length in (None, 1):
                    with pytest.raises(KeyError):
                        await archive.read_with_report("missing", length=length)
                    with pytest.raises(zstar.ZstarError) as caught:
                        await archive.read_with_report("dir/file", 13, length)
                    assert caught.value.category == "invalid-range"
                async with await archive.extractfile("dir/link") as member:
                    assert await member.read(5) == b"hello"
                    assert await member.tell() == 5
                    assert await member.seek(6) == 6
                    buffer = bytearray(6)
                    assert await member.readinto(buffer) == 6 and buffer == b"world\n"
                    await member.seek(6)
                    buffer = bytearray(b"?" * 10)
                    assert await member.readinto(memoryview(buffer)[1:]) == 6
                    assert buffer == b"?world\n???" and await member.tell() == 12
                    assert await member.readinto(buffer) == 0
                    with pytest.raises(TypeError, match="writable"):
                        await member.readinto(b"readonly")
                    buffer.extend(b"?")
                    assert await member.seek(100) == 100 and await member.read() == b""
                assert member.closed and not archive.closed
                pending = await archive.extractfile("dir/file")
            with pytest.raises(ValueError, match="closed"):
                await pending.read()
            await pending.close()
        assert not borrowed.closed and borrowed.tell() == 11

    asyncio.run(exercise())


def test_iteration_crosses_metadata_batches_and_close_invalidates_buffered_entries():
    output = io.BytesIO()
    expected = [f"file-{index:03}" for index in range(200)]
    with zstar.ArchiveWriter(output) as writer:
        for name in reversed(expected):
            writer.addfile(tarfile.TarInfo(name))
    data = output.getvalue()
    with zstar.Archive(data) as archive:
        assert list(archive.getnames()) == expected
        assert [member.name for member in archive] == expected
        names = archive.getnames()
        assert next(names) == expected[0]
    with pytest.raises(ValueError, match="closed"):
        next(names)

    async def exercise():
        async with await zstar.AsyncArchive.from_bytes(data) as archive:
            assert [name async for name in archive.getnames()] == expected
            assert [member.name async for member in archive] == expected
            names = archive.getnames()
            assert await anext(names) == expected[0]
        with pytest.raises(ValueError, match="closed"):
            await anext(names)

    asyncio.run(exercise())


def test_extractfile_resolves_only_archive_links_and_rejects_cycles():
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as archive:
        for name, target in [("a", "b"), ("b", "a"), ("absolute", "/etc/passwd")]:
            info = tarfile.TarInfo(name)
            info.type, info.linkname = tarfile.SYMTYPE, target
            archive.addfile(info)
    with zstar.Archive(output.getvalue()) as archive:
        with pytest.raises(tarfile.ExtractError, match="cyclic"):
            archive.extractfile("a")
        with pytest.raises(KeyError):
            archive.extractfile("absolute")


@pytest.mark.skipif(os.name != "posix", reason="Unix file metadata")
def test_writer_add_recurses_without_following_links_and_supports_owned_output(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    (source / "link").symlink_to("file")
    output = tmp_path / "output.zstar"
    with zstar.ArchiveWriter.open(output, mode="x") as archive:
        archive.add(source, arcname="root")
    with zstar.Archive.open(output) as archive:
        assert list(archive.getnames()) == ["root", "root/file", "root/link"]
        assert archive.getmember("root/link").issym()
        assert archive.getmember("root/link").linkname == "file"
        assert archive.getmember("root/file").mtime_ns == 0
        archive.verify()
    with pytest.raises(FileExistsError):
        zstar.ArchiveWriter.open(output, mode="x")


@pytest.mark.skipif(os.name != "posix", reason="requires no-follow symlink support")
def test_writer_open_refuses_to_truncate_symlink_target(tmp_path):
    target = tmp_path / "target"
    target.write_bytes(b"keep")
    output = tmp_path / "output.zstar"
    output.symlink_to(target)
    with pytest.raises(OSError):
        zstar.ArchiveWriter.open(output)
    assert target.read_bytes() == b"keep"


@pytest.mark.skipif(os.name != "posix", reason="Unix file metadata")
def test_writer_nonrecursive_directory_preserves_metadata(tmp_path):
    source = tmp_path / "source"
    source.mkdir(mode=0o750)
    (source / "child").write_bytes(b"excluded")
    os.utime(source, ns=(1700000000000000000, 1700000000123456789))
    metadata = source.stat()
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.add(source, "root", recursive=False, preserve_metadata=True)
    with zstar.Archive(output.getvalue()) as archive:
        assert list(archive.getnames()) == ["root"]
        entry = archive.getmember("root")
        assert entry.mode == stat.S_IMODE(metadata.st_mode)
        assert (entry.uid, entry.gid) == (metadata.st_uid, metadata.st_gid)
        assert entry.mtime_ns == metadata.st_mtime_ns
        archive.verify()


def test_writer_removes_partial_output_even_when_owned_close_fails(tmp_path, monkeypatch):
    import zstar._writer

    class FailingClose(io.BufferedWriter):
        def close(self):
            super().close()
            raise OSError("owned output close failed")

    monkeypatch.setattr(
        zstar._writer.os,
        "fdopen",
        lambda fd, mode: FailingClose(io.FileIO(fd, mode, closefd=True)),
    )
    output = tmp_path / "partial.zstar"
    with (
        pytest.raises(OSError, match="owned output close failed"),
        zstar.ArchiveWriter.open(output) as writer,
    ):
        writer.addfile(zstar.TarInfo("empty"))
        raise RuntimeError("capture failed")
    assert writer.closed
    assert not output.exists()


def test_capture_normalizes_metadata_sorts_physical_members_and_preserves_on_request(tmp_path):
    root = tmp_path / "source"
    (root / "a").mkdir(parents=True)
    (root / "a/file").write_bytes(b"one")
    file = root / "a-b"
    file.write_bytes(b"two")
    file.chmod(0o6751)
    os.utime(file, ns=(1700000000000000000, 1700000000123456789))
    # Filesystems may clear setgid or ignore Unix execute bits (Windows).
    normalized_mode = 0o755 if file.stat().st_mode & 0o111 else 0o644

    def capture(preserve=False):
        output = io.BytesIO()
        with zstar.ArchiveWriter(output) as writer:
            writer.add(root, "root", preserve_metadata=preserve)
        return output.getvalue()

    data = capture()
    data_end = int.from_bytes(data[-56:-48], "little")
    decoded = zstar.format.decode_object(data[:data_end], 4096)
    with tarfile.open(fileobj=io.BytesIO(decoded), mode="r:") as physical:
        assert physical.getnames() == ["root", "root/a", "root/a-b", "root/a/file"]
    with zstar.Archive(data) as reader:
        reader.verify()
        info = reader.getmember("root/a-b")
        assert (info.mode, info.uid, info.gid, info.mtime_ns) == (normalized_mode, 0, 0, 0)
    os.utime(file, ns=(1700000000000000000, 1700000000999999999))
    assert capture() == data
    metadata = file.stat()
    with zstar.Archive(capture(True)) as reader:
        info = reader.getmember("root/a-b")
        assert (info.mode, info.uid, info.gid, info.mtime_ns) == (
            stat.S_IMODE(metadata.st_mode),
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_mtime_ns,
        )
        reader.verify()


def test_capture_deep_directories_without_python_recursion(tmp_path):
    root = tmp_path / "deep"
    root.mkdir()
    child = root
    # Exceed the Python recursion limit without exceeding macOS's PATH_MAX.
    recursion_limit = sys.getrecursionlimit()
    depth = 250
    try:
        for _ in range(depth):
            next_child = child / "d"
            next_child.mkdir()
            child = next_child
        sys.setrecursionlimit(200)
        output = io.BytesIO()
        with zstar.ArchiveWriter(output, memory_bytes=1) as writer:
            writer.add(root, "root")
        with zstar.Archive(output.getvalue()) as reader:
            assert reader.entry_count == depth + 1
            reader.verify()
    finally:
        sys.setrecursionlimit(recursion_limit)
        while child != root:
            parent = child.parent
            child.rmdir()
            child = parent


def test_capture_merges_directory_siblings_in_utf8_path_order(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    directories = ["a", "a.b", "aé", "z", "z/a"]
    files = ["a/file", "a-b", "a.b/z", "aé/file", "z/a/z", "z/a-"]
    for name in directories:
        (root / name).mkdir()
    for name in files:
        (root / name).touch()
    expected = sorted(
        ["root", *[f"root/{name}" for name in directories + files]],
        key=lambda name: name.encode("utf-8"),
    )
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.add(root, "root")
    data = output.getvalue()
    data_end = int.from_bytes(data[-56:-48], "little")
    decoded = zstar.format.decode_object(data[:data_end], 512 * len(expected) + 1024)
    with tarfile.open(fileobj=io.BytesIO(decoded), mode="r:") as physical:
        assert physical.getnames() == expected
    with zstar.Archive(data) as reader:
        reader.verify()


@pytest.mark.parametrize("timestamp", ["1e1000000", "-1e1000000"])
def test_timestamp_range_is_checked_before_integer_conversion(timestamp):
    info = zstar.TarInfo("file")
    info.pax_headers["mtime"] = timestamp
    with pytest.raises(OverflowError, match="timestamp range"):
        _ = info.mtime_ns


@pytest.mark.parametrize(
    "timestamp", [-(1 << 63), -1234567891, 0, 1700000000123456789, (1 << 63) - 1]
)
def test_timestamps_ignore_application_decimal_context(timestamp):
    info = zstar.TarInfo("file")
    info.mtime_ns = timestamp
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.addfile(info)
    with localcontext() as context:
        context.prec = 3
        context.Emax = 3
        with zstar.Archive(output.getvalue()) as reader:
            snapshot = reader.getmember("file")
            assert snapshot.mtime_ns == timestamp
            rewritten = io.BytesIO()
            with zstar.ArchiveWriter(rewritten) as writer:
                writer.addfile(snapshot)
        with zstar.Archive(rewritten.getvalue()) as reader:
            assert reader.getmember("file").mtime_ns == timestamp


def test_writer_rejects_duplicates_at_finalization_without_retaining_names():
    output = io.BytesIO()
    writer = zstar.ArchiveWriter(output, memory_bytes=1)
    for _ in range(2):
        writer.addfile(zstar.TarInfo("file"))
    with pytest.raises(zstar.ZstarError, match=r"duplicate|order"):
        writer.close()


def test_main_reader_metadata_open_does_not_certify_unread_content(corpus_dir):
    data = (corpus_dir / "invalid/data-checksum.zstar").read_bytes()
    with zstar.Archive(data) as archive:
        assert archive.getmember("a-b").isfile()
        with archive.extractfile("a-b") as member:
            buffer = bytearray(b"?" * 5)
            with pytest.raises(zstar.ZstarError):
                member.readinto(buffer)
            assert buffer == b"?????" and member.tell() == 0
        with pytest.raises(zstar.ZstarError):
            archive.verify()


def test_missing_native_extension_reports_installation_guidance(tmp_path):
    package = tmp_path / "zstar"
    package.mkdir()
    (package / "__init__.py").write_text(Path(zstar.__file__).read_text())
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"import sys; sys.path.insert(0, {str(tmp_path)!r}); import zstar",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "native extension is unavailable" in result.stderr
    assert "just check" in result.stderr


@pytest.mark.parametrize("ranged", [False, True])
def test_sync_readers_close_after_context_exception_without_closing_source(ranged, source_factory):
    data = zstar.pack_file("x", b"hello")
    source = source_factory(data)
    archive = zstar.format.ManifestArchive(source, len(data)) if ranged else zstar.Archive(data)
    with pytest.raises(RuntimeError, match="consumer failed"), archive:
        assert archive.read("x") == b"hello"
        raise RuntimeError("consumer failed")
    assert archive.closed and not source.closed
    archive.close()
    with pytest.raises(ValueError, match="closed"), archive:
        pass


def test_writer_discards_unfinished_output_after_context_exception():
    partial = io.BytesIO()
    info = tarfile.TarInfo("x")
    info.size = 5
    with (
        pytest.raises(RuntimeError, match="producer failed"),
        zstar.ArchiveWriter(partial) as writer,
    ):
        writer.addfile(info, io.BytesIO(b"hello"))
        raise RuntimeError("producer failed")
    assert writer.closed and not partial.closed
    with pytest.raises(zstar.ZstarError):
        zstar.Archive(partial.getvalue())


def test_default_bytes_archive_external_reuse_and_bounded_closure_verification(
    object_resolver, thin_data
):
    from zstar import Archive

    encoded = thin_data

    objects = object_resolver
    archive = Archive(encoded, max_read_bytes=1, resolver=objects)
    report = archive.verify(external=True)
    assert report["external_closure_validated"]
    assert report["external_objects_sha256_checked"]
    assert objects.calls == 1
    assert archive.read("world.txt", 0, 1) == b"w"
    with pytest.raises(zstar.ZstarError, match="budget"):
        archive.read("world.txt")


def test_encoded_resolver_missing_objects_keep_stable_category(thin_data):
    from zstar import Archive

    class Missing:
        def get_encoded(self, address, hint, limit):
            raise FileNotFoundError(address)

    encoded = thin_data
    with pytest.raises(zstar.ZstarError) as caught:
        Archive(encoded).read("hello.txt", resolver=Missing())
    assert caught.value.category == "missing-external-content"
