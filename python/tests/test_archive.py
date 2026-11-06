"""Exercise the public API using standard tarfile metadata and binary streams."""

import asyncio
import io
import os
import subprocess
import sys
import tarfile
from decimal import localcontext
from pathlib import Path
from tarfile import TarInfo

import pytest
import zstar
import zstar._native
from zstar import ArchiveWriter, AsyncArchiveWriter, _native
from zstar._info import _nanoseconds


@pytest.fixture
def encoded_archive():
    output = io.BytesIO()
    with ArchiveWriter(output) as archive:
        directory = tarfile.TarInfo("dir")
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        info = TarInfo("dir/file")
        info.size, info.uid, info.gid = 12, 123, 456
        info.pax_headers["mtime"] = "-1.234567891"
        archive.addfile(info, io.BytesIO(b"hello\nworld\n"))
        link = tarfile.TarInfo("dir/link")
        link.type, link.linkname = tarfile.SYMTYPE, "file"
        archive.addfile(link)
    assert archive.closed and not output.closed
    return output.getvalue()


def test_tar_info_lazy_members_file_streams_and_owned_metadata(encoded_archive):
    data = encoded_archive
    with zstar.Archive(data) as archive:
        members = archive.members()
        assert iter(members) is members
        assert next(members).name == "dir"
        assert [member.name for member in members] == ["dir/file", "dir/link"]
        assert list(members) == []
        assert list(archive.names()) == [member.name for member in archive]
        info = archive.getinfo("dir/file")
        assert isinstance(info, TarInfo) and info.isfile()
        assert (info.size, info.uid, info.gid, _nanoseconds(info.pax_headers["mtime"])) == (
            12,
            123,
            456,
            -1234567891,
        )
        info.pax_headers["mtime"] = "0"
        assert archive.getinfo(info.name).pax_headers["mtime"] == "-1.234567891"
        with pytest.raises(IsADirectoryError):
            archive.open("dir")
        with archive.open(info) as member:
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
        with pytest.raises(tarfile.ExtractError, match="symlink"):
            archive.open("dir/link")
        assert archive.read_link("dir/link") == "file"
        assert archive.read_range("dir/file", 6, 12) == b"world\n"
        with pytest.raises(KeyError):
            archive.getinfo("missing")
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
    with zstar.Archive.from_file(path) as archive:
        assert archive.read("dir/file") == b"hello\nworld\n"
        member = archive.open("dir/file")
    with pytest.raises(ValueError, match="closed"):
        member.read()
    borrowed = io.BytesIO(data)
    borrowed.seek(7)
    with zstar.Archive.from_file(borrowed) as archive:
        assert archive.read("dir/file", 0, 5) == b"hello"
        assert borrowed.tell() == 7
    assert not borrowed.closed and borrowed.tell() == 7


def test_sync_reader_inside_running_event_loop(encoded_archive):
    async def exercise():
        with zstar.Archive(encoded_archive) as archive:
            assert archive.read("dir/file") == b"hello\nworld\n"
            assert list(archive.names()) == ["dir", "dir/file", "dir/link"]
        borrowed = io.BytesIO(encoded_archive)
        borrowed.seek(7)
        with zstar.Archive.from_file(borrowed) as archive:
            assert archive.read("dir/file") == b"hello\nworld\n"
        assert borrowed.tell() == 7 and not borrowed.closed

    asyncio.run(exercise())


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
            lambda: zstar.AsyncArchive.from_file(path),
            lambda: zstar.AsyncArchive.from_file(borrowed),
        ):
            async with await constructor() as archive:
                members = archive.members()
                assert aiter(members) is members
                assert (await anext(members)).isdir()
                assert [member.name async for member in members] == ["dir/file", "dir/link"]
                assert [name async for name in archive.names()] == [
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
                with pytest.raises(tarfile.ExtractError, match="symlink"):
                    async with archive.open("dir/link"):
                        pytest.fail("symlink opened as a file")
                assert await archive.read_link("dir/link") == "file"
                async with archive.open("dir/file") as member:
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
                async with archive.open("dir/file") as pending:
                    await archive.close()
                    with pytest.raises(ValueError, match="closed"):
                        await pending.read()
        assert not borrowed.closed and borrowed.tell() == 11

    asyncio.run(exercise())


def test_iteration_crosses_metadata_batches_and_close_invalidates_buffered_entries():
    output = io.BytesIO()
    expected = [f"file-{index:03}" for index in range(200)]
    with ArchiveWriter(output) as writer:
        for name in reversed(expected):
            writer.addfile(tarfile.TarInfo(name))
    data = output.getvalue()
    with zstar.Archive(data) as archive:
        assert list(archive.names()) == expected
        assert [member.name for member in archive] == expected
        names = archive.names()
        assert next(names) == expected[0]
    with pytest.raises(ValueError, match="closed"):
        next(names)

    async def exercise():
        async with await zstar.AsyncArchive.from_bytes(data) as archive:
            assert [name async for name in archive.names()] == expected
            assert [member.name async for member in archive] == expected
            names = archive.names()
            assert await anext(names) == expected[0]
        with pytest.raises(ValueError, match="closed"):
            await anext(names)

    asyncio.run(exercise())


def test_open_rejects_symlinks_without_resolving_targets():
    output = io.BytesIO()
    links = [("a", "b"), ("b", "a"), ("absolute", "/etc/passwd"), ("dangling", "missing")]
    with ArchiveWriter(output) as archive:
        for name, target in links:
            info = tarfile.TarInfo(name)
            info.type, info.linkname = tarfile.SYMTYPE, target
            archive.addfile(info)
    data = output.getvalue()
    with zstar.Archive(data) as archive:
        for name, target in links:
            assert archive.getinfo(name).issym()
            assert archive.read_link(name) == target
            for member in (name, archive.getinfo(name)):
                with pytest.raises(tarfile.ExtractError, match="symlink"):
                    archive.open(member)
            with pytest.raises(zstar.ZstarError):
                archive.read(name)

    async def exercise():
        async with await zstar.AsyncArchive.from_bytes(data) as archive:
            for name, target in links:
                assert (await archive.getinfo(name)).issym()
                assert await archive.read_link(name) == target
                for member in (name, await archive.getinfo(name)):
                    with pytest.raises(tarfile.ExtractError, match="symlink"):
                        async with archive.open(member):
                            pytest.fail("symlink opened as a file")
                with pytest.raises(zstar.ZstarError):
                    await archive.read(name)

    asyncio.run(exercise())


@pytest.mark.parametrize("timestamp", ["1e1000000", "-1e1000000"])
def test_timestamp_range_is_checked_before_integer_conversion(timestamp):
    info = TarInfo("file")
    info.pax_headers["mtime"] = timestamp
    with (
        pytest.raises(OverflowError, match="timestamp range"),
        ArchiveWriter.from_file(io.BytesIO(), "w") as writer,
    ):
        writer.addfile(info)


@pytest.mark.parametrize(
    "timestamp", [-(1 << 63), -1234567891, 0, 1700000000123456789, (1 << 63) - 1]
)
def test_timestamps_ignore_application_decimal_context(timestamp):
    info = TarInfo("file")
    seconds, fraction = divmod(abs(timestamp), 1_000_000_000)
    info.pax_headers["mtime"] = f"{'-' if timestamp < 0 else ''}{seconds}.{fraction:09d}"
    output = io.BytesIO()
    with ArchiveWriter(output) as writer:
        writer.addfile(info)
    with localcontext() as context:
        context.prec = 3
        context.Emax = 3
        with zstar.Archive(output.getvalue()) as reader:
            snapshot = reader.getinfo("file")
            assert _nanoseconds(snapshot.pax_headers["mtime"]) == timestamp
            rewritten = io.BytesIO()
            with ArchiveWriter(rewritten) as writer:
                writer.addfile(snapshot)
        with zstar.Archive(rewritten.getvalue()) as reader:
            assert _nanoseconds(reader.getinfo("file").pax_headers["mtime"]) == timestamp


def test_main_reader_metadata_open_does_not_certify_unread_content(corpus_dir):
    data = (corpus_dir / "invalid/data-checksum.zstar").read_bytes()
    with zstar.Archive(data) as archive:
        assert archive.getinfo("a-b").isfile()
        with archive.open("a-b") as member:
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
    data = _native.pack_file("x", b"hello")
    source = source_factory(data)
    archive = zstar._native.ManifestArchive(source, len(data)) if ranged else zstar.Archive(data)
    with pytest.raises(RuntimeError, match="consumer failed"), archive:
        assert archive.read("x") == b"hello"
        raise RuntimeError("consumer failed")
    assert archive.closed and not source.closed
    archive.close()
    with pytest.raises(ValueError, match="closed"), archive:
        pass


def test_external_closure_verification_obeys_read_budget(object_resolver, thin_data):
    from zstar import Archive

    encoded = thin_data

    objects = object_resolver
    archive = Archive(encoded, max_read_bytes=1, resolver=objects, verify_external=True)
    report = archive.verify(external=True)
    assert report["external_closure_validated"]
    assert report["external_objects_sha256_checked"]
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


def test_write_bytes_and_bounded_chunk_iteration(tmp_path):
    path = tmp_path / "archive.zstar"
    body = b"hello world"
    with ArchiveWriter.from_file(path, "w") as writer:
        writer.write_bytes("hello.txt", body, mode=0o600)
    with zstar.Archive.from_file(path, max_read_bytes=3) as archive:
        assert archive.getinfo("hello.txt").mode == 0o600
        with archive.open("hello.txt") as member:
            assert member.read(2) == b"he"
            chunks = list(member.iter_chunks(100))
            assert chunks == [b"llo", b" wo", b"rld"]
            assert list(member.iter_chunks()) == []
            member.seek(0)
            for size in (0, -1):
                with pytest.raises(ValueError, match="positive"):
                    list(member.iter_chunks(size))
            with pytest.raises(TypeError):
                list(member.iter_chunks(1.5))
        with pytest.raises(ValueError, match="closed"):
            list(member.iter_chunks())


class _ReadOnlyBody:
    """Structural Reader[bytes], without IOBase inheritance or seek/close."""

    def __init__(self, data: bytes):
        self._stream = io.BytesIO(data)

    def read(self, size: int = -1, /) -> bytes:
        return self._stream.read(min(size, 3) if size >= 0 else size)


class _SeekableInput(_ReadOnlyBody):
    def seek(self, offset: int, whence: int = 0, /) -> int:
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        return self._stream.tell()


class _WriteOnlySink:
    """Structural Writer[bytes] with flush, without read/seek/tell/close."""

    def __init__(self):
        self.data = bytearray()
        self.flushed = False

    def write(self, data: bytes, /) -> int:
        count = min(len(data), 7)
        self.data.extend(data[:count])
        return count

    def flush(self) -> None:
        self.flushed = True


def test_minimal_structural_streams():
    output = _WriteOnlySink()
    info = TarInfo("hello")
    info.size = 11
    with ArchiveWriter.from_file(output, "w") as writer:
        writer.addfile(info, _ReadOnlyBody(b"hello world"))
    assert output.flushed
    source = _SeekableInput(bytes(output.data))
    source.seek(5)
    with zstar.Archive.from_file(source) as archive:
        assert archive.read("hello") == b"hello world"
    assert source.tell() == 5

    async def exercise():
        output = _WriteOnlySink()
        async with await AsyncArchiveWriter.from_file(output, "w") as writer:
            await writer.addfile(info, _ReadOnlyBody(b"hello world"))
        assert output.flushed
        source = _SeekableInput(bytes(output.data))
        source.seek(9)
        async with await zstar.AsyncArchive.from_file(source) as archive:
            assert await archive.read("hello") == b"hello world"
        assert source.tell() == 9

    asyncio.run(exercise())


def test_borrowed_sink_flush_errors_propagate():
    class BrokenFlush(_WriteOnlySink):
        def flush(self) -> None:
            raise OSError("flush failed")

    with (
        pytest.raises(zstar.ZstarError, match="flush failed"),
        ArchiveWriter.from_file(BrokenFlush(), "w") as writer,
    ):
        writer.write_bytes("hello", b"hello")
    assert writer.closed

    async def exercise():
        with pytest.raises(zstar.ZstarError, match="flush failed"):
            async with await AsyncArchiveWriter.from_file(BrokenFlush(), "w") as writer:
                await writer.write_bytes("hello", b"hello")
        assert writer.closed

    asyncio.run(exercise())


def test_raw_s3_requires_explicit_configuration(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "environment-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
    with pytest.raises(ValueError, match="region is required"):
        _native.SyncArchive.from_s3("bucket", "archive")
    with pytest.raises(ValueError, match="requires access_key_id"):
        zstar.Archive.from_s3("bucket", "archive", region="us-east-1")
    with pytest.raises(ValueError, match="cannot include credentials"):
        zstar.Archive.from_s3(
            "bucket", "archive", region="us-east-1", anon=True, access_key_id="key"
        )

    async def exercise():
        with pytest.raises(ValueError, match="requires access_key_id"):
            await zstar.AsyncArchive.from_s3("bucket", "archive", region="us-east-1")

    asyncio.run(exercise())


def test_prefix_iteration_and_async_metadata(encoded_archive):
    with zstar.Archive(encoded_archive) as archive:
        assert list(archive.names("dir/f")) == ["dir/file"]
        assert [info.name for info in archive.members("dir/l")] == ["dir/link"]
        with pytest.raises(ValueError):
            archive.read_range("dir/file", 2, 1)

    async def run():
        async with await zstar.AsyncArchive.from_bytes(
            encoded_archive, concurrent_chunks=1
        ) as archive:
            assert await archive.read_link("dir/link") == "file"
            assert await archive.read_range("dir/file", 0, 5) == b"hello"
            assert [name async for name in archive.names("dir/f")] == ["dir/file"]

    asyncio.run(run())


def test_wire_metadata_stays_private():
    from zstar import types

    for name in ("CborValue", "EntryFields", "EntryTuple", "ExtentTuple", "ManifestOptions"):
        assert not hasattr(types, name)
    for cls in (zstar.Archive, zstar.AsyncArchive):
        for name in ("entry", "iter_entries"):
            assert not hasattr(cls, name)
    for cls in (ArchiveWriter, AsyncArchiveWriter):
        assert not hasattr(cls, "add_entry")
    with ArchiveWriter(io.BytesIO(), root_extensions={}):
        pass


@pytest.mark.parametrize("asynchronous", [False, True])
def test_projected_metadata_preserves_fields_and_skips_content(asynchronous):
    # Exercise more than one native batch and multiple extents without fetching
    # external objects. Ordinary iteration authenticates extensions without projecting them.
    output = io.BytesIO()
    extents = ((2, "sha256:" + "01" * 32, 10), (3, "sha256:" + "02" * 32, 12))
    with _native.EntryArchiveWriter(output) as writer:
        for index in range(130):
            writer.add(
                (
                    f"f{index:04}",
                    0,
                    0o640,
                    12,
                    34,
                    -1234567891,
                    extents,
                    {"x-note": b"private" * 1024},
                )
            )
        writer.add(("link", 2, 0o777, 56, 78, 1234567891, (), {"linkname": "./f0000"}))

    def check(info):
        assert isinstance(info, TarInfo)
        assert (info.size, info.mode, info.uid, info.gid) == (5, 0o640, 12, 34)
        assert info.pax_headers == {"mtime": "-1.234567891"}
        info.pax_headers.clear()

    async def run():
        data = output.getvalue()
        if asynchronous:
            async with await zstar.AsyncArchive.from_bytes(data, memory_bytes=1) as archive:
                assert [name async for name in archive.names("f01")] == [
                    f"f{i:04}" for i in range(100, 130)
                ]
                with pytest.warns(UserWarning, match="TarInfo omits Zstar manifest extensions"):
                    members = [info async for info in archive.members()]
                assert len(members) == 131
                check(members[0])
                with pytest.warns(UserWarning, match="TarInfo omits Zstar manifest extensions"):
                    info = await archive.getinfo("f0000")
                check(info)
                assert (await archive.getinfo("link")).linkname == "./f0000"
                with pytest.raises(KeyError):
                    await archive.getinfo("missing")
                pending = archive.names()
                assert await anext(pending) == "f0000"
            with pytest.raises(ValueError, match="closed"):
                await anext(pending)
        else:
            with zstar.Archive(data, memory_bytes=1) as archive:
                assert list(archive.names("f01")) == [f"f{i:04}" for i in range(100, 130)]
                with pytest.warns(UserWarning, match="TarInfo omits Zstar manifest extensions"):
                    members = list(archive.members())
                assert len(members) == 131
                check(members[0])
                with pytest.warns(UserWarning, match="TarInfo omits Zstar manifest extensions"):
                    info = archive.getinfo("f0000")
                check(info)
                assert archive.getinfo("link").linkname == "./f0000"
                with pytest.raises(KeyError):
                    archive.getinfo("missing")
                pending = archive.names()
                assert next(pending) == "f0000"
            with pytest.raises(ValueError, match="closed"):
                next(pending)

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_member_streams_share_native_bounds_and_buffer_semantics(asynchronous):
    from contextlib import AsyncExitStack

    data = bytes(_native.pack_file("data", b"hello world"))

    async def exercise():
        async with AsyncExitStack() as stack:
            if asynchronous:
                archive = await stack.enter_async_context(
                    await zstar.AsyncArchive.from_bytes(data, max_read_bytes=3)
                )
                member = await stack.enter_async_context(archive.open("data"))
            else:
                archive = stack.enter_context(zstar.Archive(data, max_read_bytes=3))
                member = stack.enter_context(archive.open("data"))

            async def call(name, *args):
                result = getattr(member, name)(*args)
                return await result if asynchronous else result

            assert member.size == 11
            assert await call("read", 2) == b"he"
            for size in (None, -1, 1 << 100):
                with pytest.raises(zstar.ZstarError) as caught:
                    await call("read", size)
                assert caught.value.category == "resource-limit"
                assert await call("tell") == 2
            with pytest.raises(TypeError):
                await call("read", 1.5)
            for offset, whence, error in (
                (-1, os.SEEK_SET, ValueError),
                (0, 123, ValueError),
                (1 << 64, os.SEEK_SET, OverflowError),
            ):
                with pytest.raises(error):
                    await call("seek", offset, whence)
                assert await call("tell") == 2

            target = bytearray(b"?" * 16)
            with memoryview(target)[2:14] as view:
                assert await call("readinto", view) == 9
            assert target == b"??llo world?????"
            assert await call("tell") == 11
            target.extend(b"!")  # Native buffer exports have been released.
            assert await call("seek", -3, os.SEEK_END) == 8
            assert await call("read", 1 << 100) == b"rld"
            assert await call("seek", (1 << 64) - 1) == (1 << 64) - 1
            assert await call("read") == b""
            assert await call("readinto", target) == 0
            with pytest.raises(OverflowError):
                await call("seek", 1, os.SEEK_CUR)
            assert await call("seek", 0) == 0
            with pytest.raises(TypeError, match="writable"):
                await call("readinto", b"readonly")
            with memoryview(target)[::2] as view, pytest.raises(TypeError):
                await call("readinto", view)
            assert await call("tell") == 0

    asyncio.run(exercise())
