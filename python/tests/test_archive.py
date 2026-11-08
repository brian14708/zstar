"""Typed archive metadata, positional reads, streams, and explicit tar conversion."""

import asyncio
import io
import subprocess
import sys
from decimal import localcontext
from pathlib import Path
from tarfile import TarInfo

import pytest
import zstar
from _support import maybe
from zstar import (
    EntryKind,
    Header,
    ReaderOptions,
    ZstarError,
    _native,
)
from zstar._fileobj import fileobj
from zstar._info import _nanoseconds
from zstar.tar import from_tarinfo, to_tarinfo


def fixture_bytes():
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        writer.addfile(Header.directory("dir"))
        writer.addfile(
            Header.file(
                "dir/data",
                11,
                mode=0o640,
                uid=12,
                gid=34,
                mtime_ns=-1_234_567_891,
                extensions={"x-meta": [1, b"v"]},
            ),
            io.BytesIO(b"hello world"),
        )
        writer.addfile(Header.symlink("link", "../dir//data"))
    return output.getvalue()


def test_tar_conversion_preserves_exact_timestamp_and_warns_on_extension_loss():
    with _native.Source.memory(fixture_bytes()) as source, zstar.open(source) as archive:
        entry = archive.entry("dir/data")
        with pytest.warns(UserWarning, match="omits .*extensions"):
            info = to_tarinfo(entry)
        assert (info.name, info.size, info.mode, info.uid, info.gid) == (
            "dir/data",
            11,
            0o640,
            12,
            34,
        )
        assert info.pax_headers["mtime"] == "-1.234567891"
        header = from_tarinfo(info, extensions=entry.extensions)
        assert header.mtime_ns == entry.mtime_ns
        assert header.extensions == entry.extensions
        assert from_tarinfo(to_tarinfo(archive.entry("link"))).linkname == "../dir//data"
        assert from_tarinfo(to_tarinfo(archive.entry("dir"))).kind is EntryKind.DIRECTORY
    assert header.path == entry.path


@pytest.mark.parametrize("timestamp", ["1e1000000", "-1e1000000", "9223372036.854775808"])
def test_timestamp_range_is_checked_before_integer_conversion(timestamp):
    info = TarInfo("data")
    info.pax_headers["mtime"] = timestamp
    with pytest.raises(OverflowError):
        from_tarinfo(info)


@pytest.mark.parametrize("timestamp", ["-1.234567891", "1734567890.123456789"])
def test_timestamps_ignore_application_decimal_context(timestamp):
    with localcontext() as context:
        context.prec = 2
        assert _nanoseconds(timestamp) == int(timestamp.replace(".", ""))


def test_tar_rejects_unsupported_members_and_invalid_metadata():
    info = TarInfo("hardlink")
    info.type = b"1"
    with pytest.raises(ValueError):
        from_tarinfo(info)
    for timestamp in ["nan", "inf", "0.0000000001"]:
        info = TarInfo("data")
        info.pax_headers["mtime"] = timestamp
        with pytest.raises(ValueError):
            from_tarinfo(info)


def test_sync_reader_inside_running_loop_and_borrowed_file_cursor():
    async def run():
        file = io.BytesIO(fixture_bytes())
        file.seek(7)
        with zstar.open(fileobj(file)) as archive:
            assert archive.read("dir/data", offset=6, length=5) == b"world"
            assert [entry.path for entry in archive.entries("dir/")] == ["dir/data"]
        assert not file.closed and file.tell() == 7

    asyncio.run(run())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_member_stream_bounds_buffers_and_decoded_reuse(asynchronous):
    data = fixture_bytes()

    async def run():
        archive = (
            await zstar.open(await _native.AsyncSource.memory(data), async_=True)
            if asynchronous
            else zstar.open(_native.Source.memory(data))
        )

        stream = await maybe(archive.open("dir/data"))
        for buffer in [b"readonly", memoryview(bytearray(8))[::2]]:
            with pytest.raises((TypeError, BufferError, ValueError)):
                await maybe(stream.readinto(buffer))
        assert await maybe(stream.tell()) == 0
        buffer = bytearray(20)
        assert await maybe(stream.readinto(buffer)) == 11
        assert buffer[:11] == b"hello world" and buffer[11:] == bytes(9)
        assert await maybe(stream.read(1)) == b""
        assert await maybe(stream.seek(20)) == 20
        assert await maybe(stream.read()) == b""
        with pytest.raises((ValueError, OverflowError)):
            await maybe(stream.seek(-1))
        await maybe(stream.close())
        await maybe(archive.close())

    asyncio.run(run())


def test_large_readinto_is_separate_from_allocating_read_budget():
    with (
        zstar.open(
            _native.Source.memory(fixture_bytes()), options=ReaderOptions(max_read_bytes=3)
        ) as archive,
        archive.open("dir/data") as stream,
    ):
        with pytest.raises(ZstarError):
            stream.read()
        assert stream.tell() == 0
        assert list(stream.iter_chunks(100)) == [b"hel", b"lo ", b"wor", b"ld"]
        stream.seek(0)
        buffer = bytearray(11)
        assert stream.readinto(buffer) == 11 and buffer == b"hello world"


def test_missing_native_extension_reports_installation_guidance(tmp_path):
    package = tmp_path / "zstar"
    package.mkdir()
    source = Path(__file__).parents[1] / "src/zstar/__init__.py"
    (package / "__init__.py").write_text(source.read_text())
    result = subprocess.run(
        [sys.executable, "-c", "import zstar"], cwd=tmp_path, capture_output=True, text=True
    )
    assert result.returncode != 0
    assert "native extension is unavailable" in result.stderr
