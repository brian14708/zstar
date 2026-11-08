"""Typed archive metadata, positional reads, and streams."""

import asyncio
import io
import subprocess
import sys
from pathlib import Path

import pytest
import zstar
from _support import maybe
from zstar import Header, ZstarError, _native
from zstar._fileobj import fileobj
from zstar.types import ReaderOptions


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
