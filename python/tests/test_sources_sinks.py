"""Native storage ownership and explicit blocking adapter contracts."""

import asyncio
import io
import subprocess
import sys
import threading

import pytest
import zstar
from _support import pack_file
from zstar import (
    Header,
    ZstarError,
    _native,
)
from zstar._fileobj import fileobj


@pytest.mark.parametrize("url", [False, True])
def test_native_file_round_trip_and_borrowed_lifetime(tmp_path, url):
    path = tmp_path / "archive ü.tar.zst"
    location = path.as_uri() if url else str(path)
    with _native.open_sink(location) as sink:
        with zstar.open(sink, mode="w") as writer:
            writer.writestr("data", b"hello")
        assert not sink.closed
    with _native.open_source(location) as source:
        with zstar.open(source) as archive:
            assert archive.read("data") == b"hello"
        assert not source.closed
    assert source.closed
    with pytest.raises(ValueError, match="closed"):
        source.read_at(0, 1)


def test_native_async_file_round_trip(tmp_path):
    async def run():
        path = str(tmp_path / "async.tar.zst")
        async with await _native.aopen_sink(path) as sink:
            async with zstar.open(sink, mode="w", async_=True) as writer:
                await writer.writestr("data", b"hello")
            assert not sink.closed
        async with await _native.aopen_source(path) as source:
            async with zstar.open(source, async_=True) as archive:
                assert await archive.read("data") == b"hello"
            assert not source.closed

    asyncio.run(run())


def test_memory_snapshots_are_owned():
    original = bytearray(pack_file("data", b"hello"))
    with _native.Source.memory(original) as source:
        original[:] = b"x" * len(original)
        with zstar.open(source) as archive:
            assert archive.read("data") == b"hello"

    async def run():
        original = bytearray(pack_file("data", b"world"))
        async with await _native.AsyncSource.memory(original) as source:
            original.clear()
            async with zstar.open(source, async_=True) as archive:
                assert await archive.read("data") == b"world"

    asyncio.run(run())


def test_file_object_adapter_preserves_cursor_and_ownership():
    data = pack_file("data", b"hello")

    class PartialFile(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 7))

    file = PartialFile(data)
    file.seek(13)
    source = fileobj(file)
    with zstar.open(source) as archive:
        assert archive.read("data") == b"hello"
        assert file.tell() == 13
    assert file.tell() == 13 and not file.closed
    with pytest.raises(ZstarError):
        source.read_at(len(data), 1)
    assert file.tell() == 13


@pytest.mark.parametrize("asynchronous", [False, True])
def test_reentry_through_multiple_file_object_adapters_is_rejected(asynchronous):
    # A subprocess makes a regression fail within a fixed time even if a callback
    # blocks on the shared file lock and cannot be cancelled by the event loop.
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            """
import asyncio
import io
import sys
import zstar
from zstar import ZstarError

class File(io.BytesIO):
    other = None
    rejected = False

    def read(self, size=-1):
        if self.other is not None:
            other, self.other = self.other, None
            try:
                other.read("data")
            except ZstarError as error:
                assert error.category == "unsupported-operation", error.category
                self.rejected = True
            else:
                raise AssertionError("callback reentry was accepted")
        return super().read(size)

output = io.BytesIO()
with zstar.open(output, "w") as writer:
    writer.writestr("data", b"hello")
file = File(output.getvalue())

async def run():
    asynchronous = sys.argv[1] == "True"
    with zstar.open(file) as second:
        if asynchronous:
            async with zstar.open(file, async_=True) as first:
                file.other = second
                assert await first.read("data") == b"hello"
        else:
            with zstar.open(file) as first:
                file.other = second
                assert first.read("data") == b"hello"
    assert file.rejected and not file.closed

asyncio.run(run())
""",
            str(asynchronous),
        ],
        check=True,
        timeout=10,
    )


def test_sink_modes_symlinks_and_failed_identity_cleanup(tmp_path):
    path = tmp_path / "archive"
    path.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        _native.open_sink(str(path), mode="x")
    assert path.read_bytes() == b"original"
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(OSError):
        _native.open_sink(str(link))
    assert path.read_bytes() == b"original"
    sink = _native.open_sink(str(path))
    writer = zstar.open(sink, mode="w")
    with pytest.raises(ZstarError):
        writer.addfile(Header.file("data", 3), io.BytesIO(b"x"))
    writer.abort()
    sink.close()
    assert not path.exists()
    sink = _native.open_sink(str(path))
    writer = zstar.open(sink, mode="w")
    writer.writestr("data", b"unfinished")
    old = tmp_path / "old"
    path.rename(old)
    path.write_bytes(b"replacement")
    writer.abort()
    sink.abort()
    assert path.read_bytes() == b"replacement"


@pytest.mark.parametrize(
    "location",
    [
        "unknown://anything",
        "ftp://example.invalid/file",
        "s3://bucket",
        "file:///tmp/archive?bad=yes",
    ],
)
def test_source_routing_rejects_invalid_locations(location):
    with pytest.raises(ValueError):
        _native.open_source(location)


@pytest.mark.parametrize(
    "location",
    ["http://example.invalid/file", "https://example.invalid/file", "unknown://anything"],
)
def test_sink_routing_rejects_unsupported_schemes(location):
    with pytest.raises(ValueError):
        _native.open_sink(location)


@pytest.mark.parametrize(
    "s3",
    [
        {"region": "test"},
        {
            "region": "test",
            "anonymous": True,
            "credentials": {"access_key_id": "key", "secret_access_key": "secret"},
        },
        {"region": "", "anonymous": True},
        {"region": "test", "credentials": {"access_key_id": "", "secret_access_key": "secret"}},
        {"region": "test", "anonymous": True, "unknown": 1},
        {
            "region": "test",
            "credentials": {"access_key_id": "key", "secret_access_key": "secret", "unknown": 1},
        },
    ],
)
def test_transport_credentials_are_explicit(s3):
    with pytest.raises((TypeError, ValueError)) as error:
        _native.open_source("s3://bucket/file", options={"s3": s3})
    assert "secret" not in str(error.value)


def test_explicit_adapters_for_source_body_sink_and_native_handles(tmp_path):
    main_thread = threading.get_ident()
    threads = []

    class Body(io.BytesIO):
        def read(self, size=-1):
            threads.append(threading.get_ident())
            return super().read(size)

    async def run():
        output = io.BytesIO()
        body = Body(b"hello")
        async with zstar.open(_native.BlockingAdapter(output), mode="w", async_=True) as writer:
            await writer.addfile(Header.file("data", 5), _native.BlockingAdapter(body))
        assert not output.closed and not body.closed
        source = fileobj(output)
        async with zstar.open(_native.BlockingAdapter(source), async_=True) as archive:
            assert await archive.read("data") == b"hello"
        with _native.Source.memory(output.getvalue()) as native:
            async with zstar.open(_native.BlockingAdapter(native), async_=True) as archive:
                assert await archive.read("data") == b"hello"
        with _native.open_sink(str(tmp_path / "native")) as native:
            async with zstar.open(_native.BlockingAdapter(native), mode="w", async_=True) as writer:
                await writer.writestr("data", b"hello")

    asyncio.run(run())
    assert threads and main_thread not in threads


@pytest.mark.parametrize("cancel_close", [False, True])
def test_blocking_callbacks_keep_resources_until_cancelled_close_drains(cancel_close):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    class Source:
        def __init__(self):
            self.data = pack_file("data", b"hello")

        def size(self):
            return len(self.data)

        def read_at(self, offset, length):
            if offset == 0:
                entered.set()
                assert release.wait(5)
                finished.set()
            return self.data[offset : offset + length]

    async def run():
        archive = await zstar.open(_native.BlockingAdapter(Source()), async_=True)
        reading = asyncio.create_task(archive.read("data"))
        assert await asyncio.to_thread(entered.wait, 2)
        reading.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reading
        closing = asyncio.create_task(archive.close())
        await asyncio.sleep(0)
        assert not closing.done() and not finished.is_set()
        if cancel_close:
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
            closing = asyncio.create_task(archive.close())
            await asyncio.sleep(0)
            assert not closing.done()
        release.set()
        await asyncio.wait_for(closing, 2)
        assert finished.is_set()

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_core_import_does_not_load_fsspec():
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys,zstar; assert 'fsspec' not in sys.modules",
        ],
        check=True,
    )


@pytest.mark.parametrize("asynchronous", [False, True])
def test_native_adapters_bypass_python_callback_dispatch(tmp_path, monkeypatch, asynchronous):
    from zstar import _callbacks

    def unexpected(*args):
        raise AssertionError("native adapters must bind directly to Rust I/O")

    monkeypatch.setattr(_callbacks, "invoke", unexpected)
    monkeypatch.setattr(_callbacks, "ainvoke", unexpected)
    path = str(tmp_path / "native")

    async def run():
        async with (
            await _native.aopen_sink(path) as sink,
            await zstar.open(sink, mode="w", async_=True) as writer,
        ):
            await writer.writestr("data", b"hello")
        async with (
            await _native.aopen_source(path) as source,
            await zstar.open(source, async_=True) as archive,
        ):
            assert await archive.read("data") == b"hello"

    if asynchronous:
        asyncio.run(run())
    else:
        with _native.open_sink(path) as sink, zstar.open(sink, mode="w") as writer:
            writer.writestr("data", b"hello")
        with _native.open_source(path) as source, zstar.open(source) as archive:
            assert archive.read("data") == b"hello"
