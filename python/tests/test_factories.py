"""Archive factories own native storage and borrow caller-supplied files."""

import asyncio
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import zstar
from _support import pack_file
from zstar import (
    AsyncSourceOptions,
    Header,
    S3Options,
    SourceOptions,
    WriterOptions,
    ZstarError,
    _native,
)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("kind", ["path", "url"])
def test_owned_round_trip(tmp_path, asynchronous, kind):
    path = tmp_path / "archive ü.tar.zst"
    location = path.as_uri().replace("file://", "FILE:", 1) if kind == "url" else path

    async def run():
        writer = await zstar.open(location, mode="x", async_=True)
        async with writer:
            await writer.writestr("data", b"hello")
        async with zstar.open(location, async_=True) as archive:
            assert await archive.read("data") == b"hello"

    if asynchronous:
        asyncio.run(run())
    else:
        writer = zstar.open(location, mode="x")
        with writer:
            writer.writestr("data", b"hello")
        with zstar.open(location) as archive:
            assert archive.read("data") == b"hello"


@pytest.mark.parametrize(
    "name",
    [
        "file:archive",
        pytest.param(
            "https://bucket/archive",
            marks=pytest.mark.skipif(
                os.name == "nt", reason="Windows reserves colons in directory names"
            ),
        ),
    ],
)
def test_path_objects_preserve_literal_local_names(tmp_path, monkeypatch, name):
    monkeypatch.chdir(tmp_path)
    Path(name).parent.mkdir(parents=True, exist_ok=True)

    with zstar.open(Path(name), mode="w") as writer:
        writer.writestr("data", b"hello")
    with zstar.open(Path(name)) as archive:
        assert archive.read("data") == b"hello"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_symlink_dot_dot_keeps_filesystem_resolution(tmp_path, asynchronous):
    local, remote = tmp_path / "local", tmp_path / "remote"
    local.mkdir()
    (remote / "child").mkdir(parents=True)
    (local / "link").symlink_to(remote / "child", target_is_directory=True)
    protected = local / "archive"
    protected.write_bytes(b"do not overwrite")
    path = local / "link" / ".." / "archive"

    async def run():
        async with zstar.open(path, mode="w", async_=True) as writer:
            await writer.writestr("data", b"correct destination")
        async with zstar.open(path, async_=True) as archive:
            assert await archive.read("data") == b"correct destination"

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(path, mode="w") as writer:
            writer.writestr("data", b"correct destination")
        with zstar.open(path) as archive:
            assert archive.read("data") == b"correct destination"
    if os.name == "nt":
        # Windows collapses '..' before following directory symlinks.
        expected = protected
        assert not (remote / "archive").exists()
    else:
        expected = remote / "archive"
        assert protected.read_bytes() == b"do not overwrite"
    with zstar.open(expected) as archive:
        assert archive.read("data") == b"correct destination"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_fileobj_factories_borrow_files_and_preserve_cursor(asynchronous):
    output = io.BytesIO(b"prefix")
    output.seek(6)
    file = io.BytesIO(pack_file("data", b"hello"))
    file.seek(13)

    async def run():
        async with zstar.open(_native.BlockingAdapter(output), mode="w", async_=True) as writer:
            await writer.writestr("data", b"hello")
        async with zstar.open(file, async_=True) as archive:
            assert await archive.read("data") == b"hello"

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(output, mode="w") as writer:
            writer.writestr("data", b"hello")
        with zstar.open(file) as archive:
            assert archive.read("data") == b"hello"
    assert not output.closed and not file.closed
    assert file.tell() == 13
    assert output.getvalue().startswith(b"prefix")
    with zstar.open(output.getvalue()[6:]) as archive:
        assert archive.read("data") == b"hello"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_open_rejects_unsupported_inputs(asynchronous):
    file = io.BytesIO(pack_file("data", b"hello"))

    async def run():
        with pytest.raises(TypeError):
            await zstar.open(object(), async_=True)
        with pytest.raises(TypeError):
            await zstar.open(object(), "w", async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(TypeError):
            zstar.open(object())
        with pytest.raises(TypeError):
            zstar.open(object(), "w")
    assert not file.closed


@pytest.mark.parametrize("asynchronous", [False, True])
def test_open_buffer_takes_a_snapshot(asynchronous):
    original = bytearray(pack_file("data", b"hello"))

    async def run():
        async with zstar.open(original, async_=True) as archive:
            original.clear()
            assert await archive.read("data") == b"hello"

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(original) as archive:
            original.clear()
            assert archive.read("data") == b"hello"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("failure", ["body", "finalize", "context", "abort", "capture"])
def test_owned_output_is_removed_on_failure(tmp_path, asynchronous, failure):
    path = tmp_path / "archive"

    async def run():

        writer = await zstar.open(path, mode="w", async_=True)
        if failure == "abort":
            await writer.abort()
            await writer.abort()
            return
        with pytest.raises((ZstarError, RuntimeError, OSError)):
            async with writer:
                if failure == "body":
                    await writer.addfile(
                        Header.file("data", 2), _native.BlockingAdapter(io.BytesIO(b"x"))
                    )
                elif failure == "capture":
                    await writer.add(tmp_path / "missing", "data")
                else:
                    await writer.writestr(
                        "missing/data" if failure == "finalize" else "data", b"hello"
                    )
                    if failure == "context":
                        raise RuntimeError("body failed")

    if asynchronous:
        asyncio.run(run())
    else:
        writer = zstar.open(path, mode="w")
        if failure == "abort":
            writer.abort()
            writer.abort()
        else:
            with pytest.raises((ZstarError, RuntimeError, OSError)), writer:
                if failure == "body":
                    writer.addfile(Header.file("data", 2), io.BytesIO(b"x"))
                elif failure == "capture":
                    writer.add(tmp_path / "missing", "data")
                else:
                    writer.writestr("missing/data" if failure == "finalize" else "data", b"hello")
                    if failure == "context":
                        raise RuntimeError("body failed")
    assert not path.exists()


def test_modes_and_missing_files(tmp_path):
    path = tmp_path / "archive"
    with pytest.raises(FileNotFoundError):
        zstar.open(path)
    path.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        zstar.open(path, mode="x")
    with pytest.raises(TypeError):
        zstar.open(path, options={"verify_external": True}, mode="w")
    assert path.read_bytes() == b"original"
    with zstar.open(path, mode="w") as writer:
        writer.writestr("data", b"replacement")
    with zstar.open(path) as archive:
        assert archive.read("data") == b"replacement"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "location,settings,message",
    [
        ("local", {"headers": {"X-Test": "value"}}, "HTTP settings"),
        ("s3://bucket/archive", {}, "S3Options"),
        (
            "https://example.invalid/archive",
            {"s3": S3Options(region="test", anonymous=True)},
            "S3 settings",
        ),
    ],
)
def test_storage_validation_precedes_io(tmp_path, asynchronous, location, settings, message):
    if location == "local":
        location = (tmp_path / "unused").as_uri()

    async def run():
        with pytest.raises(ValueError, match=message):
            await zstar.open(location, storage_options=AsyncSourceOptions(**settings), async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(ValueError, match=message):
            zstar.open(location, storage_options=SourceOptions(**settings))


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_cache_policy_rejects_invalid_fetch_limits(value):
    with pytest.raises((TypeError, ValueError)):
        zstar.open("/does-not-exist", storage_options={"range_cache": {"max_fetch_bytes": value}})


def test_owned_reader_closes_source_after_success_and_open_failure(tmp_path, monkeypatch):
    real = _native.open_source
    opened = []

    def capture(*args, **kwargs):
        source = real(*args, **kwargs)
        opened.append(source)
        return source

    monkeypatch.setattr(_native, "open_source", capture)
    path = tmp_path / "archive"
    path.write_bytes(pack_file("data", b"hello"))
    with zstar.open(path) as archive:
        assert archive.read("data") == b"hello"
    assert opened[-1].closed
    path.write_bytes(b"invalid")
    with pytest.raises(ZstarError):
        zstar.open(path)
    assert opened[-1].closed


def test_cancelled_source_acquisition_disposes_its_result(tmp_path, monkeypatch):
    path = tmp_path / "archive"
    path.write_bytes(pack_file("data", b"hello"))
    real = _native.aopen_source

    async def run():
        created, release = asyncio.Event(), asyncio.Event()
        opened = []

        async def slow(*args, **kwargs):
            source = await real(*args, **kwargs)
            opened.append(source)
            created.set()
            await release.wait()
            return source

        monkeypatch.setattr(_native, "aopen_source", slow)
        opening = asyncio.ensure_future(zstar.open(path, async_=True))
        await asyncio.wait_for(created.wait(), 2)
        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            await opening
        assert not opened[0].closed
        release.set()

        async def drained():
            while not opened[0].closed:  # noqa: ASYNC110 - native closure exposes no event
                await asyncio.sleep(0)

        await asyncio.wait_for(drained(), 2)

    asyncio.run(run())


def test_cancelled_publication_is_joined_and_publishes_once(monkeypatch):
    async def run():
        publishing, release = asyncio.Event(), asyncio.Event()
        output = io.BytesIO()
        calls = []

        class Output:
            async def write(self, data):
                return output.write(data)

            async def flush(self):
                calls.append("flush")

            async def aclose(self):
                # The bytes must already contain a finalized, readable archive.
                with zstar.open(output.getvalue()) as archive:
                    assert archive.read("data") == b"hello"
                calls.append("publish")
                publishing.set()
                await release.wait()

            async def aabort(self):
                calls.append("abort")

        async def open_sink(*args, **kwargs):
            return Output()

        monkeypatch.setattr(_native, "aopen_sink", open_sink)
        writer = await zstar.open("controlled-output", mode="w", async_=True)
        await writer.writestr("data", b"hello")
        closing = asyncio.create_task(writer.close())
        await asyncio.wait_for(publishing.wait(), 2)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        joining = asyncio.create_task(writer.close())
        aborting = asyncio.create_task(writer.abort())
        await asyncio.sleep(0)
        assert not joining.done() and not aborting.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(joining, aborting), 2)
        await writer.close()
        assert calls == ["flush", "publish"]

    asyncio.run(run())


def test_owned_mutation_cancellation_retains_cleanup_and_drains_body(tmp_path):
    import threading

    entered, release = threading.Event(), threading.Event()
    path = tmp_path / "archive"

    class Body:
        def read(self, size):
            entered.set()
            assert release.wait(5)
            return b""

    async def run():
        writer = await zstar.open(path, mode="w", async_=True)
        writing = asyncio.create_task(
            writer.addfile(Header.file("data", 1), _native.BlockingAdapter(Body()))
        )
        assert await asyncio.to_thread(entered.wait, 2)
        writing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(writing, 2)
        draining = asyncio.create_task(writer.abort())
        await asyncio.sleep(0)
        assert not draining.done()
        draining.cancel()
        with pytest.raises(asyncio.CancelledError):
            await draining
        release.set()
        await asyncio.wait_for(writer.abort(), 2)
        assert not path.exists()

    try:
        asyncio.run(run())
    finally:
        release.set()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_failed_native_writer_initialization_aborts_output(tmp_path, asynchronous):
    from zstar import Limits

    path = tmp_path / "archive"
    options = WriterOptions(limits=Limits(max_frame_decoded_bytes=512))

    async def run():
        with pytest.raises(ZstarError):
            await zstar.open(path, options=options, mode="w", async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(ZstarError):
            zstar.open(path, options=options, mode="w")
    assert not path.exists()


@pytest.mark.parametrize(
    "url", ["ftp://example.invalid/archive", "http:archive", "s3:bucket/archive", "https://"]
)
def test_open_rejects_invalid_urls_and_unsupported_schemes(url):
    with pytest.raises(ValueError):
        zstar.open(url)
    with pytest.raises(ValueError):
        zstar.open(url, mode="w")


def test_http_upload_is_rejected_before_open(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("output opened before validation")

    monkeypatch.setattr(_native, "open_sink", unexpected)
    monkeypatch.setattr(_native, "aopen_sink", unexpected)
    with pytest.raises(ValueError):
        zstar.open("https://example.invalid/archive", mode="w")

    async def run():
        with pytest.raises(ValueError):
            await zstar.open("https://example.invalid/archive", mode="w", async_=True)

    asyncio.run(run())


def test_owned_sync_close_rejects_callback_reentry_and_publishes_once(monkeypatch):
    import threading

    publishing, release = threading.Event(), threading.Event()
    published = []
    writer = None

    class Output(io.BytesIO):
        def flush(self):
            for operation in (writer.close, writer.abort):
                with pytest.raises(ZstarError) as caught:
                    operation()
                assert caught.value.category == "unsupported-operation"

        def close(self):
            published.append(self.getvalue())
            publishing.set()
            assert release.wait(5)
            super().close()

    output = Output()
    monkeypatch.setattr(_native, "open_sink", lambda *args, **kwargs: output)
    writer = zstar.open("controlled-output", mode="w")
    writer.writestr("data", b"hello")
    with ThreadPoolExecutor(4) as pool:
        first = pool.submit(writer.close)
        try:
            assert publishing.wait(2)
            others = [pool.submit(writer.close), pool.submit(writer.abort)]
            assert not first.done()
        finally:
            release.set()
        first.result(timeout=2)
        for future in others:
            future.result(timeout=2)
    assert len(published) == 1 and output.closed
    with zstar.open(published[0]) as archive:
        assert archive.read("data") == b"hello"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("mode", [None, 1, "", "a", "r+", "rb", "W"])
def test_open_rejects_invalid_modes_before_io(tmp_path, asynchronous, mode):
    path = tmp_path / "protected"
    path.write_bytes(b"original")
    error = TypeError if not isinstance(mode, str) else ValueError
    with pytest.raises(error, match="mode"):
        zstar.open(path, mode, async_=asynchronous)
    assert path.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    ("flags", "error", "message"),
    [
        ({"async_": 1}, TypeError, "async_ must be a bool"),
        ({"atomic": "yes"}, TypeError, "atomic must be a bool"),
        ({"atomic": True}, ValueError, "atomic applies only to writing"),
        ({"atomic": True, "async_": True}, ValueError, "atomic applies only to writing"),
    ],
)
def test_open_validates_flags_before_io(tmp_path, flags, error, message):
    path = tmp_path / "protected"
    path.write_bytes(b"original")
    with pytest.raises(error, match=message):
        zstar.open(path, **flags)
    assert path.read_bytes() == b"original"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("kind", ["buffer", "file", "source"])
def test_open_rejects_storage_options_for_borrowed_inputs(asynchronous, kind):
    data = pack_file("data", b"hello")
    file = io.BytesIO(data)
    reads = []

    class Source:
        def size(self):
            reads.append("size")
            return len(data)

        def read_at(self, offset, length):
            reads.append("read")
            return data[offset : offset + length]

    class AsyncSource:
        async def size(self):
            return Source().size()

        async def read_at(self, offset, length):
            return Source().read(offset, offset=length)

    source = AsyncSource() if asynchronous else Source()
    supplied = {"buffer": data, "file": file, "source": source}[kind]

    async def run():
        with pytest.raises(ValueError, match="storage_options require a path or URL"):
            await zstar.open(supplied, storage_options={}, async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(ValueError, match="storage_options require a path or URL"):
            zstar.open(supplied, storage_options={})
    assert not file.closed and file.tell() == 0
    assert not reads


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("flags", [{"mode": "x"}, {"atomic": True}, {"storage_options": {}}])
def test_open_rejects_storage_flags_for_borrowed_output(asynchronous, flags):
    output = io.BytesIO(b"prefix")
    output.seek(6)
    settings = {"mode": "w", **flags}

    async def run():
        with pytest.raises(ValueError):
            await zstar.open(_native.BlockingAdapter(output), **settings, async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(ValueError):
            zstar.open(output, **settings)
    assert not output.closed
    assert output.tell() == 6 and output.getvalue() == b"prefix"


def test_open_defaults_to_blocking_inside_an_event_loop(tmp_path):
    path = tmp_path / "archive"

    async def run():
        with zstar.open(path, "w") as writer:
            assert isinstance(writer, zstar.ArchiveWriter)
            writer.writestr("data", b"hello")
        with zstar.open(path) as archive:
            assert isinstance(archive, zstar.Archive)
            with archive.open("data") as member:
                assert member.read() == b"hello"
        async with zstar.open(path, async_=True) as archive:
            assert isinstance(archive, zstar.AsyncArchive)
            async with archive.open("data") as member:
                assert await member.read() == b"hello"

    asyncio.run(run())
