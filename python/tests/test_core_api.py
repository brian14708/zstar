"""Behavioral contracts shared by the typed sync and async facades."""

import asyncio
import contextvars
import gc
import io
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest
import zstar
from _support import AsyncSource, Source, maybe, pack_file, reader_for
from zstar import (
    AsyncReaderOptions,
    EntryKind,
    Header,
    MetadataWriterOptions,
    ReaderOptions,
    ReadRequest,
    WriterOptions,
    ZstarError,
)


class Sink(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.callback_threads = []

    def write(self, data):
        self.callback_threads.append(threading.get_ident())
        return super().write(data[:17])


class AsyncSink:
    def __init__(self):
        self.output = Sink()

    def write(self, data):
        async def result():
            return self.output.write(data)

        return result()

    async def flush(self):
        self.output.flush()

    def getvalue(self):
        return self.output.getvalue()


class AsyncBody:
    def __init__(self, data):
        self.body = io.BytesIO(data)

    async def read(self, size):
        return self.body.read(size)


async def write_fixture(mode, options=None):
    sink = AsyncSink() if mode == "async" else Sink()
    writer = await maybe(zstar.open(sink, "w", options=options, async_=mode == "async"))
    await maybe(writer.addfile(Header.directory("dir")))
    header = Header.file(
        "dir/data", 11, mtime_ns=1_234_567_890_123_456_789, extensions={"x-note": {"a": [1, b"v"]}}
    )
    body = AsyncBody(b"hello world") if mode == "async" else io.BytesIO(b"hello world")
    await maybe(writer.addfile(header, body))
    await maybe(writer.addfile(Header.file("empty", 0)))
    await maybe(writer.addfile(Header.symlink("link", "dir/data")))
    await maybe(writer.close())
    assert not sink.output.closed if mode == "async" else not sink.closed
    return sink.getvalue()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_shared_reader_writer_stream_contracts(mode):
    async def run():
        data = await write_fixture(mode)
        source = AsyncSource(data) if mode == "async" else Source(data)
        options = (
            AsyncReaderOptions(max_read_bytes=16)
            if mode == "async"
            else ReaderOptions(max_read_bytes=16)
        )
        archive = await reader_for(mode, source, options)
        assert source.sizes == 1
        # Directory-first opening must not fetch the first content frame.
        assert not any(offset == 0 for offset, _ in source.ranges)
        entry = await maybe(archive.entry("dir/data"))
        assert entry.kind is EntryKind.FILE
        assert entry.mtime_ns == 1_234_567_890_123_456_789
        assert await maybe(archive.read("dir/data")) == b"hello world"
        assert await maybe(archive.read("dir/data", offset=6, length=5)) == b"world"
        result = await maybe(archive.read("dir/data", offset=0, length=5))
        assert result == b"hello"
        assert await maybe(
            archive.read_many(
                [
                    ReadRequest("dir/data", 6, 5),
                    ReadRequest("empty", 0, 0),
                    ReadRequest("dir/data", 0, 5),
                ]
            )
        ) == [b"world", b"", b"hello"]
        before = list(source.ranges)
        with pytest.raises(ZstarError) as error:
            await maybe(archive.read_many([ReadRequest("dir/data", 0, 11)] * 2))
        assert error.value.category == "resource-limit"
        assert source.ranges == before
        for path, error_type in [
            ("missing", KeyError),
            ("dir", IsADirectoryError),
            ("link", ZstarError),
        ]:
            with pytest.raises(error_type):
                await maybe(archive.read(path, offset=0, length=0))
        with pytest.raises(ZstarError) as error:
            await maybe(archive.read("dir/data", offset=11, length=1))
        assert error.value.category == "invalid-range"
        assert await maybe(archive.read("dir/data", offset=11, length=0)) == b""
        first = await maybe(archive.open("dir/data"))
        second = await maybe(archive.open("dir/data"))
        assert await maybe(first.read(5)) == b"hello"
        assert await maybe(second.tell()) == 0
        target = bytearray(30)
        assert await maybe(first.readinto(target)) == 6
        assert target[:6] == b" world"
        assert await maybe(first.seek(100)) == 100
        assert await maybe(first.read()) == b""
        await maybe(first.close())
        if mode == "async":
            names = [item.path async for item in archive]
        else:
            names = [item.path for item in archive]
        assert names == ["dir", "dir/data", "empty", "link"]
        await maybe(archive.close())
        with pytest.raises(ValueError):
            await maybe(second.read(1))
        await maybe(second.close())
        assert entry.path == "dir/data"
        assert entry.extents[0].size == 11

    asyncio.run(run())


def test_sync_and_async_writers_produce_identical_bytes():
    sync_data = asyncio.run(write_fixture("sync"))
    async_data = asyncio.run(write_fixture("async"))
    assert sync_data == async_data


def test_sync_needs_no_loop_and_callbacks_run_off_caller_thread():
    main_thread = threading.get_ident()
    sink = Sink()
    with zstar.open(sink, mode="w") as writer:
        writer.writestr("data", bytearray(b"hello"))
    assert sink.callback_threads
    assert main_thread not in sink.callback_threads
    with zstar.open(Source(sink.getvalue())) as archive:
        assert archive.read("data") == b"hello"


def test_sync_concurrent_callers_share_one_serial_source():
    class SerialSource(Source):
        def __init__(self, data):
            super().__init__(data)
            self.active = 0
            self.maximum = 0
            self.guard = threading.Lock()

        def read_at(self, offset, length):
            with self.guard:
                self.active += 1
                self.maximum = max(self.maximum, self.active)
            try:
                return super().read_at(offset, length)
            finally:
                with self.guard:
                    self.active -= 1

    source = SerialSource(pack_file("data", b"hello world"))
    with zstar.open(source) as archive, ThreadPoolExecutor(8) as pool:
        values = list(pool.map(lambda _: archive.read("data", offset=0, length=5), range(32)))
    assert values == [b"hello"] * 32
    assert source.maximum == 1


def test_sync_operation_context_and_callback_reentry():
    context = contextvars.ContextVar("operation", default="missing")

    class ContextSource(Source):
        archive = None
        seen = None
        rejected = None

        def read_at(self, offset, length):
            if offset == 0:
                self.seen = context.get()
                try:
                    self.archive.read("data")
                except ZstarError as error:
                    self.rejected = error.category
            return super().read_at(offset, length)

    source = ContextSource(pack_file("data", b"hello"))
    with zstar.open(source) as archive:
        source.archive = archive
        context.set("read")
        assert archive.read("data") == b"hello"
        assert source.seen == "read"
        assert source.rejected == "unsupported-operation"


def test_async_operation_context_loop_and_decorated_callbacks():
    context = contextvars.ContextVar("operation", default="opening")

    async def run():
        loop = asyncio.get_running_loop()
        seen = []

        class ContextSource(AsyncSource):
            def read_at(self, offset, length):
                async def result():
                    seen.append((context.get(), asyncio.get_running_loop()))
                    return Source.read_at(self, offset, length)

                return result()

        source = ContextSource(pack_file("data", b"hello"))
        async with zstar.open(source, async_=True) as archive:
            seen.clear()
            context.set("reading")
            assert await archive.read("data") == b"hello"
            assert seen
            assert all(item == ("reading", loop) for item in seen)

    asyncio.run(run())


@pytest.mark.parametrize("projection", ["archive", "iterator", "stream"])
def test_callback_cycles_are_collectible(projection):
    source = Source(pack_file("data", b"hello"))
    archive = zstar.open(source)
    if projection == "iterator":
        source.cycle = archive._reader._iter_values()
    elif projection == "stream":
        source.cycle = archive.open("data")
    else:
        source.cycle = archive
    reference = weakref.ref(archive)
    del source, archive
    gc.collect()
    assert reference() is None


def test_async_cancellation_drains_callback_cleanup():
    async def run():
        entered, cleaned = asyncio.Event(), asyncio.Event()

        class BlockingSource(AsyncSource):
            async def read_at(self, offset, length):
                if offset == 0:
                    entered.set()
                    try:
                        await asyncio.Future()
                    finally:
                        await asyncio.sleep(0)
                        cleaned.set()
                return Source.read_at(self, offset, length)

        source = BlockingSource(pack_file("data", b"hello"))
        archive = await zstar.open(source, async_=True)
        reading = asyncio.create_task(archive.read("data"))
        await asyncio.wait_for(entered.wait(), 2)
        reading.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reading
        await asyncio.wait_for(cleaned.wait(), 2)
        await asyncio.wait_for(archive.close(), 2)

    asyncio.run(run())


def test_pre_mutation_rejection_does_not_poison_writer():
    sink = Sink()
    with zstar.open(sink, mode="w") as writer:
        with pytest.raises(ValueError):
            writer.addfile(Header.file("missing-body", 3))
        with pytest.raises(ValueError):
            writer.addfile(Header.directory("dir"), io.BytesIO())
        writer.writestr("valid", b"yes")
    with zstar.open(Source(sink.getvalue())) as archive:
        assert archive.read("valid") == b"yes"


@pytest.mark.parametrize("body", [b"ab", b"abcd"])
def test_wrong_body_length_poisoning_and_borrowed_ownership(body):
    sink = Sink()
    writer = zstar.open(sink, mode="w")
    body_stream = io.BytesIO(body)
    with pytest.raises(ZstarError):
        writer.addfile(Header.file("data", 3), body_stream)
    with pytest.raises(ValueError, match="failed"):
        writer.writestr("other", b"value")
    writer.abort()
    assert not body_stream.closed and not sink.closed


def test_native_configuration_extension_snapshots():
    roots = {"x-root": {"nested": [1, b"v"]}}
    pages = {"x-page": [{"value": [True]}]}
    metadata = MetadataWriterOptions(extension_pages=pages)
    options = WriterOptions(root_extensions=roots, metadata=metadata)
    sink = Sink()
    with zstar.open(sink, options=options, mode="w") as writer:
        roots["x-root"]["nested"].clear()
        roots.clear()
        pages.clear()
        options["frame_size"] = 0
        metadata.clear()
        writer.writestr("data", b"hello")
    with zstar.open(Source(sink.getvalue())) as archive:
        assert archive.root_extensions == {"x-root": {"nested": (1, b"v")}}
        returned = archive.root_extensions
        returned["x-root"]["nested"] = ()
        returned.clear()
        assert archive.root_extensions == {"x-root": {"nested": (1, b"v")}}
