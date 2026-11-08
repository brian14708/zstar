"""Shared-object gates, buffer delivery, and close/cancellation races."""

import asyncio
import gc
import io
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest
import zstar
from _support import AsyncSource, Source, maybe, reader_for
from zstar import (
    AsyncReaderOptions,
    ReaderOptions,
    WriterOptions,
    ZstarError,
    _native,
)


def fixture():
    output = io.BytesIO()
    with zstar.open(output, options=WriterOptions(frame_size=1024), mode="w") as writer:
        writer.writestr("data", b"x" * (32 << 10))
    return output.getvalue()


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_reentry_through_another_archive_sharing_a_source(mode):
    async def run():
        class Shared(Source):
            other = None

            def read_at(self, offset, length):
                if offset == 0 and self.other is not None:
                    with pytest.raises(ZstarError) as caught:
                        self.other.read("data")
                    assert caught.value.category == "unsupported-operation"
                return super().read_at(offset, length)

        class AsyncShared(AsyncSource):
            other = None

            async def read_at(self, offset, length):
                if offset == 0 and self.other is not None:
                    with pytest.raises(ZstarError) as caught:
                        await self.other.read("data")
                    assert caught.value.category == "unsupported-operation"
                return Source.read_at(self, offset, length)

        source = AsyncShared(fixture()) if mode == "async" else Shared(fixture())
        first = await reader_for(mode, source)
        second = await reader_for(mode, source)
        source.other = second
        assert await maybe(first.read("data")) == b"x" * (32 << 10)
        await maybe(first.close())
        await maybe(second.close())

    asyncio.run(asyncio.wait_for(run(), 5))


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_partial_readinto_failure_keeps_cursor_and_allows_retry(mode):
    async def run():
        class Failing(Source):
            armed = False
            calls = 0

            def read_at(self, offset, length):
                if self.armed:
                    self.calls += 1
                    if self.calls == 7:
                        raise OSError("selected source failed")
                return super().read_at(offset, length)

        source = Failing(fixture())
        adapted = _native.BlockingAdapter(source) if mode == "async" else source
        options = (AsyncReaderOptions if mode == "async" else ReaderOptions)(
            max_read_bytes=4096, concurrent_chunks=1
        )
        archive = await reader_for(mode, adapted, options)
        stream = await maybe(archive.open("data"))
        for invalid in [b"readonly", memoryview(bytearray(8))[::2], object()]:
            with pytest.raises((TypeError, BufferError)):
                await maybe(stream.readinto(invalid))
        target = bytearray(32 << 10)
        source.armed = True
        with pytest.raises(ZstarError):
            await maybe(stream.readinto(target))
        assert target[:4096] == b"x" * 4096
        assert target[-4096:] == bytes(4096)
        assert await maybe(stream.tell()) == 0
        source.armed = False
        assert await maybe(stream.readinto(target)) == len(target)
        assert target == b"x" * len(target)
        await maybe(archive.close())
        await maybe(stream.close())

    asyncio.run(run())


def test_sync_close_drains_admitted_work_and_rejects_new_operations():
    entered, release = threading.Event(), threading.Event()

    class Waiting(Source):
        def read_at(self, offset, length):
            if offset == 0:
                entered.set()
                assert release.wait(5)
            return super().read_at(offset, length)

    archive = zstar.open(Waiting(fixture()))
    try:
        with ThreadPoolExecutor(2) as pool:
            reading = pool.submit(archive.read, "data")
            assert entered.wait(2)
            closing = pool.submit(archive.close)
            # Native close marks admission closed before it waits for the worker.
            for _ in range(10000):
                if archive.closed:
                    break
                threading.Event().wait(0.0001)
            assert archive.closed and not closing.done()
            with pytest.raises(ValueError):
                archive.read("data")
            release.set()
            assert reading.result(2) == b"x" * (32 << 10)
            closing.result(2)
    finally:
        release.set()
        archive.close()


def test_callback_worker_admission_is_bounded_across_archives():
    release, full = threading.Event(), threading.Event()
    guard = threading.Lock()
    active = maximum = 0

    class Waiting(Source):
        def read_at(self, offset, length):
            nonlocal active, maximum
            if offset != 0:
                return super().read_at(offset, length)
            with guard:
                active += 1
                maximum = max(active, maximum)
                if active == 8:
                    full.set()
            try:
                assert release.wait(5)
                return super().read_at(offset, length)
            finally:
                with guard:
                    active -= 1

    async def run():
        data = fixture()
        archives = [
            await zstar.open(_native.BlockingAdapter(Waiting(data)), async_=True) for _ in range(12)
        ]
        tasks = [asyncio.create_task(archive.read("data")) for archive in archives]
        assert await asyncio.to_thread(full.wait, 2)
        assert maximum == 8
        release.set()
        assert all(value == b"x" * (32 << 10) for value in await asyncio.gather(*tasks))
        await asyncio.gather(*(archive.close() for archive in archives))
        assert active == 0 and maximum == 8

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_writer_sink_cycle_is_visible_to_gc():
    output = io.BytesIO()
    writer = zstar.open(output, mode="w")
    writer.writestr("data", b"hello")
    output.cycle = writer
    reference = weakref.ref(writer)
    del writer, output
    gc.collect()
    assert reference() is None


def test_cancelled_partial_readinto_leaves_cursor_unchanged():
    async def run():
        entered, cleaned = asyncio.Event(), asyncio.Event()

        class Waiting(AsyncSource):
            armed = False
            calls = 0

            async def read_at(self, offset, length):
                if self.armed:
                    self.calls += 1
                    if self.calls == 7:
                        entered.set()
                        try:
                            await asyncio.Future()
                        finally:
                            cleaned.set()
                return Source.read_at(self, offset, length)

        source = Waiting(fixture())
        archive = await zstar.open(
            source,
            options=AsyncReaderOptions(max_read_bytes=4096, concurrent_chunks=1),
            async_=True,
        )
        stream = await archive.open("data")
        target = bytearray(32 << 10)
        source.armed = True
        reading = asyncio.create_task(stream.readinto(target))
        await asyncio.wait_for(entered.wait(), 2)
        reading.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reading
        assert await stream.tell() == 0
        assert target[:4096] == b"x" * 4096 and target[-4096:] == bytes(4096)
        await asyncio.wait_for(archive.close(), 2)
        assert cleaned.is_set()
        await stream.close()

    asyncio.run(run())
