"""Public storage adapters, cancellation, and metadata-preserving rewrites."""

import asyncio
import io
from tarfile import TarInfo

import pytest
from zstar import Archive, ArchiveWriter, AsyncArchive, AsyncArchiveWriter
from zstar.types import ReadRequest


class Objects:
    def __init__(self):
        self.data = {}

    def put_encoded(self, address, encoded, raw_size):
        self.data[address] = encoded
        return len(encoded)

    def get_encoded(self, address, hint, max_bytes):
        return self.data[address]


def thin_archive():
    output, objects = io.BytesIO(), Objects()
    info = TarInfo("hello")
    info.size = 6
    with ArchiveWriter(output, root_extensions={"x-root": {"value": b"root"}}) as writer:
        writer.add_external(info, [b"abc", b"def"], objects, extensions={"x-entry": [1, True]})
    return output.getvalue(), objects


def test_external_chunks_and_lossless_metadata_rewrite():
    data, objects = thin_archive()
    output = io.BytesIO()
    with Archive.from_bytes(data, resolver=objects, verify_external=True) as archive:
        assert archive.read_many([ReadRequest(path="hello", offset=1, length=3)]) == [b"bcd"]

        metadata = archive.getmetadata("hello")
        with (
            ArchiveWriter(output, root_extensions=archive.root_extensions) as writer,
            archive.open("hello") as body,
        ):
            writer.addfile(metadata.info, body, extensions=metadata.extensions)
    with Archive.from_bytes(output.getvalue()) as rewritten:
        assert rewritten.root_extensions == {"x-root": {"value": b"root"}}
        assert rewritten.getmetadata("hello").extensions == {"x-entry": (1, True)}
        assert rewritten.read("hello") == b"abcdef"
    assert not output.closed


@pytest.mark.parametrize("chunks,size", [([b""], 0), ([b"abc"], 2), ([b"abc"], 4)])
def test_external_chunks_reject_invalid_sizes(chunks, size):
    output = io.BytesIO()
    info = TarInfo("hello")
    info.size = size
    with pytest.raises(Exception), ArchiveWriter(output) as writer:
        writer.add_external(info, chunks, Objects())
    assert not output.closed
    with pytest.raises(Exception):
        Archive(output.getvalue())


def test_async_sources_and_resolvers_run_on_owning_loop():
    data, objects = thin_archive()

    async def exercise():
        loop = asyncio.get_running_loop()

        class Source:
            async def read_range(self, offset, length):
                assert asyncio.get_running_loop() is loop
                await asyncio.sleep(0)
                return data[offset : offset + length]

        class Resolver:
            async def get_encoded(self, address, hint, max_bytes):
                assert asyncio.get_running_loop() is loop
                await asyncio.sleep(0)
                return objects.get_encoded(address, hint, max_bytes)

        async with await AsyncArchive.from_source(
            Source(), len(data), resolver=Resolver(), verify_external=True
        ) as archive:
            assert await archive.read("hello") == b"abcdef"
            assert (await archive.getmetadata("hello")).extensions == {"x-entry": (1, True)}

    asyncio.run(exercise())


def test_async_source_cancellation_reaches_pending_coroutine():
    output = io.BytesIO()
    with ArchiveWriter(output) as writer:
        writer.write_bytes("hello", b"abcdef")
    data = output.getvalue()

    async def exercise():
        entered, cancelled = asyncio.Event(), asyncio.Event()

        class Source:
            blocking = False

            async def read_range(self, offset, length):
                if self.blocking:
                    entered.set()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cancelled.set()
                return data[offset : offset + length]

        source = Source()
        async with await AsyncArchive.from_source(source, len(data)) as archive:
            source.blocking = True
            task = asyncio.create_task(archive.read("hello"))
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(cancelled.wait(), 5)

    asyncio.run(exercise())


def test_async_writer_streams_and_external_objects():
    async def exercise():
        loop = asyncio.get_running_loop()
        objects = Objects()

        class Sink:
            def __init__(self):
                self.data = bytearray()
                self.flushed = False

            async def write(self, data):
                assert asyncio.get_running_loop() is loop
                self.data.extend(data)
                return len(data)

            async def flush(self):
                self.flushed = True

        class Body:
            data = b"body"

            async def read(self, size):
                assert asyncio.get_running_loop() is loop
                value, self.data = self.data[:size], self.data[size:]
                return value

        class Store:
            async def put_encoded(self, address, encoded, raw_size):
                assert asyncio.get_running_loop() is loop
                return objects.put_encoded(address, encoded, raw_size)

        async def chunks():
            yield b"abc"
            yield b"def"

        sink = Sink()
        async with await AsyncArchiveWriter.from_file(sink) as writer:
            info = TarInfo("body")
            info.size = 4
            await writer.addfile(info, Body(), extensions={"x-test": b"yes"})
            info = TarInfo("external")
            info.size = 6
            await writer.add_external(info, chunks(), Store())
        assert sink.flushed
        async with await AsyncArchive.from_bytes(sink.data, resolver=objects) as reader:
            assert await reader.read("body") == b"body"
            assert await reader.read("external") == b"abcdef"

    asyncio.run(exercise())


def test_async_writer_cancels_pending_body_before_joining():
    async def exercise():
        entered, cancelled = asyncio.Event(), asyncio.Event()

        class Body:
            async def read(self, size):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        output = io.BytesIO()
        with pytest.raises(asyncio.CancelledError):
            async with await AsyncArchiveWriter.from_file(output) as writer:
                info = TarInfo("hello")
                info.size = 1
                task = asyncio.create_task(writer.addfile(info, Body()))
                await asyncio.wait_for(entered.wait(), 5)
                task.cancel()
                await asyncio.wait_for(task, 5)
        await asyncio.wait_for(cancelled.wait(), 5)
        assert not output.closed

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "failure,category", [("missing", "missing-external-content"), ("large", "resource-limit")]
)
def test_async_resolvers_preserve_error_categories(failure, category):
    from zstar import ZstarError

    data, _ = thin_archive()

    class Resolver:
        async def get_encoded(self, address, hint, max_bytes):
            if failure == "missing":
                raise FileNotFoundError(address)
            return b"x" * (max_bytes + 1)

    async def exercise():
        async with await AsyncArchive.from_bytes(
            data, resolver=Resolver(), limits={"max_frame_encoded_bytes": 1024}
        ) as archive:
            with pytest.raises(ZstarError) as error:
                await archive.read("hello")
            assert error.value.category == category

    asyncio.run(exercise())


def test_sync_archive_rejects_async_callbacks_without_scheduling_them():
    data, _ = thin_archive()

    class Resolver:
        async def get_encoded(self, address, hint, max_bytes):
            raise AssertionError("must not schedule an async callback from sync reads")

    class Source:
        async def read_range(self, offset, length):
            raise AssertionError("must not schedule an async callback from sync reads")

    async def exercise():
        with pytest.raises(TypeError, match="AsyncArchive"):
            Archive.from_bytes(data, resolver=Resolver())
        with pytest.raises(TypeError, match="AsyncArchive"):
            Archive.from_source(Source(), len(data))
        with Archive.from_bytes(data) as archive, pytest.raises(TypeError, match="AsyncArchive"):
            archive.read("hello", resolver=Resolver())

    asyncio.run(exercise())


def test_gc_cleanup_never_waits_under_fsspec_sync_lock():
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import asyncio
        import gc
        import io
        import threading
        from fsspec.asyn import sync
        from zstar import ZstarFileSystem

        gc.disable()
        closed = threading.Event()
        class Output(io.BytesIO):
            def close(self):
                super().close()
                closed.set()

        fs = ZstarFileSystem(fo=Output(), mode="w", skip_instance_cache=True)
        loop = fs.loop
        fs.cycle = fs
        del fs
        gate = asyncio.Event()
        original_event = threading.Event
        class CollectDuringWait(original_event):
            fired = False
            def wait(self, timeout=None):
                if not CollectDuringWait.fired:
                    CollectDuringWait.fired = True
                    # Model GC inside Event.wait while its condition is locked.
                    # The fsspec loop must finish this wait before handling cleanup.
                    with self._cond:
                        loop.call_soon_threadsafe(gate.set)
                        gc.collect()
                return super().wait(timeout)

        async def operation():
            await gate.wait()

        threading.Event = CollectDuringWait
        sync(loop, operation)
        assert closed.wait(2), "finalizer did not release owned output"
    """)
    subprocess.run([sys.executable, "-I", "-c", script], check=True, timeout=5)
