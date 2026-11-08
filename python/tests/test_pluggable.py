"""Public storage adapters, cancellation, and metadata-preserving rewrites."""

import asyncio
import io

import pytest
import zstar
from _support import aopen_bytes, aopen_source, open_bytes
from zstar import Header, ReadRequest, ZstarError, _native
from zstar.types import WriterOptions


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
    with zstar.open(
        output, options=WriterOptions(root_extensions={"x-root": {"value": b"root"}}), mode="w"
    ) as writer:
        writer.add_external(
            Header.file("hello", 6, extensions={"x-entry": [1, True]}),
            [b"abc", b"def"],
            objects,
        )
    return output.getvalue(), objects


def test_external_chunks_and_lossless_metadata_rewrite():
    data, objects = thin_archive()
    output = io.BytesIO()
    with open_bytes(data, resolver=objects, verify_external=True) as archive:
        assert archive.read_many([ReadRequest(path="hello", offset=1, length=3)]) == [b"bcd"]

        metadata = archive.entry("hello")
        with (
            zstar.open(
                output, options=WriterOptions(root_extensions=archive.root_extensions), mode="w"
            ) as writer,
            archive.open("hello") as body,
        ):
            writer.addfile(metadata.to_header(), body)
    with open_bytes(output.getvalue()) as rewritten:
        assert rewritten.root_extensions == {"x-root": {"value": b"root"}}
        assert rewritten.entry("hello").extensions == {"x-entry": (1, True)}
        assert rewritten.read("hello") == b"abcdef"
    assert not output.closed


@pytest.mark.parametrize("chunks,size", [([b""], 0), ([b"abc"], 2), ([b"abc"], 4)])
def test_external_chunks_reject_invalid_sizes(chunks, size):
    output = io.BytesIO()
    with (
        pytest.raises(Exception),
        zstar.open(output, options=WriterOptions(), mode="w") as writer,
    ):
        writer.add_external(Header.file("hello", size), chunks, Objects())
    assert not output.closed
    with pytest.raises(Exception):
        open_bytes(output.getvalue())


def test_async_sources_and_resolvers_run_on_owning_loop():
    data, objects = thin_archive()

    async def exercise():
        loop = asyncio.get_running_loop()

        class Source:
            async def size(self):
                return len(data)

            async def read_at(self, offset, length):
                assert asyncio.get_running_loop() is loop
                await asyncio.sleep(0)
                return data[offset : offset + length]

        class Resolver:
            async def get_encoded(self, address, hint, max_bytes):
                assert asyncio.get_running_loop() is loop
                await asyncio.sleep(0)
                return objects.get_encoded(address, hint, max_bytes)

        async with await aopen_source(
            Source(), len(data), blocking=False, resolver=Resolver(), verify_external=True
        ) as archive:
            assert await archive.read("hello") == b"abcdef"
            assert (await archive.entry("hello")).extensions == {"x-entry": (1, True)}

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
        async with zstar.open(sink, mode="w", async_=True) as writer:
            await writer.addfile(Header.file("body", 4, extensions={"x-test": b"yes"}), Body())
            await writer.add_external(Header.file("external", 6), chunks(), Store())
        assert sink.flushed
        async with await aopen_bytes(
            sink.data, resolver=_native.BlockingAdapter(objects)
        ) as reader:
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
            async with zstar.open(_native.BlockingAdapter(output), mode="w", async_=True) as writer:
                task = asyncio.create_task(writer.addfile(Header.file("hello", 1), Body()))
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
        async with await aopen_bytes(
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
        async def size(self):
            return len(data)

        async def read_at(self, offset, length):
            raise AssertionError("must not schedule an async callback from sync reads")

    async def exercise():
        with open_bytes(data, resolver=Resolver()) as archive:
            with pytest.raises(ZstarError) as caught:
                archive.read("hello")
            assert isinstance(caught.value.__cause__, TypeError)
        with pytest.raises(ZstarError) as caught:
            zstar.open(Source())
        assert isinstance(caught.value.__cause__, TypeError)

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
        from zstar.fsspec import ZstarFileSystem

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
