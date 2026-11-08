"""Native writer capture, bounded callbacks, errors, and publication ownership."""

import asyncio
import io
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import zstar
from zstar import (
    Header,
    MetadataWriterOptions,
    WriterOptions,
    ZstarError,
    _native,
)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.skipif(os.name != "posix", reason="Unix capture metadata")
def test_filesystem_capture_is_sorted_exact_and_does_not_follow_links(tmp_path, asynchronous):
    root = tmp_path / "input"
    (root / "nested").mkdir(parents=True)
    (root / "nested/data").write_bytes(b"hello")
    (root / "link").symlink_to("../nested//data")
    os.utime(root / "nested/data", ns=(1_734_567_890_123_456_789,) * 2)
    path = str(tmp_path / "archive")

    async def run():
        async with (
            await _native.aopen_sink(path) as sink,
            await zstar.open(sink, mode="w", async_=True) as writer,
        ):
            await writer.add(root, "root", preserve_metadata=True)
        async with (
            await _native.aopen_source(path) as source,
            await zstar.open(source, async_=True) as archive,
        ):
            return [entry async for entry in archive], await archive.read("root/nested/data")

    if asynchronous:
        entries, body = asyncio.run(run())
    else:
        with _native.open_sink(path) as sink, zstar.open(sink, mode="w") as writer:
            writer.add(root, "root", preserve_metadata=True)
        with _native.open_source(path) as source, zstar.open(source) as archive:
            entries, body = list(archive), archive.read("root/nested/data")
    assert [entry.path for entry in entries] == [
        "root",
        "root/link",
        "root/nested",
        "root/nested/data",
    ]
    assert entries[1].linkname == "../nested//data"
    assert entries[-1].mtime_ns == 1_734_567_890_123_456_789
    assert body == b"hello"


def test_nonrecursive_capture_and_normalized_metadata(tmp_path):
    root = tmp_path / "input"
    root.mkdir(mode=0o700)
    (root / "file").write_bytes(b"data")
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        writer.add(root, "dir", recursive=False)
        writer.add(root / "file", "file")
    with zstar.open(_native.Source.memory(output.getvalue())) as archive:
        assert [entry.path for entry in archive] == ["dir", "file"]
        assert archive.entry("dir").mode == 0o755
        assert archive.entry("file").mtime_ns == 0


def test_duplicates_and_missing_ancestors_fail_at_finalization():
    for names in [["same", "same"], ["missing/file"]]:
        writer = zstar.open(io.BytesIO(), mode="w")
        for name in names:
            writer.writestr(name, b"data")
        with pytest.raises(ZstarError) as first:
            writer.close()
        with pytest.raises(ValueError):
            writer.writestr("after", b"failure")
        with pytest.raises(ZstarError) as replay:
            writer.abort()
        assert replay.value is first.value


@pytest.mark.parametrize("result", [0, -1, 1000000, True, None])
def test_invalid_sink_progress_and_original_causes(result):
    class Sink:
        def write(self, data):
            return result

        def flush(self):
            pass

    writer = zstar.open(Sink(), mode="w")
    with pytest.raises(ZstarError) as first:
        writer.writestr("file", bytes(4096))
        writer.close()
    with pytest.raises(ZstarError) as replay:
        writer.abort()
    assert replay.value is first.value


def test_sink_flush_failure_preserves_callback_cause():
    cause = OSError("private sink error")

    class Sink(io.BytesIO):
        def flush(self):
            raise cause

    writer = zstar.open(Sink(), mode="w")
    writer.writestr("data", b"hello")
    with pytest.raises(ZstarError) as caught:
        writer.close()
    assert caught.value.__cause__ is cause
    assert "private sink error" not in str(caught.value)
    with pytest.raises(ZstarError) as replay:
        writer.abort()
    assert replay.value is caught.value


def test_writer_and_body_callback_cycles_are_collectible():
    import gc
    import weakref

    output = io.BytesIO()

    class Body(io.BytesIO):
        pass

    body = Body(b"hello")
    writer = zstar.open(output, mode="w")
    body.writer = writer
    writer.addfile(Header.file("data", 5), body)
    reference = weakref.ref(writer)
    del body, writer
    gc.collect()
    assert reference() is None


def test_shared_writer_serializes_concurrent_mutations():
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer, ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda i: writer.writestr(f"data{i:03}", bytes([i]) * 1000), range(80)))
    with zstar.open(_native.Source.memory(output.getvalue())) as archive:
        assert len(list(archive)) == 80
        assert archive.read("data079") == bytes([79]) * 1000


def test_writer_callback_reentry_is_rejected_before_waiting():
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:

        class Body:
            def read(self, size):
                for operation in [
                    lambda: writer.writestr("other", b""),
                    writer.close,
                    writer.abort,
                ]:
                    with pytest.raises(ZstarError) as caught:
                        operation()
                    assert caught.value.category == "unsupported-operation"
                return b""

        writer.addfile(Header.file("body", 0), Body())


@pytest.mark.parametrize("cancel_abort", [False, True])
def test_async_writer_failure_cancels_body_and_drains_blocking_callbacks(cancel_abort):
    entered, release = threading.Event(), threading.Event()

    class Body:
        def read(self, size):
            entered.set()
            assert release.wait(5)
            return b""

    async def run():
        output = io.BytesIO()
        writer = await zstar.open(_native.BlockingAdapter(output), mode="w", async_=True)
        task = asyncio.create_task(
            writer.addfile(Header.file("data", 1), _native.BlockingAdapter(Body()))
        )
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        cleanup = asyncio.create_task(writer.abort())
        await asyncio.sleep(0)
        assert not cleanup.done()
        if cancel_abort:
            cleanup.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cleanup
            cleanup = asyncio.create_task(writer.abort())
            await asyncio.sleep(0)
            assert not cleanup.done()
        release.set()
        await asyncio.wait_for(cleanup, 2)
        assert not output.closed and writer.closed

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_async_writer_enforces_loop_affinity():
    async def create():
        return await zstar.open(_native.BlockingAdapter(io.BytesIO()), mode="w", async_=True)

    writer = asyncio.run(create())

    async def misuse():
        with pytest.raises(RuntimeError, match="loop"):
            await writer.writestr("data", b"x")

    asyncio.run(misuse())


def test_page_limits_and_workers_preserve_deterministic_output():
    def encode(workers):
        output = io.BytesIO()
        options = WriterOptions(
            frame_size=512,
            workers=workers,
            metadata=MetadataWriterOptions(block_target=512, max_page_bytes=1024),
        )
        with zstar.open(output, options=options, mode="w") as writer:
            for i in range(20):
                writer.writestr(f"data{i:03}", bytes([i]) * 4000)
        return output.getvalue()

    assert encode(0) == encode(2)
