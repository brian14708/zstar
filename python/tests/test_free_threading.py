"""Thread stress for the shared Rust executor, also run on free-threaded wheels."""

import asyncio
import io
import sys
import sysconfig
import threading

import pytest
import zstar
from zstar import (
    Header,
    ReadRequest,
    WriterOptions,
    ZstarError,
    _native,
)
from zstar.fsspec import ZstarFileSystem

pytestmark = pytest.mark.free_threading
THREADS = 8
TIMEOUT = 15


def build(files):
    out = io.BytesIO()
    with zstar.open(out, options=WriterOptions(frame_size=4096), mode="w") as writer:
        for path, body in files.items():
            writer.writestr(path, body)
    return out.getvalue()


def bodies(count=32):
    return {f"f{i:03d}": bytes([i % 251]) * (500 + 37 * i) for i in range(count)}


def run_threads(function, count=THREADS):
    errors = []

    def guarded(index):
        try:
            function(index)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=guarded, args=(i,)) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive(), "worker did not finish"
    assert not errors, errors


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="free-threaded interpreter"
)
def test_native_module_does_not_reenable_gil():
    assert not sys._is_gil_enabled()
    assert build({"data": b"hello"})


def test_shared_archive_concurrent_reads_and_metadata():
    files = bodies()
    with zstar.open(_native.Source.memory(build(files))) as archive:

        def work(index):
            for round_ in range(12):
                path = f"f{(index * 7 + round_) % len(files):03d}"
                assert archive.read(path) == files[path]
                assert archive.entry(path).size == len(files[path])
                with archive.open(path) as stream:
                    stream.seek(3)
                    assert stream.read(100) == files[path][3:103]
                assert archive.read_many([ReadRequest(path, 0, 10)]) == [files[path][:10]]
            assert len(list(archive)) == len(files)

        run_threads(work)


def test_shared_native_cursor_serializes_batches_without_borrow_errors():
    files = bodies(160)
    with zstar.open(_native.Source.memory(build(files))) as archive:
        iterator = archive._reader._iter_values()
        seen, guard = [], threading.Lock()

        def work(_):
            while batch := iterator._next_batch():
                with guard:
                    seen.extend(entry.path for entry in batch)

        run_threads(work)
        assert sorted(seen) == sorted(files)


def test_close_races_with_reads_end_in_lifecycle_errors_only():
    files = bodies()
    archive = zstar.open(_native.Source.memory(build(files)))
    barrier = threading.Barrier(THREADS + 1)
    errors = []

    def work(index):
        barrier.wait(TIMEOUT)
        for round_ in range(100):
            try:
                archive.read(f"f{round_ % len(files):03d}")
            except (ValueError, ZstarError):
                return
            except BaseException as error:
                errors.append(error)
                return

    threads = [threading.Thread(target=work, args=(i,)) for i in range(THREADS)]
    for thread in threads:
        thread.start()
    barrier.wait(TIMEOUT)
    archive.close()
    for thread in threads:
        thread.join(TIMEOUT)
        assert not thread.is_alive()
    assert not errors and archive.closed


def test_async_archive_rejects_foreign_event_loops():
    async def run():
        archive = await zstar.open(
            await _native.AsyncSource.memory(build({"data": b"hello"})), async_=True
        )

        def work(_):
            async def misuse():
                with pytest.raises(RuntimeError, match="loop"):
                    await archive.read("data")

            asyncio.run(misuse())

        await asyncio.to_thread(run_threads, work, 4)
        assert await archive.read("data") == b"hello"
        await archive.close()

    asyncio.run(run())


def test_concurrent_fsspec_load_failures_do_not_wedge(monkeypatch):
    fs = ZstarFileSystem(fo=io.BytesIO(build({"data": b"ok"})))
    original = fs._open_archive

    async def fail():
        raise RuntimeError("boom")

    monkeypatch.setattr(fs, "_open_archive", fail)

    def work(index):
        for _ in range(10):
            with pytest.raises(RuntimeError, match="boom"):
                fs.info("data")

    try:
        run_threads(work)
        monkeypatch.setattr(fs, "_open_archive", original)
        assert fs.info("data")["size"] == 2
    finally:
        fs.close()


def test_writer_failure_poisons_concurrent_callers():
    class Broken(io.RawIOBase):
        def write(self, data):
            raise OSError("disk full")

    writer = zstar.open(Broken(), options=WriterOptions(frame_size=1024), mode="w")

    def work(index):
        errors = 0
        for round_ in range(12):
            try:
                writer.addfile(Header.file(f"w{index}-{round_}", 4000), io.BytesIO(b"y" * 4000))
            except (ZstarError, ValueError, OSError):
                errors += 1
        assert errors

    run_threads(work, 4)
    with pytest.raises((ValueError, ZstarError, OSError)):
        writer.close()
    writer.abort()
