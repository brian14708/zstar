"""Thread-safety stress tests, meaningful on any build and decisive under PYTHON_GIL=0.

On a GIL build they exercise lock correctness and absence of deadlocks; on a
free-threaded interpreter (3.13t/3.14t) they also catch data races because
nothing serializes Python and native code. Every test is bounded by timeouts so
a deadlock fails instead of hanging.
"""

import asyncio
import io
import sys
import sysconfig
import threading

import pytest
import zstar
from zstar.format import EntryArchiveWriter, ManifestArchive

pytestmark = pytest.mark.free_threading

THREADS = 8
TIMEOUT = 60


def build(files):
    out = io.BytesIO()
    with zstar.ArchiveWriter(out, frame_size=4096) as writer:
        for name, body in files.items():
            info = zstar.TarInfo(name)
            info.size = len(body)
            writer.addfile(info, io.BytesIO(body))
    return out.getvalue()


def bodies(count=64):
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
        assert not thread.is_alive(), "deadlock: worker did not finish"
    assert not errors, errors


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="needs a free-threaded interpreter"
)
def test_native_module_does_not_re_enable_the_gil():
    # Importing the extension must not turn the GIL back on (gil_used = false).
    assert not sys._is_gil_enabled()
    assert zstar.pack_file("a", b"x")


def test_shared_archive_concurrent_reads_and_metadata():
    files = bodies()
    data = build(files)
    with zstar.Archive(data, cache_bytes=8192) as archive:

        def work(index):
            for round_ in range(20):
                name = f"f{(index * 7 + round_) % len(files):03d}"
                assert archive.read(name) == files[name]
                assert archive.getmember(name).size == len(files[name])
                with archive.extractfile(name) as stream:
                    stream.seek(3)
                    assert stream.read(100) == files[name][3:103]
                assert archive.read_many([(name, 0, 10)]) == [files[name][:10]]
                assert archive.entry_count == len(files)
                assert archive.cache_bytes >= 0
            assert sum(1 for _ in archive.getnames()) == len(files)

        run_threads(work)


@pytest.mark.parametrize("manifest", [False, True])
def test_shared_native_iterator_serializes_without_borrow_errors(manifest):
    files = bodies()
    data = build(files)
    archive = ManifestArchive(_Bytes(data), len(data)) if manifest else zstar.Archive(data)
    with archive:
        iterator = archive.iter_entries() if manifest else archive._reader.iter_entries()
        seen, lock = [], threading.Lock()

        def work(_):
            for entry in iterator:
                with lock:
                    seen.append(entry[0])

        run_threads(work)
        assert sorted(seen) == sorted(files)


def test_close_races_with_reads_end_in_closed_errors_only():
    files = bodies()
    data = build(files)
    archive = zstar.Archive(data)
    barrier = threading.Barrier(THREADS + 1)
    errors = []

    def work(index):
        barrier.wait(TIMEOUT)
        for round_ in range(200):
            try:
                archive.read(f"f{round_ % len(files):03d}")
            except (ValueError, zstar.ZstarError):
                return
            except BaseException as error:  # pragma: no cover
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


def test_shared_async_archive_used_from_many_threads_with_their_own_loops():
    files = bodies(16)
    data = build(files)
    archive = asyncio.run(zstar.AsyncArchive.from_bytes(data, cache_bytes=4096))

    async def use(index):
        for round_ in range(10):
            name = f"f{(index + round_) % 16:03d}"
            assert await archive.read(name) == files[name]
            assert (await archive.getmember(name)).size == len(files[name])
        assert [n async for n in archive.getnames()] == sorted(files)

    run_threads(lambda index: asyncio.run(use(index)), 4)
    asyncio.run(archive.close())


def test_concurrent_fsspec_load_failures_do_not_wedge(monkeypatch):
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(build({"x": b"ok"})))
    original = fs._open_archive

    async def fail():
        raise RuntimeError("boom")

    monkeypatch.setattr(fs, "_open_archive", fail)

    def work(index):
        for _ in range(10):
            with pytest.raises(RuntimeError, match="boom"):
                fs.info("x")

    try:
        run_threads(work)
        monkeypatch.setattr(fs, "_open_archive", original)
        assert fs.info("x")["size"] == 2
    finally:
        fs.close()


def test_shared_decoded_memory_cache_between_archives_and_threads():
    files = bodies(16)
    data = build(files)
    cache = zstar.DecodedMemoryCache(1 << 20)
    archives = [
        zstar.Archive(data, shared_cache=cache, shared_cache_identity=b"snapshot", cache_bytes=0)
        for _ in range(3)
    ]

    def work(index):
        archive = archives[index % 3]
        for round_ in range(30):
            name = f"f{(index + round_) % 16:03d}"
            assert archive.read(name) == files[name]
            assert cache.used_bytes >= 0
            if index == 0 and round_ % 10 == 0:
                cache.clear()

    run_threads(work)
    for archive in archives:
        archive.close()


def test_python_decoded_cache_callbacks_run_without_native_locks():
    files = bodies(8)
    data = build(files)
    store, lock = {}, threading.Lock()

    class Cache:
        def get(self, key):
            with lock:
                return store.get(key)

        def put(self, token):
            with lock:
                store[token.key] = token

    with zstar.Archive(
        data, shared_cache=Cache(), shared_cache_identity=b"id", cache_bytes=0
    ) as archive:

        def work(index):
            for round_ in range(20):
                name = f"f{(index + round_) % 8:03d}"
                assert archive.read(name) == files[name]

        run_threads(work)
    assert store


def test_manifest_archive_rejects_callback_reentry_and_exposes_closed_without_waiting():
    data = build({"f": b"x" * 5000})
    seen = {}

    class Source:
        archive = None
        armed = False

        def read_range(self, offset, length):
            if self.armed:
                archive = self.archive
                seen["closed"] = archive.closed  # lock-free, must not block
                seen["count"] = archive.entry_count  # immutable metadata
                for name, call in {
                    "read": lambda: archive.read("f"),
                    "entry": lambda: archive.entry("f"),
                    "close": lambda: archive.close(),
                    "iter": lambda: next(archive.iter_entries()),
                }.items():
                    with pytest.raises(zstar.ZstarError) as caught:
                        call()
                    seen[name] = caught.value.category
            return data[offset : offset + length]

    source = Source()
    archive = ManifestArchive(source, len(data), cache_bytes=0)
    source.archive = archive
    source.armed = True
    assert archive.read("f") == b"x" * 5000
    assert seen["closed"] is False and seen["count"] == 1
    assert {seen[k] for k in ("read", "entry", "close", "iter")} == {"unsupported-operation"}
    source.armed = False
    assert archive.read("f", 0, 3) == b"xxx"
    archive.close()
    assert archive.closed


def test_manifest_archive_waiters_release_the_gil_for_the_running_operation():
    data = build({"f": b"x" * 100})
    entered, release = threading.Event(), threading.Event()

    class Source:
        slow = False

        def read_range(self, offset, length):
            if self.slow:
                entered.set()
                assert release.wait(TIMEOUT)
            return data[offset : offset + length]

    source = Source()
    archive = ManifestArchive(source, len(data), cache_bytes=0)
    source.slow = True
    results = []
    first = threading.Thread(target=lambda: results.append(archive.read("f")))
    first.start()
    assert entered.wait(TIMEOUT)
    # Lock-free properties answer while the operation is stuck in its callback.
    assert archive.closed is False and archive.entry_count == 1
    second = threading.Thread(target=lambda: results.append(archive.read("f", 0, 1)))
    second.start()
    # Python keeps running (this thread and the heartbeat) while the second waits.
    ticks = []
    heartbeat = threading.Thread(target=lambda: ticks.append(sum(range(1000))))
    heartbeat.start()
    heartbeat.join(TIMEOUT)
    assert ticks
    source.slow = False
    release.set()
    first.join(TIMEOUT)
    second.join(TIMEOUT)
    assert not first.is_alive() and not second.is_alive()
    assert sorted(results, key=len) == [b"x", b"x" * 100]
    archive.close()


class _Bytes:
    def __init__(self, data):
        self.data = data

    def read_range(self, offset, length):
        return self.data[offset : offset + length]


def test_writer_serializes_concurrent_adds_and_rejects_reentrant_sinks():
    output = io.BytesIO()
    writer = EntryArchiveWriter(output, frame_size=2048)

    def work(index):
        for round_ in range(10):
            name = f"t{index}-{round_:02d}"
            body = name.encode() * 20
            writer.add((name, 0, 420, 0, 0, 0, ()), io.BytesIO(body), len(body))

    run_threads(work)
    writer.close()
    with zstar.Archive(output.getvalue()) as archive:
        assert archive.entry_count == THREADS * 10
        assert archive.read("t3-07") == b"t3-07" * 20

    # A body callback that re-enters the same writer is rejected, not deadlocked.
    sink = io.BytesIO()
    writer = EntryArchiveWriter(sink)
    outcome = []

    class Body:
        def read(self, size):
            with pytest.raises(zstar.ZstarError) as caught:
                writer.add(("other", 0, 420, 0, 0, 0, ()))
            outcome.append(caught.value.category)
            with pytest.raises(zstar.ZstarError):
                writer.close()
            assert writer.closed is False  # lock-free
            return b""

    writer.add(("body", 0, 420, 0, 0, 0, ()), Body(), 0)
    assert outcome == ["unsupported-operation"]
    writer.close()
    assert writer.closed


def test_writer_failure_poisons_every_thread_without_borrow_errors():
    class Broken(io.RawIOBase):
        def writable(self):
            return True

        def write(self, data):
            raise OSError("disk full")

    writer = EntryArchiveWriter(Broken(), frame_size=1024)

    def work(index):
        errors = 0
        for round_ in range(20):
            body = b"y" * 4000
            try:
                writer.add((f"w{index}-{round_}", 0, 420, 0, 0, 0, ()), io.BytesIO(body), 4000)
            except (zstar.ZstarError, ValueError, OSError):
                errors += 1
        assert errors

    run_threads(work, 4)
    with pytest.raises((ValueError, zstar.ZstarError, OSError)):
        writer.close()
