"""Archive-relative fsspec paths, sparse reads, snapshots, and ownership."""

import asyncio
import gc
import io
import os
import random
import sys
import threading
import warnings
import weakref
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from tarfile import TarInfo
from typing import ClassVar

import fsspec
import pytest
import zstar
from _support import pack_directory, pack_file
from zstar import WriterOptions
from zstar.fsspec import ZstarFileSystem
from zstar.tar import from_tarinfo


def test_filesystem_reads_seeks_and_lists():
    fs = ZstarFileSystem(fo=io.BytesIO(pack_file("hello.txt", b"hello world")))
    try:
        assert fs.ls("") == [{"name": "hello.txt", "size": 11, "type": "file"}]
        assert fs.info("zstar:///") == {"name": "", "size": 0, "type": "directory"}
        assert fs.exists("/hello.txt")
        assert not fs.exists("missing")
        with fs.open("zstar:///./hello.txt", block_size=3) as member:
            assert member.read(5) == b"hello"
            assert member.seek(-5, 2) == 6
            assert member.read() == b"world"
        with pytest.raises(NotImplementedError):
            fs.open("hello.txt", "wb")
        with pytest.raises(IsADirectoryError):
            fs.open("")
    finally:
        fs.close()


def test_filesystem_chained_local_paths_and_globs(tmp_path):
    root = tmp_path / "input"
    (root / "dir" / "nested").mkdir(parents=True)
    (root / "dir" / "file.txt").write_bytes(b"hello")
    (root / "dir" / "nested" / "file.txt").write_bytes(b"hidden")
    path = tmp_path / "test.zstar"
    path.write_bytes(pack_directory(root))
    fs, member = fsspec.core.url_to_fs(f"zstar://dir/file.txt::{path.as_uri()}")
    try:
        assert member == "dir/file.txt"
        assert fs.ls("/dir/", detail=False) == ["dir/file.txt", "dir/nested"]
        assert fs.ls("dir/file.txt", detail=False) == ["dir/file.txt"]
        assert fs.glob("**/*.txt") == ["dir/file.txt", "dir/nested/file.txt"]
        with fs.open(member) as handle:
            assert handle.read() == b"hello"
        with pytest.raises(FileNotFoundError):
            fs.ls("missing")
    finally:
        fs.close()
    with fsspec.open(f"zstar://dir/file.txt::{path.as_uri()}") as handle:
        assert handle.read() == b"hello"
    fs = ZstarFileSystem(fo=path)
    try:
        with fs.open("dir/file.txt") as handle:
            assert handle.read() == b"hello"
    finally:
        fs.close()


def test_filesystems_are_not_instance_cached(tmp_path):
    path = tmp_path / "test.zstar"
    path.write_bytes(pack_file("x", b"hello"))
    first = fsspec.filesystem("zstar", fo=path)
    second = fsspec.filesystem("zstar", fo=path)
    assert first is not second
    first.close()
    try:
        with second.open("x") as handle:
            assert handle.read() == b"hello"
    finally:
        second.close()


@pytest.mark.parametrize("cache_type, expected", [(None, 12), ("readahead", 12)])
def test_sparse_reads_avoid_unrequested_logical_bytes(monkeypatch, cache_type, expected):
    # Observe member ranges, independently of compression ratio or frame size.
    from zstar._fsspec import _MemberFile

    ranges = []
    original = _MemberFile._fetch_range

    def fetch(self, start, end):
        ranges.append((start, end))
        return original(self, start, end)

    monkeypatch.setattr(_MemberFile, "_fetch_range", fetch)
    fs = ZstarFileSystem(fo=io.BytesIO(pack_file("x", b"x" * (16 << 20))))
    try:
        options = {} if cache_type is None else {"cache_type": cache_type}
        with fs.open("x", **options) as handle:
            handle.seek(-8, 2)
            assert handle.read(8) == b"x" * 8
            handle.seek(0)
            assert handle.read(4) == b"x" * 4
        assert sum(end - start for start, end in ranges) == expected
    finally:
        fs.close()


@pytest.mark.parametrize("nanoseconds", [0, 1234567890, -1, -(1 << 63), (1 << 63) - 1])
def test_modified_uses_exact_member_timestamp_and_utc(nanoseconds):
    sink = io.BytesIO()
    with zstar.open(sink, options=WriterOptions(), mode="w") as writer:
        info = TarInfo("x")
        info.size = 1
        info.pax_headers["mtime"] = str(Decimal(nanoseconds) / 1_000_000_000)
        writer.addfile(from_tarinfo(info), io.BytesIO(b"x"))
    fs = ZstarFileSystem(fo=io.BytesIO(sink.getvalue()))
    try:
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        assert fs.modified("/x") == epoch + timedelta(microseconds=nanoseconds // 1000)
        assert fs.modified("") == epoch
        with pytest.raises(FileNotFoundError):
            fs.modified("missing")
    finally:
        fs.close()


def test_close_owns_streams_and_is_terminal():
    source = io.BytesIO(pack_file("x", b"hello"))
    fs = ZstarFileSystem(fo=source)
    handle = fs.open("x")
    assert handle.read(2) == b"he"
    fs.close()
    assert source.closed
    with pytest.raises(ValueError):
        handle.read()
    handle.close()
    with pytest.raises(ValueError, match="filesystem closed"):
        fs.info("x")
    fs.close()
    unopened = io.BytesIO(b"unused")
    ZstarFileSystem(fo=unopened).close()
    assert unopened.closed


def test_cleanup_errors_preserve_primary_failure():
    class FailingClose(io.BytesIO):
        def close(self):
            super().close()
            raise OSError("source cleanup failed")

    fs = ZstarFileSystem(fo=FailingClose(pack_file("x", b"hello")))
    assert fs.info("x")["size"] == 5
    with pytest.raises(OSError, match="source cleanup failed"):
        fs.close()
    fs.close()
    fs = ZstarFileSystem(fo=FailingClose(b"bad"))
    with pytest.raises(ExceptionGroup) as caught:
        fs.info("x")
    assert isinstance(caught.value.exceptions[0], zstar.ZstarError)
    assert isinstance(caught.value.exceptions[1], OSError)
    fs.close()


@pytest.mark.parametrize("loaded", [False, True])
def test_dropping_filesystem_releases_owned_source(loaded):
    closed = threading.Event()

    class Source(io.BytesIO):
        def close(self):
            super().close()
            closed.set()

    source = Source(pack_file("x", b"hello"))
    fs = ZstarFileSystem(fo=source)
    if loaded:
        assert fs.info("x")["size"] == 5
    del fs
    gc.collect()
    assert closed.wait(5)


def test_close_drains_admitted_loads_and_rejects_new_work(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    source = io.BytesIO(pack_file("x", b"hello"))
    fs = ZstarFileSystem(fo=source)
    original = fs._open_archive

    async def load():
        entered.set()
        assert await asyncio.to_thread(release.wait, 5)
        return await original()

    monkeypatch.setattr(fs, "_open_archive", load)
    with ThreadPoolExecutor(max_workers=3) as pool:
        loading = pool.submit(fs.info, "x")
        assert entered.wait(5)
        closing = pool.submit(fs.close)
        from fsspec.asyn import sync

        async def wait_for_close():
            while not fs._closed:  # noqa: ASYNC110 - observing filesystem close state
                await asyncio.sleep(0)

        sync(fs.loop, wait_for_close)
        assert not closing.done()
        with pytest.raises(ValueError, match="filesystem closed"):
            fs.info("x")
        joined_close = pool.submit(fs.close)
        release.set()
        with pytest.raises(ValueError, match="filesystem closed"):
            loading.result(timeout=5)
        closing.result(timeout=5)
        joined_close.result(timeout=5)
    assert source.closed
    assert fs._resources is None and not fs._loading


def test_external_members_expose_logical_bytes(thin_data, corpus_dir):
    objects = fsspec.filesystem(
        "dir", path=str(corpus_dir / "objects"), fs=fsspec.filesystem("file")
    )
    fs = ZstarFileSystem(fo=io.BytesIO(thin_data), external=objects)
    try:
        # Filesystem metadata never builds TarInfo, so it has no extension loss to report.
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            assert fs.info("hello.txt")["size"] == 6
        with fs.open("hello.txt", block_size=3) as member:
            assert member.read(2) == b"he"
            member.seek(1)
            assert member.read() == b"ello\n"
    finally:
        fs.close()
    assert objects.ls("")


def test_external_configuration_is_write_capable():
    with pytest.raises(ValueError, match="external_threshold"):
        ZstarFileSystem(fo=io.BytesIO(), mode="w", external_threshold=-1)
    with pytest.raises(TypeError, match="low-level API"):
        ZstarFileSystem(fo=io.BytesIO(), resolver=object())


def test_external_missing_object(thin_data):
    objects = fsspec.filesystem(
        "dir", path="/missing-zstar-objects", fs=fsspec.filesystem("memory")
    )
    with ZstarFileSystem(fo=io.BytesIO(thin_data), external=objects) as fs:
        with pytest.raises(zstar.ZstarError) as error:
            fs.cat_file("hello.txt")
        assert error.value.category == "missing-external-content"


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot replace an open Python file")
def test_local_snapshot_survives_atomic_path_replacement(tmp_path):
    path = tmp_path / "snapshot.zstar"
    path.write_bytes(pack_file("x", b"AAAAAA"))
    fs = ZstarFileSystem(fo=path)
    try:
        with fs.open("x", block_size=1) as member:
            assert member.read(6) == b"AAAAAA"
            replacement = tmp_path / "new.zstar"
            replacement.write_bytes(pack_file("x", b"BBBBBB"))
            replacement.replace(path)
            member.seek(0)
            assert member.read(6) == b"AAAAAA"
    finally:
        fs.close()


def test_symlink_metadata_is_preserved_without_following_links(tmp_path):
    root = tmp_path / "input"
    (root / "dir" / "nested").mkdir(parents=True)
    (root / "dir" / "x").write_bytes(b"hello")
    (root / "link").symlink_to("dir", target_is_directory=True)
    fs = ZstarFileSystem(fo=io.BytesIO(pack_directory(root)))
    try:
        assert fs.info("link")["type"] == "link"
        assert fs.ls("link", detail=False) == ["link"]
        assert next(item for item in fs.ls("") if item["name"] == "link")["type"] == "link"
        with pytest.raises(OSError, match="symlink"):
            fs.open("link")
        with pytest.raises(OSError, match="symlink"):
            fs.cat_ranges(["link"], [0], [1], on_error="raise")
        for operation in (fs.info, fs.modified, fs.open, fs.ls):
            with pytest.raises(FileNotFoundError):
                operation("link/x")
        with fs.open("dir/x") as member:
            assert member.read() == b"hello"
        with pytest.raises(FileNotFoundError):
            fs.open("../dir/x")
    finally:
        fs.close()


def test_generic_memory_backend_supports_range_reads():
    backend = fsspec.filesystem("memory")
    backend.pipe("zstar-test/archive", pack_file("x", b"hello"))
    fs = ZstarFileSystem(fo="memory://zstar-test/archive", fs=backend)
    try:
        with fs.open("x") as handle:
            assert handle.read() == b"hello"
    finally:
        fs.close()
        backend.rm("zstar-test/archive")


def test_backend_options_survive_chaining(tmp_path, monkeypatch):
    import fsspec.core

    path = tmp_path / "archive.zstar"
    path.write_bytes(pack_file("x", b"hello"))
    original = fsspec.filesystem
    received = []

    def capture(protocol, **options):
        received.append((protocol, options))
        return original(protocol, **options)

    monkeypatch.setattr(fsspec, "filesystem", capture)
    fs, member = fsspec.core.url_to_fs(
        f"zstar://x::{path.as_uri()}",
        file={"auto_mkdir": True},
        zstar={"reader_options": zstar.AsyncReaderOptions(verify_external=True)},
    )
    try:
        assert fs.info(member)["size"] == 5
        assert received == [("file", {"auto_mkdir": True})]
    finally:
        fs.close()


@pytest.mark.parametrize("mode", ["a", "x", "rb"])
def test_constructor_rejects_invalid_modes(mode):
    with pytest.raises(ValueError, match="mode"):
        ZstarFileSystem(fo="unused.zstar", mode=mode)


def test_async_filesystem_metadata_ranges_and_member_handles(tmp_path):
    root = tmp_path / "input"
    (root / "dir").mkdir(parents=True)
    (root / "dir" / "x").write_bytes(b"hello world")
    data = pack_directory(root)

    async def exercise():
        from fsspec.asyn import AsyncFileSystem

        source = io.BytesIO(data)
        async with ZstarFileSystem(fo=source, asynchronous=True) as fs:
            assert isinstance(fs, AsyncFileSystem)
            assert await fs._info("dir/x") == {"name": "dir/x", "size": 11, "type": "file"}
            assert await fs._ls("dir", detail=False) == ["dir/x"]
            assert await fs._glob("**/*") == ["dir", "dir/x"]
            assert await fs._exists("dir/x")
            assert not await fs._exists("missing")
            assert await fs._cat_file("dir/x", start=-5) == b"world"
            assert await fs._cat_file("dir/x", start=5, end=2) == b""
            async with await fs.open_async("dir/x") as handle:
                assert await handle.read(5) == b"hello"
                assert handle.seek(-5, 2) == 6
                buffer = bytearray(5)
                assert await handle.readinto(buffer) == 5
                assert buffer == b"world"
            await fs.close()
        assert source.closed
        await fs.close()
        with pytest.raises(ValueError, match="filesystem closed"):
            await fs._info("dir/x")

    asyncio.run(exercise())


def test_cancelled_async_waiter_does_not_cancel_shared_load(monkeypatch):
    async def exercise():
        fs = ZstarFileSystem(fo=io.BytesIO(pack_file("x", b"hello")), asynchronous=True)
        entered, release = asyncio.Event(), asyncio.Event()
        original = fs._open_archive
        loads = []

        async def load():
            loads.append(True)
            entered.set()
            await release.wait()
            return await original()

        monkeypatch.setattr(fs, "_open_archive", load)
        first = asyncio.create_task(fs._info("x"))
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(fs._cat_file("x"))
        release.set()
        assert await second == b"hello"
        assert loads == [True]
        await fs.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("loaded", [False, True])
def test_dropping_member_on_io_loop_closes_owned_filesystem(loaded):
    from fsspec.asyn import sync

    source = io.BytesIO(pack_file("x", b"hello"))
    fs = ZstarFileSystem(fo=source)
    handles = [fs.open("x") if loaded else fs]
    loop = fs.loop
    del fs

    async def drop():
        handles.clear()
        gc.collect()
        while not source.closed:  # noqa: ASYNC110 - finalizer exposes no asyncio event
            await asyncio.sleep(0.001)

    sync(loop, drop, timeout=5)
    assert source.closed


@pytest.mark.parametrize("finalize", [False, True])
def test_cleanup_survives_collection_while_suspended(monkeypatch, finalize):
    original = ZstarFileSystem._drain_close

    async def exercise():
        started, finished = asyncio.Event(), asyncio.Event()
        pending = []

        async def suspended_close(fs):
            future = asyncio.get_running_loop().create_future()
            pending.append(weakref.ref(future))
            started.set()
            await future
            await original(fs)
            finished.set()

        monkeypatch.setattr(ZstarFileSystem, "_drain_close", suspended_close)
        source = io.BytesIO(pack_file("x", b"hello"))
        fs = ZstarFileSystem(fo=source, asynchronous=True)
        assert await fs._cat_file("x") == b"hello"
        if not finalize:
            close = asyncio.create_task(fs.close())
            await asyncio.wait_for(started.wait(), 5)
            close.cancel()
            with pytest.raises(asyncio.CancelledError):
                await close
            del close
        del fs
        gc.collect()
        await asyncio.wait_for(started.wait(), 5)
        gc.collect()
        future = pending[0]()
        assert future is not None, "pending cleanup was garbage collected"
        future.set_result(None)
        await asyncio.wait_for(finished.wait(), 5)
        assert source.closed

    asyncio.run(exercise())


class RangeFile(fsspec.spec.AbstractBufferedFile):
    def __init__(self, fs, path, **options):
        options.setdefault("cache_type", fs.default_cache_type)
        if options.get("block_size") is None:
            options["block_size"] = fs.default_block_size
        options.setdefault("size", len(fs.data))
        super().__init__(fs, path, **options)

    def _fetch_range(self, start, end):
        self.fs.ranges.append((start, end))
        return self.fs.data[start:end]


class RangeFileSystem(fsspec.spec.AbstractFileSystem):
    protocol = "zstar-test-range"
    cachable = False
    instances: ClassVar[list] = []

    def __init__(
        self, data, credential=None, default_cache_type="none", default_block_size=256, **options
    ):
        super().__init__(**options)
        self.data, self.credential = data, credential
        self.default_cache_type = default_cache_type
        self.default_block_size = default_block_size
        self.ranges, self.opened = [], []
        self.instances.append(self)

    def info(self, path, **kwargs):
        return {"name": path, "size": len(self.data), "type": "file"}

    def _open(self, path, mode="rb", **options):
        self.opened.append((path, mode, options.copy()))
        return RangeFile(self, path, mode=mode, **options)


def test_chaining_uses_resolved_filesystem_and_exact_ranges(monkeypatch):
    data = bytes(pack_file("x", b"hello" * (1 << 20)))
    credential = object()
    fsspec.register_implementation(RangeFileSystem.protocol, RangeFileSystem, clobber=True)

    async def reject_native(*args, **kwargs):
        raise AssertionError("generic chaining must use its fsspec filesystem")

    monkeypatch.setattr(zstar._native, "aopen_source", reject_native)
    with fsspec.open(
        "zstar://x::zstar-test-range://archive",
        **{"zstar-test-range": {"data": data, "credential": credential}},
    ) as member:
        assert member.read(5) == b"hello"
        member.seek(-5, 2)
        assert member.read(5) == b"hello"
    backend = RangeFileSystem.instances[-1]
    assert backend.credential is credential
    assert backend.opened[0][0:2] == ("archive", "rb")
    assert backend.ranges
    assert all(end - start < len(data) or end - start <= 128 << 10 for start, end in backend.ranges)
    assert sum(end - start for start, end in backend.ranges) < len(data) + (128 << 10)


def test_injected_filesystem_is_borrowed_and_keeps_credentials():
    backend = RangeFileSystem(bytes(pack_file("x", b"hello")), credential=object())
    fs = ZstarFileSystem(fo="archive", fs=backend)
    try:
        assert fs.cat_file("x") == b"hello"
    finally:
        fs.close()
    # Closing Zstar's stream leaves the caller's filesystem usable.
    with backend.open("archive", cache_type="none") as stream:
        assert stream.read(4) == backend.data[:4]


@pytest.mark.parametrize("cache_type", ["none", "blockcache"])
def test_source_honors_backend_cache_defaults(cache_type):
    backend = RangeFileSystem(bytes(pack_file("x", b"hello" * 1000)), default_cache_type=cache_type)
    with ZstarFileSystem(fo="archive", fs=backend) as fs:
        assert fs.cat_file("x", start=0, end=5) == b"hello"
        requests = len(backend.ranges)
        assert fs.cat_file("x", start=0, end=5) == b"hello"
        if cache_type == "blockcache":
            assert len(backend.ranges) == requests
        else:
            assert len(backend.ranges) > requests


def test_disk_blockcache_chaining_reuses_source_across_opens(tmp_path):
    payload = random.Random(0).randbytes(5 << 20)
    data = bytes(pack_file("x", payload))
    fsspec.register_implementation(RangeFileSystem.protocol, RangeFileSystem, clobber=True)
    options = {
        "blockcache": {
            "cache_storage": str(tmp_path / "cache"),
            "check_files": True,
            "skip_instance_cache": True,
        },
        "zstar-test-range": {"data": data},
    }
    url = "zstar://x::blockcache::zstar-test-range://archive"
    fs, path = fsspec.core.url_to_fs(url, **options)
    # fsspec MMapCache._makefile finalizes its backing file after creating the mmap.
    with pytest.warns(ResourceWarning, match="unclosed file"), fs, fs.open(path) as member:
        assert member.read(5) == payload[:5]
    backend = RangeFileSystem.instances[-1]
    assert backend.ranges
    assert sum(end - start for start, end in backend.ranges) < len(data) + (128 << 10)
    fs, path = fsspec.core.url_to_fs(url, **options)
    with pytest.warns(ResourceWarning, match="unclosed file"), fs, fs.open(path) as member:
        assert member.read(5) == payload[:5]
    reopened_backend = RangeFileSystem.instances[-1]
    assert reopened_backend is not backend
    assert not reopened_backend.ranges


def test_fsspec_is_optional_and_s3fs_is_separate():
    from importlib.metadata import requires

    from packaging.requirements import Requirement

    dependencies = [Requirement(value) for value in requires("zstar-py")]
    assert any(
        requirement.name == "fsspec"
        and requirement.marker is not None
        and requirement.marker.evaluate({"extra": "fsspec"})
        and not requirement.marker.evaluate({"extra": ""})
        for requirement in dependencies
    )
    assert all(requirement.name != "s3fs" for requirement in dependencies)


def test_injected_async_filesystem_uses_exact_ranges():
    from fsspec.asyn import AsyncFileSystem

    data = bytes(pack_file("x", b"hello"))

    class AsyncRanges(AsyncFileSystem):
        async_impl = True

        def __init__(self):
            super().__init__(asynchronous=True)
            self.ranges = []

        async def _info(self, path, **kwargs):
            assert path == "archive"
            return {"name": path, "size": len(data), "type": "file"}

        async def _cat_file(self, path, start=None, end=None, **kwargs):
            assert path == "archive"
            self.ranges.append((start, end))
            await asyncio.sleep(0)
            return data[start:end]

        def _open(self, *args, **kwargs):
            raise AssertionError("asynchronous backends must use async range I/O")

    async def exercise():
        backend = AsyncRanges()
        async with ZstarFileSystem(fo="archive", fs=backend, asynchronous=True) as fs:
            assert await fs._cat_file("x", start=1, end=4) == b"ell"
        assert backend.ranges
        assert all(
            end - start < len(data) or end - start <= 128 << 10 for start, end in backend.ranges
        )
        assert await backend._cat_file("archive", start=0, end=4) == data[:4]

    asyncio.run(exercise())


@pytest.mark.parametrize("source", ["direct", "chained", "injected"])
def test_s3_preserves_backend_credentials(monkeypatch, source):
    from importlib import import_module

    class MockS3(RangeFileSystem):
        protocol = "s3"

    monkeypatch.setitem(import_module("fsspec.registry")._registry, "s3", MockS3)
    data = bytes(pack_file("x", b"hello"))
    credential = object()
    backend = MockS3(data, credential=credential)

    async def reject_native(*args, **kwargs):
        raise AssertionError("fsspec S3 access must use its selected filesystem")

    monkeypatch.setattr(zstar._native, "aopen_source", reject_native)
    if source == "direct":
        path = "zstar://x"
        options = {
            "fo": "s3://bucket/archive.zstar",
            "target_options": {"data": data, "credential": credential},
        }
    else:
        path = "zstar://x::s3://bucket/archive.zstar"
        options = (
            {"zstar": {"fs": backend}}
            if source == "injected"
            else {"s3": {"data": data, "credential": credential}}
        )
    with fsspec.open(path, **options) as member:
        assert member.read() == b"hello"
    if source != "injected":
        backend = MockS3.instances[-1]
    assert backend.credential is credential
    assert backend.opened[0][0] == "bucket/archive.zstar"


def test_bulk_helpers_and_batched_ranges(tmp_path, monkeypatch):
    output = io.BytesIO()
    with zstar.open(output, options=WriterOptions(), mode="w") as writer:
        writer.writestr("a.txt", b"hello")
        writer.writestr("b.txt", b"world")
    batches = []
    original = zstar.AsyncArchive.read_many

    async def read_many(self, requests, **kwargs):
        batches.append(requests)
        return await original(self, requests, **kwargs)

    monkeypatch.setattr(zstar.AsyncArchive, "read_many", read_many)
    with ZstarFileSystem(fo=io.BytesIO(output.getvalue())) as fs:
        assert fs.cat_ranges(["a.txt", "b.txt"], 1, 4) == [b"ell", b"orl"]
        assert len(batches) == 1 and len(batches[0]) == 2
        assert fs.cat_ranges(["a.txt", "a.txt"], [4, 99], [2, None]) == [b"", b""]
        assert len(batches) == 1
        assert fs.cat_ranges(["a.txt", "b.txt"], [-2, 9], [None, 20]) == [b"lo", b""]
        values = fs.cat_ranges(["missing", "a.txt"], 0, 2)
        assert isinstance(values[0], FileNotFoundError) and values[1] == b"he"
        with pytest.raises(FileNotFoundError):
            fs.cat_ranges(["missing"], 0, 2, on_error="raise")
        assert fs.cat(["a.txt", "b.txt"]) == {"a.txt": b"hello", "b.txt": b"world"}
        assert fs.find("") == ["a.txt", "b.txt"]
        destination = tmp_path / "copy.txt"
        if os.name == "posix":
            fs.get("a.txt", str(destination))
            assert destination.read_bytes() == b"hello"
        else:
            with pytest.raises(NotImplementedError, match="descriptor-relative no-follow I/O"):
                fs.get("a.txt", str(destination))
            assert not destination.exists()
        assert dict(fs.get_mapper())["b.txt"] == b"world"


def test_reader_options_apply_to_fsspec():
    with ZstarFileSystem(
        fo=io.BytesIO(pack_file("x", b"hello")),
        reader_options=zstar.AsyncReaderOptions(max_read_bytes=2, concurrent_chunks=1),
    ) as fs:
        assert fs.cat_file("x") == b"hello"  # Streaming reads obey the per-read budget.
        assert fs.cat_ranges(["x"], 0, 2) == [b"he"]
        assert fs.cat_ranges(["x"], 0, 5) == [b"hello"]


@pytest.mark.parametrize("setting", ["reader_options", "writer_options"])
def test_fsspec_rejects_unknown_policy_keys(setting):
    with pytest.raises(TypeError, match="unknown option"):
        ZstarFileSystem(fo=io.BytesIO(), **{setting: {"typo": 1}})


def test_get_file_borrowed_output_and_callbacks():
    from fsspec.callbacks import Callback

    data = b"hello world"
    with ZstarFileSystem(fo=io.BytesIO(pack_file("x", data))) as fs:
        target = io.BytesIO()
        callback = Callback()
        fs.get_file("x", outfile=target, callback=callback)
        assert target.getvalue() == data
        assert callback.size == len(data)


def test_async_batched_ranges():
    async def run():
        async with ZstarFileSystem(
            fo=io.BytesIO(pack_file("x", b"hello")), asynchronous=True
        ) as fs:
            assert await fs._cat_ranges(["x", "x"], [0, 2], [2, None]) == [b"he", b"llo"]

    asyncio.run(run())


@pytest.mark.skipif(os.name != "posix", reason="Descriptor-relative local paths")
@pytest.mark.parametrize("parent_link", [False, True])
def test_download_rejects_destination_symlinks(tmp_path, parent_link):
    output = io.BytesIO()
    with zstar.open(output, options=WriterOptions(), mode="w") as writer:
        writer.writestr("file", b"replacement")
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "file"
    victim.write_bytes(b"original")
    destination = tmp_path / "destination"
    if parent_link:
        destination.symlink_to(outside, target_is_directory=True)
    else:
        destination.mkdir()
        (destination / "file").symlink_to(victim)
    with ZstarFileSystem(io.BytesIO(output.getvalue())) as fs, pytest.raises(OSError):
        fs.get_file("file", destination / "file")
    assert victim.read_bytes() == b"original"


@pytest.mark.skipif(os.name != "posix", reason="Descriptor-relative local paths")
def test_download_parent_replacement_cannot_redirect_write(tmp_path, monkeypatch):
    from zstar import _fsspec

    output = io.BytesIO()
    with zstar.open(output, options=WriterOptions(), mode="w") as writer:
        writer.writestr("file", b"replacement")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "file").write_bytes(b"original")
    destination = tmp_path / "destination"
    destination.mkdir()
    parked = tmp_path / "parked"
    original_open = _fsspec.os.open
    replaced = False

    def swap_before_leaf(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal replaced
        if path == "file" and dir_fd is not None and not replaced:
            replaced = True
            destination.rename(parked)
            destination.symlink_to(outside, target_is_directory=True)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(_fsspec.os, "open", swap_before_leaf)
    with ZstarFileSystem(io.BytesIO(output.getvalue())) as fs:
        fs.get_file("file", destination / "file")
    assert replaced
    assert (outside / "file").read_bytes() == b"original"
    assert (parked / "file").read_bytes() == b"replacement"
