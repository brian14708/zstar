"""Archive-relative fsspec paths, sparse reads, snapshots, and ownership."""

import asyncio
import gc
import io
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import ClassVar

import fsspec
import pytest
import zstar


def test_filesystem_reads_seeks_and_lists():
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(zstar.pack_file("hello.txt", b"hello world")))
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
    path.write_bytes(zstar.pack_directory(root))
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
    fs = zstar.ZstarFileSystem(fo=path)
    try:
        with fs.open("dir/file.txt") as handle:
            assert handle.read() == b"hello"
    finally:
        fs.close()


def test_filesystems_are_not_instance_cached(tmp_path):
    path = tmp_path / "test.zstar"
    path.write_bytes(zstar.pack_file("x", b"hello"))
    first = fsspec.filesystem("zstar", fo=path)
    second = fsspec.filesystem("zstar", fo=path)
    assert first is not second
    first.close()
    try:
        with second.open("x") as handle:
            assert handle.read() == b"hello"
    finally:
        second.close()


@pytest.mark.parametrize("cache_type, expected", [(None, 12), ("readahead", 5242892)])
def test_sparse_reads_avoid_unrequested_logical_bytes(monkeypatch, cache_type, expected):
    # Observe member ranges, independently of compression ratio or frame size.
    from zstar.fsspec import _MemberFile

    ranges = []
    original = _MemberFile._fetch_range

    def fetch(self, start, end):
        ranges.append((start, end))
        return original(self, start, end)

    monkeypatch.setattr(_MemberFile, "_fetch_range", fetch)
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(zstar.pack_file("x", b"x" * (16 << 20))))
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
    with zstar.ArchiveWriter(sink) as writer:
        info = zstar.TarInfo("x")
        info.size = 1
        info.mtime_ns = nanoseconds
        writer.addfile(info, io.BytesIO(b"x"))
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(sink.getvalue()))
    try:
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        assert fs.modified("/x") == epoch + timedelta(microseconds=nanoseconds // 1000)
        assert fs.modified("") == epoch
        with pytest.raises(FileNotFoundError):
            fs.modified("missing")
    finally:
        fs.close()


def test_close_owns_streams_and_is_terminal():
    source = io.BytesIO(zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(fo=source)
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
    zstar.ZstarFileSystem(fo=unopened).close()
    assert unopened.closed


def test_cleanup_errors_preserve_primary_failure():
    class FailingClose(io.BytesIO):
        def close(self):
            super().close()
            raise OSError("source cleanup failed")

    fs = zstar.ZstarFileSystem(fo=FailingClose(zstar.pack_file("x", b"hello")))
    assert fs.info("x")["size"] == 5
    with pytest.raises(OSError, match="source cleanup failed"):
        fs.close()
    fs.close()
    fs = zstar.ZstarFileSystem(fo=FailingClose(b"bad"))
    with pytest.raises(ExceptionGroup) as caught:
        fs.info("x")
    assert isinstance(caught.value.exceptions[0], zstar.ZstarError)
    assert isinstance(caught.value.exceptions[1], OSError)
    fs.close()


@pytest.mark.parametrize("loaded", [False, True])
def test_dropping_filesystem_releases_owned_source(loaded):
    source = io.BytesIO(zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(fo=source)
    if loaded:
        assert fs.info("x")["size"] == 5
    del fs
    gc.collect()
    assert source.closed


def test_close_drains_admitted_loads_and_rejects_new_work(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    source = io.BytesIO(zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(fo=source)
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
            while not fs._closed:
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


def test_external_members_expose_logical_bytes(thin_data, object_resolver):
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(thin_data), resolver=object_resolver)
    try:
        assert fs.info("hello.txt")["size"] == 6
        with fs.open("hello.txt", block_size=3) as member:
            assert member.read(2) == b"he"
            member.seek(1)
            assert member.read() == b"ello\n"
    finally:
        fs.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Windows cannot replace an open Python file")
def test_local_snapshot_survives_atomic_path_replacement(tmp_path):
    path = tmp_path / "snapshot.zstar"
    path.write_bytes(zstar.pack_file("x", b"AAAAAA"))
    fs = zstar.ZstarFileSystem(fo=path, cache_bytes=0)
    try:
        with fs.open("x", block_size=1) as member:
            assert member.read(6) == b"AAAAAA"
            replacement = tmp_path / "new.zstar"
            replacement.write_bytes(zstar.pack_file("x", b"BBBBBB"))
            replacement.replace(path)
            member.seek(0)
            assert member.read(6) == b"AAAAAA"
    finally:
        fs.close()


def test_directory_symlink_listing_preserves_alias(tmp_path):
    root = tmp_path / "input"
    (root / "dir" / "nested").mkdir(parents=True)
    (root / "dir" / "x").write_bytes(b"hello")
    (root / "link").symlink_to("dir", target_is_directory=True)
    fs = zstar.ZstarFileSystem(fo=io.BytesIO(zstar.pack_directory(root)))
    try:
        assert fs.info("link")["type"] == "directory"
        assert fs.ls("link", detail=False) == ["link/nested", "link/x"]
        assert fs.info("link/x")["size"] == 5
        assert fs.modified("link/x") == fs.modified("dir/x")
        with fs.open("link/x") as member:
            assert member.read() == b"hello"
        assert fs.ls("link/nested") == []
        with pytest.raises(FileNotFoundError):
            fs.open("../dir/x")
    finally:
        fs.close()


def test_fsspec_is_loaded_only_when_requested():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys, zstar; assert 'fsspec' not in sys.modules; "
            "from zstar import ZstarFileSystem; assert 'fsspec' in sys.modules",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_generic_memory_backend_supports_range_reads():
    backend = fsspec.filesystem("memory")
    backend.pipe("zstar-test/archive", zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(fo="memory://zstar-test/archive", fs=backend)
    try:
        with fs.open("x") as handle:
            assert handle.read() == b"hello"
    finally:
        fs.close()
        backend.rm("zstar-test/archive")


def test_backend_options_survive_chaining(tmp_path, monkeypatch):
    import fsspec.core

    path = tmp_path / "archive.zstar"
    path.write_bytes(zstar.pack_file("x", b"hello"))
    original = fsspec.filesystem
    received = []

    def capture(protocol, **options):
        received.append((protocol, options))
        return original(protocol, **options)

    monkeypatch.setattr(fsspec, "filesystem", capture)
    fs, member = fsspec.core.url_to_fs(
        f"zstar://x::{path.as_uri()}", file={"auto_mkdir": True}, zstar={"cache_bytes": 1024}
    )
    try:
        assert fs.info(member)["size"] == 5
        assert fs.cache_bytes == 1024
        assert received == [("file", {"auto_mkdir": True})]
    finally:
        fs.close()


@pytest.mark.parametrize("mode", ["w", "a", "rb"])
def test_constructor_rejects_non_read_modes(mode):
    with pytest.raises(ValueError, match="read-only"):
        zstar.ZstarFileSystem(fo="unused.zstar", mode=mode)


@pytest.mark.parametrize(
    "location, protocol",
    [("s3://bucket/nested/archive.zstar", None)],
)
def test_s3_uses_native_reader_without_s3fs(monkeypatch, location, protocol):
    calls = []
    data = zstar.pack_file("x", b"hello")

    async def from_s3(bucket, key, **options):
        calls.append((bucket, key, options))
        return await zstar.AsyncArchive.from_bytes(data)

    def reject_backend(*args, **kwargs):
        raise AssertionError("native S3 must not construct an fsspec S3 backend")

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", from_s3)
    monkeypatch.setattr(fsspec, "filesystem", reject_backend)
    monkeypatch.setattr(fsspec.core, "url_to_fs", reject_backend)
    fs = zstar.ZstarFileSystem(
        fo=location,
        target_protocol=protocol,
        target_options={"endpoint": "http://localhost:9000", "region": "us-east-1", "anon": True},
        cache_bytes=1024,
    )
    try:
        with fs.open("x") as handle:
            assert handle.read() == b"hello"
        assert calls == [
            (
                "bucket",
                "nested/archive.zstar",
                {
                    "endpoint": "http://localhost:9000",
                    "region": "us-east-1",
                    "object_prefix": None,
                    "anon": True,
                    "access_key_id": None,
                    "secret_access_key": None,
                    "session_token": None,
                    "cache_bytes": 1024,
                    "verify_external": True,
                    "resolver": None,
                },
            )
        ]
    finally:
        fs.close()


@pytest.mark.parametrize("location", ["s3://", "s3://bucket", "s3://bucket/"])
def test_s3_rejects_missing_bucket_or_key(location):
    fs = zstar.ZstarFileSystem(fo=location)
    try:
        with pytest.raises(ValueError, match="S3 archive path"):
            fs.info("x")
    finally:
        fs.close()


def test_s3_rejects_unsupported_options_before_network_io():
    fs = zstar.ZstarFileSystem(fo="s3://bucket/archive", target_options={"session": object()})
    try:
        with pytest.raises(ValueError, match="native S3 target_options"):
            fs.info("x")
    finally:
        fs.close()


@pytest.mark.parametrize("backend", ["native", "s3fs"])
def test_s3_rejects_removed_backend_switch(backend):
    with pytest.raises(TypeError, match="zstar chaining"):
        zstar.ZstarFileSystem(fo="s3://bucket/archive", s3_backend=backend)


def test_native_s3_accepts_fsspec_credentials_and_client_options(monkeypatch):
    calls = []

    async def from_s3(bucket, key, **options):
        calls.append(options)
        return await zstar.AsyncArchive.from_bytes(zstar.pack_file("x", b"hello"))

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", from_s3)
    fs = zstar.ZstarFileSystem(
        fo="s3://bucket/archive",
        target_options={
            "key": "test-key",
            "secret": "test-secret",
            "token": "test-token",
            "client_kwargs": {"endpoint_url": "http://localhost:9000", "region_name": "us-east-1"},
        },
    )
    try:
        assert fs.info("x")["size"] == 5
        assert calls[0]["access_key_id"] == "test-key"
        assert calls[0]["secret_access_key"] == "test-secret"
        assert calls[0]["session_token"] == "test-token"
        assert calls[0]["endpoint"] == "http://localhost:9000"
        assert calls[0]["region"] == "us-east-1"
    finally:
        fs.close()


@pytest.mark.parametrize(
    "options",
    [
        {"client_kwargs": {"api_version": "bad"}},
        {"client_kwargs": None},
        {"endpoint": "http://a", "client_kwargs": {"endpoint_url": "http://b"}},
        {"region": "a", "client_kwargs": {"region_name": "b"}},
    ],
)
def test_native_s3_rejects_unsupported_or_conflicting_client_options(options):
    fs = zstar.ZstarFileSystem(fo="s3://bucket/archive", target_options=options)
    try:
        with pytest.raises(ValueError):
            fs.info("x")
    finally:
        fs.close()


def test_async_filesystem_metadata_ranges_and_member_handles(tmp_path):
    root = tmp_path / "input"
    (root / "dir").mkdir(parents=True)
    (root / "dir" / "x").write_bytes(b"hello world")
    data = zstar.pack_directory(root)

    async def exercise():
        from fsspec.asyn import AsyncFileSystem

        source = io.BytesIO(data)
        async with zstar.ZstarFileSystem(fo=source, asynchronous=True) as fs:
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
            with pytest.raises(RuntimeError, match="aclose"):
                fs.close()
        assert source.closed
        await fs.aclose()
        with pytest.raises(ValueError, match="filesystem closed"):
            await fs._info("dir/x")

    asyncio.run(exercise())


def test_cancelled_async_waiter_does_not_cancel_shared_load(monkeypatch):
    async def exercise():
        fs = zstar.ZstarFileSystem(fo=io.BytesIO(zstar.pack_file("x", b"hello")), asynchronous=True)
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
        await fs.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("loaded", [False, True])
def test_dropping_member_on_io_loop_closes_owned_filesystem(loaded):
    from fsspec.asyn import sync

    source = io.BytesIO(zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(fo=source)
    handles = [fs.open("x") if loaded else fs]
    loop = fs.loop
    del fs

    async def drop():
        handles.clear()
        gc.collect()
        while not source.closed:
            await asyncio.sleep(0.001)

    sync(loop, drop, timeout=5)
    assert source.closed


class RangeFile(fsspec.spec.AbstractBufferedFile):
    def __init__(self, fs, path, **options):
        super().__init__(fs, path, size=len(fs.data), **options)

    def _fetch_range(self, start, end):
        self.fs.ranges.append((start, end))
        return self.fs.data[start:end]


class RangeFileSystem(fsspec.spec.AbstractFileSystem):
    protocol = "zstar-test-range"
    cachable = False
    instances: ClassVar[list] = []

    def __init__(self, data, credential=None, **options):
        super().__init__(**options)
        self.data, self.credential = data, credential
        self.ranges, self.opened = [], []
        self.instances.append(self)

    def _open(self, path, mode="rb", **options):
        self.opened.append((path, mode, options.copy()))
        return RangeFile(self, path, mode=mode, **options)


def test_chaining_uses_resolved_filesystem_and_exact_ranges(monkeypatch):
    data = bytes(zstar.pack_file("x", b"hello" * (1 << 20)))
    credential = object()
    fsspec.register_implementation(RangeFileSystem.protocol, RangeFileSystem, clobber=True)

    async def reject_native(*args, **kwargs):
        raise AssertionError("generic chaining must use its fsspec filesystem")

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", reject_native)
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
    assert backend.opened[0][2]["cache_type"] == "none"
    assert backend.ranges
    assert all(end - start < len(data) for start, end in backend.ranges)
    assert sum(end - start for start, end in backend.ranges) < len(data)


def test_injected_filesystem_is_borrowed_and_keeps_credentials():
    backend = RangeFileSystem(bytes(zstar.pack_file("x", b"hello")), credential=object())
    fs = zstar.ZstarFileSystem(fo="archive", fs=backend)
    try:
        assert fs.cat_file("x") == b"hello"
    finally:
        fs.close()
    assert backend.opened[0][2]["cache_type"] == "none"
    # Closing Zstar's stream leaves the caller's filesystem usable.
    with backend.open("archive", cache_type="none") as stream:
        assert stream.read(4) == backend.data[:4]


@pytest.mark.parametrize(
    "url",
    [
        "zstar+s3://",
        "zstar+s3://bucket",
        "zstar+s3://bucket/",
        "zstar+s3://user:secret@bucket/key",
        "zstar+s3://bucket:9000/key",
        "zstar+s3://bucket/key?versionId=1",
        "zstar+s3://bucket/key#x",
    ],
)
def test_native_s3_url_rejects_invalid_locations(url):
    with pytest.raises(ValueError, match="native S3 URL"):
        fsspec.core.url_to_fs(url)
    with pytest.raises(ValueError, match="native S3 URL"):
        fsspec.filesystem("zstar+s3", fo=url)


@pytest.mark.parametrize("fo", ["archive.zstar", "s3://bucket/archive.zstar"])
def test_native_s3_filesystem_requires_native_archive_url(fo):
    with pytest.raises(ValueError, match="native S3 URL"):
        fsspec.filesystem("zstar+s3", fo=fo)


@pytest.mark.parametrize("option", ["profile", "session", "config_kwargs"])
def test_native_s3_filesystem_rejects_fsspec_credential_options(option):
    with pytest.raises(TypeError, match="unsupported native S3 options"):
        fsspec.filesystem("zstar+s3", fo="zstar+s3://bucket/archive.zstar", **{option: "test"})


def test_native_s3_url_routes_options_and_member_without_s3fs(monkeypatch, tmp_path):
    calls = []
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/x").write_bytes(b"hello")
    data = zstar.pack_directory(tmp_path)

    async def from_s3(bucket, key, **options):
        calls.append((bucket, key, options))
        return await zstar.AsyncArchive.from_bytes(data)

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", from_s3)
    monkeypatch.setitem(sys.modules, "s3fs", None)
    with fsspec.open(
        "zstar://nested/x::zstar+s3://bucket/nested/archive%20v1%2520.zstar",
        **{
            "zstar+s3": {
                "endpoint": "http://localhost:9000",
                "region": "us-east-1",
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
                "session_token": "test-token",
            }
        },
    ) as member:
        assert member.read() == b"hello"
    bucket, key, options = calls[0]
    assert (bucket, key) == ("bucket", "nested/archive v1%20.zstar")
    assert options["endpoint"] == "http://localhost:9000"
    assert options["region"] == "us-east-1"
    assert options["access_key_id"] == "test-key"
    assert options["secret_access_key"] == "test-secret"
    assert options["session_token"] == "test-token"


def test_base_and_fsspec_install_do_not_require_s3fs():
    from importlib.metadata import requires

    from packaging.requirements import Requirement

    dependencies = [Requirement(value) for value in requires("zstar-py")]
    for extra in ("", "fsspec"):
        active = {
            requirement.name
            for requirement in dependencies
            if requirement.marker is None or requirement.marker.evaluate({"extra": extra})
        }
        assert "s3fs" not in active
    assert all(requirement.name != "s3fs" for requirement in dependencies)


def test_injected_async_filesystem_uses_exact_ranges():
    from fsspec.asyn import AsyncFileSystem

    data = bytes(zstar.pack_file("x", b"hello"))

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
        async with zstar.ZstarFileSystem(fo="archive", fs=backend, asynchronous=True) as fs:
            assert await fs._cat_file("x", start=1, end=4) == b"ell"
        assert backend.ranges
        assert all(end - start < len(data) for start, end in backend.ranges)
        assert await backend._cat_file("archive", start=0, end=4) == data[:4]

    asyncio.run(exercise())


def test_native_s3_filesystem_uses_member_paths(monkeypatch, tmp_path):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/readme.txt").write_bytes(b"hello")
    data = zstar.pack_directory(tmp_path)

    async def from_s3(bucket, key, **options):
        assert (bucket, key) == ("bucket", "archive.zstar")
        assert options["region"] == "us-east-1"
        return await zstar.AsyncArchive.from_bytes(data)

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", from_s3)
    fs = fsspec.filesystem("zstar+s3", fo="zstar+s3://bucket/archive.zstar", region="us-east-1")
    try:
        with fs.open("docs/readme.txt") as member:
            assert member.read() == b"hello"
        assert fs.exists("docs/readme.txt")
        with pytest.raises(IsADirectoryError):
            fs.open("")
    finally:
        fs.close()
    with pytest.raises(TypeError, match=r"fs\.open"):
        fsspec.filesystem(
            "zstar+s3", fo="zstar+s3://bucket/archive.zstar", member="docs/readme.txt"
        )


def test_generic_s3_chain_uses_injected_fsspec_filesystem(monkeypatch):
    from importlib import import_module

    class MockS3(RangeFileSystem):
        protocol = "s3"

    monkeypatch.setitem(import_module("fsspec.registry")._registry, "s3", MockS3)
    backend = MockS3(bytes(zstar.pack_file("x", b"hello")), credential=object())

    async def reject_native(*args, **kwargs):
        raise AssertionError("generic S3 chaining must use the supplied filesystem")

    monkeypatch.setattr(zstar.AsyncArchive, "from_s3", reject_native)
    with fsspec.open("zstar://x::s3://bucket/archive.zstar", zstar={"fs": backend}) as member:
        assert member.read() == b"hello"
    assert backend.opened[0][0] == "bucket/archive.zstar"
    assert backend.opened[0][2]["cache_type"] == "none"
