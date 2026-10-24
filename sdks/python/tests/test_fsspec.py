"""Optional fsspec integration, member ownership, and archive eviction."""

import io
import subprocess
import sys

import pytest
import zstar


def test_filesystem_reads_seeks_and_lists():
    archive = zstar.pack_file("hello.txt", b"hello world")
    fs = zstar.ZstarFileSystem(opener=lambda path: io.BytesIO(archive), skip_instance_cache=True)
    assert fs.ls("example.zstar!/") == [
        {"name": "example.zstar!/hello.txt", "size": 11, "type": "file"}
    ]
    assert fs.exists("example.zstar!/hello.txt")
    assert not fs.exists("example.zstar!/missing")
    with fs.open("example.zstar!/hello.txt", block_size=3) as member:
        assert member.read(5) == b"hello"
        assert member.seek(-5, 2) == 6
        assert member.read() == b"world"
    with pytest.raises(NotImplementedError):
        fs.open("example.zstar!/hello.txt", "wb")


def test_filesystem_default_local_backend(tmp_path):
    path = tmp_path / "test.zstar"
    path.write_bytes(zstar.pack_file("x", b"hello"))
    fs = zstar.ZstarFileSystem(skip_instance_cache=True)
    with fs.open(f"{path}!/x") as member:
        assert member.read() == b"hello"
    fs.close()


def test_filesystem_nested_listing_preserves_paths_and_member_types(tmp_path):
    root = tmp_path / "input"
    (root / "dir" / "nested").mkdir(parents=True)
    (root / "dir" / "file").write_bytes(b"hello")
    (root / "dir" / "nested" / "file").write_bytes(b"hidden")
    fs = zstar.ZstarFileSystem(
        opener=lambda _: zstar.pack_directory(root), skip_instance_cache=True
    )
    try:
        assert fs.ls("archive!/dir") == [
            {"name": "archive!/dir/file", "size": 5, "type": "file"},
            {"name": "archive!/dir/nested", "size": 0, "type": "directory"},
        ]
        assert fs.ls("archive!/dir/", detail=False) == [
            "archive!/dir/file",
            "archive!/dir/nested",
        ]
        assert fs.ls("archive!/dir/file", detail=False) == ["archive!/dir/file"]
        assert fs.ls("archive!/dir/nested") == [
            {"name": "archive!/dir/nested/file", "size": 6, "type": "file"}
        ]
        with pytest.raises(FileNotFoundError):
            fs.ls("archive!/missing")
    finally:
        fs.close()


def test_registered_fsspec_and_archive_lru_preserves_open_members():
    import fsspec

    data = bytes(zstar.pack_file("x", b"hello"))
    sources = []

    def opener(path):
        source = io.BytesIO(data)
        sources.append(source)
        return source

    fs = fsspec.filesystem(
        "zstar",
        opener=opener,
        max_open_archives=1,
        cache_bytes=65536,
        skip_instance_cache=True,
    )
    try:
        member = fs.open("a!/x")
        assert member.read(2) == b"he"
        with pytest.raises(OSError, match="open members"):
            fs.info("b!/x")
        assert not sources[0].closed
        assert member.read() == b"llo"
        member.close()
        assert fs.info("b!/x")["size"] == 5
        assert sources[0].closed
        assert len(fs._archives) == 1
        with fs.open("b!/x") as member:
            assert member.read() == b"hello"
    finally:
        fs.close()
    assert all(source.closed for source in sources)


def test_fsspec_close_releases_retained_readers_and_preserves_cleanup_errors():
    data = zstar.pack_file("x", b"hello")
    sources = []

    class FailingClose(io.BytesIO):
        def close(self):
            super().close()
            raise OSError("source cleanup failed")

    def opener(path):
        source = FailingClose(data)
        sources.append(source)
        return source

    fs = zstar.ZstarFileSystem(opener=opener, skip_instance_cache=True)
    assert fs.info("a!/x")["size"] == 5
    assert fs.info("b!/x")["size"] == 5
    retained = list(fs._archives.values())
    with pytest.raises(ExceptionGroup) as caught:
        fs.close()
    assert len(caught.value.exceptions) == 2
    assert all(isinstance(error, OSError) for error in caught.value.exceptions)
    assert all(slot.archive.closed for slot in retained)
    assert all(source.closed for source in sources)
    assert fs._archives == {}
    fs.close()

    fs = zstar.ZstarFileSystem(opener=lambda _: FailingClose(b"bad"), skip_instance_cache=True)
    with pytest.raises(ExceptionGroup) as caught:
        fs.info("bad!/x")
    assert isinstance(caught.value.exceptions[0], zstar.ZstarError)
    assert isinstance(caught.value.exceptions[1], OSError)


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


@pytest.mark.parametrize("stream", [False, True])
def test_external_members_expose_logical_bytes(thin_data, object_resolver, stream):
    def opener(path):
        return io.BytesIO(thin_data) if stream else thin_data

    fs = zstar.ZstarFileSystem(opener=opener, resolver=object_resolver, skip_instance_cache=True)
    try:
        assert fs.info("thin!/hello.txt")["size"] == 6
        with fs.open("thin!/hello.txt", block_size=3) as member:
            assert member.read(2) == b"he"
            member.seek(1)
            assert member.read() == b"ello\n"
    finally:
        fs.close()


def test_local_snapshot_survives_atomic_path_replacement(tmp_path):
    path = tmp_path / "snapshot.zstar"
    path.write_bytes(zstar.pack_file("x", b"AAAAAA"))
    fs = zstar.ZstarFileSystem(cache_bytes=0, skip_instance_cache=True)
    try:
        with fs.open(f"{path}!/x", cache_type="none", block_size=1) as member:
            assert member.read(6) == b"AAAAAA"
            replacement = tmp_path / "new.zstar"
            replacement.write_bytes(zstar.pack_file("x", b"BBBBBB"))
            replacement.replace(path)
            member.seek(0)
            assert member.read(6) == b"AAAAAA"
    finally:
        fs.close()


def test_eviction_cleanup_failure_releases_new_pin():
    data = zstar.pack_file("x", b"hello")

    class FailingClose(io.BytesIO):
        def close(self):
            super().close()
            raise OSError("eviction cleanup failed")

    fs = zstar.ZstarFileSystem(
        opener=lambda path: FailingClose(data) if path == "a" else io.BytesIO(data),
        max_open_archives=1,
        skip_instance_cache=True,
    )
    try:
        fs.info("a!/x")
        with pytest.raises(OSError, match="eviction cleanup"):
            fs.info("b!/x")
        assert fs._archives["b"].pins == 0
        assert fs.info("c!/x")["size"] == 5
    finally:
        fs.close()


def test_close_drains_admitted_loads_and_rejects_new_work():
    import threading
    from concurrent.futures import ThreadPoolExecutor

    data = zstar.pack_file("x", b"hello")
    entered, release = threading.Event(), threading.Event()
    sources = []

    def opener(path):
        entered.set()
        assert release.wait(5)
        source = io.BytesIO(data)
        sources.append(source)
        return source

    fs = zstar.ZstarFileSystem(opener=opener, skip_instance_cache=True)
    with ThreadPoolExecutor(max_workers=3) as pool:
        load = pool.submit(fs.info, "a!/x")
        assert entered.wait(5)
        closing = pool.submit(fs.close)
        # Synchronize with terminal state rather than relying on a sleep.
        with fs._condition:
            assert fs._condition.wait_for(lambda: fs._closed, timeout=5)
        assert not closing.done()
        with pytest.raises(ValueError, match="filesystem closed"):
            fs.info("b!/x")
        joined_close = pool.submit(fs.close)
        release.set()
        with pytest.raises(ValueError, match="filesystem closed"):
            load.result(timeout=5)
        closing.result(timeout=5)
        joined_close.result(timeout=5)
    assert sources and all(source.closed for source in sources)
    assert not fs._archives and not fs._loading


def test_directory_symlink_listing_preserves_alias(tmp_path):
    root = tmp_path / "input"
    (root / "dir" / "nested").mkdir(parents=True)
    (root / "dir" / "x").write_bytes(b"hello")
    (root / "link").symlink_to("dir", target_is_directory=True)
    data = zstar.pack_directory(root)
    fs = zstar.ZstarFileSystem(opener=lambda _: data, skip_instance_cache=True)
    try:
        assert fs.info("a!/link")["type"] == "directory"
        assert fs.ls("a!/link", detail=False) == ["a!/link/nested", "a!/link/x"]
        assert fs.ls("a!/link") == [
            {"name": "a!/link/nested", "size": 0, "type": "directory"},
            {"name": "a!/link/x", "size": 5, "type": "file"},
        ]
        assert fs.info("a!/link/x")["size"] == 5
        with fs.open("a!/link/x") as member:
            assert member.read() == b"hello"
        assert fs.ls("a!/link/nested") == []
    finally:
        fs.close()


@pytest.mark.parametrize(
    "info, condition",
    [
        ({"VersionId": "immutable", "ETag": '"tag"'}, {"version_id": "immutable"}),
        ({"VersionId": "null", "ETag": '"tag"'}, {"IfMatch": '"tag"'}),
    ],
)
def test_s3_fsspec_ranges_carry_snapshot_condition(info, condition):
    from zstar.fsspec import _FsspecSource

    class Backend:
        def info(self, path):
            return {"size": 6, **info}

        def open(self, path, mode, **kwargs):
            class PinnedFile(io.BytesIO):
                def __init__(self, data):
                    super().__init__(data)
                    self.read_kw = {"IfMatch": '"tag"'}

            assert mode == "rb" and kwargs == {"cache_type": "none", "block_size": 0}
            return PinnedFile(b"hello!")

        def cat_file(self, path, *, start, end, **kwargs):
            assert path == "bucket/key" and (start, end) == (1, 4)
            assert kwargs == condition
            return b"ell"

    source = _FsspecSource(Backend(), "bucket/key")
    assert source.read_range(1, 3) == b"ell"
    if source.file is not None:
        source.file.close()


def test_unsupported_backend_requires_immutable_opener():
    fs = zstar.ZstarFileSystem(skip_instance_cache=True)
    try:
        with pytest.raises(OSError, match="immutable archive snapshot"):
            fs.info("memory://snapshot!/x")
    finally:
        fs.close()


@pytest.mark.parametrize("tag", [None, 'W/"weak"', "unquoted"])
def test_s3_source_rejects_missing_or_invalid_identity(tag):
    from zstar.fsspec import _FsspecSource

    class Backend:
        def info(self, path):
            return {"size": 6, "ETag": tag}

    with pytest.raises(OSError, match="immutable version or ETag"):
        _FsspecSource(Backend(), "bucket/key")


def test_s3_source_rejects_disabled_conditions_and_closes_handle():
    from zstar.fsspec import _FsspecSource

    file = io.BytesIO(b"hello")

    class Backend:
        def info(self, path):
            return {"size": 5, "ETag": '"tag"'}

        def open(self, *args, **kwargs):
            return file  # No S3File read_kw proving the enforced condition.

    with pytest.raises(OSError, match="enforce the pinned ETag"):
        _FsspecSource(Backend(), "bucket/key")
    assert file.closed


def test_pending_distinct_loads_are_bounded(monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    entered, release, waited = threading.Event(), threading.Event(), threading.Event()
    data = zstar.pack_file("x", b"hello")
    calls = []

    def opener(path):
        calls.append(path)
        if path == "a":
            entered.set()
            assert release.wait(5)
        return data

    fs = zstar.ZstarFileSystem(opener=opener, max_open_archives=1, skip_instance_cache=True)
    wait = fs._condition.wait

    def observed_wait(*args, **kwargs):
        waited.set()
        return wait(*args, **kwargs)

    monkeypatch.setattr(fs._condition, "wait", observed_wait)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(fs.info, "a!/x")
            assert entered.wait(5)
            second = pool.submit(fs.info, "b!/x")
            try:
                assert waited.wait(5)
                assert calls == ["a"]
                with fs._lock:
                    assert len(fs._loading) == 1
            finally:
                release.set()
            assert first.result(timeout=5)["size"] == 5
            try:
                assert second.result(timeout=5)["size"] == 5
            except OSError as error:
                # The first lease can still be pinned during capacity admission.
                assert "open members" in str(error)
            assert fs.info("b!/x")["size"] == 5
    finally:
        release.set()
        fs.close()


def test_backend_options_survive_fsspec_instance_construction(tmp_path, monkeypatch):
    import fsspec.core

    data = zstar.pack_file("x", b"hello")
    path = tmp_path / "archive.zstar"
    path.write_bytes(data)
    original = fsspec.core.url_to_fs
    received = []

    def capture(path, **options):
        received.append(options)
        return original(path, **options)

    monkeypatch.setattr(fsspec.core, "url_to_fs", capture)
    fs = zstar.ZstarFileSystem(storage_options={"auto_mkdir": True}, skip_instance_cache=True)
    try:
        assert fs.info(f"{path}!/x")["size"] == 5
        assert received == [{"auto_mkdir": True}]
    finally:
        fs.close()
