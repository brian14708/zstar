"""Archive creation through standard fsspec filesystem operations."""

import asyncio
import io
import threading
from concurrent.futures import ThreadPoolExecutor

import fsspec
import pytest
import zstar
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractBufferedFile
from zstar import ZstarFileSystem


def test_write_text_seek_directories_and_round_trip(tmp_path):
    archive = tmp_path / "output.zstar"
    with fsspec.filesystem("zstar", fo=archive, mode="w", member_spool_bytes=4) as fs:
        fs.makedirs("empty")
        with fs.open("nested/data.txt", "wt", encoding="utf-8") as member:
            member.write("h\u00e9llo")
        with fs.open("seek.bin", "wb") as member:
            assert member.write(b"hello") == 5
            assert member.seek(1) == 1
            assert member.write(b"a") == 1
            assert member.tell() == 2
        assert fs.ls("nested", detail=False) == ["nested/data.txt"]
        assert fs.info("seek.bin")["size"] == 5
        assert fs.isdir("empty")
        with pytest.raises(PermissionError):
            fs.cat_file("seek.bin")
    assert member.closed
    with ZstarFileSystem(fo=archive) as fs:
        assert fs.cat_file("nested/data.txt") == "h\u00e9llo".encode()
        assert fs.cat_file("seek.bin") == b"hallo"
    with zstar.Archive.from_file(archive) as reader:
        assert reader.verify()["full_archive_verified"]
        assert list(reader.names()) == ["empty", "nested", "nested/data.txt", "seek.bin"]


def test_empty_archive_close_and_closed_operations(tmp_path):
    path = tmp_path / "empty.zstar"
    fs = ZstarFileSystem(fo=path, mode="w")
    fs.close()
    fs.close()
    with zstar.Archive.from_file(path) as reader:
        assert list(reader.names()) == []
    with pytest.raises(ValueError, match="closed"):
        fs.open("x", "wb")
    with pytest.raises(ValueError, match="closed"):
        fs.makedirs("x")


def test_creation_rules_and_single_active_member(tmp_path):
    with ZstarFileSystem(fo=tmp_path / "archive", mode="w") as fs:
        with fs.open("x", "wb") as member:
            member.write(b"x")
            with pytest.raises(RuntimeError, match="active member"):
                fs.open("other", "wb")
        with pytest.raises(FileExistsError):
            fs.open("x", "wb")
        with pytest.raises(NotADirectoryError):
            fs.open("x/child", "wb")
        fs.makedirs("dir")
        with pytest.raises(FileExistsError):
            fs.makedirs("dir")
        fs.makedirs("dir", exist_ok=True)
        with pytest.raises(IsADirectoryError):
            fs.open("dir", "wb")
        with pytest.raises(FileNotFoundError):
            fs.mkdir("missing/child", create_parents=False)
        with pytest.raises(NotImplementedError):
            fs.open("append", "ab")
        for name in ["../escape", "a/../escape", "a\\b", "a\x00b"]:
            with pytest.raises(ValueError):
                fs.open(name, "wb")


def test_put_recursive_uses_standard_filesystem_operations(tmp_path):
    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    (source / "a.txt").write_bytes(b"a")
    (source / "nested/b.txt").write_bytes(b"b")
    with ZstarFileSystem(fo=tmp_path / "archive", mode="w") as fs:
        fs.put(str(source) + "/", "data", recursive=True)
    with zstar.Archive.from_file(tmp_path / "archive") as reader:
        assert reader.read("data/a.txt") == b"a"
        assert reader.read("data/nested/b.txt") == b"b"


def test_bulk_write_modes_and_protocol_paths(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"local")
    path = tmp_path / "archive"
    with ZstarFileSystem(fo=path, mode="w") as fs:
        fs.mkdir("zstar://empty")
        fs.makedirs("zstar://nested/empty")
        fs.put_file(str(source), "zstar://dir/local", mode="create")
        fs.pipe_file("zstar://dir/bytes", b"bytes", mode="create")
        for mode in ("create", "overwrite"):
            with pytest.raises(FileExistsError):
                fs.put_file(str(source), "dir/local", mode=mode)
            with pytest.raises(FileExistsError):
                fs.pipe_file("dir/bytes", b"replacement", mode=mode)
        for operation, args in ((fs.put_file, (str(source), "bad")), (fs.pipe_file, ("bad", b"x"))):
            with pytest.raises(NotImplementedError):
                operation(*args, mode="append")
    with zstar.Archive.from_file(path) as archive:
        assert list(archive.names()) == [
            "dir",
            "dir/bytes",
            "dir/local",
            "empty",
            "nested",
            "nested/empty",
        ]
        assert archive.read("dir/local") == b"local"


def test_backend_commit_failure_discards_uncommitted_output(tmp_path):
    events = []

    class Backend(LocalFileSystem):
        def _open(self, *args, **kwargs):
            output = super()._open(*args, **kwargs)
            discard = output.discard

            def commit_output():
                events.append("commit")
                raise OSError("commit failed")

            def discard_output():
                events.append("discard")
                discard()

            output.commit, output.discard = commit_output, discard_output
            return output

    path = tmp_path / "archive"
    path.write_bytes(b"original")
    with (
        pytest.raises(OSError, match="commit failed"),
        ZstarFileSystem(fo=path, fs=Backend(skip_instance_cache=True), mode="w") as fs,
    ):
        fs.pipe_file("data", b"hello")
    assert events == ["commit", "discard"]
    assert path.read_bytes() == b"original"


def test_abort_preserves_existing_archive_and_discards_staging(tmp_path):
    path = tmp_path / "archive"
    path.write_bytes(b"original")
    with pytest.raises(RuntimeError, match="abort"), ZstarFileSystem(fo=path, mode="w") as fs:
        with fs.open("x", "wb") as member:
            member.write(b"partial")
        raise RuntimeError("abort")
    assert path.read_bytes() == b"original"

    with (
        pytest.raises(RuntimeError, match="open writable member"),
        ZstarFileSystem(fo=path, mode="w") as fs,
    ):
        member = fs.open("x", "wb")
        member.write(b"partial")
    assert member.closed
    assert path.read_bytes() == b"original"


def test_abort_closes_local_output_before_discard(tmp_path):
    class WindowsLocalFileSystem(LocalFileSystem):
        def _open(self, *args, **kwargs):
            output = super()._open(*args, **kwargs)
            discard = output.discard

            def discard_output():
                if not output.closed:
                    raise PermissionError("cannot delete an open file")
                discard()

            output.discard = discard_output
            return output

    path = tmp_path / "archive"
    path.write_bytes(b"original")
    with (
        pytest.raises(RuntimeError, match="abort"),
        ZstarFileSystem(
            fo=path, fs=WindowsLocalFileSystem(skip_instance_cache=True), mode="w"
        ) as fs,
    ):
        fs.pipe_file("data", b"partial")
        raise RuntimeError("abort")
    assert path.read_bytes() == b"original"


def test_abort_discards_buffered_output_without_uploading(tmp_path):
    outputs = []

    class BufferedOutput(AbstractBufferedFile):
        def _upload_chunk(self, final=False):
            raise AssertionError("aborted output must not be uploaded")

        def discard(self):
            assert self.closed
            outputs.append(self)

    class BufferedFileSystem(LocalFileSystem):
        def _open(self, path, mode="rb", **kwargs):
            return BufferedOutput(self, path, mode=mode, **kwargs)

    with (
        pytest.raises(RuntimeError, match="abort"),
        ZstarFileSystem(
            fo=tmp_path / "archive", fs=BufferedFileSystem(skip_instance_cache=True), mode="w"
        ) as fs,
    ):
        fs.pipe_file("data", b"partial")
        raise RuntimeError("abort")
    assert len(outputs) == 1


@pytest.mark.parametrize("external", [False, True], ids=["archive", "external-object"])
def test_publication_preserves_commit_and_cleanup_errors(tmp_path, external):
    commit_error = OSError("commit failed")
    cleanup_error = OSError("discard failed")

    class BrokenPublication(LocalFileSystem):
        def _open(self, *args, **kwargs):
            output = super()._open(*args, **kwargs)
            discard = output.discard

            def fail_commit():
                raise commit_error

            def fail_discard():
                discard()
                raise cleanup_error

            output.commit, output.discard = fail_commit, fail_discard
            return output

    backend = BrokenPublication(skip_instance_cache=True)
    path = tmp_path / "archive"
    path.write_bytes(b"original")
    objects = fsspec.filesystem("dir", path=str(tmp_path / "objects"), fs=backend)
    with (
        pytest.raises(zstar.ZstarError if external else ExceptionGroup) as caught,
        ZstarFileSystem(
            fo=path,
            fs=None if external else backend,
            mode="w",
            external=objects if external else None,
            external_threshold=0,
        ) as fs,
    ):
        fs.pipe_file("data", b"hello")
    failure = caught.value.__cause__ if external else caught.value
    assert isinstance(failure, ExceptionGroup)
    assert failure.exceptions == (commit_error, cleanup_error)
    assert path.read_bytes() == b"original"
    assert not list((tmp_path / "objects").glob("*.zst"))


def test_staging_close_failure_still_discards_archive(tmp_path, monkeypatch):
    class BrokenClose(io.BytesIO):
        def close(self):
            if not self.closed:
                super().close()
                raise OSError("spool close failed")

    monkeypatch.setattr(
        "zstar._publication.tempfile.SpooledTemporaryFile", lambda **kwargs: BrokenClose()
    )
    path = tmp_path / "archive"
    path.write_bytes(b"original")
    fs = ZstarFileSystem(fo=path, mode="w")
    member = fs.open("unfinished", "wb")
    member.write(b"partial")
    with pytest.raises(ExceptionGroup) as caught:
        fs.close()
    assert any(isinstance(error, OSError) for error in caught.value.exceptions)
    assert member.closed
    assert path.read_bytes() == b"original"


def test_member_failure_cannot_be_committed(tmp_path):
    path = tmp_path / "archive"
    with (
        pytest.raises(ValueError, match="writer failed"),
        ZstarFileSystem(fo=path, mode="w") as fs,
        pytest.raises(RuntimeError),
        fs.open("x", "wb"),
    ):
        raise RuntimeError("member failed")
    assert not path.exists()


def test_memory_backend_and_writer_options():
    backend = fsspec.filesystem("memory")
    with ZstarFileSystem(
        fo="/writer-test/output.zstar", fs=backend, mode="w", writer_options={"frame_size": 1 << 16}
    ) as fs:
        fs.pipe_file("dir/x", b"hello world")
    with ZstarFileSystem(fo="/writer-test/output.zstar", fs=backend) as fs:
        assert fs.cat_file("dir/x") == b"hello world"
    # Caller-owned filesystems remain available.
    assert backend.exists("/writer-test/output.zstar")


def test_external_write_and_readback(tmp_path):
    archive_path = tmp_path / "external.zstar"
    objects_root = tmp_path / "objects"
    objects_root.mkdir()
    objects = fsspec.filesystem("dir", path=str(objects_root), fs=fsspec.filesystem("file"))
    payload = b"z" * (1 << 20)
    with ZstarFileSystem(
        fo=archive_path, mode="w", external=objects, external_threshold=1 << 20
    ) as fs:
        fs.pipe_file("large.bin", payload)
    object_paths = objects.find("")
    assert len(object_paths) == 1
    with ZstarFileSystem(fo=archive_path, external=objects) as fs:
        assert fs.cat_file("large.bin") == payload


@pytest.mark.parametrize("method", ["put", "pipe", "stream"])
def test_external_threshold_mixed_members_and_url(tmp_path, method):
    path, root = tmp_path / "archive", tmp_path / "objects"
    payload = b"hello external" * 100
    with ZstarFileSystem(
        fo=path, mode="w", external=root.as_uri(), external_threshold=len(payload)
    ) as fs:
        fs.mkdir("empty-dir")
        fs.pipe_file("empty", b"")
        fs.pipe_file("small", b"small")
        if method == "put":
            source = tmp_path / "input"
            source.write_bytes(payload)
            fs.put(str(source), "large")
        elif method == "pipe":
            fs.pipe_file("large", payload)
        else:
            with fs.open("large", "wb") as member:
                member.write(payload[:17])
                member.write(payload[17:])
    assert len(list(root.glob("*.zst"))) == 1
    with ZstarFileSystem(fo=path) as fs:
        assert fs.cat_file("small") == b"small"
        assert fs.cat_file("empty") == b""
        with pytest.raises(zstar.ZstarError, match="requires resolver"):
            fs.cat_file("large")
    with ZstarFileSystem(fo=path, external=root.as_uri()) as fs:
        assert fs.cat_file("large") == payload


def test_external_default_threshold_and_multichunk(tmp_path):
    data = b"x" * (32 << 20)
    path, root = tmp_path / "archive", tmp_path / "objects"
    with ZstarFileSystem(fo=path, external=root.as_uri(), mode="w") as fs:
        assert fs.external_threshold == 16 << 20
        fs.pipe_file("large", data)
    assert path.stat().st_size < 1 << 20
    # The two identical 16 MiB chunks share a content-addressed object.
    assert len(list(root.glob("*.zst"))) == 1
    with ZstarFileSystem(fo=path, external=root.as_uri()) as fs:
        assert fs.cat_file("large", start=(16 << 20) - 10, end=(16 << 20) + 10) == b"x" * 20
        assert fs.cat_file("large") == data


def test_external_reuse_does_not_download_or_overwrite(tmp_path, monkeypatch):
    path, root = tmp_path / "archive", tmp_path / "objects"
    payload = b"external data"
    for _ in range(2):
        with ZstarFileSystem(fo=path, external=root.as_uri(), external_threshold=0, mode="w") as fs:
            fs.pipe_file("data", payload)
        object_path = next(root.glob("*.zst"))
        if _ == 0:
            original_mtime = object_path.stat().st_mtime_ns
    assert object_path.stat().st_mtime_ns == original_mtime
    from zstar._fsspec import _ExternalObjects

    def deny_download(*args):
        raise AssertionError("duplicate chunks must not be downloaded")

    monkeypatch.setattr(_ExternalObjects, "get_encoded", deny_download)
    with ZstarFileSystem(fo=path, external=root.as_uri(), external_threshold=0, mode="w") as fs:
        fs.pipe_file("data", payload)
    assert object_path.stat().st_mtime_ns == original_mtime


@pytest.mark.parametrize("threshold", [-1, True, 1.5])
def test_external_threshold_validation(tmp_path, threshold):
    with pytest.raises((TypeError, ValueError), match="external_threshold"):
        ZstarFileSystem(fo=tmp_path / "archive", external_threshold=threshold)


def test_async_external_backend_and_reuse(tmp_path):
    class Objects(AsyncFileSystem):
        def __init__(self):
            super().__init__(asynchronous=True)
            self.data = {}
            self.uploads = 0
            self.downloads = 0
            self.owner = threading.get_ident()

        async def _info(self, path, **kwargs):
            assert threading.get_ident() == self.owner
            if path not in self.data:
                raise FileNotFoundError(path)
            return {"name": path, "size": len(self.data[path]), "type": "file"}

        async def _pipe_file(self, path, value, **kwargs):
            assert threading.get_ident() == self.owner
            await asyncio.sleep(0)
            self.data[path] = bytes(value)
            self.uploads += 1

        async def _cat_file(self, path, start=None, end=None, **kwargs):
            assert threading.get_ident() == self.owner
            self.downloads += 1
            return self.data[path][start:end]

    async def exercise():
        objects = Objects()
        path = tmp_path / "archive"
        async with ZstarFileSystem(
            fo=path, mode="w", external=objects, external_threshold=0, asynchronous=True
        ) as fs:
            await fs._pipe_file("a", b"async external data")
            async with await fs.open_async("b", "wb") as member:
                await member.write(b"async external data")
        assert objects.uploads == 1
        assert objects.downloads == 0
        async with ZstarFileSystem(fo=path, external=objects, asynchronous=True) as fs:
            assert await fs._cat_file("a") == b"async external data"
            assert await fs._cat_file("b") == b"async external data"
        assert objects.data

    asyncio.run(exercise())


def test_external_upload_failure_preserves_archive(tmp_path):
    class BrokenObjects(LocalFileSystem):
        def _open(self, path, mode="rb", **kwargs):
            if "w" in mode:
                raise OSError("object upload failed")
            return super()._open(path, mode, **kwargs)

    path = tmp_path / "archive"
    path.write_bytes(b"original")
    objects = fsspec.filesystem(
        "dir", path=str(tmp_path / "objects"), fs=BrokenObjects(skip_instance_cache=True)
    )
    with (
        pytest.raises(zstar.ZstarError, match="external object upload failed"),
        ZstarFileSystem(fo=path, mode="w", external=objects, external_threshold=0) as fs,
    ):
        fs.pipe_file("bad", b"not uploaded")
    assert path.read_bytes() == b"original"


def test_concurrent_external_archives_share_objects(tmp_path):
    objects = (tmp_path / "objects").as_uri()

    def write(index):
        path = tmp_path / f"archive-{index}"
        with ZstarFileSystem(fo=path, mode="w", external=objects, external_threshold=0) as fs:
            fs.pipe_file("data", b"shared content")
        with ZstarFileSystem(fo=path, external=objects) as fs:
            assert fs.cat_file("data") == b"shared content"

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(write, range(2)))
    assert len(list((tmp_path / "objects").glob("*.zst"))) == 1


def test_async_write_put_and_member_staging(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"local")
    path = tmp_path / "archive"

    async def exercise():
        async with ZstarFileSystem(fo=path, mode="w", asynchronous=True) as fs:
            await fs._makedirs("dir")
            async with await fs.open_async("dir/x", "wb") as member:
                assert await member.write(b"hello") == 5
                await member.seek(0)
                await member.write(b"H")
            await fs._put_file(str(source), "local")
        async with ZstarFileSystem(fo=path, asynchronous=True) as fs:
            assert await fs._cat_file("dir/x") == b"Hello"
            assert await fs._cat_file("local") == b"local"

    asyncio.run(exercise())


def test_async_backend_sink_runs_on_owner_loop():
    class Backend(AsyncFileSystem):
        def __init__(self):
            super().__init__(asynchronous=True)
            self.output = bytearray()
            self.closed = False
            self.owner = threading.get_ident()

        async def open_async(self, path, mode="rb", **kwargs):
            assert path == "archive" and mode == "wb"
            return self

        async def write(self, data):
            assert threading.get_ident() == self.owner
            self.output.extend(data)
            return len(data)

        async def close(self):
            assert threading.get_ident() == self.owner
            self.closed = True

    async def exercise():
        backend = Backend()
        async with (
            ZstarFileSystem(fo="archive", fs=backend, mode="w", asynchronous=True) as fs,
            await fs.open_async("x", "wb") as member,
        ):
            await member.write(b"hello")
        assert backend.closed
        with zstar.Archive(bytes(backend.output)) as reader:
            assert reader.read("x") == b"hello"

    asyncio.run(exercise())


def test_writer_callbacks_run_off_async_loop():
    class Sink(io.BytesIO):
        def write(self, data):
            assert threading.get_ident() != owner
            return super().write(data)

        def close(self):
            self.archive = self.getvalue()
            super().close()

    owner = threading.get_ident()
    sink = Sink()

    async def exercise():
        async with (
            ZstarFileSystem(fo=sink, mode="w", asynchronous=True) as fs,
            await fs.open_async("x", "wb") as member,
        ):
            await member.write(b"hello")

    asyncio.run(exercise())
    assert sink.closed
    with zstar.Archive(sink.archive) as reader:
        assert reader.read("x") == b"hello"


def test_shared_writer_serializes_parallel_known_size_members(tmp_path):
    path = tmp_path / "archive"
    with ZstarFileSystem(fo=path, mode="w") as fs:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(fs.pipe_file, f"dir{index % 3}/file{index}", bytes([index]) * 2000)
                for index in range(12)
            ]
            for future in futures:
                future.result(timeout=10)
        assert len(fs.find("")) == 12
    with zstar.Archive.from_file(path) as archive:
        for index in range(12):
            assert archive.read(f"dir{index % 3}/file{index}") == bytes([index]) * 2000
        assert archive.verify()["full_archive_verified"]


def test_member_emission_and_spool_cleanup_errors_are_preserved(tmp_path, monkeypatch):
    read_error = OSError("spool read failed")
    close_error = OSError("spool close failed")

    class BrokenSpool(io.BytesIO):
        def read(self, size=-1):
            raise read_error

        def close(self):
            if not self.closed:
                super().close()
                raise close_error

    monkeypatch.setattr(
        "zstar._publication.tempfile.SpooledTemporaryFile", lambda **kwargs: BrokenSpool()
    )
    path = tmp_path / "archive"
    path.write_bytes(b"original")
    fs = ZstarFileSystem(fo=path, mode="w")
    member = fs.open("broken", "wb")
    member.write(b"payload")
    with pytest.raises(ExceptionGroup) as caught:
        member.close()
    primary, cleanup = caught.value.exceptions
    assert isinstance(primary, zstar.ZstarError)
    assert primary.__cause__ is read_error
    assert cleanup is close_error
    assert member.closed
    with pytest.raises(ValueError, match="writer failed"):
        fs.close()
    assert path.read_bytes() == b"original"
