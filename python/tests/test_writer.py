"""Native writer modes, async writing, and resource ownership."""

import asyncio
import io
import os
import stat
import sys
import tarfile
import threading
from tarfile import TarInfo

import pytest
import zstar
from zstar import ArchiveWriter, AsyncArchiveWriter, _native
from zstar._info import _nanoseconds


def test_sync_round_trip_modes_and_borrowed_streams(tmp_path):
    path = tmp_path / "archive.zstar"
    body = b"hello world"
    info = TarInfo("hello.txt")
    info.size = len(body)
    with ArchiveWriter.from_file(path, "x") as writer:
        writer.addfile(info, io.BytesIO(body))
    with pytest.raises(FileExistsError):
        ArchiveWriter.from_file(path, "x")
    with zstar.Archive.from_file(path) as reader:
        assert reader.read("hello.txt", offset=6) == b"world"
        assert reader.verify() is None

    output = io.BytesIO()
    with ArchiveWriter.from_file(output, "w") as writer:
        writer.addfile(info, io.BytesIO(body))
    assert not output.closed
    with zstar.Archive.from_file(output) as reader:
        assert reader.read("hello.txt") == body
    assert not output.closed

    with ArchiveWriter.from_file(path, "w"):
        pass
    with zstar.Archive.from_file(path) as reader:
        assert list(reader) == []


def test_writer_factory_rejects_invalid_inputs_and_modes(tmp_path):
    with pytest.raises(ValueError, match="exclusive creation"):
        ArchiveWriter.from_file(io.BytesIO(), "x")
    with pytest.raises(ValueError, match="mode"):
        ArchiveWriter.from_file(tmp_path / "archive", "a")


def test_writer_creation_and_finalization_failures_discard_new_output(tmp_path):
    path = tmp_path / "archive.zstar"
    with pytest.raises(zstar.ZstarError):
        ArchiveWriter.from_file(path, "x", frame_size=0)
    assert not path.exists()

    with pytest.raises(zstar.ZstarError), ArchiveWriter.from_file(path, "x") as writer:
        writer.write_bytes("same", b"one")
        writer.write_bytes("same", b"two")
    assert not path.exists()


@pytest.mark.parametrize("failure", ["creation", "close", "context_exit"])
def test_owned_writer_closes_output_before_removal(tmp_path, monkeypatch, failure):
    path = tmp_path / "archive.zstar"
    outputs = []
    fdopen, unlink = os.fdopen, os.unlink

    def track_output(*args, **kwargs):
        output = fdopen(*args, **kwargs)
        outputs.append(output)
        return output

    def windows_unlink(name, *args, **kwargs):
        if os.fspath(name) == str(path) and any(not output.closed for output in outputs):
            raise PermissionError("cannot delete an open file")
        return unlink(name, *args, **kwargs)

    monkeypatch.setattr("zstar._writer.os.fdopen", track_output)
    monkeypatch.setattr("zstar._writer.os.unlink", windows_unlink)
    with pytest.raises(zstar.ZstarError):
        if failure == "creation":
            ArchiveWriter.from_file(path, "x", frame_size=0)
        else:
            writer = ArchiveWriter.from_file(path, "x")
            writer.write_bytes("same", b"one")
            writer.write_bytes("same", b"two")
            if failure == "close":
                writer.close()
            else:
                writer.__exit__(None, None, None)
    assert outputs and all(output.closed for output in outputs)
    assert not path.exists()


def test_writer_abort_uses_stable_output_path(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    target = second / "archive.zstar"
    target.write_bytes(b"unrelated")
    original = os.getcwd()
    try:
        os.chdir(first)
        with pytest.raises(RuntimeError), ArchiveWriter.from_file("archive.zstar", "x"):
            os.chdir(second)
            raise RuntimeError("abort")
    finally:
        os.chdir(original)
    assert not (first / "archive.zstar").exists()
    assert target.read_bytes() == b"unrelated"


def test_finalized_writer_cannot_delete_completed_or_replaced_output(tmp_path):
    path = tmp_path / "archive.zstar"
    writer = ArchiveWriter.from_file(path, "x")
    with pytest.raises(RuntimeError, match="after close"), writer:
        writer.write_bytes("data", b"complete")
        writer.close()
        raise RuntimeError("after close")
    with zstar.Archive.from_file(path) as archive:
        assert archive.read("data") == b"complete"
    path.write_bytes(b"replacement")
    writer.__exit__(RuntimeError, None, None)
    assert path.read_bytes() == b"replacement"


def test_async_round_trip_and_filesystem_capture(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    (source / "file.txt").write_bytes(b"hello")
    path = tmp_path / "archive.zstar"

    async def exercise():
        loop_thread = threading.get_ident()

        class Sink(io.BytesIO):
            def write(self, data):
                assert threading.get_ident() != loop_thread
                return super().write(data)

            def flush(self):
                assert threading.get_ident() != loop_thread
                return super().flush()

        async with await AsyncArchiveWriter.from_file(path, "x") as writer:
            await writer.add(source, arcname="data")
            info = TarInfo("extra.txt")
            info.size = 5
            await writer.addfile(info, io.BytesIO(b"extra"))
        assert writer.closed
        await writer.close()
        async with await zstar.AsyncArchive.from_file(path) as reader:
            assert [name async for name in reader.names()] == [
                "data",
                "data/file.txt",
                "extra.txt",
            ]
            assert await reader.read("data/file.txt") == b"hello"
            assert await reader.verify() is None
        with pytest.raises(FileExistsError):
            async with await AsyncArchiveWriter.from_file(path, "x"):
                pass

        output = Sink()
        async with await AsyncArchiveWriter.from_file(output, "w") as writer:
            await writer.addfile(info, io.BytesIO(b"extra"))
        assert not output.closed
        async with await zstar.AsyncArchive.from_file(output) as reader:
            assert await reader.read("extra.txt") == b"extra"
        assert not output.closed

        async with await AsyncArchiveWriter.from_file(path, "w"):
            pass
        with zstar.Archive.from_file(path) as reader:
            assert list(reader) == []
        with pytest.raises(ValueError, match="exclusive creation"):
            async with await AsyncArchiveWriter.from_file(output, "x"):
                pass
        with pytest.raises(ValueError, match="mode"):
            async with await AsyncArchiveWriter.from_file(path, "a"):
                pass

    asyncio.run(exercise())


def test_async_callbacks_run_off_loop_and_cancellation_joins_before_cleanup(tmp_path):
    path = tmp_path / "archive.zstar"

    async def exercise():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()
        loop_thread = threading.get_ident()

        class Body(io.BytesIO):
            def read(self, size=-1):
                assert threading.get_ident() != loop_thread
                loop.call_soon_threadsafe(started.set)
                assert release.wait(10), "test did not release body"
                return super().read(size)

        async def write():
            async with await AsyncArchiveWriter.from_file(path, "x") as writer:
                info = TarInfo("file")
                info.size = 5
                await writer.addfile(info, Body(b"hello"))

        task = asyncio.create_task(write())
        try:
            await asyncio.wait_for(started.wait(), 10)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()  # Repeated cancellation still cannot race cleanup.
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not path.exists()

    asyncio.run(exercise())


def test_native_writer_workers_retain_capacity_after_cancellation():
    async def exercise():
        loop = asyncio.get_running_loop()
        release = threading.Event()
        full = asyncio.Event()
        ninth_started = asyncio.Event()
        finished = asyncio.Event()
        started = set()
        completed = set()

        def record_start(index):
            started.add(index)
            if len(started) == 8:
                full.set()
            if index == 8:
                ninth_started.set()

        def record_finish(index):
            completed.add(index)
            if len(completed) == 9:
                finished.set()

        def operation(index):
            loop.call_soon_threadsafe(record_start, index)
            try:
                assert release.wait(10), "test did not release worker"
            finally:
                loop.call_soon_threadsafe(record_finish, index)

        tasks = [
            asyncio.ensure_future(_native._writer_call(lambda i=i: operation(i))) for i in range(8)
        ]
        try:
            await asyncio.wait_for(full.wait(), 10)
            tasks[0].cancel()
            await asyncio.sleep(0)
            tasks.append(asyncio.ensure_future(_native._writer_call(lambda: operation(8))))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(ninth_started.wait(), 0.1)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.wait_for(finished.wait(), 10)

    asyncio.run(exercise())


def test_cancelled_async_open_discards_new_output(tmp_path, monkeypatch):
    path = tmp_path / "archive.zstar"
    original = ArchiveWriter.from_file

    async def exercise():
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        release = threading.Event()

        def blocked_open(cls, *args, **kwargs):
            writer = original(*args, **kwargs)
            loop.call_soon_threadsafe(started.set)
            assert release.wait(10), "test did not release open"
            return writer

        monkeypatch.setattr(ArchiveWriter, "from_file", classmethod(blocked_open))

        async def open_archive():
            async with await AsyncArchiveWriter.from_file(path, "x"):
                pass

        task = asyncio.create_task(open_archive())
        try:
            await asyncio.wait_for(started.wait(), 10)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not path.exists()

    asyncio.run(exercise())


def test_async_context_error_discards_output_and_borrows_sink(tmp_path):
    path = tmp_path / "archive.zstar"
    output = io.BytesIO()

    async def exercise():
        for file in (path, output):
            with pytest.raises(RuntimeError, match="stop"):
                async with await AsyncArchiveWriter.from_file(file, "w") as writer:
                    await writer.addfile(TarInfo("empty"))
                    raise RuntimeError("stop")
            assert writer.closed
        assert not path.exists()
        assert not output.closed

    asyncio.run(exercise())


def test_async_writer_serializes_concurrent_adds_and_propagates_body_errors(tmp_path):
    path = tmp_path / "archive.zstar"

    class BrokenBody(io.BytesIO):
        def read(self, size=-1):
            raise OSError("broken body")

    async def exercise():
        async with await AsyncArchiveWriter.from_file(path, "x") as writer:
            await asyncio.gather(*(writer.addfile(TarInfo(f"file-{i}")) for i in range(20)))
        async with await zstar.AsyncArchive.from_file(path) as archive:
            assert archive.entry_count == 20
            assert await archive.verify() is None

        with pytest.raises(zstar.ZstarError, match="broken body"):
            async with await AsyncArchiveWriter.from_file(path, "w") as writer:
                info = TarInfo("broken")
                info.size = 1
                await writer.addfile(info, BrokenBody())
        assert writer.closed
        assert not path.exists()

    asyncio.run(exercise())


@pytest.mark.skipif(os.name != "posix", reason="Unix file metadata")
def test_writer_add_recurses_without_following_links_and_supports_owned_output(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    (source / "link").symlink_to("file")
    output = tmp_path / "output.zstar"
    with ArchiveWriter.from_file(output, mode="x") as archive:
        archive.add(source, arcname="root")
    with zstar.Archive.from_file(output) as archive:
        assert list(archive.names()) == ["root", "root/file", "root/link"]
        assert archive.getinfo("root/link").issym()
        assert archive.getinfo("root/link").linkname == "file"
        assert _nanoseconds(archive.getinfo("root/file").pax_headers["mtime"]) == 0
        archive.verify()
    with pytest.raises(FileExistsError):
        ArchiveWriter.from_file(output, mode="x")


@pytest.mark.skipif(os.name != "posix", reason="requires no-follow symlink support")
def test_writer_open_refuses_to_truncate_symlink_target(tmp_path):
    target = tmp_path / "target"
    target.write_bytes(b"keep")
    output = tmp_path / "output.zstar"
    output.symlink_to(target)
    with pytest.raises(OSError):
        ArchiveWriter.from_file(output)
    assert target.read_bytes() == b"keep"


@pytest.mark.skipif(os.name != "posix", reason="Unix file metadata")
def test_writer_nonrecursive_directory_preserves_metadata(tmp_path):
    source = tmp_path / "source"
    source.mkdir(mode=0o750)
    (source / "child").write_bytes(b"excluded")
    os.utime(source, ns=(1700000000000000000, 1700000000123456789))
    metadata = source.stat()
    output = io.BytesIO()
    with ArchiveWriter(output) as writer:
        writer.add(source, "root", recursive=False, preserve_metadata=True)
    with zstar.Archive(output.getvalue()) as archive:
        assert list(archive.names()) == ["root"]
        entry = archive.getinfo("root")
        assert entry.mode == stat.S_IMODE(metadata.st_mode)
        assert (entry.uid, entry.gid) == (metadata.st_uid, metadata.st_gid)
        assert _nanoseconds(entry.pax_headers["mtime"]) == metadata.st_mtime_ns
        archive.verify()


def test_writer_removes_partial_output_even_when_owned_close_fails(tmp_path, monkeypatch):
    import zstar._writer

    class FailingClose(io.BufferedWriter):
        def close(self):
            super().close()
            raise OSError("owned output close failed")

    monkeypatch.setattr(
        zstar._writer.os,
        "fdopen",
        lambda fd, mode: FailingClose(io.FileIO(fd, mode, closefd=True)),
    )
    output = tmp_path / "partial.zstar"
    with (
        pytest.raises(OSError, match="owned output close failed"),
        ArchiveWriter.from_file(output) as writer,
    ):
        writer.addfile(TarInfo("empty"))
        raise RuntimeError("capture failed")
    assert writer.closed
    assert not output.exists()


@pytest.mark.skipif(os.name != "posix", reason="Unix file metadata")
def test_capture_normalizes_metadata_sorts_physical_members_and_preserves_on_request(tmp_path):
    root = tmp_path / "source"
    (root / "a").mkdir(parents=True)
    (root / "a/file").write_bytes(b"one")
    file = root / "a-b"
    file.write_bytes(b"two")
    file.chmod(0o6751)
    os.utime(file, ns=(1700000000000000000, 1700000000123456789))
    # Filesystems may clear setgid or ignore Unix execute bits (Windows).
    normalized_mode = 0o755 if file.stat().st_mode & 0o111 else 0o644

    def capture(preserve=False):
        output = io.BytesIO()
        with ArchiveWriter(output) as writer:
            writer.add(root, "root", preserve_metadata=preserve)
        return output.getvalue()

    data = capture()
    data_end = len(data) - 28 - int.from_bytes(data[-20:-12], "little")
    decoded = _native.decode_object(data[:data_end], 4096)
    with tarfile.open(fileobj=io.BytesIO(decoded), mode="r:") as physical:
        assert physical.getnames() == ["root", "root/a", "root/a-b", "root/a/file"]
    with zstar.Archive(data) as reader:
        reader.verify()
        info = reader.getinfo("root/a-b")
        assert (info.mode, info.uid, info.gid, _nanoseconds(info.pax_headers["mtime"])) == (
            normalized_mode,
            0,
            0,
            0,
        )
    os.utime(file, ns=(1700000000000000000, 1700000000999999999))
    assert capture() == data
    metadata = file.stat()
    with zstar.Archive(capture(True)) as reader:
        info = reader.getinfo("root/a-b")
        assert (info.mode, info.uid, info.gid, _nanoseconds(info.pax_headers["mtime"])) == (
            stat.S_IMODE(metadata.st_mode),
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_mtime_ns,
        )
        reader.verify()


def test_capture_deep_directories_without_python_recursion(tmp_path):
    root = tmp_path / "deep"
    root.mkdir()
    child = root
    # Exceed the Python recursion limit without exceeding macOS's PATH_MAX.
    recursion_limit = sys.getrecursionlimit()
    depth = 250
    try:
        for _ in range(depth):
            next_child = child / "d"
            next_child.mkdir()
            child = next_child
        sys.setrecursionlimit(200)
        output = io.BytesIO()
        with ArchiveWriter(output, memory_bytes=1) as writer:
            writer.add(root, "root")
        with zstar.Archive(output.getvalue()) as reader:
            assert reader.entry_count == depth + 1
            reader.verify()
    finally:
        sys.setrecursionlimit(recursion_limit)
        while child != root:
            parent = child.parent
            child.rmdir()
            child = parent


def test_capture_merges_directory_siblings_in_utf8_path_order(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    directories = ["a", "a.b", "aé", "z", "z/a"]
    files = ["a/file", "a-b", "a.b/z", "aé/file", "z/a/z", "z/a-"]
    for name in directories:
        (root / name).mkdir()
    for name in files:
        (root / name).touch()
    expected = sorted(
        ["root", *[f"root/{name}" for name in directories + files]],
        key=lambda name: name.encode("utf-8"),
    )
    output = io.BytesIO()
    with ArchiveWriter(output) as writer:
        writer.add(root, "root")
    data = output.getvalue()
    data_end = len(data) - 28 - int.from_bytes(data[-20:-12], "little")
    decoded = _native.decode_object(data[:data_end], 512 * len(expected) + 1024)
    with tarfile.open(fileobj=io.BytesIO(decoded), mode="r:") as physical:
        assert physical.getnames() == expected
    with zstar.Archive(data) as reader:
        reader.verify()


def test_writer_rejects_duplicates_at_finalization_without_retaining_names():
    output = io.BytesIO()
    writer = ArchiveWriter(output, memory_bytes=1)
    for _ in range(2):
        writer.addfile(TarInfo("file"))
    with pytest.raises(zstar.ZstarError, match=r"duplicate|order"):
        writer.close()


def test_writer_discards_unfinished_output_after_context_exception():
    partial = io.BytesIO()
    info = tarfile.TarInfo("x")
    info.size = 5
    with (
        pytest.raises(RuntimeError, match="producer failed"),
        ArchiveWriter(partial) as writer,
    ):
        writer.addfile(info, io.BytesIO(b"hello"))
        raise RuntimeError("producer failed")
    assert writer.closed and not partial.closed
    with pytest.raises(zstar.ZstarError):
        zstar.Archive(partial.getvalue())


def test_writer_large_external_entry(corpus_dir):
    with zstar.Archive((corpus_dir / "limits/many-extents.zstar").read_bytes()) as reader:
        entry = reader._reader.entry("file")
    assert len(entry[6]) == 65_537
    output = io.BytesIO()
    with ArchiveWriter(output) as writer:
        writer._writer.add(entry)
    with zstar.Archive(output.getvalue()) as reader:
        assert len(reader._reader.entry("file")[6]) == 65_537
        assert reader.verify() is None


def test_writer_preflights_impossible_inline_metadata():
    class Unreadable:
        def read(self, size=-1):
            raise AssertionError("preflight must not consume the body")

    output = io.BytesIO()
    info = TarInfo("huge")
    info.size = 1 << 50
    with (
        pytest.raises(zstar.ZstarError) as caught,
        ArchiveWriter(output, limits={"max_entry_bytes": 8 << 20}) as writer,
    ):
        writer.addfile(info, Unreadable())
    assert caught.value.category == "resource-limit"
    assert output.getvalue() == b""


def test_frame_sizes_are_caller_policy():
    output = io.BytesIO()
    with ArchiveWriter(output, frame_size=32 << 20) as writer:
        writer.write_bytes("large", b"x" * (17 << 20))
    encoded = output.getvalue()
    with zstar.Archive(encoded) as reader:
        assert reader.read_range("large", (17 << 20) - 1, 17 << 20) == b"x"
        assert reader.verify() is None
    with pytest.raises(zstar.ZstarError) as caught:
        zstar.Archive(encoded, limits={"max_frame_decoded_bytes": 16 << 20})
    assert caught.value.category == "resource-limit"
    with pytest.raises(zstar.ZstarError) as caught:
        ArchiveWriter(
            io.BytesIO(), frame_size=32 << 20, limits={"max_frame_decoded_bytes": 16 << 20}
        )
    assert caught.value.category == "resource-limit"
