"""Common workflows retain native batching, ownership, and publication contracts."""

import asyncio
import io
from concurrent.futures import ThreadPoolExecutor

import pytest
import zstar
from _support import pack_file
from zstar import (
    Archive,
    AsyncArchive,
    Header,
    ReadRequest,
    ZstarError,
)


class Source:
    def __init__(self, data):
        self.data = data
        self.ranges = []

    def size(self):
        return len(self.data)

    def read_at(self, offset, length):
        self.ranges.append((offset, length))
        return self.data[offset : offset + length]


class AsyncSource(Source):
    async def size(self):
        return super().size()

    async def read_at(self, offset, length):
        return super().read_at(offset, length)


class AsyncSink:
    def __init__(self):
        self.output = io.BytesIO()

    async def write(self, data):
        return self.output.write(data)

    async def flush(self):
        self.output.flush()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_custom_bytes_metadata_and_idempotent_directories(tmp_path, asynchronous):
    path = tmp_path / "archive"
    captured = tmp_path / "input"
    captured.mkdir()
    header = Header.file(
        "docs/nested/data", 5, mode=0o600, mtime_ns=123456789, extensions={"x-note": b"test"}
    )

    async def run():
        async with zstar.open(path, mode="w", async_=True) as writer:
            await writer.addfile(Header.directory("docs", mode=0o700))
            await writer.add(captured, "captured")
            await asyncio.gather(
                *(writer.ensure_directory(name) for name in ["docs/nested", "docs", "captured"] * 3)
            )
            for invalid in [Header.file("bad", 6), Header.directory("bad")]:
                with pytest.raises(ValueError):
                    await writer.writestr(invalid, b"hello")
            with pytest.raises(ZstarError):
                await writer.ensure_directory("../invalid")
            await writer.writestr(header, memoryview(b"hello"))

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(path, mode="w") as writer:
            writer.addfile(Header.directory("docs", mode=0o700))
            writer.add(captured, "captured")
            with ThreadPoolExecutor(4) as pool:
                list(pool.map(writer.ensure_directory, ["docs/nested", "docs", "captured"] * 3))
            for invalid in [Header.file("bad", 6), Header.directory("bad")]:
                with pytest.raises(ValueError):
                    writer.writestr(invalid, b"hello")
            with pytest.raises(ZstarError):
                writer.ensure_directory("../invalid")
            writer.writestr(header, memoryview(b"hello"))
    with zstar.open(path) as archive:
        assert [entry.path for entry in archive] == [
            "captured",
            "docs",
            "docs/nested",
            "docs/nested/data",
        ]
        assert archive.entry("docs").mode == 0o700
        entry = archive.entry(header.path)
        assert (entry.mode, entry.mtime_ns, entry.extensions) == (
            header.mode,
            header.mtime_ns,
            header.extensions,
        )
        assert archive.read(header.path) == b"hello"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_whole_member_batches_share_content_and_enforce_aggregate_budget(asynchronous):
    data = pack_file("data", b"hello world")
    source = AsyncSource(data) if asynchronous else Source(data)
    requests = [ReadRequest("data"), ReadRequest("data", 6), ReadRequest("data", 0, 5)]

    async def run():
        async with zstar.open(source, async_=True) as archive:
            source.ranges.clear()
            assert await archive.read_many(requests) == [b"hello world", b"world", b"hello"]
            assert sum(offset == 0 for offset, _ in source.ranges) == 1
            assert await archive.read_many([ReadRequest("data", 11)]) == [b""]
            with pytest.raises(ZstarError) as caught:
                await archive.read_many([ReadRequest("data", 12)])
            assert caught.value.category == "invalid-range"
        async with zstar.open(source, options={"max_read_bytes": 11}, async_=True) as archive:
            source.ranges.clear()
            with pytest.raises(ZstarError) as caught:
                await archive.read_many([ReadRequest("data")] * 2)
            assert caught.value.category == "resource-limit"
            assert not any(offset == 0 for offset, _ in source.ranges)

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(source) as archive:
            source.ranges.clear()
            assert archive.read_many(requests) == [b"hello world", b"world", b"hello"]
            assert sum(offset == 0 for offset, _ in source.ranges) == 1
            assert archive.read_many([ReadRequest("data", 11)]) == [b""]
            with pytest.raises(ZstarError) as caught:
                archive.read_many([ReadRequest("data", 12)])
            assert caught.value.category == "invalid-range"
        with zstar.open(source, options={"max_read_bytes": 11}) as archive:
            source.ranges.clear()
            with pytest.raises(ZstarError) as caught:
                archive.read_many([ReadRequest("data")] * 2)
            assert caught.value.category == "resource-limit"
            assert not any(offset == 0 for offset, _ in source.ranges)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_lookup_and_full_reads_propagate_errors(asynchronous, monkeypatch):
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        writer.addfile(Header.directory("dir"))
        writer.addfile(Header.symlink("link", "dir/data"))
        writer.writestr("dir/data", b"hello")
        writer.writestr("empty", b"")
    data = output.getvalue()
    sentinel = object()

    def no_listing(*args, **kwargs):
        pytest.fail("lookup must not list the archive")

    monkeypatch.setattr(Archive, "entries", no_listing)
    monkeypatch.setattr(AsyncArchive, "entries", no_listing)

    async def run():
        async with zstar.open(data, async_=True) as archive:
            assert (await archive.get_entry("dir/data")).size == 5
            assert await archive.get_entry("missing") is None
            assert await archive.get_entry("missing", sentinel) is sentinel
            assert await archive.has_entry("dir")
            assert await archive.has_entry("link")
            assert not await archive.has_entry("missing")
            result = await archive.read("dir/data")
            assert result == await archive.read("dir/data", offset=0, length=5)
            assert (await archive.read("empty")) == b""
            for path, error in [
                ("missing", KeyError),
                ("dir", IsADirectoryError),
                ("link", ZstarError),
            ]:
                with pytest.raises(error):
                    await archive.read(path)
                with pytest.raises(error):
                    await archive.read_many([ReadRequest(path)])
        with pytest.raises(ValueError):
            await archive.get_entry("missing")

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(data) as archive:
            assert archive.get_entry("dir/data").size == 5
            assert archive.get_entry("missing") is None
            assert archive.get_entry("missing", sentinel) is sentinel
            assert "dir" in archive and "link" in archive
            assert "missing" not in archive and object() not in archive
            assert archive.has_entry("dir/data")
            result = archive.read("dir/data")
            assert result == archive.read("dir/data", offset=0, length=5)
            assert archive.read("empty") == b""
            for path, error in [
                ("missing", KeyError),
                ("dir", IsADirectoryError),
                ("link", ZstarError),
            ]:
                with pytest.raises(error):
                    archive.read(path)
                with pytest.raises(error):
                    archive.read_many([ReadRequest(path)])
        with pytest.raises(ValueError):
            archive.get_entry("missing")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("outcome", ["success", "abort", "exception", "invalid-archive"])
def test_atomic_replacement_preserves_original_until_commit(tmp_path, asynchronous, outcome):
    path = tmp_path / "archive"
    original = pack_file("original", b"old")
    path.write_bytes(original)

    async def run():
        writer = await zstar.open(path, atomic=True, mode="w", async_=True)
        await writer.writestr("missing/data" if outcome == "invalid-archive" else "data", b"new")
        assert path.read_bytes() == original
        if outcome == "success":
            await writer.close()
            await writer.close()
        elif outcome == "abort":
            await writer.abort()
        elif outcome == "exception":
            with pytest.raises(RuntimeError, match="body failed"):
                async with writer:
                    raise RuntimeError("body failed")
        else:
            with pytest.raises(ZstarError):
                await writer.close()

    if asynchronous:
        asyncio.run(run())
    else:
        writer = zstar.open(path, atomic=True, mode="w")
        writer.writestr("missing/data" if outcome == "invalid-archive" else "data", b"new")
        assert path.read_bytes() == original
        if outcome == "success":
            writer.close()
            writer.close()
        elif outcome == "abort":
            writer.abort()
        elif outcome == "exception":
            with pytest.raises(RuntimeError, match="body failed"), writer:
                raise RuntimeError("body failed")
        else:
            with pytest.raises(ZstarError):
                writer.close()
    if outcome == "success":
        with zstar.open(path) as archive:
            assert archive.read("data") == b"new"
    else:
        assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_atomic_exclusive_creation_never_overwrites_concurrent_creator(tmp_path, asynchronous):
    path = tmp_path / "archive"

    async def run():
        writer = await zstar.open(path, atomic=True, mode="x", async_=True)
        assert not path.exists()
        await writer.writestr("data", b"new")
        path.write_bytes(b"concurrent")
        with pytest.raises(ZstarError):
            await writer.close()
        with pytest.raises(FileExistsError):
            await zstar.open(path, atomic=True, mode="x", async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        writer = zstar.open(path, atomic=True, mode="x")
        assert not path.exists()
        writer.writestr("data", b"new")
        path.write_bytes(b"concurrent")
        with pytest.raises(ZstarError):
            writer.close()
        with pytest.raises(FileExistsError):
            zstar.open(path, atomic=True, mode="x")
    assert path.read_bytes() == b"concurrent"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("exclusive", [False, True])
def test_atomic_new_file_and_file_url(tmp_path, asynchronous, exclusive):
    path = tmp_path / "archive"
    flags = {"atomic": True, "mode": "x" if exclusive else "w"}

    async def run():
        async with zstar.open(path.as_uri(), **flags, async_=True) as writer:
            assert not path.exists()
            await writer.writestr("data", b"hello")

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open(path.as_uri(), **flags) as writer:
            assert not path.exists()
            writer.writestr("data", b"hello")
    with zstar.open(path) as archive:
        assert archive.read("data") == b"hello"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_atomic_output_rejects_symlinks_and_preserves_replaced_staging(tmp_path, asynchronous):
    path = tmp_path / "archive"
    victim = tmp_path / "victim"
    victim.write_bytes(b"untouched")
    path.symlink_to(victim)

    async def run():
        with pytest.raises(OSError):
            await zstar.open(path, atomic=True, mode="w", async_=True)
        path.unlink()
        writer = await zstar.open(path, atomic=True, mode="w", async_=True)
        await writer.writestr("data", b"hello")
        staging = next(tmp_path.glob(".zstar-*.tmp"))
        staging.unlink()
        staging.write_bytes(b"replacement")
        with pytest.raises(ZstarError):
            await writer.close()
        return staging

    if asynchronous:
        staging = asyncio.run(run())
    else:
        with pytest.raises(OSError):
            zstar.open(path, atomic=True, mode="w")
        path.unlink()
        writer = zstar.open(path, atomic=True, mode="w")
        writer.writestr("data", b"hello")
        staging = next(tmp_path.glob(".zstar-*.tmp"))
        staging.unlink()
        staging.write_bytes(b"replacement")
        with pytest.raises(ZstarError):
            writer.close()
    assert not path.exists()
    assert staging.read_bytes() == b"replacement"
    assert victim.read_bytes() == b"untouched"


def test_async_factories_support_contexts_and_explicit_acquisition(tmp_path):
    data = pack_file("data", b"hello")
    path = tmp_path / "archive"
    path.write_bytes(data)
    file = io.BytesIO(data)
    sink = AsyncSink()

    async def run():
        for opening in [
            lambda: zstar.open(path, async_=True),
            lambda: zstar.open(path.as_uri(), async_=True),
            lambda: zstar.open(data, async_=True),
            lambda: zstar.open(file, async_=True),
            lambda: zstar.open(AsyncSource(data), async_=True),
        ]:
            async with opening() as archive:
                assert await archive.read("data") == b"hello"
            assert archive.closed
            archive = await opening()
            assert await archive.read("data") == b"hello"
            await archive.close()
        acquisition = zstar.open(data, async_=True)
        async with acquisition as archive:
            assert await archive.read("data") == b"hello"
        with pytest.raises(RuntimeError):
            await acquisition
        for opening in [
            lambda: zstar.open(tmp_path / "output", mode="w", async_=True),
            lambda: zstar.open((tmp_path / "output").as_uri(), mode="w", async_=True),
            lambda: zstar.open(sink, mode="w", async_=True),
        ]:
            async with opening() as writer:
                await writer.writestr("data", b"hello")
            assert writer.closed
        assert not sink.output.closed and not file.closed

    asyncio.run(run())


def test_cancelled_atomic_writer_aborts_and_preserves_destination(tmp_path):
    path = tmp_path / "archive"
    path.write_bytes(b"original")

    async def run():
        entered = asyncio.Event()

        class Body:
            async def read(self, size):
                entered.set()
                await asyncio.Future()

        async def write():
            async with zstar.open(path, atomic=True, mode="w", async_=True) as writer:
                await writer.addfile(Header.file("data", 1), Body())

        task = asyncio.create_task(write())
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert path.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_standard_writer_operations_and_range_reads(tmp_path, monkeypatch, asynchronous):
    monkeypatch.chdir(tmp_path)
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "data").write_bytes(b"hello world")
    text = "h\N{LATIN SMALL LETTER E WITH ACUTE}llo"
    encoded = text.encode("utf-8")

    async def run():
        async with zstar.open("archive", "w", async_=True) as writer:
            await writer.add("tree")
            await writer.writestr("text", text)
            with pytest.raises(ValueError):
                await writer.writestr(Header.file("custom", len(text)), text)
            await writer.writestr(Header.file("custom", len(encoded), mode=0o600), text)
            await writer.addfile(
                Header.file("streamed", 5),
                fileobj=zstar._native.BlockingAdapter(io.BytesIO(b"hello")),
            )
        async with zstar.open("archive", async_=True) as archive:
            assert await archive.read("tree/data") == b"hello world"
            assert await archive.read("tree/data", offset=6) == b"world"
            assert await archive.read("tree/data", offset=11) == b""
            assert await archive.read("text") == encoded
            assert await archive.read("text", offset=1, length=2) == encoded[1:3]
            assert await archive.read("custom") == encoded
            assert (await archive.entry("custom")).mode == 0o600
            assert await archive.read("streamed") == b"hello"
            with pytest.raises(ZstarError) as caught:
                await archive.read("tree/data", offset=12)
            assert caught.value.category == "invalid-range"

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open("archive", "w") as writer:
            writer.add("tree")
            writer.writestr("text", text)
            with pytest.raises(ValueError):
                writer.writestr(Header.file("custom", len(text)), text)
            writer.writestr(Header.file("custom", len(encoded), mode=0o600), text)
            writer.addfile(Header.file("streamed", 5), fileobj=io.BytesIO(b"hello"))
        with zstar.open("archive") as archive:
            assert archive.read("tree/data") == b"hello world"
            assert archive.read("tree/data", offset=6) == b"world"
            assert archive.read("tree/data", offset=11) == b""
            assert archive.read("text") == encoded
            assert archive.read("text", offset=1, length=2) == encoded[1:3]
            assert archive.read("custom") == encoded
            assert archive.entry("custom").mode == 0o600
            assert archive.read("streamed") == b"hello"
            with pytest.raises(ZstarError) as caught:
                archive.read("tree/data", offset=12)
            assert caught.value.category == "invalid-range"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_add_defaults_and_explicit_arcname(tmp_path, monkeypatch, asynchronous):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "input").write_bytes(b"hello")

    async def run():
        async with zstar.open("archive", "w", async_=True) as writer:
            await writer.add("input")
            await writer.add(tmp_path / "input", arcname="renamed")
            with pytest.raises(TypeError, match="arcname"):
                await writer.add("input", arcname=1)
        async with zstar.open("archive", async_=True) as archive:
            assert [entry.path async for entry in archive] == ["input", "renamed"]
            assert await archive.read("input") == await archive.read("renamed") == b"hello"

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.open("archive", "w") as writer:
            writer.add("input")
            writer.add(tmp_path / "input", arcname="renamed")
            with pytest.raises(TypeError, match="arcname"):
                writer.add("input", arcname=1)
        with zstar.open("archive") as archive:
            assert [entry.path for entry in archive] == ["input", "renamed"]
            assert archive.read("input") == archive.read("renamed") == b"hello"
