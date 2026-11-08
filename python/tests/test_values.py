"""Shared native metadata and policy contracts for both execution facades."""

import asyncio
import io
from collections.abc import Sequence

import pytest
import zstar
from zstar import EntryKind, Header, ReadRequest, ZstarError, _native
from zstar.types import WriterOptions


def metadata_archive():
    output = io.BytesIO()
    with zstar.open(output, options=WriterOptions(frame_size=512), mode="w") as writer:
        writer.addfile(
            Header.file(
                "data",
                4096,
                mode=0o640,
                uid=12,
                gid=34,
                mtime_ns=1_734_567_890_123_456_789,
                extensions={"x-meta": {"nested": [1, b"v"]}},
            ),
            io.BytesIO(b"a" * 4096),
        )
    return output.getvalue()


def assert_snapshot(entry):
    assert entry.kind is EntryKind.FILE
    assert entry.size == 4096
    assert entry.mode == 0o640
    assert (entry.uid, entry.gid) == (12, 34)
    assert entry.mtime_ns == 1_734_567_890_123_456_789
    assert not entry.is_external
    assert isinstance(entry.extents, Sequence)
    assert len(entry.extents) > 1
    assert sum(extent.size for extent in entry.extents) == 4096
    assert entry.extents[-1].size == entry.extents[::-1][0].size
    assert entry.extents[:0] == ()
    assert len(entry.extents[::2]) == (len(entry.extents) + 1) // 2
    with pytest.raises(IndexError):
        _ = entry.extents[len(entry.extents)]
    with pytest.raises(ValueError):
        _ = entry.extents[::0]
    for value, attr in [(entry, "path"), (entry.extents[0], "offset")]:
        with pytest.raises(AttributeError):
            setattr(value, attr, 3)
    extension = entry.extensions
    extension["x-meta"]["nested"] = (False,)
    assert entry.extensions["x-meta"]["nested"] == (1, b"v")
    header = entry.to_header()
    assert header.kind is EntryKind.FILE
    assert (header.path, header.size, header.mtime_ns) == (entry.path, entry.size, entry.mtime_ns)
    assert header.extensions == entry.extensions
    assert not hasattr(header, "extents")
    with pytest.raises(AttributeError):
        header.path = "other"


def test_sync_owned_values_and_lazy_extent_view():
    with zstar.open(_native.Source.memory(metadata_archive())) as archive:
        entry = archive.entry("data")
        listed = list(archive.entries())
        extents = entry.extents
    assert [value.path for value in listed] == ["data"]
    assert sum(value.size for value in extents) == 4096
    assert_snapshot(entry)


def test_async_owned_values():
    async def run():
        archive = await zstar.open(
            await _native.AsyncSource.memory(metadata_archive()), async_=True
        )
        entry = await archive.entry("data")
        listed = [entry async for entry in archive.entries()]
        await archive.close()
        assert [value.path for value in listed] == ["data"]
        assert_snapshot(entry)

    asyncio.run(run())


def test_headers_copy_extensions_and_preserve_nanoseconds():
    original = {"x-meta": {"nested": [True, b"data"]}}
    header = Header.file("data", 5, mtime_ns=-1_234_567_891, extensions=original)
    original["x-meta"]["nested"].clear()
    returned = header.extensions
    returned["x-meta"]["nested"] = ()
    assert header.extensions == {"x-meta": {"nested": (True, b"data")}}
    assert header.mtime_ns == -1_234_567_891
    assert Header.directory("dir").kind is EntryKind.DIRECTORY
    link = Header.symlink("link", "target")
    assert link.kind is EntryKind.SYMLINK
    assert link.linkname == "target"


def test_extension_values_roundtrip_through_public_metadata():
    extensions = {"x-values": (2**64 - 1, -(2**63), b"bytes", True, {"text": "value"})}
    output = io.BytesIO()
    with zstar.open(output, mode="w") as writer:
        writer.addfile(Header.file("data", 0, extensions=extensions))
    with zstar.open(_native.Source.memory(output.getvalue())) as archive:
        assert archive.entry("data").extensions == extensions
    with pytest.raises(TypeError):
        Header.file("data", 0, extensions={"x-float": 0.5})


def test_header_extension_nesting_boundary():
    value = 0
    # The entry and attributes containers count toward the format depth limit.
    for _ in range(62):
        value = (value,)
    assert Header.file("data", 0, extensions={"x-nested": value}).extensions == {"x-nested": value}
    for _ in range(2):
        value = (value,)
        with pytest.raises(ZstarError, match="nesting"):
            Header.file("data", 0, extensions={"x-nested": value})


@pytest.mark.parametrize(
    "value,error", [(-1, ValueError), (1 << 64, OverflowError), (1.5, TypeError), (True, TypeError)]
)
def test_numeric_contract(value, error):
    with pytest.raises(error):
        Header.file("data", value)
    with pytest.raises(error):
        ReadRequest("data", value, 0)
    with pytest.raises(TypeError):
        zstar.open(metadata_archive(), options={"max_read_bytes": value})


def test_omitted_options_use_rust_defaults():
    data = metadata_archive()
    for options in [None, {}, {"limits": {}, "metadata": {}}]:
        with zstar.open(data, options=options) as archive:
            assert archive._max_read_bytes == 64 << 20
            assert archive.read("data") == b"a" * 4096
    with zstar.open(data, options={"max_read_bytes": 4096}) as archive:
        assert archive._max_read_bytes == 4096
    with pytest.raises(ValueError, match="concurrent_chunks"):
        zstar.open(data, options={"concurrent_chunks": 1025})


@pytest.mark.parametrize(
    "options",
    [
        {"unknown": 1},
        {"metadata": {"unknown": 1}},
        {"metadata": {"max_page_bytes": True}},
        {"limits": {"unknown": 1}},
        {1: True},
        {"metadata": None},
        {"max_read_bytes": None},
        {"resolver": object()},
    ],
)
def test_invalid_reader_options_fail_before_source_io(options):
    class Unused:
        def size(self):
            pytest.fail("invalid options reached source I/O")

        def read_at(self, offset, length):
            pytest.fail("invalid options reached source I/O")

    with pytest.raises(TypeError):
        zstar.open(Unused(), options=options)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_invalid_reader_callback_is_rejected_before_opening_storage(asynchronous):
    async def run():
        with pytest.raises(TypeError, match="resolver"):
            await zstar.open("/does-not-exist", options={"resolver": object()}, async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises(TypeError, match="resolver"):
            zstar.open("/does-not-exist", options={"resolver": object()})


@pytest.mark.parametrize(
    "options",
    [
        {"unknown": 1},
        {"frame_size": 0},
        {"frame_size": True},
        {"workers": 17},
        {"compression_level": True},
        {"metadata": {"block_target": 511}},
        {"metadata": {"max_buffer_bytes": True}},
        {"root_extensions": {"bad": 1}},
        {"metadata": {"extension_pages": {"x-page": 1}}},
    ],
)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_invalid_writer_options_preserve_existing_output(tmp_path, options, asynchronous):
    path = tmp_path / "archive"
    path.write_bytes(b"original")

    async def run():
        with pytest.raises((TypeError, ValueError)):
            await zstar.open(path, options=options, mode="w", async_=True)

    if asynchronous:
        asyncio.run(run())
    else:
        with pytest.raises((TypeError, ValueError)):
            zstar.open(path, options=options, mode="w")
    assert path.read_bytes() == b"original"


def test_reader_snapshots_nested_options():
    options = {"max_read_bytes": 4096, "limits": {"max_entry_bytes": 4096}}
    with zstar.open(metadata_archive(), options=options) as archive:
        options["max_read_bytes"] = 0
        options["limits"]["max_entry_bytes"] = 1
        assert archive._max_read_bytes == 4096
        assert archive.read("data") == b"a" * 4096
