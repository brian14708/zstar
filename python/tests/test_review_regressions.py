import asyncio
import io
import os
import tarfile

import pytest
import zstar
from zstar import _native


@pytest.mark.parametrize(
    "root,entry,empty", [(True, False, True), (True, False, False), (False, True, False)]
)
@pytest.mark.parametrize("asynchronous", [False, True])
def test_metadata_projection_reports_extension_loss(root, entry, empty, asynchronous):
    output = io.BytesIO()
    with _native.EntryArchiveWriter(
        output, root_extensions={"x-root": True} if root else {}
    ) as writer:
        if not empty:
            writer.add(
                ("file", 0, 0o644, 0, 0, 0, (), {"x-entry": True})
                if entry
                else ("file", 0, 0o644, 0, 0, 0, ())
            )

    async def run():
        async with await zstar.AsyncArchive.from_bytes(output.getvalue()) as archive:
            with pytest.warns(UserWarning, match="extensions"):
                assert len([info async for info in archive.members()]) == (0 if empty else 1)
            if not empty:
                with pytest.warns(UserWarning, match="extensions"):
                    await archive.getinfo("file")

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.Archive(output.getvalue()) as archive:
            with pytest.warns(UserWarning, match="extensions"):
                assert len(list(archive.members())) == (0 if empty else 1)
            if not empty:
                with pytest.warns(UserWarning, match="extensions"):
                    archive.getinfo("file")


@pytest.mark.skipif(os.name != "posix", reason="Descriptor-relative local paths")
@pytest.mark.parametrize("parent_link", [False, True])
def test_download_rejects_destination_symlinks(tmp_path, parent_link):
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.write_bytes("file", b"replacement")
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
    with zstar.ZstarFileSystem(io.BytesIO(output.getvalue())) as fs, pytest.raises(OSError):
        fs.get_file("file", destination / "file")
    assert victim.read_bytes() == b"original"


@pytest.mark.skipif(os.name != "posix", reason="Unix restoration")
def test_extract_returns_metadata_omissions(tmp_path):
    output = io.BytesIO()
    info = tarfile.TarInfo("file")
    info.uid, info.gid, info.mode = 12345, 12345, 0o4644
    with zstar.ArchiveWriter(output) as writer:
        writer.addfile(info)
    report = _native.extract(output.getvalue(), tmp_path / "out")
    assert not report["lossless"]
    assert {item["field"] for item in report["omitted"]} == {"ownership", "special-mode-bits"}


@pytest.mark.parametrize("cache_bytes,expected", [(2 << 20, 1), (0, 128), (1024, 128)])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_member_stream_reuses_decoded_source(cache_bytes, expected, asynchronous):
    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.write_bytes("file", b"x" * (1 << 20))

    class Source:
        calls = 0

        def read_range(self, offset, length):
            self.calls += 1
            return output.getvalue()[offset : offset + length]

    source = Source()

    async def run():
        async with await zstar.AsyncArchive.from_source(source, len(output.getvalue())) as archive:
            source.calls = 0
            async with archive.open("file", cache_bytes=cache_bytes) as stream:
                for _ in range(128):
                    assert await stream.read(1) == b"x"
            assert source.calls == expected

    if asynchronous:
        asyncio.run(run())
    else:
        with zstar.Archive.from_source(source, len(output.getvalue())) as archive:
            source.calls = 0
            with archive.open("file", cache_bytes=cache_bytes) as stream:
                for _ in range(128):
                    assert stream.read(1) == b"x"
            assert source.calls == expected


@pytest.mark.skipif(os.name != "posix", reason="Descriptor-relative local paths")
def test_download_parent_replacement_cannot_redirect_write(tmp_path, monkeypatch):
    from zstar import _fsspec

    output = io.BytesIO()
    with zstar.ArchiveWriter(output) as writer:
        writer.write_bytes("file", b"replacement")
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
    with zstar.ZstarFileSystem(io.BytesIO(output.getvalue())) as fs:
        fs.get_file("file", destination / "file")
    assert replaced
    assert (outside / "file").read_bytes() == b"original"
    assert (parked / "file").read_bytes() == b"replacement"
