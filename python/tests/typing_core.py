"""Consumer checks for typed metadata, policies, and structural storage protocols."""

import io
from collections.abc import AsyncIterator, Awaitable
from pathlib import Path
from typing import assert_type

import zstar
from zstar import (
    Archive,
    ArchiveFile,
    ArchiveWriter,
    AsyncArchive,
    AsyncArchiveFile,
    AsyncArchiveWriter,
    AsyncReaderOptions,
    AsyncSourceOptions,
    Credentials,
    Entry,
    EntryKind,
    Extent,
    Header,
    Limits,
    MetadataReaderOptions,
    MetadataWriterOptions,
    RangeCacheOptions,
    ReaderOptions,
    ReadRequest,
    S3Options,
    SinkOptions,
    SourceOptions,
    WriterOptions,
)
from zstar.types import (
    AsyncBinaryWriter,
    AsyncRangeSource,
    BinaryWriter,
    RangeSource,
    SignedRequest,
)


class CustomSource:
    def size(self) -> int:
        return 0

    def read_at(self, offset: int, length: int) -> bytes:
        return b""


class CustomAsyncSource:
    def size(self) -> Awaitable[int]:
        async def result() -> int:
            return 0

        return result()

    def read_at(self, offset: int, length: int) -> Awaitable[bytes]:
        async def result() -> bytes:
            return b""

        return result()


sync_source: RangeSource = CustomSource()
async_source: AsyncRangeSource = CustomAsyncSource()
sync_sink: BinaryWriter = io.BytesIO()


class CustomAsyncSink:
    async def write(self, data: bytes) -> int:
        return len(data)

    async def flush(self) -> None:
        pass


async_sink: AsyncBinaryWriter = CustomAsyncSink()
options = ReaderOptions(
    limits=Limits(max_extents=50),
    metadata=MetadataReaderOptions(max_page_bytes=8 << 20),
)
writer_options = WriterOptions(
    metadata=MetadataWriterOptions(block_target=1024), root_extensions={"x-test": [True]}
)
with zstar.open(sync_source, options=options) as archive:
    entry: Entry = archive.entry("data")
    assert_type(archive.get_entry("data"), Entry | None)
    assert_type(archive.get_entry("data", "missing"), Entry | str)
    assert_type(archive.has_entry("data"), bool)
    assert_type("data" in archive, bool)
    kind: EntryKind = entry.kind
    extent: Extent = entry.extents[0]
    header: Header = entry.to_header()
    data: bytes = archive.read("data")
    data = archive.read("data", offset=0, length=1)
    batch: list[bytes] = archive.read_many([ReadRequest("data", 0, 1)])
    result: bytes = archive.read("data", offset=0, length=1)
    assert_type(archive.read("data"), bytes)
    assert_type(archive.read_many([ReadRequest("data"), ReadRequest("data", 1)]), list[bytes])
    stream: ArchiveFile = archive.open("data")
    count: int = stream.readinto(bytearray(8))
with zstar.open(sync_sink, options=writer_options, mode="w") as writer:
    writer.addfile(Header.file("data", 1), io.BytesIO(b"a"))
    writer.ensure_directory("docs/nested")
    writer.writestr(Header.file("docs/nested/data", 1, mode=0o600), b"a")


async def consume_async() -> None:
    async with zstar.open(async_source, options=AsyncReaderOptions(), async_=True) as archive:
        entry: Entry = await archive.entry("data")
        assert_type(await archive.get_entry("data"), Entry | None)
        assert_type(await archive.get_entry("data", "missing"), Entry | str)
        assert_type(await archive.has_entry("data"), bool)
        assert_type(await archive.read("data"), bytes)
        entries: AsyncIterator[Entry] = archive.entries()
        data: bytes = await archive.read("data", offset=0, length=1)
        result: bytes = await archive.read("data", offset=0, length=1)
        stream: AsyncArchiveFile = await archive.open("data")
        count: int = await stream.readinto(bytearray(8))
        await stream.close()
    async with zstar.open(async_sink, mode="w", async_=True) as writer:
        await writer.writestr("data", b"a")
    async with zstar.open(async_source, async_=True) as direct:
        assert_type(direct, AsyncArchive)
    async with zstar.open(async_sink, mode="w", async_=True) as direct_writer:
        assert_type(direct_writer, AsyncArchiveWriter)
        await direct_writer.ensure_directory("docs")
        await direct_writer.writestr(Header.file("docs/data", 1), b"a")
    assert entry.path and entries and result == data and count >= 0


# Opening preserves concrete return types and separates transport from archive policy.

storage = SourceOptions(range_cache=RangeCacheOptions(capacity_bytes=1024))
s3 = S3Options(
    region="test", credentials=Credentials(access_key_id="key", secret_access_key="secret")
)
s3_storage = SourceOptions(s3=s3)
output_storage = SinkOptions(s3=s3)
assert_type(zstar.open(Path("archive")), Archive)
assert_type(zstar.open(Path("archive"), "r", async_=False), Archive)
assert_type(zstar.open(Path("archive"), "w"), ArchiveWriter)
assert_type(zstar.open(Path("archive"), storage_options=storage), Archive)
assert_type(zstar.open(io.BytesIO()), Archive)
assert_type(zstar.open(bytearray()), Archive)
assert_type(zstar.open("s3://bucket/archive", storage_options=s3_storage), Archive)
assert_type(zstar.open(io.BytesIO(), mode="w"), ArchiveWriter)
assert_type(zstar.open(Path("archive"), mode="x"), ArchiveWriter)
assert_type(zstar.open(Path("archive"), atomic=True, mode="w"), ArchiveWriter)
assert_type(
    zstar.open("s3://bucket/archive", storage_options=output_storage, mode="w"), ArchiveWriter
)


def sign(method: str, refresh: bool) -> SignedRequest:
    return {"url": "https://example.invalid/archive"}


async def async_sign(method: str, refresh: bool) -> SignedRequest:
    return sign(method, refresh)


async def consume_open() -> None:
    assert_type(await zstar.open(Path("archive"), async_=True), AsyncArchive)
    assert_type(await zstar.open(Path("archive"), "w", async_=True), AsyncArchiveWriter)
    assert_type(await zstar.open(Path("archive"), "x", async_=True), AsyncArchiveWriter)
    transport = AsyncSourceOptions(request_provider=async_sign, range_cache=RangeCacheOptions())
    assert_type(
        await zstar.open("https://example.invalid/archive", storage_options=transport, async_=True),
        AsyncArchive,
    )
    assert_type(
        await zstar.open(io.BytesIO(), async_=True),
        AsyncArchive,
    )
    assert_type(await zstar.open(b"", async_=True), AsyncArchive)
    assert_type(await zstar.open(async_sink, mode="w", async_=True), AsyncArchiveWriter)
    assert_type(
        await zstar.open(
            "s3://bucket/archive", storage_options=output_storage, mode="w", async_=True
        ),
        AsyncArchiveWriter,
    )


def consume_blocking_lifecycle(
    archive: Archive, member: ArchiveFile, writer: ArchiveWriter
) -> None:
    assert_type(archive.close(), None)
    assert_type(member.close(), None)
    assert_type(writer.close(), None)
    assert_type(writer.abort(), None)
    assert_type(writer.add("file", arcname="data"), None)
    assert_type(writer.writestr("text", "hello"), None)
    assert_type(archive.read("data", offset=1), bytes)


async def consume_async_lifecycle(
    archive: AsyncArchive, member: AsyncArchiveFile, writer: AsyncArchiveWriter
) -> None:
    assert_type(await archive.close(), None)
    assert_type(await member.close(), None)
    assert_type(await writer.close(), None)
    assert_type(await writer.abort(), None)
    assert_type(await writer.add("file", arcname="data"), None)
    assert_type(await writer.writestr("text", "hello"), None)
    assert_type(await archive.read("data", offset=1), bytes)
