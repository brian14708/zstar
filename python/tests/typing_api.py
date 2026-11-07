"""Static API checks against the installed wheel; these functions are not executed.

Checker-specific unused-ignore checks ensure rejected calls stay rejected.
"""

from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tarfile import TarInfo as StandardTarInfo
from typing import Literal, assert_type

from typing_extensions import Reader
from zstar import (
    Archive,
    ArchiveWriter,
    AsyncArchive,
    AsyncArchiveWriter,
    ZstarError,
)
from zstar.types import (
    AsyncBinaryWriter,
    AsyncObjectResolver,
    AsyncObjectWriter,
    AsyncRangeSource,
    AsyncReader,
    AsyncReaderOptions,
    BinaryWriter,
    EntryMetadata,
    ErrorCategory,
    ExtensionValue,
    FileInfo,
    FileSystemReaderOptions,
    ObjectResolver,
    RangeSource,
    ReaderOptions,
    ReadReport,
    ReadRequest,
    SeekableReader,
    SignedRequest,
)


async def provider(method: Literal["GET", "PUT"], force_refresh: bool) -> SignedRequest:
    return {"url": "https://example.com/archive", "expires_in": 300}


def synchronous(source: RangeSource, resolver: ObjectResolver) -> None:
    options: ReaderOptions = {
        "resolver": resolver,
        "create_spill_store": BytesIO,
    }
    with Archive.from_source(source, 1024, **options) as archive:
        assert_type(archive, Archive)
        assert_type(archive.members(), Iterator[StandardTarInfo])
        assert_type(archive.names(), Iterator[str])
        assert_type(archive.read_range("hello", 0, 1), bytes)
        assert_type(archive.getinfo("hello"), StandardTarInfo)
        assert_type(archive.read("hello"), bytes)
        assert_type(archive.read_many([("hello", 0, 1)]), list[bytes])
        assert_type(archive.read_with_report("hello"), tuple[bytes, ReadReport])
        assert_type(archive.entry_count, int)
        member = archive.open(StandardTarInfo("hello"))
        with member as file:
            assert_type(file.read(), bytes)
            assert_type(file.readinto(bytearray(4)), int)
            assert_type(file.iter_chunks(), Iterator[bytes])
        archive.read("hello", offset="zero")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    Archive(b"", unknown_option=True)  # type: ignore[call-arg]  # pyright: ignore[reportCallIssue]  # pyrefly: ignore[unexpected-keyword]
    with ArchiveWriter(BytesIO(), create_spill_store=BytesIO) as writer:
        writer.addfile(StandardTarInfo("empty"))
        writer.add(Path("input"))
        assert_type(writer.write_bytes("hello", b"world"), None)
    assert_type(Archive.from_file(Path("archive.zstar")), Archive)
    assert_type(Archive.from_s3("bucket", "archive.zstar", region="us-east-1", anon=True), Archive)


async def asynchronous(source: RangeSource, resolver: ObjectResolver) -> None:
    async with await AsyncArchiveWriter.from_file(BytesIO()) as writer:
        assert_type(writer, AsyncArchiveWriter)
        assert_type(await writer.addfile(StandardTarInfo("empty")), None)
        assert_type(await writer.add(Path("input")), None)
        assert_type(await writer.write_bytes("hello", b"world"), None)
        assert_type(writer.closed, bool)
    async with await AsyncArchive.from_source(source, 1024, resolver=resolver) as archive:
        assert_type(archive, AsyncArchive)
        assert_type(archive.members(), AsyncIterator[StandardTarInfo])
        assert_type(archive.names(), AsyncIterator[str])
        assert_type(await archive.read_range("hello", 0, 1), bytes)
        assert_type(await archive.getinfo("hello"), StandardTarInfo)
        assert_type(await archive.read("hello"), bytes)
        assert_type(await archive.read_many([("hello", 0, 1)]), list[bytes])
        assert_type(await archive.read_with_report("hello"), tuple[bytes, ReadReport])
        assert_type(await archive.entry_count, int)
        async with archive.open("hello") as file:
            assert_type(await file.read(), bytes)
            assert_type(await file.readinto(memoryview(bytearray(4))), int)
            assert_type(file.iter_chunks(), AsyncIterator[bytes])
    assert_type(await AsyncArchive.from_file(Path("archive.zstar")), AsyncArchive)
    assert_type(await AsyncArchive.from_url(provider=provider), AsyncArchive)
    assert_type(
        await AsyncArchive.from_s3("bucket", "archive.zstar", region="us-east-1", anon=True),
        AsyncArchive,
    )
    await AsyncArchive.from_url(provider=lambda method, refresh: b"")  # type: ignore[arg-type, return-value]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]


def metadata(error: ZstarError) -> None:
    assert_type(error.category, ErrorCategory)


def filesystem() -> None:
    from zstar import ZstarFileSystem

    options: FileSystemReaderOptions = {"max_read_bytes": 8 << 20}
    fs = ZstarFileSystem(
        fo=Path("archive.zstar"),
        reader_options=options,
        verify_external=True,
        target_options={"auto_mkdir": False},
    )
    ZstarFileSystem(fo="archive.zstar", reader_options={"verify_external": False})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    ZstarFileSystem(fo="archive.zstar", reader_options={"resolver": None})  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    assert_type(fs.modified("hello"), datetime)
    fs.close()


async def async_filesystem() -> None:
    from zstar import ZstarFileSystem

    fs = ZstarFileSystem(fo="archive.zstar", asynchronous=True)
    assert_type(await fs._info("hello"), FileInfo)
    assert_type(await fs._cat_file("hello"), bytes)
    member = await fs.open_async("hello", "rb")
    assert_type(member.seek(0), int)
    assert_type(await member.read(5), bytes)
    assert_type(await member.readinto(bytearray(5)), int)
    await member.close()
    await fs.aclose()


def stream_protocols(body: Reader[bytes], source: SeekableReader, sink: BinaryWriter) -> None:
    assert_type(Archive.from_file(source), Archive)
    with ArchiveWriter(sink) as writer:
        writer.addfile(StandardTarInfo("body"), body)
    Archive.from_file(body)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    Archive.from_file(sink)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    with Archive.from_file(source) as archive:
        member: Reader[bytes] = archive.open("body")
        assert_type(member.read(), bytes)


async def async_stream_protocols(
    body: Reader[bytes], source: SeekableReader, sink: BinaryWriter
) -> None:
    async with await AsyncArchive.from_file(source) as archive:
        assert_type(archive, AsyncArchive)
    async with await AsyncArchiveWriter.from_file(sink) as writer:
        await writer.addfile(StandardTarInfo("body"), body)
    await AsyncArchive.from_file(body)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]
    await AsyncArchive.from_file(sink)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # pyrefly: ignore[bad-argument-type]


async def custom_async_storage(
    source: "AsyncRangeSource",
    resolver: "AsyncObjectResolver",
    sink: "AsyncBinaryWriter",
    body: "AsyncReader",
    objects: "AsyncObjectWriter",
    chunks: AsyncIterator[bytes],
) -> None:
    content: AsyncReaderOptions = {"resolver": resolver, "concurrent_chunks": 4}
    async with await AsyncArchive.from_source(source, 1024, **content) as archive:
        assert_type(await archive.getmetadata("hello"), EntryMetadata)
        assert_type(archive.root_extensions, dict[str, ExtensionValue])
        assert_type(await archive.read_many([ReadRequest("hello", 0, 1)]), list[bytes])
    async with await AsyncArchiveWriter.from_file(sink) as writer:
        await writer.addfile(StandardTarInfo("hello"), body, extensions={"x-owner": "app"})
        await writer.add_external(StandardTarInfo("external"), chunks, objects)
