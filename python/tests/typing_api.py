"""Static API checks against the installed wheel; these functions are not executed.

Pyrefly's unused-ignore check ensures the rejected calls stay rejected.
"""

from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from io import BytesIO
from pathlib import Path
from tarfile import TarInfo as StandardTarInfo
from typing import Literal, assert_type

from zstar import (
    Archive,
    ArchiveWriter,
    AsyncArchive,
    TarInfo,
    ZstarError,
    ZstarFileSystem,
    extract,
    materialize,
    pack_directory,
    pack_file,
    put_presigned,
    s3_archive_metadata,
)
from zstar.format import ManifestArchive, decode_manifest_block, encode_manifest_block
from zstar.fsspec import FileInfo
from zstar.types import (
    CborValue,
    DecodedCache,
    ErrorCategory,
    ObjectResolver,
    RangeSource,
    ReaderOptions,
    ReadReport,
    SignedRequest,
    VerificationReport,
)


async def provider(method: Literal["GET", "PUT"], force_refresh: bool) -> SignedRequest:
    return {"url": "https://example.com/archive", "expires_in": 300}


def synchronous(source: RangeSource, resolver: ObjectResolver, cache: DecodedCache) -> None:
    options: ReaderOptions = {
        "resolver": resolver,
        "shared_cache": cache,
        "create_spill_store": BytesIO,
    }
    with Archive.from_source(source, 1024, **options) as archive:
        assert_type(archive, Archive)
        assert_type(archive.getmembers(), Iterator[TarInfo])
        assert_type(archive.getnames(), Iterator[str])
        assert_type(archive.getmember("hello"), TarInfo)
        assert_type(archive.read("hello"), bytes)
        assert_type(archive.read_many([("hello", 0, 1)]), list[bytes])
        assert_type(archive.read_with_report("hello"), tuple[bytes, ReadReport])
        assert_type(archive.verify(), VerificationReport)
        member = archive.extractfile(StandardTarInfo("hello"))
        if member is not None:
            with member as file:
                assert_type(file.read(), bytes)
                assert_type(file.readinto(bytearray(4)), int)
        archive.read("hello", offset="zero")  # pyrefly: ignore[bad-argument-type]
    Archive(b"", cache_bytes="large")  # pyrefly: ignore[bad-argument-type]
    Archive(b"", unknown_option=True)  # pyrefly: ignore[unexpected-keyword]
    ManifestArchive(source, 1024, resolver=resolver)  # pyrefly: ignore[unexpected-keyword]
    with ArchiveWriter(BytesIO(), create_spill_store=BytesIO) as writer:
        writer.addfile(StandardTarInfo("empty"))
        writer.add(Path("input"))
    assert_type(Archive.open(Path("archive.zstar")), Archive)
    assert_type(Archive.from_s3("bucket", "archive.zstar", anon=True), Archive)
    assert_type(pack_file("hello", b"world"), bytes)
    assert_type(s3_archive_metadata(b""), dict[str, str])
    assert_type(pack_directory(Path("input")), bytes)
    extract(b"", Path("output"), objects=Path("objects"))
    assert_type(materialize(b"", objects=Path("objects")), bytes)


async def asynchronous(source: RangeSource, resolver: ObjectResolver) -> None:
    async with await AsyncArchive.from_source(source, 1024, resolver=resolver) as archive:
        assert_type(archive, AsyncArchive)
        assert_type(archive.getmembers(), AsyncIterator[TarInfo])
        assert_type(archive.getnames(), AsyncIterator[str])
        assert_type(await archive.getmember("hello"), TarInfo)
        assert_type(await archive.read("hello"), bytes)
        assert_type(await archive.read_many([("hello", 0, 1)]), list[bytes])
        assert_type(await archive.read_with_report("hello"), tuple[bytes, ReadReport])
        assert_type(await archive.verify(), VerificationReport)
        member = await archive.extractfile("hello")
        if member is not None:
            async with member as file:
                assert_type(await file.read(), bytes)
                assert_type(await file.readinto(memoryview(bytearray(4))), int)
    assert_type(await AsyncArchive.open(Path("archive.zstar")), AsyncArchive)
    assert_type(await AsyncArchive.from_url(provider=provider), AsyncArchive)
    assert_type(await AsyncArchive.from_s3("bucket", "archive.zstar", anon=True), AsyncArchive)
    assert_type(await put_presigned(b"", provider=provider), None)
    await AsyncArchive.from_url(provider=lambda method, refresh: b"")  # pyrefly: ignore[bad-argument-type]


def metadata(error: ZstarError, fs: ZstarFileSystem) -> None:
    assert_type(error.category, ErrorCategory)
    info = TarInfo("hello")
    info.extensions["x-example"] = {"count": 1, "labels": ("a", "b")}
    info.extensions["x-example"] = None  # pyrefly: ignore[unsupported-operation]
    assert_type(decode_manifest_block(b""), CborValue)
    encode_manifest_block(1.5)  # pyrefly: ignore[bad-argument-type]
    assert_type(fs.info("hello"), FileInfo)
    assert_type(fs.ls(""), list[FileInfo])
    assert_type(fs.ls("", detail=False), list[str])


def filesystem() -> None:
    fs = ZstarFileSystem(fo=Path("archive.zstar"), target_options={"auto_mkdir": False})
    assert_type(fs.modified("hello"), datetime)
    fs.close()


async def async_filesystem() -> None:
    fs = ZstarFileSystem(fo="archive.zstar", asynchronous=True)
    assert_type(await fs._info("hello"), FileInfo)
    assert_type(await fs._cat_file("hello"), bytes)
    member = await fs.open_async("hello")
    assert_type(member.seek(0), int)
    assert_type(await member.read(5), bytes)
    assert_type(await member.readinto(bytearray(5)), int)
    await member.close()
    await fs.aclose()
