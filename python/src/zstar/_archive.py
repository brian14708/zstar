"""Tar-style archive readers backed by native indexed I/O."""

from __future__ import annotations

import os
import posixpath
from collections.abc import AsyncIterator, Iterator, Sequence
from tarfile import ExtractError
from tarfile import TarInfo as _TarInfo
from types import TracebackType
from typing import BinaryIO, Generic, Self, TypeVar, Unpack

from typing_extensions import Buffer

from . import _native
from ._file import ArchiveFile, AsyncArchiveFile
from ._info import TarInfo, _from_entry
from ._source import _FileSource
from .types import (
    EntryTuple,
    ObjectResolver,
    RangeSource,
    ReaderOptions,
    ReadReport,
    RequestProvider,
    VerificationReport,
)


def _name(member: str | _TarInfo) -> str:
    if isinstance(member, _TarInfo):
        member = member.name
    if not isinstance(member, str):
        raise TypeError("member must be a name or TarInfo")
    return member


def _link_target(info: TarInfo) -> str:
    return posixpath.normpath(posixpath.join(posixpath.dirname(info.name), info.linkname))


def _member(archive: Archive, member: str | _TarInfo) -> TarInfo:
    """Resolve archive-internal symlinks to the final member (like ``stat``)."""
    info = archive.getmember(_name(member))
    visited: set[str] = set()
    while info.issym():
        if info.name in visited:
            raise ExtractError("cyclic archive symlink")
        visited.add(info.name)
        info = archive.getmember(_link_target(info))
    return info


async def _amember(archive: AsyncArchive, member: str | _TarInfo) -> TarInfo:
    info = await archive.getmember(_name(member))
    visited: set[str] = set()
    while info.issym():
        if info.name in visited:
            raise ExtractError("cyclic archive symlink")
        visited.add(info.name)
        info = await archive.getmember(_link_target(info))
    return info


_DEFAULT_MAX_READ_BYTES = 64 << 20
_Reader = TypeVar("_Reader", _native.IndexedArchive, _native.AsyncArchive)


class _Metadata(Generic[_Reader]):
    _reader: _Reader
    _max_read_bytes: int = _DEFAULT_MAX_READ_BYTES

    @classmethod
    def _wrap(cls, reader: _Reader, options: ReaderOptions) -> Self:
        archive = cls.__new__(cls)
        archive._reader = reader
        archive._max_read_bytes = options.get("max_read_bytes", _DEFAULT_MAX_READ_BYTES)
        return archive

    @property
    def closed(self) -> bool:
        return self._reader.closed

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("archive closed")

    @property
    def entry_count(self) -> int:
        self._check_open()
        return self._reader.entry_count

    @property
    def cache_bytes(self) -> int:
        return self._reader.cache_bytes


class Archive(_Metadata[_native.IndexedArchive]):
    """Synchronous indexed reader. Opening validates metadata; verify checks all content."""

    def __init__(self, data: Buffer, **options: Unpack[ReaderOptions]) -> None:
        """Open any bytes-like snapshot; the archive keeps one private copy."""
        self._reader = _native.IndexedArchive.from_bytes(data, **options)
        self._max_read_bytes = options.get("max_read_bytes", _DEFAULT_MAX_READ_BYTES)

    def getmember(self, name: str) -> TarInfo:
        """Return an owned TarInfo snapshot; raise KeyError for a missing name."""
        self._check_open()
        entry = self._reader.entry(name)
        if entry is None:
            raise KeyError(name)
        return _from_entry(entry)

    @classmethod
    def from_source(cls, source: RangeSource, size: int, **options: Unpack[ReaderOptions]) -> Self:
        return cls._wrap(_native.IndexedArchive(source, size, **options), options)

    @classmethod
    def open(
        cls,
        name: str | os.PathLike[str] | None = None,
        *,
        fileobj: BinaryIO | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open a local path or borrow a seekable binary file object."""
        if (name is None) == (fileobj is None):
            raise TypeError("supply exactly one of name or fileobj")
        if fileobj is not None:
            source = _FileSource(fileobj)
            return cls.from_source(source, source.size, **options)
        assert name is not None
        return cls._wrap(_native.IndexedArchive.from_file(os.fspath(name), **options), options)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open HTTP ranges pinned to one strong ETag, without requiring HEAD."""
        return cls._wrap(_native.IndexedArchive.from_url(url, headers=headers, **options), options)

    @classmethod
    def from_s3(
        cls,
        bucket: str,
        key: str,
        *,
        endpoint: str | None = None,
        region: str | None = None,
        object_prefix: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open native S3 ranges pinned by object version and/or ETag."""
        return cls._wrap(
            _native.IndexedArchive.from_s3(
                bucket,
                key,
                endpoint=endpoint,
                region=region,
                object_prefix=object_prefix,
                anon=anon,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                **options,
            ),
            options,
        )

    def _entries(self, prefix: str | None = None) -> Iterator[EntryTuple]:
        self._check_open()
        for entry in self._reader.iter_entries(prefix):
            self._check_open()
            yield entry

    def getmembers(self) -> Iterator[TarInfo]:
        """Lazily yield owned members in path order. Use list(...) for eager collection."""
        for entry in self._entries():
            yield _from_entry(entry)

    def _iter_prefix(self, prefix: str) -> Iterator[TarInfo]:
        for entry in self._entries(prefix):
            yield _from_entry(entry)

    def getnames(self) -> Iterator[str]:
        for entry in self._entries():
            yield entry[0]

    def __iter__(self) -> Iterator[TarInfo]:
        return self.getmembers()

    def extractfile(self, member: str | _TarInfo) -> ArchiveFile | None:
        """Open a read-only member stream, following archive links; directories return None."""
        info = _member(self, member)
        return ArchiveFile(self, info.name, info.size) if info.isfile() else None

    def read(
        self,
        name: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> bytes:
        self._check_open()
        return self._reader.read(name, offset, length, resolver=resolver)

    def read_many(
        self, requests: Sequence[tuple[str, int, int]], *, resolver: ObjectResolver | None = None
    ) -> list[bytes]:
        self._check_open()
        return self._reader.read_many(requests, resolver=resolver)

    def read_with_report(
        self,
        name: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> tuple[bytes, ReadReport]:
        self._check_open()
        return self._reader.read_with_report(name, offset, length, resolver=resolver)

    def verify(self, *, external: bool = False) -> VerificationReport:
        self._check_open()
        return self._reader.verify(external=external)

    def close(self) -> None:
        self._reader.close()

    def __enter__(self) -> Self:
        self._check_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class AsyncArchive(_Metadata[_native.AsyncArchive]):
    """Async indexed reader over bytes, local files, custom sources, HTTP, or S3."""

    def __init__(self) -> None:
        raise TypeError("use await AsyncArchive.open() or a from_* constructor")

    async def getmember(self, name: str) -> TarInfo:
        """Return an owned TarInfo snapshot; raise KeyError for a missing name.

        Awaitable so slow metadata sources never block the event loop.
        """
        self._check_open()
        entry = await self._reader.entry(name)
        if entry is None:
            raise KeyError(name)
        return _from_entry(entry)

    @classmethod
    async def from_bytes(cls, data: Buffer, **options: Unpack[ReaderOptions]) -> Self:
        return cls._wrap(await _native.AsyncArchive.from_bytes(data, **options), options)

    @classmethod
    async def from_source(
        cls, source: RangeSource, size: int, **options: Unpack[ReaderOptions]
    ) -> Self:
        return cls._wrap(await _native.AsyncArchive.from_source(source, size, **options), options)

    @classmethod
    async def open(
        cls,
        name: str | os.PathLike[str] | None = None,
        *,
        fileobj: BinaryIO | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open a local path asynchronously, or borrow a seekable binary file object."""
        if (name is None) == (fileobj is None):
            raise TypeError("supply exactly one of name or fileobj")
        if fileobj is not None:
            # File sizing is a synchronous caller-owned operation. Perform it
            # on a native callback worker with the rest of metadata opening.
            return cls._wrap(await _native.AsyncArchive.from_fileobj(fileobj, **options), options)
        assert name is not None
        return cls._wrap(await _native.AsyncArchive.from_file(os.fspath(name), **options), options)

    @classmethod
    async def from_url(
        cls,
        url: str | None = None,
        *,
        headers: dict[str, str] | None = None,
        provider: RequestProvider | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        return cls._wrap(
            await _native.AsyncArchive.from_url(url, headers=headers, provider=provider, **options),
            options,
        )

    @classmethod
    async def from_s3(
        cls,
        bucket: str,
        key: str,
        *,
        endpoint: str | None = None,
        region: str | None = None,
        object_prefix: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        return cls._wrap(
            await _native.AsyncArchive.from_s3(
                bucket,
                key,
                endpoint=endpoint,
                region=region,
                object_prefix=object_prefix,
                anon=anon,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
                **options,
            ),
            options,
        )

    async def _entries(self, prefix: str | None = None) -> AsyncIterator[EntryTuple]:
        self._check_open()
        iterator = self._reader.iter_entries(prefix)
        while batch := await iterator._next_batch():
            for entry in batch:
                self._check_open()
                yield entry

    async def getmembers(self) -> AsyncIterator[TarInfo]:
        """Lazily yield owned members without fetching payloads."""
        async for entry in self._entries():
            yield _from_entry(entry)

    async def _iter_prefix(self, prefix: str) -> AsyncIterator[TarInfo]:
        async for entry in self._entries(prefix):
            yield _from_entry(entry)

    async def getnames(self) -> AsyncIterator[str]:
        async for entry in self._entries():
            yield entry[0]

    def __aiter__(self) -> AsyncIterator[TarInfo]:
        return self.getmembers()

    async def extractfile(self, member: str | _TarInfo) -> AsyncArchiveFile | None:
        """Open a member stream, following archive links; directories return None."""
        info = await _amember(self, member)
        if not info.isfile():
            return None
        cursor = await self._reader._open_member(info.name)
        return AsyncArchiveFile(self, info.name, info.size, cursor)

    async def read(
        self,
        name: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> bytes:
        self._check_open()
        return await self._reader.read(name, offset, length, resolver=resolver)

    async def read_many(
        self, requests: Sequence[tuple[str, int, int]], *, resolver: ObjectResolver | None = None
    ) -> list[bytes]:
        self._check_open()
        return await self._reader.read_many(requests, resolver=resolver)

    async def read_with_report(
        self,
        name: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> tuple[bytes, ReadReport]:
        self._check_open()
        return await self._reader.read_with_report(name, offset, length, resolver=resolver)

    async def verify(self, *, external: bool = False) -> VerificationReport:
        self._check_open()
        return await self._reader.verify(external=external)

    async def close(self) -> None:
        await self._reader.close()

    async def __aenter__(self) -> Self:
        self._check_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
