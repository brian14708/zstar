"""Tar-style archive readers backed by native indexed I/O."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager
from tarfile import TarInfo
from types import TracebackType
from typing import Generic, Self, TypeVar, Unpack

from typing_extensions import Buffer

from . import _native
from ._file import ArchiveFile, AsyncArchiveFile
from ._info import _from_metadata
from .types import (
    ObjectResolver,
    RangeSource,
    ReaderOptions,
    ReadReport,
    RequestProvider,
    SeekableReader,
    VerificationReport,
)


def _name(member: str | TarInfo) -> str:
    if isinstance(member, TarInfo):
        member = member.name
    if not isinstance(member, str):
        raise TypeError("member must be a name or TarInfo")
    return member


_Reader = TypeVar("_Reader", _native.SyncArchive, _native.AsyncArchive)


class _Metadata(Generic[_Reader]):
    _reader: _Reader

    @classmethod
    def _wrap(cls, reader: _Reader) -> Self:
        archive = cls.__new__(cls)
        archive._reader = reader
        return archive

    @property
    def _max_read_bytes(self) -> int:
        return self._reader.max_read_bytes

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


class Archive(_Metadata[_native.SyncArchive]):
    """Synchronous indexed reader. Opening validates metadata; verify checks all content."""

    def __init__(self, data: Buffer, **options: Unpack[ReaderOptions]) -> None:
        """Open any bytes-like snapshot; the archive keeps one private copy."""
        self._reader = _native.SyncArchive.from_bytes(data, **options)

    def getinfo(self, name: str) -> TarInfo:
        """Return an owned TarInfo snapshot; raise KeyError for a missing name."""
        self._check_open()
        metadata = self._reader._member_info(name)
        if metadata is None:
            raise KeyError(name)
        return _from_metadata(metadata)

    @classmethod
    def from_source(cls, source: RangeSource, size: int, **options: Unpack[ReaderOptions]) -> Self:
        return cls._wrap(_native.SyncArchive.from_source(source, size, **options))

    @classmethod
    def from_file(
        cls,
        file: str | os.PathLike[str] | SeekableReader,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open a local path or borrow a seekable binary file object."""
        if not isinstance(file, (str, os.PathLike)):
            return cls._wrap(_native.SyncArchive.from_fileobj(file, **options))
        return cls._wrap(_native.SyncArchive.from_file(os.fspath(file), **options))

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open HTTP ranges pinned to one strong ETag, without requiring HEAD."""
        return cls._wrap(_native.SyncArchive.from_url(url, headers=headers, **options))

    @classmethod
    def from_s3(
        cls,
        bucket: str,
        key: str,
        *,
        region: str,
        endpoint: str | None = None,
        object_prefix: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open native S3 ranges with explicit region and credentials (or anon=True).

        AWS environment variables and credential discovery are not used.
        """
        return cls._wrap(
            _native.SyncArchive.from_s3(
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
        )

    def members(self, prefix: str | None = None) -> Iterator[TarInfo]:
        """Lazily yield owned members in path order. Use list(...) for eager collection."""
        self._check_open()
        iterator = self._reader._iter_members(prefix)
        while batch := iterator._next_batch():
            for metadata in batch:
                self._check_open()
                yield _from_metadata(metadata)

    def names(self, prefix: str | None = None) -> Iterator[str]:
        """Lazily yield names in path order without constructing member metadata."""
        self._check_open()
        iterator = self._reader._iter_names(prefix)
        while batch := iterator._next_batch():
            for name in batch:
                self._check_open()
                yield name

    def __iter__(self) -> Iterator[TarInfo]:
        return self.members()

    def open(self, member: str | TarInfo) -> ArchiveFile:
        """Open a read-only regular-file stream without following symlinks.

        Missing members raise KeyError; directories raise IsADirectoryError;
        symlinks raise tarfile.ExtractError without resolving their targets.
        The stream borrows this archive and must be closed before its owner.
        """
        self._check_open()
        name = _name(member)
        return ArchiveFile(self, name, self._reader._open_member(name))

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

    def read_range(self, name: str, start: int, end: int) -> bytes:
        """Read the half-open byte range ``[start, end)`` from a member."""
        if end < start:
            raise ValueError("end must be greater than or equal to start")
        return self.read(name, start, end - start)

    def read_link(self, name: str) -> str:
        """Return a symlink's stored target without resolving it."""
        self._check_open()
        return self._reader.read_link(name)

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
        raise TypeError("use await AsyncArchive.from_file() or another from_* constructor")

    async def getinfo(self, name: str) -> TarInfo:
        """Return an owned TarInfo snapshot; raise KeyError for a missing name.

        Awaitable so slow metadata sources never block the event loop.
        """
        self._check_open()
        metadata = await self._reader._member_info(name)
        if metadata is None:
            raise KeyError(name)
        return _from_metadata(metadata)

    @classmethod
    async def from_bytes(cls, data: Buffer, **options: Unpack[ReaderOptions]) -> Self:
        return cls._wrap(await _native.AsyncArchive.from_bytes(data, **options))

    @classmethod
    async def from_source(
        cls, source: RangeSource, size: int, **options: Unpack[ReaderOptions]
    ) -> Self:
        return cls._wrap(await _native.AsyncArchive.from_source(source, size, **options))

    @classmethod
    async def from_file(
        cls,
        file: str | os.PathLike[str] | SeekableReader,
        **options: Unpack[ReaderOptions],
    ) -> Self:
        """Open a local path asynchronously, or borrow a seekable binary file object."""
        if not isinstance(file, (str, os.PathLike)):
            # File sizing is a synchronous caller-owned operation. Perform it
            # on a native callback worker with the rest of metadata opening.
            return cls._wrap(await _native.AsyncArchive.from_fileobj(file, **options))
        return cls._wrap(await _native.AsyncArchive.from_file(os.fspath(file), **options))

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
        )

    @classmethod
    async def from_s3(
        cls,
        bucket: str,
        key: str,
        *,
        region: str,
        endpoint: str | None = None,
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
        )

    async def members(self, prefix: str | None = None) -> AsyncIterator[TarInfo]:
        """Lazily yield owned members without fetching payloads."""
        self._check_open()
        iterator = self._reader._iter_members(prefix)
        while batch := await iterator._next_batch():
            for metadata in batch:
                self._check_open()
                yield _from_metadata(metadata)

    async def names(self, prefix: str | None = None) -> AsyncIterator[str]:
        """Lazily yield names without constructing member metadata."""
        self._check_open()
        iterator = self._reader._iter_names(prefix)
        while batch := await iterator._next_batch():
            for name in batch:
                self._check_open()
                yield name

    def __aiter__(self) -> AsyncIterator[TarInfo]:
        return self.members()

    @asynccontextmanager
    async def open(self, member: str | TarInfo) -> AsyncGenerator[AsyncArchiveFile]:
        """Open a member with ``async with archive.open(name)``.

        Missing members raise KeyError; directories raise IsADirectoryError;
        symlinks raise tarfile.ExtractError without resolving their targets.
        The member closes on context exit; its owner stays open.
        """
        self._check_open()
        name = _name(member)
        cursor = await self._reader._open_member(name)
        async with AsyncArchiveFile(self, name, cursor) as stream:
            yield stream

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

    async def read_range(self, name: str, start: int, end: int) -> bytes:
        """Read the half-open byte range ``[start, end)`` from a member."""
        if end < start:
            raise ValueError("end must be greater than or equal to start")
        return await self.read(name, start, end - start)

    async def read_link(self, name: str) -> str:
        """Return a symlink's stored target without resolving it."""
        self._check_open()
        return await self._reader.read_link(name)

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
