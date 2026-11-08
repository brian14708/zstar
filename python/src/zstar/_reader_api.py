"""Thin sync and async execution facades over one native archive session."""

import asyncio
import os
from collections.abc import AsyncIterator, Iterator, Sequence
from types import TracebackType
from typing import Generic, Self, TypeVar, cast, overload

from typing_extensions import Buffer, override

from . import _native
from ._async_open import _async_factory
from ._file import ArchiveFile, AsyncArchiveFile
from ._fileobj import fileobj
from ._location import _location
from ._metadata import Entry
from ._open_options import reader_options
from ._ownership import _acleanup, _acquire, _cleanup
from ._request import ReadRequest
from .types import AsyncRangeSource, ExtensionValue, RangeSource, ReaderOptions, SeekableReader

_Reader = TypeVar("_Reader", _native.SyncArchive, _native.AsyncArchive)
_Default = TypeVar("_Default")


def _has_methods(value: object, *names: str) -> bool:
    return all(callable(getattr(value, name, None)) for name in names)


class _ArchiveBase(Generic[_Reader]):
    """State and checks shared by the sync and async facades."""

    __slots__ = ("__weakref__", "_reader")
    _reader: _Reader

    @property
    def closed(self) -> bool:
        return self._reader.closed

    @property
    def _max_read_bytes(self) -> int:
        return self._reader.max_read_bytes

    def _check_open(self) -> None:
        # Native operations perform this check themselves; it is only needed
        # for Python-side work that does not enter the native reader.
        self._reader._check_access()
        if self.closed:
            raise ValueError("archive closed")


class Archive(_ArchiveBase[_native.SyncArchive]):
    """Concurrent positional reads; sync callbacks run on serial blocking workers.

    Paths, URLs, and buffers own storage; caller-supplied sources and files remain borrowed.
    Resolvers are borrowed. Opening validates the metadata directory;
    entry pages and selected content are validated when accessed.
    """

    __slots__ = ()

    def __init__(self) -> None:
        raise TypeError("use zstar.open(source)")

    @classmethod
    def _from_config(
        cls, source: RangeSource, options: _native.ReaderConfig, *, owned: bool = False
    ) -> Self:
        archive = cls.__new__(cls)
        archive._reader = _native.SyncArchive._from_source(source, options, owned=owned)
        return archive

    @classmethod
    def _from_owned(cls, source: _native.Source, options: _native.ReaderConfig) -> Self:
        return cls._from_config(source, options, owned=True)

    @classmethod
    def _open(
        cls,
        source: str | os.PathLike[str] | Buffer | SeekableReader | RangeSource,
        *,
        options: ReaderOptions | None = None,
    ) -> Self:
        """Open a path, URL, buffer snapshot, borrowed binary file, or range source.

        PathLike values always denote local paths. Transport settings apply only
        to paths and URLs. Caller-supplied files retain their cursor and ownership.
        """
        policy, source_options = reader_options(options)
        if isinstance(source, (str, os.PathLike)):
            location, local = _location(source)
            return cls._from_owned(
                _native.open_source(location, source_options, local_path=local), policy
            )
        if source_options is not None:
            raise ValueError("source options require a path or URL")
        if _has_methods(source, "size", "read_at"):
            return cls._from_config(cast(RangeSource, source), policy)
        if _has_methods(source, "read", "seek", "tell"):
            return cls._from_config(fileobj(cast(SeekableReader, source)), policy)
        return cls._from_owned(_native.Source.memory(cast(Buffer, source)), policy)

    @property
    def root_extensions(self) -> dict[str, ExtensionValue]:
        return self._reader.root_extensions

    def entry(self, path: str) -> Entry:
        entry = self._reader._entry_value(path)
        if entry is None:
            raise KeyError(path)
        return entry

    @overload
    def get_entry(self, path: str, default: None = None) -> Entry | None: ...
    @overload
    def get_entry(self, path: str, default: _Default) -> Entry | _Default: ...
    def get_entry(self, path: str, default: _Default | None = None) -> Entry | _Default | None:
        """Look up metadata without listing; return default only for an absent path."""
        entry = self._reader._entry_value(path)
        return default if entry is None else entry

    def has_entry(self, path: str) -> bool:
        """Whether the path names a file, directory, or symlink."""
        return self._reader._entry_value(path) is not None

    def __contains__(self, path: object) -> bool:
        self._check_open()
        return isinstance(path, str) and self.has_entry(path)

    def entries(self, prefix: str | None = None) -> Iterator[Entry]:
        """Scan all metadata before yielding entries, optionally filtered by prefix.

        The complete validated index stays retained until the archive closes.
        Use entry or get_entry for selective metadata lookup.
        """
        iterator = self._reader._iter_values(prefix)
        while batch := iterator._next_batch():
            for entry in batch:
                self._check_open()
                yield entry

    def __iter__(self) -> Iterator[Entry]:
        return self.entries()

    def read(self, path: str, *, offset: int = 0, length: int | None = None) -> bytes:
        """Read a member, an exact range, or from offset through EOF."""
        return self._reader.read(path, offset, length)

    def read_many(self, requests: Sequence[ReadRequest]) -> list[bytes]:
        """Batch exact ranges and through-EOF requests, preserving request order."""
        return self._reader._read_many_values(requests)

    def open(self, path: str) -> ArchiveFile:
        return ArchiveFile(self, path, self._reader._open_member(path))

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
        if exc_value is None:
            self.close()
        else:
            _cleanup(exc_value, self.close)


class AsyncArchive(_ArchiveBase[_native.AsyncArchive]):
    """Positional reader confined to its opening Python event loop.

    Async callbacks return awaitables and run with the initiating task's context.
    Custom sources and resolvers implement async protocols.
    """

    __slots__ = ("_loop",)
    _loop: asyncio.AbstractEventLoop

    def __init__(self) -> None:
        raise TypeError("use zstar.open(source, async_=True)")

    @classmethod
    async def _from_config(
        cls, source: AsyncRangeSource, options: _native.ReaderConfig, *, owned: bool = False
    ) -> Self:
        archive = cls.__new__(cls)
        archive._loop = asyncio.get_running_loop()
        archive._reader = await _native.AsyncArchive._from_source(source, options, owned=owned)
        return archive

    @classmethod
    async def _from_owned(cls, source: _native.AsyncSource, options: _native.ReaderConfig) -> Self:
        return await _acquire(
            cls._from_config(source, options, owned=True), lambda archive: archive.close()
        )

    @classmethod
    async def _open(
        cls,
        source: str | os.PathLike[str] | Buffer | SeekableReader | AsyncRangeSource,
        *,
        options: ReaderOptions | None = None,
    ) -> Self:
        """Open storage as an awaitable context manager on the current loop.

        Paths, URLs, and buffers own storage. Range sources and seekable binary
        files remain borrowed; blocking file I/O runs on native workers.
        """
        policy, source_options = reader_options(options)
        if isinstance(source, (str, os.PathLike)):
            location, local = _location(source)
            native = await _acquire(
                _native.aopen_source(location, source_options, local_path=local),
                lambda native: native.aclose(),
            )
        else:
            if source_options is not None:
                raise ValueError("source options require a path or URL")
            if _has_methods(source, "size", "read_at"):
                return await cls._from_config(cast(AsyncRangeSource, source), policy)
            if _has_methods(source, "read", "seek", "tell"):
                return await cls._from_config(
                    _native.BlockingAdapter(fileobj(cast(SeekableReader, source))), policy
                )
            native = await _acquire(
                _native.AsyncSource.memory(cast(Buffer, source)), lambda native: native.aclose()
            )
        return await cls._from_owned(native, policy)

    @property
    def root_extensions(self) -> dict[str, ExtensionValue]:
        self._check_loop()
        return self._reader.root_extensions

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("archive belongs to another event loop")

    @override
    def _check_open(self) -> None:
        self._check_loop()
        super()._check_open()

    async def entry(self, path: str) -> Entry:
        self._check_loop()
        entry = await self._reader._entry_value(path)
        if entry is None:
            raise KeyError(path)
        return entry

    @overload
    async def get_entry(self, path: str, default: None = None) -> Entry | None: ...
    @overload
    async def get_entry(self, path: str, default: _Default) -> Entry | _Default: ...
    async def get_entry(
        self, path: str, default: _Default | None = None
    ) -> Entry | _Default | None:
        """Look up metadata without listing; return default only for an absent path."""
        self._check_loop()
        entry = await self._reader._entry_value(path)
        return default if entry is None else entry

    async def has_entry(self, path: str) -> bool:
        """Whether the path names a file, directory, or symlink."""
        self._check_loop()
        return await self._reader._entry_value(path) is not None

    async def entries(self, prefix: str | None = None) -> AsyncIterator[Entry]:
        """Scan all metadata before yielding, retaining the complete validated index.

        A prefix filters results after the scan; use entry or get_entry for
        selective lookup.
        """
        self._check_loop()
        iterator = self._reader._iter_values(prefix)
        while batch := await iterator._next_batch():
            for entry in batch:
                self._check_open()
                yield entry

    def __aiter__(self) -> AsyncIterator[Entry]:
        return self.entries()

    async def read(self, path: str, *, offset: int = 0, length: int | None = None) -> bytes:
        """Read a member, an exact range, or from offset through EOF."""
        self._check_loop()
        return await self._reader.read(path, offset, length)

    async def read_many(self, requests: Sequence[ReadRequest]) -> list[bytes]:
        """Batch exact ranges and through-EOF requests, preserving request order."""
        self._check_loop()
        return await self._reader._read_many_values(requests)

    @_async_factory
    async def open(self, path: str) -> AsyncArchiveFile:
        self._check_loop()
        return AsyncArchiveFile(self, path, await self._reader._open_member(path))

    async def close(self) -> None:
        self._check_loop()
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
        if exc_value is None:
            await self.close()
        else:
            await _acleanup(exc_value, self.close())
