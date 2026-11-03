"""Borrowed member streams with ordinary binary-file cursor semantics."""

import asyncio
import io
import operator
import threading
from collections.abc import AsyncIterator, Iterator
from types import TracebackType
from typing import TYPE_CHECKING, Self

from typing_extensions import Buffer, override

from . import _native

if TYPE_CHECKING:
    from ._archive import Archive, AsyncArchive


class ArchiveFile(io.RawIOBase):
    """A read-only, seekable member stream borrowing its parent archive.

    Allocating reads obey the archive's returned-read budget. readinto may fill
    larger caller-owned buffers in bounded chunks. A per-stream lock covers reads,
    cursor updates, and close.
    """

    def __init__(self, archive: "Archive", name: str, cursor: _native.MemberCursor) -> None:
        self._lock = threading.Lock()
        super().__init__()
        self._archive: Archive | None = archive
        self.name = name
        self.mode = "rb"
        self._cursor: _native.MemberCursor | None = cursor
        self.size = cursor.size

    def _owner(self) -> "Archive":
        archive = self._archive
        if self.closed or archive is None:
            raise ValueError("I/O operation on closed file")
        archive._check_open()
        return archive

    @override
    def readable(self) -> bool:
        with self._lock:
            self._owner()
        return True

    @override
    def seekable(self) -> bool:
        with self._lock:
            self._owner()
        return True

    @override
    def tell(self) -> int:
        with self._lock:
            self._owner()
            assert self._cursor is not None
            return self._cursor.tell()

    @override
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            self._owner()
            assert self._cursor is not None
            return self._cursor.seek(operator.index(offset), operator.index(whence))

    @override
    def read(self, size: int | None = -1) -> bytes:
        with self._lock:
            archive = self._owner()
            assert self._cursor is not None
            data = archive._reader._read_member(
                self._cursor, None if size is None else operator.index(size)
            )
            self._cursor.advance(len(data))
            return data

    def iter_chunks(self, chunk_size: int = 64 << 10) -> Iterator[bytes]:
        """Yield bytes from the current position, bounded by the read budget."""
        chunk_size = operator.index(chunk_size)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        chunk_size = min(chunk_size, max(1, self._owner()._max_read_bytes))
        while chunk := self.read(chunk_size):
            yield chunk

    @override
    def readall(self) -> bytes:
        return self.read(-1)

    @override
    def readinto(self, buffer: "Buffer") -> int:
        with self._lock:
            archive = self._owner()
            assert self._cursor is not None
            count = archive._reader._read_member_into(self._cursor, buffer)
            self._cursor.advance(count)
            return count

    @override
    def close(self) -> None:
        with self._lock:
            super().close()
            self._archive = None
            self._cursor = None


class AsyncArchiveFile:
    """An async member stream; cursor operations serialize with reads.

    Read, seek, tell, readinto, and close are awaitable. Cancellation leaves the
    cursor unchanged when a read has not returned. Closing does not close the archive.

    Event-loop affinity: the internal ``asyncio.Lock`` serializes tasks on a
    single event loop and gives no cross-thread synchronization. Use a stream
    only from the loop that first awaited it. From other threads, submit the
    call with ``asyncio.run_coroutine_threadsafe(stream.read(n), loop)``.
    """

    def __init__(self, archive: "AsyncArchive", name: str, cursor: _native.MemberCursor) -> None:
        self._archive: AsyncArchive | None = archive
        self.name, self.mode = name, "rb"
        self._cursor: _native.MemberCursor | None = cursor
        self.size = cursor.size
        self._lock = asyncio.Lock()

    @property
    def closed(self) -> bool:
        return self._archive is None

    def _owner(self) -> "AsyncArchive":
        if self._archive is None:
            raise ValueError("I/O operation on closed file")
        self._archive._check_open()
        return self._archive

    def readable(self) -> bool:
        self._owner()
        return True

    def seekable(self) -> bool:
        self._owner()
        return True

    async def read(self, size: int | None = -1) -> bytes:
        async with self._lock:
            archive = self._owner()
            assert self._cursor is not None
            data = await archive._reader._read_member(
                self._cursor, None if size is None else operator.index(size)
            )
            self._cursor.advance(len(data))
            return data

    async def iter_chunks(self, chunk_size: int = 64 << 10) -> AsyncIterator[bytes]:
        """Yield bytes from the current position, bounded by the read budget."""
        chunk_size = operator.index(chunk_size)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        chunk_size = min(chunk_size, max(1, self._owner()._max_read_bytes))
        while chunk := await self.read(chunk_size):
            yield chunk

    async def readinto(self, buffer: "Buffer") -> int:
        async with self._lock:
            archive = self._owner()
            assert self._cursor is not None
            count = await archive._reader._read_member_into(self._cursor, buffer)
            self._cursor.advance(count)
            return count

    async def _readinto_at(self, offset: int, buffer: "Buffer") -> int:
        archive = self._owner()
        assert self._cursor is not None
        return await archive._reader._read_member_into_at(self._cursor, offset, buffer)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        async with self._lock:
            self._owner()
            assert self._cursor is not None
            return self._cursor.seek(operator.index(offset), operator.index(whence))

    async def tell(self) -> int:
        async with self._lock:
            self._owner()
            assert self._cursor is not None
            return self._cursor.tell()

    async def close(self) -> None:
        async with self._lock:
            self._archive = None
            self._cursor = None

    async def __aenter__(self) -> Self:
        self._owner()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
