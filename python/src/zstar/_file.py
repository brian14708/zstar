"""Borrowed member streams with ordinary binary-file cursor semantics."""

import asyncio
import io
import operator
import threading
from types import TracebackType
from typing import TYPE_CHECKING, Self

from typing_extensions import Buffer, override

from . import _native

if TYPE_CHECKING:
    from ._archive import Archive, AsyncArchive


def _position(current: int, end: int, offset: int, whence: int) -> int:
    offset, whence = operator.index(offset), operator.index(whence)
    if whence == io.SEEK_SET:
        position = offset
    elif whence == io.SEEK_CUR:
        position = current + offset
    elif whence == io.SEEK_END:
        position = end + offset
    else:
        raise ValueError("invalid whence")
    if position < 0:
        raise ValueError("negative seek position")
    if position > (1 << 64) - 1:
        raise OverflowError("seek position exceeds the archive range")
    return position


def _step(max_read_bytes: int) -> int:
    return max(1, max_read_bytes)


def _count(position: int, end: int, size: int | None) -> int:
    available = max(0, end - position)
    size = -1 if size is None else operator.index(size)
    return available if size < 0 else min(size, available)


def _check_count(count: int, limit: int) -> None:
    if count > limit:
        error = _native.ZstarError("returned read size exceeds budget")
        error.category = "resource-limit"
        raise error


class ArchiveFile(io.RawIOBase):
    """A read-only, seekable member stream borrowing its parent archive.

    Allocating reads obey the archive's returned-read budget. readinto may fill
    larger caller-owned buffers in bounded chunks. Each stream retains one
    validated native chunk independently of the shared cache. A per-stream
    lock covers reads, cursor updates, and close.
    """

    def __init__(self, archive: "Archive", name: str, size: int) -> None:
        self._lock = threading.Lock()
        super().__init__()
        self._archive: Archive | None = archive
        self.name = name
        self.mode = "rb"
        self._size, self._position = size, 0
        self._cursor: _native.MemberCursor | None = archive._reader._open_member(name)

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
            return self._position

    @override
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            self._owner()
            self._position = _position(self._position, self._size, offset, whence)
            return self._position

    @override
    def read(self, size: int | None = -1) -> bytes:
        with self._lock:
            archive = self._owner()
            count = _count(self._position, self._size, size)
            if count == 0:
                return b""
            _check_count(count, archive._max_read_bytes)
            assert self._cursor is not None
            data = archive._reader._read_member(self._cursor, self._position, count)
            self._position += len(data)
            return data

    @override
    def readall(self) -> bytes:
        return self.read(-1)

    @override
    def readinto(self, buffer: "Buffer") -> int:
        with memoryview(buffer).cast("B") as view:
            if view.readonly:
                raise TypeError("readinto requires a writable buffer")
            with self._lock:
                archive = self._owner()
                count = _count(self._position, self._size, len(view))
                step = _step(archive._max_read_bytes)
                done = 0
                while done < count:
                    assert self._cursor is not None
                    got = archive._reader._read_member_into(
                        self._cursor,
                        self._position + done,
                        view[done : done + min(count - done, step)],
                    )
                    if got == 0:
                        break
                    done += got
                self._position += done
                return done

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

    def __init__(
        self, archive: "AsyncArchive", name: str, size: int, cursor: _native.MemberCursor
    ) -> None:
        self._archive: AsyncArchive | None = archive
        self.name, self.mode = name, "rb"
        self._size, self._position = size, 0
        self._cursor: _native.MemberCursor | None = cursor
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
            count = _count(self._position, self._size, size)
            if count == 0:
                return b""
            _check_count(count, archive._max_read_bytes)
            assert self._cursor is not None
            data = await archive._reader._read_member(self._cursor, self._position, count)
            self._position += len(data)
            return data

    async def readinto(self, buffer: "Buffer") -> int:
        with memoryview(buffer).cast("B") as view:
            if view.readonly:
                raise TypeError("readinto requires a writable buffer")
            async with self._lock:
                archive = self._owner()
                count = _count(self._position, self._size, len(view))
                if count == 0:
                    return 0
                step = _step(archive._max_read_bytes)
                done = 0
                while done < count:
                    assert self._cursor is not None
                    got = await archive._reader._read_member_into(
                        self._cursor,
                        self._position + done,
                        view[done : done + min(count - done, step)],
                    )
                    if got == 0:
                        break
                    done += got
                self._position += done
                return done

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        async with self._lock:
            self._owner()
            self._position = _position(self._position, self._size, offset, whence)
            return self._position

    async def tell(self) -> int:
        async with self._lock:
            self._owner()
            return self._position

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
