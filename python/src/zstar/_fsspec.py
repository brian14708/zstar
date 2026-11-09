"""Native async archive filesystems with fsspec's synchronous facade."""

from __future__ import annotations

import asyncio
import errno
import io
import os
import sys
import threading
from collections.abc import Coroutine, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from functools import partial
from os import PathLike, fspath
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Literal, cast, overload

from fsspec.archive import AbstractArchiveFileSystem
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem, sync
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from typing_extensions import Buffer, override

from . import _native
from ._file import AsyncArchiveFile
from ._fileobj import fileobj
from ._fsspec_options import (
    FileInfo,
    ProgressCallback,
)
from ._metadata import Entry, EntryKind
from ._native import ZstarError
from ._options import AsyncReaderOptions, ReadRequest, WriterOptions
from ._ownership import _acquire
from ._publication import _Publication, _StagedMember, _WriteSession
from ._reader_api import AsyncArchive
from ._transport_options import AsyncSourceOptions
from ._writer import _writer_call

__all__ = ["FileInfo", "ZstarFileSystem"]

_cleanup_tasks: set[asyncio.Task[None]] = set()
_cleanup_tasks_lock = threading.Lock()


def _release_cleanup(task: asyncio.Task[None]) -> None:
    with _cleanup_tasks_lock:
        _cleanup_tasks.discard(task)


def _retain_cleanup(task: asyncio.Task[None]) -> asyncio.Task[None]:
    # Event loops keep only weak task references; retain cleanup until done.
    with _cleanup_tasks_lock:
        _cleanup_tasks.add(task)
    task.add_done_callback(_release_cleanup)
    return task


def _write_all(output: BinaryIO, data: bytes) -> None:
    """Honor short writes from caller-supplied streams."""
    remaining = memoryview(data)
    while remaining:
        count = output.write(remaining)
        if count is None:
            raise BlockingIOError(errno.EAGAIN, "output stream would block")
        if count <= 0 or count > len(remaining):
            raise OSError("invalid output stream write count")
        remaining = remaining[count:]


if sys.platform == "win32":

    def _local_destination(path: Path, *, directory: bool = False) -> BinaryIO | None:
        raise NotImplementedError(
            "confined local downloads require descriptor-relative no-follow I/O"
        )

else:

    def _local_destination(path: Path, *, directory: bool = False) -> BinaryIO | None:
        """Walk pinned directory descriptors; reject symlinks at every component."""
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            raise NotImplementedError(
                "confined local downloads require descriptor-relative no-follow I/O"
            )
        # Resolve '.' and '..' lexically before opening anything, without following links.
        absolute = Path(os.path.abspath(path))
        parts = absolute.parts[1:]
        fd = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            parents = parts if directory else parts[:-1]
            for component in parents:
                with suppress(FileExistsError):
                    os.mkdir(component, dir_fd=fd)
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            if directory:
                return None
            leaf = os.open(
                parts[-1],
                os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                0o666,
                dir_fd=fd,
            )
            try:
                import stat

                if not stat.S_ISREG(os.fstat(leaf).st_mode):
                    raise OSError("local download destination must be a regular file")
                os.ftruncate(leaf, 0)
                return os.fdopen(leaf, "wb")
            except BaseException:
                os.close(leaf)
                raise
        finally:
            os.close(fd)


def _range_bounds(size: int, start: int | None, end: int | None) -> tuple[int, int]:
    """Clip fsspec's positive and end-relative offsets to a member's bounds."""
    start, end, _ = slice(start, end).indices(size)
    return start, max(start, end)


_INFO_TYPES: dict[EntryKind, Literal["file", "directory", "link"]] = {
    EntryKind.FILE: "file",
    EntryKind.DIRECTORY: "directory",
    EntryKind.SYMLINK: "link",
}


def _entry_info(name: str, entry: Entry) -> FileInfo:
    return {"name": name, "size": entry.size, "type": _INFO_TYPES[entry.kind]}


class _AsyncFsspecSource:
    """Exact backend ranges through the native async-source seam."""

    def __init__(self, backend: AbstractFileSystem, path: str, size: int):
        self.backend, self.path, self._size = backend, path, size

    async def size(self) -> int:
        return self._size

    async def read_at(self, offset: int, length: int) -> bytes:
        return cast(
            bytes, await self.backend._cat_file(self.path, start=offset, end=offset + length)
        )


class _ExternalObjects:
    """Borrowed filesystem rooted at the encoded object directory."""

    def __init__(self, fs: AbstractFileSystem, root: str):
        self.fs, self.root = fs, root.rstrip("/")
        self._prepared = False

    def _path(self, address: str) -> str:
        name = f"{address.removeprefix('sha256:')}.zst"
        return f"{self.root}/{name}" if self.root else name

    async def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes:
        path = self._path(address)
        if getattr(self.fs, "asynchronous", False):
            data = cast(bytes, await self.fs._cat_file(path, start=0, end=max_bytes + 1))
        else:

            def read() -> bytes:
                with self.fs.open(path, "rb", cache_type="none") as file:
                    chunks = bytearray()
                    while len(chunks) <= max_bytes:
                        chunk = file.read(min(1 << 20, max_bytes + 1 - len(chunks)))
                        if not chunk:
                            break
                        chunks.extend(chunk)
                    return bytes(chunks)

            data = await _writer_call(read)
        if len(data) > max_bytes:
            raise ValueError("external object exceeds encoded size limit")
        return data

    async def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> int:
        path = self._path(address)
        if getattr(self.fs, "asynchronous", False):
            try:
                info = await self.fs._info(path)
            except FileNotFoundError:
                pass
            else:
                return int(info["size"])
            await self.fs._pipe_file(path, encoded)
            return len(encoded)
        try:
            info = await _writer_call(partial(self.fs.info, path))
        except FileNotFoundError:
            pass
        else:
            return int(info["size"])
        if not self._prepared:
            parent = self.fs._parent(path)
            if parent or self.fs.protocol == "dir":
                await _writer_call(partial(self.fs.makedirs, parent, exist_ok=True))
            self._prepared = True
        publication = await _Publication.open(self.fs, path)
        try:
            await _writer_call(partial(_write_all, cast(BinaryIO, publication.output), encoded))
        except BaseException as primary:
            await publication.finish(abort=True, errors=(primary,))
            raise
        await publication.finish()
        return len(encoded)


class _ArchiveResources:
    def __init__(
        self,
        archive: AsyncArchive,
        file: BinaryIO | None = None,
    ):
        self.archive, self.file = archive, file

    async def close(self) -> None:
        errors: list[BaseException] = []
        try:
            await self.archive.close()
        except BaseException as error:
            errors.append(error)
        if self.file is not None:
            try:
                await asyncio.to_thread(self.file.close)
            except BaseException as error:
                errors.append(error)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("archive cleanup failed", errors)


class _AsyncMemberFile(AbstractAsyncStreamedFile):
    def __init__(self, fs: ZstarFileSystem, path: str, stream: AsyncArchiveFile):
        self._stream = stream
        super().__init__(fs, path, mode="rb", size=stream.size, cache_type="none")

    async def read(self, length: int | None = -1) -> bytes:
        return cast(bytes, await super().read(length))

    def seek(self, offset: int, whence: int = 0) -> int:
        return cast(int, super().seek(offset, whence))

    async def readinto(self, buffer: Buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        count = await self._stream._readinto_at(self.loc, buffer)
        self.loc += count
        return count

    async def _fetch_range(self, start: int, end: int) -> bytes:
        end = min(end, self.size)
        output = bytearray(max(0, end - start))
        count = await self._stream._readinto_at(start, output)
        return bytes(memoryview(output)[:count])

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            await self._stream.__aexit__(None, None, None)

    def __del__(self) -> None:
        # The stream borrows the archive. Dropping it releases its cursor;
        # calling an async close from AbstractBufferedFile.__del__ would leak a coroutine.
        self.closed = True


class _MemberFile(AbstractBufferedFile):
    def __init__(self, fs: ZstarFileSystem, path: str, stream: _AsyncMemberFile, **kwargs: object):
        self._stream = stream
        # Disable fsspec read-ahead caching; archive reads must issue only the
        # requested ranges regardless of caller-supplied file options.
        kwargs.pop("cache_type", None)
        kwargs.pop("cache_options", None)
        kwargs["cache_type"] = "none"
        kwargs["block_size"] = 0
        try:
            super().__init__(fs, path, mode="rb", size=stream.size, **kwargs)
        except BaseException:
            sync(fs.loop, stream.close)
            raise

    def close(self) -> None:
        if getattr(self, "closed", False):
            return
        try:
            super().close()
        finally:
            if hasattr(self, "_stream"):
                sync(self.fs.loop, self._stream.close)

    def __del__(self) -> None:
        # A borrowed async cursor releases its native state when dropped. GC
        # can run on the I/O loop, where a synchronous close would deadlock.
        self.closed = True

    def _fetch_range(self, start: int, end: int) -> bytes:
        return sync(self.fs.loop, self._stream._fetch_range, start, end)


class _WriterMemberFile(io.BufferedIOBase):
    mode = "wb"

    def __init__(self, fs: ZstarFileSystem, stream: _StagedMember):
        self.fs, self._stream = fs, stream
        self.name = stream.path

    @property
    @override
    def closed(self) -> bool:
        return self._stream.closed

    @override
    def writable(self) -> bool:
        return not self.closed

    @override
    def readable(self) -> bool:
        return False

    @override
    def seekable(self) -> bool:
        return True

    @override
    def write(self, data: Buffer) -> int:
        return sync(self.fs.loop, self._stream.write, data)

    @override
    def seek(self, offset: int, whence: int = 0) -> int:
        return sync(self.fs.loop, self._stream.seek, offset, whence)

    @override
    def tell(self) -> int:
        return sync(self.fs.loop, self._stream.tell)

    @override
    def flush(self) -> None:
        if not self.closed:
            sync(self.fs.loop, self._stream.flush)

    @override
    def close(self) -> None:
        sync(self.fs.loop, self._stream.close)

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        sync(self.fs.loop, self._stream.__aexit__, exc_type, exc_val, exc_tb)

    @override
    def __del__(self) -> None:
        # GC must not commit unfinished members or block the filesystem's loop.
        pass


class ZstarFileSystem(AsyncFileSystem, AbstractArchiveFileSystem):
    """Read or create an archive with async I/O and a standard sync facade.

    Use ``zstar://member::file:///path/archive.zstar`` or construct with ``fo``.
    Member paths are relative; ``""`` is the root. ``asynchronous=True`` enables
    fsspec's async methods and ``open_async``. Use ``await fs.close()`` for async
    cleanup. The default sync facade shares the same async reader. Use explicit
    close or a context manager; garbage-collection cleanup is best effort.

    ``mode="w"`` creates a new archive. Close each member before opening the
    next; ancestors are created automatically and duplicate names are rejected.
    Members spool to memory up to ``member_spool_bytes``, then to disk, since
    their sizes must be known before compression. ``writer_options`` configures
    the archive encoder. Normal filesystem close finalizes the archive;
    exceptional context exit discards it when the backend supports transactions.
    Supplied file objects are closed; supplied filesystems remain caller-owned.
    ``external`` is a directory URL or a borrowed filesystem rooted at external
    objects, stored as ``<lowercase sha256>.zst``. Files of at least
    ``external_threshold`` bytes (16 MiB by default) are externalized on write
    using FastCDC with 2/8/16 MiB minimum/target/maximum chunks. Custom resolvers
    belong to the direct API. ``reader_options`` accepts ``AsyncReaderOptions``,
    including the external SHA-256 verification policy. ``writer_options`` uses
    the same ``WriterOptions`` as the direct writer.

    Chained URLs and ``fs=`` use fsspec seekable range streams. Keep their
    underlying archive objects immutable. S3 paths use fsspec/s3fs credentials
    and configuration, whether supplied directly or through chaining. Source
    streams honor backend caching defaults; use a ``blockcache`` filesystem for
    disk caching. Async backends use direct range I/O without a stream cache.
    Supplied streams are owned and must remain immutable. Loading
    is lazy; concurrent requests share one load, and close drains admitted work.

    Member streams privately retain one decoded source within 2 MiB, including
    cache bookkeeping.
    Entry metadata and returned buffers are separate. Local downloads reject
    destination symlinks and require POSIX descriptor-relative no-follow I/O.
    Downloads copy bytes and reject symlink members; they do not restore metadata.
    """

    protocol = "zstar"
    root_marker = ""
    cachable = False
    async_impl = True
    mirror_sync_methods = True

    def __init__(
        self,
        fo: str | PathLike[str] | BinaryIO,
        mode: str = "r",
        target_protocol: str | None = None,
        target_options: Mapping[str, object] | None = None,
        fs: AbstractFileSystem | None = None,
        external: str | AbstractFileSystem | None = None,
        external_threshold: int = 16 << 20,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        member_spool_bytes: int = 8 << 20,
        writer_options: WriterOptions | None = None,
        reader_options: AsyncReaderOptions | None = None,
        **kwargs: object,
    ) -> None:
        if "member_cache_bytes" in kwargs:
            raise TypeError("member_cache_bytes is no longer configurable")
        if "resolver" in kwargs:
            raise TypeError("use external=filesystem; custom resolvers belong to the low-level API")
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)
        if mode not in {"r", "w"}:
            raise ValueError("mode must be 'r' or 'w'")
        if isinstance(external_threshold, bool) or not isinstance(external_threshold, int):
            raise TypeError("external_threshold must be an integer")
        if external_threshold < 0:
            raise ValueError("external_threshold must be nonnegative")
        if not isinstance(member_spool_bytes, int) or member_spool_bytes <= 0:
            raise ValueError("member_spool_bytes must be a positive integer")
        if fs is not None and (not isinstance(fo, (str, PathLike)) or target_options):
            raise ValueError(
                "fs requires an archive path and is already configured; omit target_options"
            )
        self.fo: str | PathLike[str] | BinaryIO = fo
        self.target_protocol: str | None = target_protocol
        self.mode: str = mode
        self.member_spool_bytes = member_spool_bytes
        self._reader_options = _native.ReaderConfig(reader_options)
        if reader_options is not None and reader_options.get("resolver") is not None:
            raise TypeError("use external=filesystem instead of a resolver")
        self._writer_options = _native.WriterConfig(writer_options)
        self._write_session: _WriteSession | None = None
        self._write_loading: asyncio.Task[_WriteSession] | None = None
        self._aborting = False
        self._filesystem = fs
        self.external: str | AbstractFileSystem | None = external
        self.external_threshold: int = external_threshold
        self._backend_options = dict(target_options or {})
        self._resources: _ArchiveResources | None = None
        self._loading: asyncio.Task[AsyncArchive] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._bound_loop: asyncio.AbstractEventLoop | None = None
        self._closed = False

    def _check_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._bound_loop is None:
            self._bound_loop = loop
        elif self._bound_loop is not loop:
            raise RuntimeError("filesystem belongs to another event loop")

    async def _load(self) -> AsyncArchive:
        resources = await self._open_archive()
        if self._closed:
            try:
                await resources.close()
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "operation and cleanup failed", [ValueError("filesystem closed"), cleanup]
                ) from None
            raise ValueError("filesystem closed")
        self._resources = resources
        return resources.archive

    def _backend(self, location: str) -> tuple[AbstractFileSystem, str]:
        from fsspec import filesystem
        from fsspec.core import url_to_fs

        if self._filesystem is not None:
            return self._filesystem, self._filesystem._strip_protocol(location)
        if self.target_protocol is not None:
            backend = filesystem(self.target_protocol, **self._backend_options)
            return backend, backend._strip_protocol(location)
        return url_to_fs(location, **self._backend_options)

    def _external_store(self) -> _ExternalObjects:
        if isinstance(self.external, str):
            from fsspec.core import url_to_fs

            backend, root = url_to_fs(self.external)
            return _ExternalObjects(backend, root)
        assert self.external is not None
        return _ExternalObjects(self.external, "")

    async def _load_writer(self) -> _WriteSession:
        external = await _writer_call(self._external_store) if self.external is not None else None
        if isinstance(self.fo, (str, PathLike)):
            backend, path = await _writer_call(partial(self._backend, fspath(self.fo)))
            publication = await _Publication.open(backend, path)
        else:
            publication = _Publication(self.fo)
        session = await _WriteSession.create(
            publication,
            self.member_spool_bytes,
            self._writer_options,
            external=external,
            external_threshold=self.external_threshold,
        )
        self._write_session = session
        return session

    async def _get_writer(self) -> _WriteSession:
        self._check_loop()
        if self._closed:
            raise ValueError("filesystem closed")
        if self.mode != "w":
            raise PermissionError("filesystem is open for reading")
        if self._write_session is not None:
            return self._write_session
        if self._write_loading is None:
            self._write_loading = asyncio.create_task(self._load_writer())
        task = self._write_loading
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._write_loading is task:
                self._write_loading = None

    async def _get_archive(self) -> AsyncArchive:
        self._check_loop()
        if self.mode != "r":
            raise PermissionError("filesystem is open for writing")
        if self._closed:
            raise ValueError("filesystem closed")
        if self._resources is not None:
            return self._resources.archive
        if self._loading is None:
            self._loading = asyncio.create_task(self._load())
        task = self._loading
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._loading is task:
                self._loading = None

    async def _open_archive(self) -> _ArchiveResources:
        external = await _writer_call(self._external_store) if self.external is not None else None
        options = self._reader_options.with_resolver(external)
        if not isinstance(self.fo, (str, PathLike)):
            try:
                archive = await AsyncArchive._from_config(
                    _native.BlockingAdapter(fileobj(self.fo)), options
                )
            except BaseException as primary:
                try:
                    await asyncio.to_thread(self.fo.close)
                except BaseException as cleanup:
                    raise BaseExceptionGroup(
                        "operation and cleanup failed", [primary, cleanup]
                    ) from None
                raise
            return _ArchiveResources(archive, self.fo)
        location = fspath(self.fo)
        if self._filesystem is not None or self.target_protocol is not None:
            return await self._open_fsspec(location, options)
        if self.target_protocol in (None, "http", "https") and location.startswith(
            ("http://", "https://")
        ):
            unknown = set(self._backend_options) - {"headers"}
            if unknown:
                raise ValueError("HTTP target_options supports headers only")
            source = await _acquire(
                _native.aopen_source(
                    location,
                    options=AsyncSourceOptions(
                        headers=cast("dict[str, str]", self._backend_options.get("headers") or {})
                    ),
                ),
                lambda source: source.aclose(),
            )
            return _ArchiveResources(await AsyncArchive._from_owned(source, options))
        return await self._open_fsspec(location, options)

    async def _open_fsspec(self, location: str, options: _native.ReaderConfig) -> _ArchiveResources:
        backend, path = await asyncio.to_thread(self._backend, location)
        if getattr(backend, "asynchronous", False):
            info = await backend._info(path)
            source = _AsyncFsspecSource(backend, path, info["size"])
            return _ArchiveResources(await AsyncArchive._from_config(source, options))
        # Let the backend choose its source cache, including disk-cache wrappers.
        file = cast(BinaryIO, await asyncio.to_thread(backend.open, path, "rb"))
        try:
            archive = await AsyncArchive._from_config(
                _native.BlockingAdapter(fileobj(file)), options
            )
        except BaseException as primary:
            try:
                await asyncio.to_thread(file.close)
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "operation and cleanup failed", [primary, cleanup]
                ) from None
            raise
        return _ArchiveResources(archive, file)

    async def _drain_close(self) -> None:
        if self.mode == "w":
            if self._write_loading is not None:
                with suppress(BaseException):
                    await self._write_loading
            if self._write_session is not None:
                await self._write_session.close(abort=self._aborting)
            elif not isinstance(self.fo, (str, PathLike)):
                await asyncio.to_thread(self.fo.close)
            return
        if self._loading is not None:
            with suppress(BaseException):
                await self._loading
        resources, self._resources = self._resources, None
        if resources is not None:
            await resources.close()
        elif not isinstance(self.fo, (str, PathLike)) and not self.fo.closed:
            await asyncio.to_thread(self.fo.close)

    async def _close(self) -> None:
        self._check_loop()
        if self.mode == "w" and not self._closed and not self._aborting:
            await self._get_writer()
        if self._closed and self._close_task is not None and self._close_task.done():
            return
        self._closed = True
        if self._close_task is None:
            self._close_task = _retain_cleanup(asyncio.create_task(self._drain_close()))
        await asyncio.shield(self._close_task)

    def close(self) -> Coroutine[object, object, None] | None:
        """Close storage; await the result for an asynchronous filesystem."""
        if self.asynchronous:
            return self._close()
        sync(self.loop, self._close)
        return None

    def __enter__(self) -> ZstarFileSystem:
        if self.asynchronous:
            raise RuntimeError("use async with for an asynchronous filesystem")
        if self._closed:
            raise ValueError("filesystem closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        sync(self.loop, self.__aexit__, exc_type, exc_value, traceback)

    async def _finalize(self) -> None:
        self._aborting = True
        with suppress(Exception):
            await self._close()

    def _start_finalizer(self) -> None:
        _retain_cleanup(asyncio.create_task(self._finalize()))

    def __del__(self) -> None:
        if not hasattr(self, "_closed") or self._closed:
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        owner_loop = self._bound_loop or (self._loop if not self.asynchronous else None)
        if running_loop is not None and running_loop is owner_loop:
            self._start_finalizer()
        elif owner_loop is not None and owner_loop.is_running():
            # GC can run while fsspec's sync facade holds an Event condition.
            # Waiting here would stop that loop completing the enclosing call.
            with suppress(RuntimeError):
                owner_loop.call_soon_threadsafe(self._start_finalizer)
        elif not isinstance(self.fo, (str, PathLike)):
            with suppress(Exception):
                self.fo.close()

    async def __aenter__(self) -> ZstarFileSystem:
        if self._closed:
            raise ValueError("filesystem closed")
        return self

    async def __aexit__(self, exc_type: object, *args: object) -> None:
        self._aborting = self._aborting or exc_type is not None
        try:
            await self._close()
        except BaseException as cleanup:
            if args and isinstance(args[0], BaseException):
                raise BaseExceptionGroup(
                    "operation and cleanup failed", [args[0], cleanup]
                ) from None
            raise

    @classmethod
    def _strip_protocol(cls, path: str | list[str]) -> str | list[str]:
        stripped = super()._strip_protocol(path)
        if isinstance(stripped, list):
            return [cast(str, cls._strip_protocol(item)) for item in stripped]
        return "/".join(part for part in stripped.split("/") if part not in ("", "."))

    @staticmethod
    async def _lookup(archive: AsyncArchive, member: str) -> Entry:
        """Look up the exact entry without following symlinks."""
        try:
            return await archive.entry(member)
        except KeyError:
            raise FileNotFoundError(member) from None

    @overload
    async def open_async(
        self, path: str, mode: Literal["rb"] = "rb", **kwargs: object
    ) -> _AsyncMemberFile: ...

    @overload
    async def open_async(
        self, path: str, mode: Literal["wb", "xb"], **kwargs: object
    ) -> _StagedMember: ...

    @overload
    async def open_async(
        self, path: str, mode: str, **kwargs: object
    ) -> _AsyncMemberFile | _StagedMember: ...

    async def open_async(
        self, path: str, mode: str = "rb", **kwargs: object
    ) -> _AsyncMemberFile | _StagedMember:
        member = cast(str, self._strip_protocol(path))
        if self.mode == "w":
            if mode not in {"wb", "xb"} or kwargs:
                raise NotImplementedError(
                    "writer members support wb or xb without buffering options"
                )
            session = await self._get_writer()
            return await session.open(member)
        if mode != "rb" or kwargs:
            raise NotImplementedError(
                "open_async supports read-only binary members without buffering options"
            )
        if not member:
            raise IsADirectoryError(path)
        archive = await self._get_archive()
        try:
            stream = await archive.open(member)
            await stream.__aenter__()
        except KeyError:
            raise FileNotFoundError(path) from None
        except IsADirectoryError:
            raise IsADirectoryError(path) from None
        except ZstarError:
            # Only failures pay for a second lookup to classify symlinks.
            entry = await self._lookup(archive, member)
            if entry.kind is EntryKind.SYMLINK:
                raise OSError(errno.ELOOP, "cannot open a symlink as a file", path) from None
            raise
        try:
            return _AsyncMemberFile(self, member, stream)
        except BaseException:
            await stream.__aexit__(None, None, None)
            raise

    def _open(
        self, path: str, mode: str = "rb", **kwargs: object
    ) -> _MemberFile | _WriterMemberFile:
        if self.mode == "w":
            if kwargs.get("autocommit") is False:
                raise NotImplementedError("member transactions are unsupported; commit the archive")
            stream = sync(self.loop, self.open_async, path, mode)
            return _WriterMemberFile(self, cast(_StagedMember, stream))
        stream = sync(self.loop, self.open_async, path, mode)
        return _MemberFile(self, cast(str, self._strip_protocol(path)), stream, **kwargs)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: object) -> None:
        session = await self._get_writer()
        await session.mkdir(cast(str, self._strip_protocol(path)), parents=create_parents)

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        session = await self._get_writer()
        await session.mkdir(cast(str, self._strip_protocol(path)), exist_ok=exist_ok)

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        mode: str = "overwrite",
        callback: ProgressCallback = DEFAULT_CALLBACK,
        **kwargs: object,
    ) -> None:
        if mode not in {"overwrite", "create"} or kwargs:
            raise NotImplementedError("put_file supports overwrite or create without extra options")
        session = await self._get_writer()
        path = cast(str, self._strip_protocol(rpath))
        if await _writer_call(partial(os.path.isdir, lpath)):
            await session.mkdir(path, exist_ok=True)
            return
        source = await _writer_call(partial(open, lpath, "rb"), discard=lambda file: file.close())
        try:
            size = await _writer_call(lambda: os.fstat(source.fileno()).st_size)
            callback.set_size(size)
            await session.add_file(path, source, size)
            callback.relative_update(size)
        finally:
            await _writer_call(source.close)

    async def _pipe_file(
        self,
        path: str,
        value: bytes,
        mode: str = "overwrite",
        **kwargs: object,
    ) -> None:
        """Insert one in-memory member using fsspec's standard ``pipe_file`` API."""
        if mode not in {"overwrite", "create"} or kwargs:
            raise NotImplementedError(
                "pipe_file supports overwrite or create without extra options"
            )
        session = await self._get_writer()
        await session.add_file(cast(str, self._strip_protocol(path)), io.BytesIO(value), len(value))

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: object
    ) -> bytes:
        if self.mode != "r":
            raise PermissionError("filesystem is open for writing")
        async with await self.open_async(path, "rb") as stream:
            start, end = _range_bounds(stream.size, start, end)
            stream.seek(start)
            return await stream.read(end - start)

    async def _get_file(
        self,
        rpath: str,
        lpath: str | PathLike[str] | BinaryIO | None = None,
        callback: ProgressCallback = DEFAULT_CALLBACK,
        outfile: BinaryIO | None = None,
        **kwargs: object,
    ) -> None:
        """Stream a member to a local path or a borrowed binary output stream."""
        if outfile is None and hasattr(lpath, "write"):
            outfile = cast(BinaryIO, lpath)
        if outfile is None:
            if not isinstance(lpath, (str, PathLike)):
                raise TypeError("get_file requires a local path or binary output stream")
            destination = Path(lpath)
        else:
            destination = None
        try:
            stream = await self.open_async(rpath, "rb", **kwargs)
        except IsADirectoryError:
            if destination is None:
                raise
            await _writer_call(partial(_local_destination, destination, directory=True))
            return
        async with stream:
            if destination is not None:
                outfile = await _writer_call(
                    partial(_local_destination, destination),
                    discard=lambda file: file.close() if file is not None else None,
                )
            assert outfile is not None
            try:
                callback.set_size(stream.size)
                while data := await stream.read(self.blocksize):
                    await _writer_call(partial(_write_all, outfile, data))
                    callback.relative_update(len(data))
            finally:
                if destination is not None:
                    await _writer_call(outfile.close)

    async def _cat_ranges(
        self,
        paths: list[str],
        starts: int | list[int | None] | None,
        ends: int | list[int | None] | None,
        max_gap: int | None = None,
        batch_size: int | None = None,
        on_error: str = "return",
        **kwargs: object,
    ) -> list[bytes | Exception]:
        """Read several member ranges, batching their archive work in Rust."""
        if kwargs:
            raise TypeError(f"unsupported cat_ranges options: {', '.join(kwargs)}")
        if not isinstance(paths, list):
            raise TypeError("paths must be a list")
        if max_gap not in (None, 0):
            raise NotImplementedError("max_gap coalescing is not supported")
        start_list = starts if isinstance(starts, list) else [starts] * len(paths)
        end_list = ends if isinstance(ends, list) else [ends] * len(paths)
        if len(start_list) != len(paths) or len(end_list) != len(paths):
            raise ValueError("paths, starts, and ends must have matching lengths")
        if on_error not in {"return", "raise"}:
            raise ValueError("on_error must be 'return' or 'raise'")
        if batch_size is not None and batch_size != -1 and batch_size <= 0:
            raise ValueError("batch_size must be positive or -1")
        if not paths:
            return []
        archive = await self._get_archive()
        results: list[bytes | Exception] = []
        requests: list[ReadRequest] = []
        positions: list[int] = []
        members: dict[str, Entry] = {}
        for index, (path, raw_start, raw_end) in enumerate(
            zip(paths, start_list, end_list, strict=True)
        ):
            try:
                member = cast(str, self._strip_protocol(path))
                if not member:
                    raise IsADirectoryError(path)
                if member not in members:
                    members[member] = await self._lookup(archive, member)
                entry = members[member]
                if entry.kind is EntryKind.SYMLINK:
                    raise OSError(errno.ELOOP, "cannot read a symlink as a file", path)
                if entry.kind is not EntryKind.FILE:
                    raise IsADirectoryError(path)
                start, end = _range_bounds(entry.size, raw_start, raw_end)
                if end > start:
                    requests.append(ReadRequest(entry.path, start, end - start))
                    positions.append(index)
                results.append(b"")
            except Exception as exc:
                if on_error == "raise":
                    raise
                results.append(exc)
        batch = 4096 if batch_size is None or batch_size == -1 else min(batch_size, 4096)
        for offset in range(0, len(requests), batch):
            selected = requests[offset : offset + batch]
            indices = positions[offset : offset + batch]
            try:
                values = await archive.read_many(selected)
                for index, value in zip(indices, values, strict=True):
                    results[index] = value
            except Exception:
                # Preserve per-range errors and partial success when a batch
                # contains one invalid range or missing external object.
                for request, index in zip(selected, indices, strict=True):
                    try:
                        results[index] = await self._cat_file(
                            request.path,
                            request.offset,
                            None if request.length is None else request.offset + request.length,
                        )
                    except Exception as individual:
                        if on_error == "raise":
                            raise
                        results[index] = individual
        return results

    async def _info(self, path: str, **kwargs: object) -> FileInfo:
        member = cast(str, self._strip_protocol(path))
        if self.mode == "w":
            session = await self._get_writer()
            entries = session.snapshot()
            if not member:
                return {"name": "", "type": "directory", "size": 0}
            if member not in entries:
                raise FileNotFoundError(member)
            return entries[member]
        archive = await self._get_archive()
        if not member:
            return {"name": "", "type": "directory", "size": 0}
        return _entry_info(member, await self._lookup(archive, member))

    def info(self, path: str, **kwargs: object) -> FileInfo:
        return sync(self.loop, self._info, path, **kwargs)

    async def _modified(self, path: str) -> datetime:
        member = cast(str, self._strip_protocol(path))
        archive = await self._get_archive()
        ns = (await self._lookup(archive, member)).mtime_ns if member else 0
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=ns // 1000)

    def modified(self, path: str) -> datetime:
        return sync(self.loop, self._modified, path)

    @overload
    def ls(self, path: str, detail: Literal[True] = True, **kwargs: object) -> list[FileInfo]: ...
    @overload
    def ls(self, path: str, detail: Literal[False], **kwargs: object) -> list[str]: ...
    @overload
    def ls(self, path: str, detail: bool, **kwargs: object) -> list[FileInfo] | list[str]: ...
    def ls(self, path: str, detail: bool = True, **kwargs: object) -> list[FileInfo] | list[str]:
        return sync(self.loop, self._ls, path, detail=detail, **kwargs)

    async def _ls(
        self, path: str, detail: bool = True, **kwargs: object
    ) -> list[FileInfo] | list[str]:
        member = cast(str, self._strip_protocol(path))
        if self.mode == "w":
            session = await self._get_writer()
            entries = session.snapshot()
            if member and member not in entries:
                raise FileNotFoundError(member)
            if member and entries[member]["type"] != "directory":
                return [entries[member]] if detail else [member]
            prefix = member + "/" if member else ""
            items: list[FileInfo] = [
                info
                for name, info in sorted(entries.items())
                if name.startswith(prefix) and "/" not in name[len(prefix) :] and name != member
            ]
            return items if detail else [item["name"] for item in items]
        archive = await self._get_archive()
        requested = member
        if member:
            entry = await self._lookup(archive, member)
            if entry.kind is not EntryKind.DIRECTORY:
                return [_entry_info(member, entry)] if detail else [member]
            member = entry.path
        prefix = member + "/" if member else ""
        display_prefix = requested + "/" if requested else ""
        if not detail:
            return [
                f"{display_prefix}{name[len(prefix) :]}"
                async for entry in archive.entries(prefix)
                if "/" not in (name := entry.path)[len(prefix) :]
            ]
        items = []
        async for entry in archive.entries(prefix):
            if "/" in entry.path[len(prefix) :]:
                continue
            items.append(_entry_info(f"{display_prefix}{entry.path[len(prefix) :]}", entry))
        return items
