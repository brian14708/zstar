"""Native async archive filesystems with fsspec's synchronous facade."""

from __future__ import annotations

import asyncio
import errno
import io
import posixpath
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from functools import partial
from os import PathLike, fspath
from pathlib import Path
from tarfile import TarInfo
from types import TracebackType
from typing import Any, BinaryIO, Literal, TypedDict, cast, overload

from fsspec.archive import AbstractArchiveFileSystem
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem, sync
from fsspec.callbacks import DEFAULT_CALLBACK
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from typing_extensions import Buffer, override

from ._archive import AsyncArchive, _link_target
from ._file import AsyncArchiveFile
from ._fs_writer import _member_name, _StagedMember, _WriteSession
from ._info import _nanoseconds
from ._writer import _writer_call
from .types import ReaderOptions, WriterOptions

__all__ = ["FileInfo", "ZstarFileSystem"]


def _write_all(output: BinaryIO, data: bytes) -> None:
    """Honor short writes from caller-supplied streams."""
    remaining = memoryview(data)
    while remaining:
        count = output.write(remaining)
        if count is None:
            return
        if count <= 0 or count > len(remaining):
            raise OSError("invalid output stream write count")
        remaining = remaining[count:]


def _range_bounds(size: int, start: int | None, end: int | None) -> tuple[int, int]:
    """Clip fsspec's positive and end-relative offsets to a member's bounds."""
    start, end, _ = slice(start, end).indices(size)
    return start, max(start, end)


class FileInfo(TypedDict):
    name: str
    size: int
    type: Literal["file", "directory"]


def _file_info(name: str, entry: TarInfo) -> FileInfo:
    return {"name": name, "size": entry.size, "type": "directory" if entry.isdir() else "file"}


class _AsyncFsspecSource:
    """Exact async backend ranges bridged from native callback workers."""

    def __init__(self, backend: AbstractFileSystem, path: str, size: int):
        self.backend, self.path, self.size = backend, path, size
        self.loop = asyncio.get_running_loop()

    def read_range(self, offset: int, length: int) -> bytes:
        return cast(
            bytes,
            asyncio.run_coroutine_threadsafe(
                self.backend._cat_file(self.path, start=offset, end=offset + length), self.loop
            ).result(),
        )


class _ExternalObjects:
    """Borrowed filesystem rooted at the encoded object directory."""

    def __init__(self, fs: AbstractFileSystem, root: str, loop: asyncio.AbstractEventLoop):
        self.fs, self.root = fs, root.rstrip("/")
        self.loop = loop
        self._prepared = False

    def _path(self, address: str) -> str:
        name = f"{address.removeprefix('sha256:')}.zst"
        return f"{self.root}/{name}" if self.root else name

    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes:
        path = self._path(address)
        if getattr(self.fs, "asynchronous", False):
            data = cast(
                bytes,
                asyncio.run_coroutine_threadsafe(
                    self.fs._cat_file(path, start=0, end=max_bytes + 1), self.loop
                ).result(),
            )
        else:
            with self.fs.open(path, "rb", cache_type="none") as file:
                data = file.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise ValueError("external object exceeds encoded size limit")
        return data

    def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> int:
        path = self._path(address)

        if getattr(self.fs, "asynchronous", False):
            try:
                info = asyncio.run_coroutine_threadsafe(self.fs._info(path), self.loop).result()
            except FileNotFoundError:
                pass
            else:
                return int(info["size"])
            asyncio.run_coroutine_threadsafe(self.fs._pipe_file(path, encoded), self.loop).result()
            return len(encoded)
        try:
            info = self.fs.info(path)
        except FileNotFoundError:
            pass
        else:
            return int(info["size"])
        if not self._prepared:
            parent = self.fs._parent(path)
            if parent or self.fs.protocol == "dir":
                self.fs.makedirs(parent, exist_ok=True)
            self._prepared = True
        file = self.fs.open(path, "wb", autocommit=False)
        try:
            position = 0
            while position < len(encoded):
                count = file.write(memoryview(encoded)[position:])
                if not isinstance(count, int) or count <= 0 or count > len(encoded) - position:
                    raise OSError("invalid external object write count")
                position += count
            file.close()
            commit = getattr(file, "commit", None)
            if commit is not None:
                commit()
        except BaseException as primary:
            try:
                discard = getattr(file, "discard", None)
                if discard is not None:
                    try:
                        discard()
                    finally:
                        if isinstance(file, AbstractBufferedFile):
                            file.forced = True
                        file.close()
                else:
                    file.close()
            except BaseException as cleanup:
                raise BaseExceptionGroup(
                    "operation and cleanup failed", [primary, cleanup]
                ) from None
            raise
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
    def __init__(self, fs: ZstarFileSystem, path: str, stream: AsyncArchiveFile, size: int):
        self._stream = stream
        self._range_lock = asyncio.Lock()
        super().__init__(fs, path, mode="rb", size=size, cache_type="none")

    async def read(self, length: int | None = -1) -> bytes:
        return cast(bytes, await super().read(length))

    def seek(self, offset: int, whence: int = 0) -> int:
        return cast(int, super().seek(offset, whence))

    async def readinto(self, buffer: Buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        with memoryview(buffer).cast("B") as view:
            if view.readonly:
                raise TypeError("readinto requires a writable buffer")
            async with self._range_lock:
                await self._stream.seek(self.loc)
                count = await self._stream.readinto(view)
                self.loc += count
                return count

    async def _fetch_range(self, start: int, end: int) -> bytes:
        end = min(end, self.size)
        output = bytearray(max(0, end - start))
        async with self._range_lock:
            await self._stream.seek(start)
            count = await self._stream.readinto(output)
        return bytes(memoryview(output)[:count])

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            await self._stream.close()

    def __del__(self) -> None:
        # The stream borrows the archive. Dropping it releases its cursor;
        # calling an async close from AbstractBufferedFile.__del__ would leak a coroutine.
        self.closed = True


class _MemberFile(AbstractBufferedFile):
    def __init__(self, fs: ZstarFileSystem, path: str, stream: _AsyncMemberFile, **kwargs: object):
        self._stream = stream
        kwargs.setdefault("cache_type", "none")
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


class _AsyncSink:
    """Bridge native worker writes to a caller-supplied async backend."""

    def __init__(self, stream: object, loop: asyncio.AbstractEventLoop):
        self.stream, self.loop = cast(Any, stream), loop

    def write(self, data: Buffer) -> int:
        return asyncio.run_coroutine_threadsafe(self.stream.write(bytes(data)), self.loop).result()

    def flush(self) -> None:
        # Async streamed backends flush buffered output when closed.
        pass

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self.stream.close(), self.loop).result()


class _AsyncWriterMember:
    mode = "wb"

    def __init__(self, member: _StagedMember):
        self._member = member
        self.path = member.name

    @property
    def closed(self) -> bool:
        return self._member.closed

    async def write(self, data: Buffer) -> int:
        return await _writer_call(partial(self._member.write, data))

    async def seek(self, offset: int, whence: int = 0) -> int:
        return await _writer_call(partial(self._member.seek, offset, whence))

    async def tell(self) -> int:
        return await _writer_call(self._member.tell)

    async def flush(self) -> None:
        await _writer_call(self._member.flush)

    async def close(self) -> None:
        await _writer_call(self._member.close)

    async def __aenter__(self) -> _AsyncWriterMember:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self

    async def __aexit__(self, exc_type: object, *args: object) -> None:
        await _writer_call(partial(self._member.close, abort=exc_type is not None))


class _WriterMemberFile(io.BufferedIOBase):
    mode = "wb"

    def __init__(self, fs: ZstarFileSystem, stream: _AsyncWriterMember):
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
    fsspec's async methods and ``open_async``. Use ``await fs.aclose()`` for async
    cleanup. The default sync facade shares the same async reader and caches.

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
    belong to the low-level API.

    Chained URLs and ``fs=`` use fsspec seekable range streams. Keep their
    underlying archive objects immutable. S3 paths use fsspec/s3fs credentials
    and configuration, whether supplied directly or through chaining.
    Supplied streams are owned and must remain immutable. Loading
    is lazy; concurrent requests share one load, and close drains admitted work.
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
        cache_bytes: int = 16 << 20,
        verify_external: bool = True,
        external: str | AbstractFileSystem | None = None,
        external_threshold: int = 16 << 20,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        member_spool_bytes: int = 8 << 20,
        writer_options: WriterOptions | None = None,
        reader_options: ReaderOptions | None = None,
        metadata: Mapping[str, str] | None = None,
        **kwargs: object,
    ) -> None:
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
        if not isinstance(cache_bytes, int) or cache_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        if fs is not None and (not isinstance(fo, (str, PathLike)) or target_options):
            raise ValueError(
                "fs requires an archive path and is already configured; omit target_options"
            )
        self.fo, self.target_protocol, self.mode = fo, target_protocol, mode
        self.member_spool_bytes = member_spool_bytes
        self._reader_options = cast(ReaderOptions, dict(reader_options or {}))
        if "resolver" in self._reader_options:
            raise TypeError("use external=filesystem instead of a resolver")
        self._writer_options = cast(WriterOptions, dict(writer_options or {}))
        self._metadata = dict(metadata or {})
        if any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in self._metadata.items()
        ):
            raise TypeError("metadata keys and values must be strings")
        if self._metadata and (mode != "w" or not isinstance(fo, (str, PathLike))):
            raise ValueError("metadata requires mode='w' and an archive path")
        self._write_session: _WriteSession | None = None
        self._write_loading: asyncio.Task[_WriteSession] | None = None
        self._write_lock = asyncio.Lock()
        self._aborting = False
        self._filesystem = fs
        self.cache_bytes, self.verify_external, self.external, self.external_threshold = (
            cache_bytes,
            verify_external,
            external,
            external_threshold,
        )
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

    def _external_store(self, loop: asyncio.AbstractEventLoop) -> _ExternalObjects:
        if isinstance(self.external, str):
            from fsspec.core import url_to_fs

            backend, root = url_to_fs(self.external)
            return _ExternalObjects(backend, root, loop)
        assert self.external is not None
        return _ExternalObjects(self.external, "", loop)

    def _new_write_session(self, loop: asyncio.AbstractEventLoop) -> _WriteSession:
        commit: Callable[[], None] | None = None
        discard: Callable[[], None] | None = None
        after_commit: Callable[[], None] | None = None
        if isinstance(self.fo, (str, PathLike)):
            backend, path = self._backend(fspath(self.fo))
            backend_async = bool(getattr(backend, "asynchronous", False))
            setxattr = getattr(backend, "_setxattr" if backend_async else "setxattr", None)
            if self._metadata and not callable(setxattr):
                raise NotImplementedError(
                    "metadata requires a backend with setxattr(), such as s3fs"
                )
            if backend_async:
                stream = asyncio.run_coroutine_threadsafe(
                    backend.open_async(path, "wb"), loop
                ).result()
                output = cast(BinaryIO, _AsyncSink(stream, loop))
            else:
                output = cast(BinaryIO, backend.open(path, "wb", autocommit=False))
                commit = getattr(output, "commit", None)
                discard = getattr(output, "discard", None)
            if self._metadata:
                setxattr_fn = cast(Callable[..., Any], setxattr)

                def update_metadata() -> None:
                    if backend_async:
                        asyncio.run_coroutine_threadsafe(
                            setxattr_fn(path, **self._metadata), loop
                        ).result()
                    else:
                        setxattr_fn(path, **self._metadata)

                after_commit = update_metadata
        else:
            output = self.fo
        try:
            external = self._external_store(loop) if self.external is not None else None
            return _WriteSession(
                output,
                self.member_spool_bytes,
                self._writer_options,
                commit,
                discard,
                after_commit,
                external=external.put_encoded if external is not None else None,
                external_threshold=self.external_threshold,
            )
        except BaseException:
            try:
                output.close()
            finally:
                if discard is not None:
                    discard()
            raise

    async def _load_writer(self) -> _WriteSession:
        session = await _writer_call(partial(self._new_write_session, asyncio.get_running_loop()))
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
        external = (
            await asyncio.to_thread(self._external_store, asyncio.get_running_loop())
            if self.external is not None
            else None
        )
        options: ReaderOptions = {
            "cache_bytes": self.cache_bytes,
            "verify_external": self.verify_external,
            "resolver": external,
        }
        options.update(self._reader_options)
        if not isinstance(self.fo, (str, PathLike)):
            try:
                archive = await AsyncArchive.from_file(self.fo, **options)
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
            archive = await AsyncArchive.from_url(
                location,
                headers=cast("dict[str, str] | None", self._backend_options.get("headers")),
                **options,
            )
            return _ArchiveResources(archive)
        return await self._open_fsspec(location, options)

    async def _open_fsspec(self, location: str, options: ReaderOptions) -> _ArchiveResources:
        backend, path = await asyncio.to_thread(self._backend, location)
        if getattr(backend, "asynchronous", False):
            info = await backend._info(path)
            source = _AsyncFsspecSource(backend, path, info["size"])
            return _ArchiveResources(await AsyncArchive.from_source(source, source.size, **options))
        # A seekable fsspec file fetches ranges. Disable its read-ahead cache:
        # the indexed reader already owns bounded decoded caching.
        file = cast(BinaryIO, await asyncio.to_thread(backend.open, path, "rb", cache_type="none"))
        try:
            archive = await AsyncArchive.from_file(file, **options)
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
                await _writer_call(partial(self._write_session.close, abort=self._aborting))
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

    async def aclose(self) -> None:
        self._check_loop()
        if self.mode == "w" and not self._closed and not self._aborting:
            await self._get_writer()
        if self._closed and self._close_task is not None and self._close_task.done():
            return
        self._closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._drain_close())
        await asyncio.shield(self._close_task)

    def close(self) -> None:
        if self.asynchronous:
            raise RuntimeError("use await fs.aclose() for an asynchronous filesystem")
        sync(self.loop, self.aclose)

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
            await self.aclose()

    def __del__(self) -> None:
        if not hasattr(self, "_closed") or self._closed:
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        owner_loop = self._bound_loop or (self._loop if not self.asynchronous else None)
        if running_loop is not None and running_loop is owner_loop:
            running_loop.create_task(self._finalize())
        elif not self.asynchronous:
            self._aborting = True
            with suppress(Exception):
                self.close()
        elif self._bound_loop is not None and self._bound_loop.is_running():
            self._bound_loop.call_soon_threadsafe(lambda: asyncio.create_task(self._finalize()))
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
            await self.aclose()
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
    async def _resolve(archive: AsyncArchive, member: str) -> TarInfo:
        """Resolve final and ancestor links within the archive, at most 40 hops."""
        current = member
        visited: set[str] = set()
        for _ in range(41):
            if current in visited:
                break
            visited.add(current)
            parts = current.split("/")
            for i in range(len(parts)):
                path = "/".join(parts[: i + 1])
                try:
                    info = await archive.getinfo(path)
                except KeyError:
                    raise FileNotFoundError(member) from None
                if info.issym():
                    current = posixpath.normpath(
                        posixpath.join(_link_target(info), *parts[i + 1 :])
                    )
                    break
                if i == len(parts) - 1:
                    return info
                if not info.isdir():
                    raise NotADirectoryError(member)
            else:  # pragma: no cover
                raise FileNotFoundError(member)
        raise OSError(errno.ELOOP, "cyclic archive symlink or too many links", member)

    @overload
    async def open_async(
        self, path: str, mode: Literal["rb"] = "rb", **kwargs: object
    ) -> _AsyncMemberFile: ...

    @overload
    async def open_async(
        self, path: str, mode: Literal["wb", "xb"], **kwargs: object
    ) -> _AsyncWriterMember: ...

    @overload
    async def open_async(
        self, path: str, mode: str, **kwargs: object
    ) -> _AsyncMemberFile | _AsyncWriterMember: ...

    async def open_async(
        self, path: str, mode: str = "rb", **kwargs: object
    ) -> _AsyncMemberFile | _AsyncWriterMember:
        member = cast(str, self._strip_protocol(path))
        if self.mode == "w":
            if mode not in {"wb", "xb"} or kwargs:
                raise NotImplementedError(
                    "writer members support wb or xb without buffering options"
                )
            session = await self._get_writer()
            member = _member_name(member)
            staged = await _writer_call(
                partial(session.open, member),
                discard=lambda staged: staged.close(abort=True),
            )
            return _AsyncWriterMember(staged)
        if mode != "rb" or kwargs:
            raise NotImplementedError(
                "open_async supports read-only binary members without buffering options"
            )
        if not member:
            raise IsADirectoryError(path)
        archive = await self._get_archive()
        entry = await self._resolve(archive, member)
        if not entry.isfile():
            raise IsADirectoryError(path)
        stream = await archive._open_member(entry.name)
        return _AsyncMemberFile(self, member, stream, entry.size)

    def _open(
        self, path: str, mode: str = "rb", **kwargs: object
    ) -> _MemberFile | _WriterMemberFile:
        if self.mode == "w":
            if kwargs.get("autocommit") is False:
                raise NotImplementedError("member transactions are unsupported; commit the archive")
            stream = sync(self.loop, self.open_async, path, mode)
            return _WriterMemberFile(self, cast(_AsyncWriterMember, stream))
        stream = sync(self.loop, self.open_async, path, mode)
        return _MemberFile(self, cast(str, self._strip_protocol(path)), stream, **kwargs)

    async def _mkdir(self, path: str, create_parents: bool = True, **kwargs: object) -> None:
        session = await self._get_writer()
        await _writer_call(
            partial(session.mkdir, cast(str, self._strip_protocol(path)), parents=create_parents)
        )

    async def _makedirs(self, path: str, exist_ok: bool = False) -> None:
        session = await self._get_writer()
        await _writer_call(
            partial(session.mkdir, cast(str, self._strip_protocol(path)), exist_ok=exist_ok)
        )

    async def _put_file(
        self,
        lpath: str,
        rpath: str,
        mode: str = "overwrite",
        callback: Any = DEFAULT_CALLBACK,
        **kwargs: object,
    ) -> None:
        if mode not in {"overwrite", "create"} or kwargs:
            raise NotImplementedError("put_file supports overwrite or create without extra options")
        # fsspec batches put_file concurrently. Archive insertion stays serial.
        async with self._write_lock:
            session = await self._get_writer()

            def copy() -> None:
                import os

                if os.path.isdir(lpath):
                    session.mkdir(cast(str, self._strip_protocol(rpath)), exist_ok=True)
                    return
                with open(lpath, "rb") as source:
                    size = os.fstat(source.fileno()).st_size
                    callback.set_size(size)
                    session.add_file(cast(str, self._strip_protocol(rpath)), source, size)
                    callback.relative_update(size)

            await _writer_call(copy)

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
        async with self._write_lock:
            session = await self._get_writer()

            def write() -> None:
                session.add_file(
                    cast(str, self._strip_protocol(path)), io.BytesIO(value), len(value)
                )

            await _writer_call(write)

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
        callback: Any = DEFAULT_CALLBACK,
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
            await _writer_call(partial(destination.mkdir, parents=True, exist_ok=True))
            return
        async with stream:
            if destination is not None:
                await _writer_call(partial(destination.parent.mkdir, parents=True, exist_ok=True))
                outfile = await _writer_call(
                    partial(destination.open, "wb"), discard=lambda file: file.close()
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
        requests: list[tuple[str, int, int]] = []
        positions: list[int] = []
        members: dict[str, TarInfo] = {}
        for index, (path, raw_start, raw_end) in enumerate(
            zip(paths, start_list, end_list, strict=True)
        ):
            try:
                member = cast(str, self._strip_protocol(path))
                if not member:
                    raise IsADirectoryError(path)
                if member not in members:
                    members[member] = await self._resolve(archive, member)
                info = members[member]
                if not info.isfile():
                    raise IsADirectoryError(path)
                start, end = _range_bounds(info.size, raw_start, raw_end)
                if end > start:
                    requests.append((info.name, start, end - start))
                    positions.append(index)
                results.append(b"")
            except Exception as exc:
                if on_error == "raise":
                    raise
                results.append(exc)
        batch = 4096 if batch_size in (None, -1) else min(batch_size, 4096)
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
                for (name, start, length), index in zip(selected, indices, strict=True):
                    try:
                        results[index] = await self._cat_file(name, start, start + length)
                    except Exception as individual:
                        if on_error == "raise":
                            raise
                        results[index] = individual
        return results

    async def _info(self, path: str, **kwargs: object) -> FileInfo:
        member = cast(str, self._strip_protocol(path))
        if self.mode == "w":
            session = await self._get_writer()
            entries = await _writer_call(session.snapshot)
            if not member:
                return {"name": "", "type": "directory", "size": 0}
            if member not in entries:
                raise FileNotFoundError(member)
            return _file_info(member, entries[member])
        archive = await self._get_archive()
        if not member:
            return {"name": "", "type": "directory", "size": 0}
        return _file_info(member, await self._resolve(archive, member))

    def info(self, path: str, **kwargs: object) -> FileInfo:
        return sync(self.loop, self._info, path, **kwargs)

    async def _modified(self, path: str) -> datetime:
        member = cast(str, self._strip_protocol(path))
        archive = await self._get_archive()
        info = await self._resolve(archive, member) if member else None
        ns = _nanoseconds(info.pax_headers.get("mtime", info.mtime)) if info else 0
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
            entries = await _writer_call(session.snapshot)
            if member and member not in entries:
                raise FileNotFoundError(member)
            if member and not entries[member].isdir():
                return [_file_info(member, entries[member])] if detail else [member]
            prefix = member + "/" if member else ""
            items = [
                _file_info(name, entry)
                for name, entry in sorted(entries.items())
                if name.startswith(prefix) and "/" not in name[len(prefix) :] and name != member
            ]
            return items if detail else [item["name"] for item in items]
        archive = await self._get_archive()
        requested = member
        if member:
            entry = await self._resolve(archive, member)
            if not entry.isdir():
                return [_file_info(member, entry)] if detail else [member]
            member = entry.name
        prefix = member + "/" if member else ""
        display_prefix = requested + "/" if requested else ""
        if not detail:
            return [
                f"{display_prefix}{name[len(prefix) :]}"
                async for entry in archive.members(prefix)
                if "/" not in (name := entry.name)[len(prefix) :]
            ]
        items: list[FileInfo] = []
        async for entry in archive.members(prefix):
            if "/" in entry.name[len(prefix) :]:
                continue
            target = entry
            if entry.issym():
                # Dangling links list as the link itself.
                with suppress(OSError):
                    target = await self._resolve(archive, entry.name)
            items.append(_file_info(f"{display_prefix}{entry.name[len(prefix) :]}", target))
        return items
