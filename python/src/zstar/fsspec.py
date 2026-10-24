"""Async fsspec integration for reading members from Zstar archives."""

from __future__ import annotations

import asyncio
import errno
import posixpath
from collections.abc import Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from os import PathLike, fspath
from typing import BinaryIO, Literal, TypedDict, cast, overload
from urllib.parse import unquote, urlsplit

try:
    from fsspec.archive import AbstractArchiveFileSystem
    from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem, sync
    from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
except ModuleNotFoundError as exc:  # pragma: no cover
    if exc.name != "fsspec":
        raise
    raise ImportError("install zstar-py[fsspec] to use ZstarFileSystem") from exc

from typing_extensions import Buffer

from . import AsyncArchive
from ._archive import _link_target
from ._file import AsyncArchiveFile
from ._info import TarInfo
from .types import ObjectResolver, ReaderOptions


class FileInfo(TypedDict):
    name: str
    size: int
    type: Literal["file", "directory"]


def _file_info(name: str, entry: TarInfo) -> FileInfo:
    return {"name": name, "size": entry.size, "type": "directory" if entry.isdir() else "file"}


def _s3_alias(*values: object) -> str | None:
    supplied = [value for value in values if value is not None]
    if supplied and any(value != supplied[0] for value in supplied):
        raise ValueError("conflicting native S3 options")
    return cast("str | None", supplied[0] if supplied else None)


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


class ZstarFileSystem(AsyncFileSystem, AbstractArchiveFileSystem):
    """Read-only archive filesystem with async I/O and a standard sync facade.

    Use ``zstar://member::file:///path/archive.zstar`` or construct with ``fo``.
    Member paths are relative; ``""`` is the root. ``asynchronous=True`` enables
    fsspec's async methods and ``open_async``. Use ``await fs.aclose()`` for async
    cleanup. The default sync facade shares the same async reader and caches.

    Chained URLs and ``fs=`` use fsspec seekable range streams. Keep their
    underlying archive objects immutable. Unchained S3 URLs use Rust by default.
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
        resolver: ObjectResolver | None = None,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: object,
    ) -> None:
        if "s3_backend" in kwargs:
            raise TypeError("use zstar chaining or fs= to select an fsspec backend")
        super().__init__(asynchronous=asynchronous, loop=loop, **kwargs)
        if mode != "r":
            raise ValueError("ZstarFileSystem is read-only; mode must be 'r'")
        if not isinstance(cache_bytes, int) or cache_bytes < 0:
            raise ValueError("cache_bytes must be a nonnegative integer")
        if fs is not None and (not isinstance(fo, (str, PathLike)) or target_options):
            raise ValueError(
                "fs requires an archive path and is already configured; omit target_options"
            )
        self.fo, self.target_protocol = fo, target_protocol
        self._filesystem = fs
        self.cache_bytes, self.verify_external, self.resolver = (
            cache_bytes,
            verify_external,
            resolver,
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

    async def _get_archive(self) -> AsyncArchive:
        self._check_loop()
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

    async def _open_s3(
        self,
        location: str,
        options: ReaderOptions,
        storage_options: Mapping[str, object] | None = None,
    ) -> _ArchiveResources:
        backend_options = self._backend_options if storage_options is None else storage_options
        bucket, sep, key = location.removeprefix("s3://").partition("/")
        if not bucket or not sep or not key:
            raise ValueError("S3 archive path must be s3://bucket/key or bucket/key")
        unknown = set(backend_options) - _NATIVE_S3_OPTIONS
        if unknown:
            raise ValueError("unsupported native S3 target_options; use zstar chaining or fs=")
        client = backend_options.get("client_kwargs", {})
        if not isinstance(client, Mapping) or set(client) - {"endpoint_url", "region_name"}:
            raise ValueError("native S3 client_kwargs supports endpoint_url and region_name only")
        archive = await AsyncArchive.from_s3(
            bucket,
            key,
            endpoint=_s3_alias(
                backend_options.get("endpoint"),
                backend_options.get("endpoint_url"),
                client.get("endpoint_url"),
            ),
            region=_s3_alias(backend_options.get("region"), client.get("region_name")),
            object_prefix=cast("str | None", backend_options.get("object_prefix")),
            anon=cast("bool | None", backend_options.get("anon")),
            access_key_id=_s3_alias(
                backend_options.get("access_key_id"), backend_options.get("key")
            ),
            secret_access_key=_s3_alias(
                backend_options.get("secret_access_key"), backend_options.get("secret")
            ),
            session_token=_s3_alias(
                backend_options.get("session_token"), backend_options.get("token")
            ),
            **options,
        )
        return _ArchiveResources(archive)

    async def _open_archive(self) -> _ArchiveResources:
        options: ReaderOptions = {
            "cache_bytes": self.cache_bytes,
            "verify_external": self.verify_external,
            "resolver": self.resolver,
        }
        if not isinstance(self.fo, (str, PathLike)):
            try:
                archive = await AsyncArchive.open(fileobj=self.fo, **options)
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
        if self.target_protocol == "zstar+s3" and self._filesystem is None:
            native_options = dict(self._backend_options)
            backing = native_options.pop("fo", location)
            if not isinstance(backing, str):
                raise TypeError("native S3 archive fo must be a URL")
            if backing.startswith("zstar+s3://"):
                backing = _native_s3_url(backing)
            nested = native_options.pop("target_options", {})
            if not isinstance(nested, Mapping):
                raise TypeError("native S3 target_options must be a mapping")
            storage = dict(nested)
            for name, value in native_options.items():
                if name in storage and storage[name] != value:
                    raise ValueError("conflicting native S3 options")
                storage[name] = value
            return await self._open_s3(backing, options, storage_options=storage)
        if self._filesystem is not None or self.target_protocol is not None:
            return await self._open_fsspec(location, options)
        if location.startswith("s3://"):
            return await self._open_s3(location, options)
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
        def resolve() -> tuple[AbstractFileSystem, str]:
            from fsspec import filesystem
            from fsspec.core import url_to_fs

            if self._filesystem is not None:
                backend = self._filesystem
                path = backend._strip_protocol(location)
            elif self.target_protocol is None:
                backend, path = url_to_fs(location, **self._backend_options)
            else:
                backend = filesystem(self.target_protocol, **self._backend_options)
                path = backend._strip_protocol(location)
            return backend, path

        backend, path = await asyncio.to_thread(resolve)
        if getattr(backend, "asynchronous", False):
            info = await backend._info(path)
            source = _AsyncFsspecSource(backend, path, info["size"])
            return _ArchiveResources(await AsyncArchive.from_source(source, source.size, **options))
        # A seekable fsspec file fetches ranges. Disable its read-ahead cache:
        # the indexed reader already owns bounded decoded caching.
        file = cast(BinaryIO, await asyncio.to_thread(backend.open, path, "rb", cache_type="none"))
        try:
            archive = await AsyncArchive.open(fileobj=file, **options)
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

    async def _finalize(self) -> None:
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
            with suppress(Exception):
                self.close()
        elif self._bound_loop is not None and self._bound_loop.is_running():
            self._bound_loop.call_soon_threadsafe(lambda: asyncio.create_task(self._finalize()))
        elif not isinstance(self.fo, (str, PathLike)):
            with suppress(Exception):
                self.fo.close()

    async def __aenter__(self) -> ZstarFileSystem:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

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
                    info = await archive.getmember(path)
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

    async def open_async(self, path: str, mode: str = "rb", **kwargs: object) -> _AsyncMemberFile:
        if mode != "rb" or kwargs:
            raise NotImplementedError(
                "open_async supports read-only binary members without buffering options"
            )
        member = cast(str, self._strip_protocol(path))
        if not member:
            raise IsADirectoryError(path)
        archive = await self._get_archive()
        entry = await self._resolve(archive, member)
        if not entry.isfile():
            raise IsADirectoryError(path)
        stream = await archive.extractfile(entry.name)
        assert stream is not None
        return _AsyncMemberFile(self, member, stream, entry.size)

    def _open(self, path: str, mode: str = "rb", **kwargs: object) -> _MemberFile:
        stream = sync(self.loop, self.open_async, path, mode)
        return _MemberFile(self, cast(str, self._strip_protocol(path)), stream, **kwargs)

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: object
    ) -> bytes:
        async with await self.open_async(path) as stream:
            size = stream.size
            start = (
                0 if start is None else (max(0, size + start) if start < 0 else min(size, start))
            )
            end = size if end is None else (max(0, size + end) if end < 0 else min(size, end))
            stream.seek(start)
            return await stream.read(max(0, end - start))

    async def _info(self, path: str, **kwargs: object) -> FileInfo:
        member = cast(str, self._strip_protocol(path))
        archive = await self._get_archive()
        if not member:
            return {"name": "", "type": "directory", "size": 0}
        return _file_info(member, await self._resolve(archive, member))

    def info(self, path: str, **kwargs: object) -> FileInfo:
        return sync(self.loop, self._info, path, **kwargs)

    async def _modified(self, path: str) -> datetime:
        member = cast(str, self._strip_protocol(path))
        archive = await self._get_archive()
        ns = (await self._resolve(archive, member)).mtime_ns if member else 0
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
                async for entry in archive._iter_prefix(prefix)
                if "/" not in (name := entry.name)[len(prefix) :]
            ]
        items: list[FileInfo] = []
        async for entry in archive._iter_prefix(prefix):
            if "/" in entry.name[len(prefix) :]:
                continue
            target = entry
            if entry.issym():
                # Dangling links list as the link itself.
                with suppress(OSError):
                    target = await self._resolve(archive, entry.name)
            items.append(_file_info(f"{display_prefix}{entry.name[len(prefix) :]}", target))
        return items


_NATIVE_S3_OPTIONS = {
    "endpoint",
    "endpoint_url",
    "region",
    "object_prefix",
    "anon",
    "key",
    "secret",
    "token",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "client_kwargs",
}


def _native_s3_url(url: str) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "zstar+s3"
        or not parsed.netloc
        or ":" in parsed.netloc
        or "@" in parsed.netloc
        or not parsed.path.removeprefix("/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "native S3 URL must be zstar+s3://bucket/key; "
            "select members through filesystem paths or zstar chaining"
        )
    return f"s3://{parsed.netloc}/{unquote(parsed.path[1:], errors='strict')}"


class ZstarS3FileSystem(ZstarFileSystem):
    """Native S3 archive URLs, with filesystem paths selecting archive members.

    Native options such as endpoint, region, and credentials are forwarded to
    Rust. This protocol does not construct an fsspec S3 filesystem or need s3fs.
    """

    protocol = "zstar+s3"

    def __init__(
        self,
        fo: str,
        mode: str = "r",
        target_protocol: str | None = None,
        target_options: Mapping[str, object] | None = None,
        fs: AbstractFileSystem | None = None,
        cache_bytes: int = 16 << 20,
        verify_external: bool = True,
        resolver: ObjectResolver | None = None,
        asynchronous: bool = False,
        loop: asyncio.AbstractEventLoop | None = None,
        **kwargs: object,
    ) -> None:
        if "member" in kwargs:
            raise TypeError("use fs.open(member_path) or zstar://member::zstar+s3://archive")
        if fs is not None:
            raise ValueError(
                "zstar+s3 selects native S3; use zstar chaining for fsspec filesystems"
            )
        if target_protocol is not None:
            raise ValueError("zstar+s3 does not accept target_protocol")
        options = dict(target_options or {})
        for name in _NATIVE_S3_OPTIONS & kwargs.keys():
            value = kwargs.pop(name)
            if name in options and options[name] != value:
                raise ValueError("conflicting native S3 options")
            options[name] = value
        unknown = kwargs.keys() - {
            "batch_size",
            "use_listings_cache",
            "listings_expiry_time",
            "max_paths",
        }
        if unknown:
            raise TypeError(
                f"unsupported native S3 options: {', '.join(sorted(unknown))}; "
                "use zstar chaining for fsspec credentials"
            )
        fo = _native_s3_url(fo)
        super().__init__(
            fo=fo,
            mode=mode,
            target_protocol=None,
            target_options=options,
            fs=None,
            cache_bytes=cache_bytes,
            verify_external=verify_external,
            resolver=resolver,
            asynchronous=asynchronous,
            loop=loop,
            **kwargs,
        )

    @staticmethod
    def _get_kwargs_from_urls(path: str) -> dict[str, str]:
        _native_s3_url(path)
        return {"fo": path}

    @classmethod
    def _strip_protocol(cls, path: str | list[str]) -> str | list[str]:
        if isinstance(path, list):
            return [cast(str, cls._strip_protocol(item)) for item in path]
        if path.startswith("zstar+s3://"):
            _native_s3_url(path)
            return ""
        return ZstarFileSystem._strip_protocol(path)
