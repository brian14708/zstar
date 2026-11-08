"""Typed writer facades over the shared async Rust builder."""

import asyncio
import os
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from types import TracebackType
from typing import Literal, Self, cast

from typing_extensions import Buffer

from . import _native
from ._location import _location
from ._metadata import Header
from ._open_options import writer_options
from ._ownership import _acleanup, _acquire, _cleanup
from .types import (
    AsyncBinaryWriter,
    AsyncObjectStore,
    AsyncReader,
    BinaryWriter,
    ObjectStore,
    Reader,
    SinkOptions,
    WriterOptions,
)


def _output_location(
    destination: object, mode: str, atomic: bool, sink_options: SinkOptions | None
) -> tuple[str, bool] | None:
    if not isinstance(mode, str):
        raise TypeError("mode must be a string")
    if mode not in ("w", "x"):
        raise ValueError("mode must be w or x")
    if not isinstance(atomic, bool):
        raise TypeError("atomic must be a bool")
    if isinstance(destination, (str, os.PathLike)):
        location, local = _location(destination)
        if not local and location.lower().startswith(("http:", "https:")):
            raise ValueError("HTTP upload is unsupported")
        return location, local
    if sink_options is not None:
        raise ValueError("sink options require a path or URL")
    if mode != "w" or atomic:
        raise ValueError("mode x and atomic require a local path or file URL")
    if not all(callable(getattr(destination, name, None)) for name in ("write", "flush")):
        raise TypeError("destination must be a path, URL, or binary writer")
    return None


def _capture_name(name: str | os.PathLike[str], arcname: str | None) -> tuple[str, str]:
    path = os.fspath(name)
    if not isinstance(path, str):
        raise TypeError("name must be a string or a text path")
    if arcname is None:
        arcname = os.path.splitdrive(path)[1].replace(os.sep, "/").lstrip("/")
    if not isinstance(arcname, str):
        raise TypeError("arcname must be a string")
    return path, arcname


class _SyncChunks:
    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = chunks
        self._iterator: Iterator[bytes] | None = None

    def next_chunk(self) -> bytes | None:
        if self._iterator is None:
            self._iterator = iter(self._chunks)
        return next(self._iterator, None)


class _AsyncChunks:
    def __init__(self, chunks: AsyncIterable[bytes]) -> None:
        self._chunks = chunks
        self._iterator: AsyncIterator[bytes] | None = None

    async def next_chunk(self) -> bytes | None:
        if self._iterator is None:
            self._iterator = aiter(self._chunks)
        return await anext(self._iterator, None)


class ArchiveWriter:
    """Encode through the bounded native async pipeline.

    Paths and URLs own output and publish on successful close. Caller-supplied
    binary writers remain borrowed. Exceptional context exit aborts without finalizing.
    """

    __slots__ = ("__weakref__", "_writer")
    _writer: _native.WriterSession

    def __init__(self) -> None:
        raise TypeError('use zstar.open(destination, "w")')

    @classmethod
    def _to_config(
        cls, sink: BinaryWriter, options: _native.WriterConfig, *, owned: bool = False
    ) -> Self:
        writer = cls.__new__(cls)
        writer._writer = _native.WriterSession.to_sink(sink, options, owned=owned)
        return writer

    @classmethod
    def _to_owned(cls, sink: _native.Sink, options: _native.WriterConfig) -> Self:
        return cls._to_config(sink, options, owned=True)

    @classmethod
    def _open(
        cls,
        destination: str | os.PathLike[str] | BinaryWriter,
        *,
        mode: Literal["w", "x"] = "w",
        atomic: bool = False,
        options: WriterOptions | None = None,
    ) -> Self:
        """Write a path or URL, or borrow a binary writer at its current position.

        Local atomic output publishes after successful finalization. Mode x
        exclusively creates local output. Opening flags do not apply to borrowed sinks.
        """
        policy, sink_options = writer_options(options)
        location = _output_location(destination, mode, atomic, sink_options)
        if location is None:
            return cls._to_config(cast(BinaryWriter, destination), policy)
        path, local = location
        return cls._to_owned(
            _native.open_sink(path, sink_options, local_path=local, mode=mode, atomic=atomic),
            policy,
        )

    @property
    def closed(self) -> bool:
        return self._writer.closed

    def add(
        self,
        name: str | os.PathLike[str],
        arcname: str | None = None,
        *,
        preserve_metadata: bool = False,
        recursive: bool = True,
    ) -> None:
        """Capture a file, symlink, or directory without following symlinks.

        The archive name defaults to name without its drive or leading separators.
        Ancestors remain caller supplied. Portable metadata is used by default.
        """
        path, arcname = _capture_name(name, arcname)
        self._writer.append_path(path, arcname, preserve_metadata, recursive)

    def addfile(self, header: Header, fileobj: Reader | None = None) -> None:
        self._writer.append(header, fileobj)

    def writestr(self, path: str | Header, data: Buffer | str) -> None:
        """Add bytes or UTF-8 text, optionally with an exact-size custom file header."""
        self._writer.append_bytes(path, data.encode("utf-8") if isinstance(data, str) else data)

    def ensure_directory(self, path: str) -> None:
        """Create this directory and missing ancestors, retaining existing directory metadata."""
        self._writer.ensure_directory(path)

    def add_external(self, header: Header, chunks: Iterable[bytes], store: ObjectStore) -> None:
        self._writer.append_external(header, _SyncChunks(chunks), store)

    def close(self) -> None:
        self._writer.close()

    def abort(self) -> None:
        self._writer.abort()

    def __enter__(self) -> Self:
        if self.closed:
            raise ValueError("writer closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            assert exc_value is not None
            _cleanup(exc_value, self.abort)


class AsyncArchiveWriter:
    """Async writer with callbacks on its opening loop and caller task context."""

    __slots__ = ("__weakref__", "_loop", "_writer")
    _writer: _native.WriterSession
    _loop: asyncio.AbstractEventLoop

    def __init__(self) -> None:
        raise TypeError('use zstar.open(destination, "w", async_=True)')

    @classmethod
    async def _to_config(
        cls, sink: AsyncBinaryWriter, options: _native.WriterConfig, *, owned: bool = False
    ) -> Self:
        writer = cls.__new__(cls)
        writer._loop = asyncio.get_running_loop()
        writer._writer = await _native.WriterSession.ato_sink(sink, options, owned=owned)
        return writer

    @classmethod
    async def _to_owned(cls, sink: _native.AsyncSink, options: _native.WriterConfig) -> Self:
        return await _acquire(
            cls._to_config(sink, options, owned=True), lambda writer: writer.abort()
        )

    @classmethod
    async def _open(
        cls,
        destination: str | os.PathLike[str] | AsyncBinaryWriter,
        *,
        mode: Literal["w", "x"] = "w",
        atomic: bool = False,
        options: WriterOptions | None = None,
    ) -> Self:
        """Open output as an awaitable context manager on the current loop.

        Paths and URLs own output. Async binary writers remain borrowed.
        Local atomic output publishes only after successful finalization.
        """
        policy, sink_options = writer_options(options)
        location = _output_location(destination, mode, atomic, sink_options)
        if location is None:
            return await cls._to_config(cast(AsyncBinaryWriter, destination), policy)
        path, local = location
        sink = await _acquire(
            _native.aopen_sink(path, sink_options, local_path=local, mode=mode, atomic=atomic),
            lambda sink: sink.aabort(),
        )
        return await cls._to_owned(sink, policy)

    def _check_loop(self) -> None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("writer belongs to another event loop")

    @property
    def closed(self) -> bool:
        return self._writer.closed

    async def add(
        self,
        name: str | os.PathLike[str],
        arcname: str | None = None,
        *,
        preserve_metadata: bool = False,
        recursive: bool = True,
    ) -> None:
        """Capture a file, symlink, or directory without following symlinks.

        Naming and metadata defaults match the blocking writer's add method.
        """
        self._check_loop()
        path, arcname = _capture_name(name, arcname)
        await self._writer.aappend_path(path, arcname, preserve_metadata, recursive)

    async def addfile(self, header: Header, fileobj: AsyncReader | None = None) -> None:
        self._check_loop()
        await self._writer.aappend(header, fileobj)

    async def writestr(self, path: str | Header, data: Buffer | str) -> None:
        """Add bytes or UTF-8 text, optionally with an exact-size custom file header."""
        self._check_loop()
        await self._writer.aappend_bytes(
            path, data.encode("utf-8") if isinstance(data, str) else data
        )

    async def ensure_directory(self, path: str) -> None:
        """Create this directory and missing ancestors, retaining existing directory metadata."""
        self._check_loop()
        await self._writer.aensure_directory(path)

    async def add_external(
        self,
        header: Header,
        chunks: AsyncIterable[bytes],
        store: AsyncObjectStore,
    ) -> None:
        self._check_loop()
        await self._writer.aappend_external(header, _AsyncChunks(chunks), store)

    async def close(self) -> None:
        self._check_loop()
        await self._writer.aclose()

    async def abort(self) -> None:
        self._check_loop()
        await self._writer.aabort()

    async def __aenter__(self) -> Self:
        self._check_loop()
        if self.closed:
            raise ValueError("writer closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            await self.close()
        else:
            assert exc_value is not None
            await _acleanup(exc_value, self.abort())
