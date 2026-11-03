"""Tar-style archive writing without wire-format tuples."""

import asyncio
import os
from collections.abc import Callable
from contextlib import ExitStack, suppress
from functools import partial
from io import BytesIO
from pathlib import Path
from tarfile import TarInfo as _TarInfo
from types import TracebackType
from typing import Self, TypeVar, Unpack

from typing_extensions import Reader

from . import _native
from ._info import _to_entry
from .types import BinaryWriter, CborValue, EntryTuple, Limits, SpillStore, WriterOptions


def _open_no_follow(path: str | os.PathLike[str], flags: int, mode: int = 0o666) -> int:
    return os.open(os.fspath(path), flags | getattr(os, "O_NOFOLLOW", 0), mode)


def _discard_path(path: str, identity: tuple[int, int]) -> None:
    with suppress(OSError):
        current = os.stat(path, follow_symlinks=False)
        if (current.st_dev, current.st_ino) == identity:
            os.unlink(path)


class ArchiveWriter:
    """Streaming writer with tarfile-compatible addfile and filesystem add methods.

    The sink remains caller-owned. Successful close finalizes and flushes the
    archive; exceptional context exit discards unfinished output.
    """

    def __init__(
        self,
        fileobj: BinaryWriter,
        *,
        limits: Limits | None = None,
        frame_size: int = 1 << 20,
        compression_level: int = 3,
        memory_bytes: int = 8 << 20,
        block_target: int = 64 << 10,
        max_spool_bytes: int = 4 << 30,
        create_spill_store: Callable[[], SpillStore] | None = None,
        workers: int = 0,
        root_extensions: dict[str, CborValue] | None = None,
    ) -> None:
        self._writer = _native.EntryArchiveWriter(
            fileobj,
            limits=limits,
            frame_size=frame_size,
            compression_level=compression_level,
            memory_bytes=memory_bytes,
            block_target=block_target,
            max_spool_bytes=max_spool_bytes,
            create_spill_store=create_spill_store,
            workers=workers,
            root_extensions=root_extensions,
        )
        self._owned: ExitStack | None = None
        self._path: tuple[str, tuple[int, int]] | None = None

    @classmethod
    def from_file(
        cls,
        file: str | os.PathLike[str] | BinaryWriter,
        mode: str = "w",
        **options: Unpack[WriterOptions],
    ) -> Self:
        if mode not in ("w", "x"):
            raise ValueError("writer mode must be 'w' or 'x'")
        if not isinstance(file, (str, os.PathLike)):
            if mode == "x":
                raise ValueError("exclusive creation requires a path")
            return cls(file, **options)
        name = file
        cleanup_path = os.path.abspath(os.fspath(name))
        with ExitStack() as owner:
            identity: tuple[int, int] | None = None
            try:
                if mode == "x":
                    fd = os.open(os.fspath(name), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
                else:
                    fd = _open_no_follow(name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                output = owner.enter_context(os.fdopen(fd, "wb"))
                stat = os.fstat(output.fileno())
                identity = (stat.st_dev, stat.st_ino)
                writer = cls(output, **options)
            except BaseException:
                if identity is not None:
                    _discard_path(cleanup_path, identity)
                raise
            writer._owned = owner.pop_all()
            writer._path = (cleanup_path, identity)
        return writer

    @property
    def closed(self) -> bool:
        return self._writer.closed

    def addfile(self, tarinfo: _TarInfo, fileobj: Reader[bytes] | None = None) -> None:
        """Add a regular file, directory, or symlink using its TarInfo metadata.

        Supply a binary body for a nonempty regular file. Include ancestor
        directory members, as required by the archive format.
        """
        if self.closed:
            raise ValueError("writer closed")
        entry = _to_entry(tarinfo)
        if fileobj is not None and not tarinfo.isfile():
            raise ValueError("fileobj requires a regular file")
        if tarinfo.isfile() and tarinfo.size and fileobj is None:
            raise ValueError("nonempty regular file requires fileobj")
        self._writer.add(entry, fileobj, tarinfo.size if tarinfo.isfile() else 0)

    def add_entry(
        self, entry: EntryTuple, body: Reader[bytes] | None = None, size: int = 0
    ) -> None:
        """Write lossless manifest metadata, optionally with a sized file body.

        File bodies receive new inline extents. Without a body, a regular-file
        entry may reference external objects; inline archive offsets cannot be
        copied between archives. Directories and symlinks have no body.
        """
        if self.closed:
            raise ValueError("writer closed")
        if size and body is None:
            raise ValueError("size requires a body")
        if body is not None and entry[1] != 0:
            raise ValueError("body requires a regular file")
        self._writer.add(entry, body, size)

    def write_bytes(self, name: str, data: bytes, *, mode: int = 0o644) -> None:
        """Write bytes with normalized metadata; ancestor directories must exist."""
        info = _TarInfo(name)
        info.size = len(data)
        info.mode = mode
        self.addfile(info, BytesIO(data))

    def add(
        self,
        name: str | os.PathLike[str],
        arcname: str | None = None,
        *,
        recursive: bool = True,
        preserve_metadata: bool = False,
    ) -> None:
        """Capture a local path without following symbolic links.

        The default archive name is the source basename. Recursive capture is
        sorted by UTF-8 path bytes. Metadata is normalized by default; select
        preserve_metadata to retain each entry's mode, ownership, and timestamp.
        """
        path = Path(name)
        # ``Path(".").name`` is empty; resolve the real directory name lexically.
        archive_name = (
            (path.name or Path(os.path.abspath(path)).name) if arcname is None else arcname
        )
        if not archive_name:
            raise ValueError("arcname is required when the path has no name")
        self._writer.add_path(
            path,
            archive_name,
            preserve_metadata=preserve_metadata,
            recursive=recursive,
        )

    def _close_owned(self) -> None:
        owned, self._owned = self._owned, None
        if owned is not None:
            owned.close()

    def close(self) -> None:
        try:
            self._writer.close()
            self._path = None
        except BaseException:
            self._discard_output()
            raise
        finally:
            self._close_owned()

    def _discard_output(self) -> None:
        path, self._path = self._path, None
        if path is not None:
            _discard_path(*path)

    def __enter__(self) -> Self:
        if self.closed:
            raise ValueError("writer closed")
        self._writer.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self._writer.__exit__(exc_type, exc_value, traceback)
            if exc_type is None:
                self._path = None
        except BaseException:
            self._discard_output()
            raise
        finally:
            try:
                self._close_owned()
            finally:
                if exc_type is not None:
                    # Partial output is not a valid archive; remove files we created.
                    self._discard_output()


_T = TypeVar("_T")


async def _writer_call(
    operation: Callable[[], _T], *, discard: Callable[[_T], None] | None = None
) -> _T:
    """Join native work before propagating cancellation or releasing ownership."""
    future = asyncio.ensure_future(_native._writer_call(operation))
    cancelled: asyncio.CancelledError | None = None
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError as error:
            cancelled = error
        except BaseException:
            break
    if cancelled is not None:
        try:
            result = future.result()
        except BaseException:
            pass
        else:
            if discard is not None:
                await _writer_call(partial(discard, result))
        raise cancelled
    return future.result()


def _discard_writer(writer: ArchiveWriter) -> None:
    writer.__exit__(asyncio.CancelledError, None, None)


class AsyncArchiveWriter:
    """Async writer with the same operations as ArchiveWriter.

    File access, compression, and synchronous body/sink/spill callbacks run on
    Rust workers, with at most eight admitted jobs across writers. Operations
    serialize per writer. Cancellation waits for the active worker before
    propagating; exceptional context exit discards output.
    Borrowed sinks remain open. Use an async with block to guarantee cleanup.
    """

    _writer: ArchiveWriter
    _lock: asyncio.Lock

    def __init__(self) -> None:
        raise TypeError("use await AsyncArchiveWriter.from_file()")

    @classmethod
    async def from_file(
        cls,
        file: str | os.PathLike[str] | BinaryWriter,
        mode: str = "w",
        **options: Unpack[WriterOptions],
    ) -> Self:
        writer = await _writer_call(
            partial(ArchiveWriter.from_file, file, mode, **options),
            discard=_discard_writer,
        )
        result = cls.__new__(cls)
        result._writer = writer
        result._lock = asyncio.Lock()
        return result

    @property
    def closed(self) -> bool:
        return self._writer.closed

    async def addfile(self, tarinfo: _TarInfo, fileobj: Reader[bytes] | None = None) -> None:
        async with self._lock:
            await _writer_call(partial(self._writer.addfile, tarinfo, fileobj))

    async def add_entry(
        self, entry: EntryTuple, body: Reader[bytes] | None = None, size: int = 0
    ) -> None:
        """Write lossless entry metadata and an optional sized body on a worker."""
        async with self._lock:
            await _writer_call(partial(self._writer.add_entry, entry, body, size))

    async def write_bytes(self, name: str, data: bytes, *, mode: int = 0o644) -> None:
        """Write bytes on a native worker, serializing with other operations."""
        async with self._lock:
            await _writer_call(partial(self._writer.write_bytes, name, data, mode=mode))

    async def add(
        self,
        name: str | os.PathLike[str],
        arcname: str | None = None,
        *,
        recursive: bool = True,
        preserve_metadata: bool = False,
    ) -> None:
        async with self._lock:
            await _writer_call(
                partial(
                    self._writer.add,
                    name,
                    arcname,
                    recursive=recursive,
                    preserve_metadata=preserve_metadata,
                )
            )

    async def close(self) -> None:
        async with self._lock:
            await _writer_call(self._writer.close)

    async def __aenter__(self) -> Self:
        async with self._lock:
            await _writer_call(self._writer.__enter__, discard=_discard_writer)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        async with self._lock:
            await _writer_call(partial(self._writer.__exit__, exc_type, exc_value, traceback))
