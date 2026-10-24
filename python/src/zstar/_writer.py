"""Tar-style archive writing without wire-format tuples."""

import os
from collections.abc import Callable
from contextlib import ExitStack, suppress
from pathlib import Path
from tarfile import TarInfo as _TarInfo
from types import TracebackType
from typing import BinaryIO, Self, Unpack

from . import _native
from ._info import _to_entry
from .types import SpillStore, WriterOptions


def _open_no_follow(path: str | os.PathLike[str], flags: int, mode: int = 0o666) -> int:
    return os.open(os.fspath(path), flags | getattr(os, "O_NOFOLLOW", 0), mode)


class ArchiveWriter:
    """Streaming writer with tarfile-compatible addfile and filesystem add methods.

    The sink remains caller-owned. Successful close finalizes and flushes the
    archive; exceptional context exit discards unfinished output.
    """

    def __init__(
        self,
        fileobj: BinaryIO,
        *,
        frame_size: int = 1 << 20,
        compression_level: int = 3,
        memory_bytes: int = 8 << 20,
        block_target: int = 64 << 10,
        max_spool_bytes: int = 4 << 30,
        create_spill_store: Callable[[], SpillStore] | None = None,
    ) -> None:
        self._writer = _native.EntryArchiveWriter(
            fileobj,
            frame_size=frame_size,
            compression_level=compression_level,
            memory_bytes=memory_bytes,
            block_target=block_target,
            max_spool_bytes=max_spool_bytes,
            create_spill_store=create_spill_store,
        )
        self._owned: ExitStack | None = None
        self._path: str | os.PathLike[str] | None = None

    @classmethod
    def open(
        cls, name: str | os.PathLike[str], mode: str = "w", **options: Unpack[WriterOptions]
    ) -> Self:
        if mode not in ("w", "x"):
            raise ValueError("writer mode must be 'w' or 'x'")
        with ExitStack() as owner:
            if mode == "x":
                fd = os.open(os.fspath(name), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
            else:
                fd = _open_no_follow(name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
            output = owner.enter_context(os.fdopen(fd, "wb"))
            writer = cls(output, **options)
            writer._owned = owner.pop_all()
            writer._path = name
        return writer

    @property
    def closed(self) -> bool:
        return self._writer.closed

    def addfile(self, tarinfo: _TarInfo, fileobj: BinaryIO | None = None) -> None:
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

    def add_external(self, tarinfo: _TarInfo, body: _native.ExternalFile) -> None:
        """Add an explicitly externalized body, preserving its v1 chunk references."""
        if self.closed:
            raise ValueError("writer closed")
        if not tarinfo.isfile() or tarinfo.size != body.size:
            raise ValueError("external body requires a regular file with matching size")
        self._writer.add(body._entry(_to_entry(tarinfo)), None, 0)

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
        finally:
            self._close_owned()

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
        owned_path = self._path
        try:
            self._writer.__exit__(exc_type, exc_value, traceback)
        finally:
            try:
                self._close_owned()
            finally:
                if exc_type is not None and owned_path is not None:
                    # Partial output is not a valid archive; remove files we created.
                    with suppress(OSError):
                        os.unlink(owned_path)
