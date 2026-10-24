"""Tar-style archive writing without wire-format tuples."""

import os
import stat
from collections.abc import Callable
from contextlib import ExitStack, suppress
from pathlib import Path
from tarfile import DIRTYPE, REGTYPE, SYMTYPE
from tarfile import TarInfo as _TarInfo
from types import TracebackType
from typing import BinaryIO, Self, Unpack

from . import _native
from ._info import TarInfo, _to_entry
from .types import SpillStore, WriterOptions


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
        self._names: set[str] = set()
        self._path: str | os.PathLike[str] | None = None

    @classmethod
    def open(
        cls, name: str | os.PathLike[str], mode: str = "w", **options: Unpack[WriterOptions]
    ) -> Self:
        if mode not in ("w", "x"):
            raise ValueError("writer mode must be 'w' or 'x'")
        with ExitStack() as owner:
            output = owner.enter_context(open(name, "wb" if mode == "w" else "xb"))
            writer = cls(output, **options)
            writer._owned = owner.pop_all()
            writer._path = name
        return writer

    @property
    def closed(self) -> bool:
        return bool(self._writer.closed)

    def addfile(self, tarinfo: _TarInfo, fileobj: BinaryIO | None = None) -> None:
        """Add a regular file, directory, or symlink using its TarInfo metadata.

        Supply a binary body for a nonempty regular file. Include ancestor
        directory members, as required by the archive format.
        """
        if self.closed:
            raise ValueError("writer closed")
        entry = _to_entry(tarinfo)
        if tarinfo.name in self._names:
            raise ValueError(f"duplicate member name: {tarinfo.name!r}")
        if fileobj is not None and not tarinfo.isfile():
            raise ValueError("fileobj requires a regular file")
        if tarinfo.isfile() and tarinfo.size and fileobj is None:
            raise ValueError("nonempty regular file requires fileobj")
        self._writer.add(entry, fileobj, tarinfo.size if tarinfo.isfile() else 0)
        self._names.add(tarinfo.name)

    def add(
        self, name: str | os.PathLike[str], arcname: str | None = None, *, recursive: bool = True
    ) -> None:
        """Capture a local path with its metadata; never follow symbolic links.

        The default archive name is the source basename. Recursive capture is
        deterministic and includes directory members before their descendants.
        """
        path = Path(name)
        # ``Path(".").name`` is empty; resolve the real directory name lexically.
        archive_name = (
            (path.name or Path(os.path.abspath(path)).name) if arcname is None else arcname
        )
        if not archive_name:
            raise ValueError("arcname is required when the path has no name")
        metadata = path.lstat()
        info = TarInfo(archive_name)
        info.mode = stat.S_IMODE(metadata.st_mode)
        info.uid, info.gid = metadata.st_uid, metadata.st_gid
        info.mtime_ns = metadata.st_mtime_ns
        if stat.S_ISLNK(metadata.st_mode):
            info.type, info.linkname = SYMTYPE, os.readlink(path)
            self.addfile(info)
        elif stat.S_ISDIR(metadata.st_mode):
            info.type = DIRTYPE
            self.addfile(info)
            if recursive:
                for child in sorted(path.iterdir()):
                    self.add(child, f"{archive_name}/{child.name}", recursive=True)
        elif stat.S_ISREG(metadata.st_mode):
            info.type, info.size = REGTYPE, metadata.st_size
            with path.open("rb") as source:
                self.addfile(info, source)
        else:
            raise ValueError("only regular files, directories, and symlinks are supported")

    def close(self) -> None:
        try:
            self._writer.close()
        finally:
            owned, self._owned = self._owned, None
            if owned is not None:
                owned.close()

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
            owned, self._owned = self._owned, None
            if owned is not None:
                owned.close()
            if exc_type is not None and owned_path is not None:
                # Partial output is not a valid archive; remove files we created.
                with suppress(OSError):
                    os.unlink(owned_path)
