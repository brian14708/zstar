"""Staged archive creation shared by the synchronous and async filesystems."""

from __future__ import annotations

import io
import posixpath
import tempfile
import threading
from collections.abc import Callable
from tarfile import DIRTYPE, TarInfo
from typing import BinaryIO

from fsspec.spec import AbstractBufferedFile
from typing_extensions import Buffer, Reader

from . import _native
from ._writer import ArchiveWriter
from .types import WriterOptions


def _member_name(path: str) -> str:
    if "\x00" in path or "\\" in path or any(part == ".." for part in path.split("/")):
        raise ValueError("member paths must be archive-relative without '..', NUL, or backslashes")
    return "/".join(part for part in path.split("/") if part not in ("", "."))


class _WriteSession:
    """All methods run on native workers; the lock serializes shared state."""

    def __init__(
        self,
        output: BinaryIO,
        spool_memory: int,
        options: WriterOptions,
        commit: Callable[[], None] | None = None,
        discard: Callable[[], None] | None = None,
        after_commit: Callable[[], None] | None = None,
        external: Callable[[str, bytes, int], int] | None = None,
        external_threshold: int = 16 << 20,
    ) -> None:
        self.output, self.spool_memory = output, spool_memory
        self.commit, self.discard = commit, discard
        self.after_commit = after_commit
        self.external, self.external_threshold = external, external_threshold
        self.compression_level = options.get("compression_level", 3)
        self.lock = threading.RLock()
        self.entries: dict[str, TarInfo] = {}
        self.active: _StagedMember | None = None
        self.closed = False
        self.failed = False
        self.writer = ArchiveWriter(output, **options)

    def _check(self) -> None:
        if self.closed:
            raise ValueError("filesystem closed")
        if self.failed:
            raise ValueError("archive writer failed; discard this filesystem")

    def _add(self, info: TarInfo, body: Reader[bytes] | None = None) -> None:
        try:
            if (
                self.external is not None
                and info.isfile()
                and info.size > 0
                and info.size >= self.external_threshold
            ):
                assert body is not None
                external = _native._externalize(
                    body,
                    info.size,
                    self.external,
                    chunking=_native.Chunking(
                        "fastcdc", min_bytes=2 << 20, target_bytes=8 << 20, max_bytes=16 << 20
                    ),
                    compression_level=self.compression_level,
                )
                self.writer._add_external(info, external)
            else:
                self.writer.addfile(info, body)
        except BaseException:
            self.failed = True
            raise
        self.entries[info.name] = info

    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = False) -> None:
        with self.lock:
            self._check()
            path = _member_name(path)
            if not path:
                if exist_ok:
                    return
                raise FileExistsError(path)
            entry = self.entries.get(path)
            if entry is not None:
                if not entry.isdir() or not exist_ok:
                    raise FileExistsError(path)
                return
            if self.active is not None and self.active.name == path:
                raise FileExistsError(path)
            parent = posixpath.dirname(path)
            if parent:
                if parents:
                    self.mkdir(parent, exist_ok=True)
                elif parent not in self.entries:
                    raise FileNotFoundError(parent)
                elif not self.entries[parent].isdir():
                    raise NotADirectoryError(parent)
            info = TarInfo(path)
            info.type, info.mode = DIRTYPE, 0o755
            self._add(info)

    def open(self, path: str) -> _StagedMember:
        with self.lock:
            path = self._prepare_file(path)
            member = _StagedMember(self, path)
            self.active = member
            return member

    def _prepare_file(self, path: str) -> str:
        self._check()
        path = _member_name(path)
        if not path:
            raise IsADirectoryError(path)
        if self.active is not None:
            raise RuntimeError("close the active member before opening another")
        if path in self.entries:
            if self.entries[path].isdir():
                raise IsADirectoryError(path)
            raise FileExistsError(path)
        parent = posixpath.dirname(path)
        if parent:
            if parent in self.entries and not self.entries[parent].isdir():
                raise NotADirectoryError(parent)
            self.mkdir(parent, exist_ok=True)
        return path

    def add_file(self, path: str, body: Reader[bytes], size: int) -> None:
        """Stream a known-size body directly, avoiding member staging."""
        with self.lock:
            path = self._prepare_file(path)
            info = TarInfo(path)
            info.size, info.mode = size, 0o644
            self._add(info, body)

    def snapshot(self) -> dict[str, TarInfo]:
        with self.lock:
            self._check()
            return dict(self.entries)

    def close(self, abort: bool = False) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            errors: list[BaseException] = []
            if self.active is not None:
                try:
                    self.active._discard()
                except BaseException as error:
                    errors.append(error)
                finally:
                    self.active = None
                if not abort:
                    errors.append(RuntimeError("archive closed with an open writable member"))
            if self.failed and not abort and not errors:
                errors.append(ValueError("archive writer failed"))
            abort = abort or self.failed or bool(errors)
            try:
                if abort:
                    self.writer.__exit__(RuntimeError, None, None)
                else:
                    self.writer.close()
            except BaseException as error:
                errors.append(error)
                abort = True
            discarded = False
            if abort and self.discard is not None:
                try:
                    self.discard()
                    discarded = True
                except BaseException as error:
                    errors.append(error)
                finally:
                    if isinstance(self.output, AbstractBufferedFile):
                        self.output.forced = True
            try:
                self.output.close()
            except BaseException as error:
                errors.append(error)
                abort = True
            if not abort and self.commit is not None:
                try:
                    self.commit()
                except BaseException as error:
                    errors.append(error)
                    abort = True
            if abort and self.discard is not None and not discarded:
                try:
                    self.discard()
                except BaseException as error:
                    errors.append(error)
            if not abort and self.after_commit is not None:
                try:
                    self.after_commit()
                except BaseException as error:
                    errors.append(error)
            if len(errors) == 1:
                raise errors[0]
            if errors:
                raise BaseExceptionGroup("archive cleanup failed", errors)


class _StagedMember:
    def __init__(self, session: _WriteSession, name: str) -> None:
        self.session, self.name = session, name
        self.body = tempfile.SpooledTemporaryFile(  # noqa: SIM115 - lifetime follows member
            max_size=session.spool_memory, mode="w+b"
        )
        self.closed = False

    def _check(self) -> None:
        self.session._check()
        if self.closed:
            raise ValueError("I/O operation on closed file")

    def write(self, data: Buffer) -> int:
        with self.session.lock:
            self._check()
            return self.body.write(data)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self.session.lock:
            self._check()
            return self.body.seek(offset, whence)

    def tell(self) -> int:
        with self.session.lock:
            self._check()
            return self.body.tell()

    def flush(self) -> None:
        with self.session.lock:
            self._check()
            self.body.flush()

    def _discard(self) -> None:
        self.closed = True
        self.body.close()

    def close(self, abort: bool = False) -> None:
        with self.session.lock:
            if self.closed:
                return
            try:
                self._check()
                if abort:
                    self.session.failed = True
                    return
                info = TarInfo(self.name)
                info.size, info.mode = self.body.seek(0, io.SEEK_END), 0o644
                self.body.seek(0)
                self.session._add(info, self.body)
            finally:
                try:
                    self._discard()
                except BaseException:
                    self.session.failed = True
                    raise
                finally:
                    self.session.active = None
