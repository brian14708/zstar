"""Backend output publication shared by archives and external objects."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable
from functools import partial
from tarfile import TarInfo
from typing import BinaryIO, Protocol, cast

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from typing_extensions import Buffer, Reader

from . import _native
from ._info import _from_entry
from .types import WriterOptions


class _AsyncOutput(Protocol):
    async def write(self, data: bytes, /) -> int: ...
    async def close(self) -> None: ...


class _AsyncSink:
    """Bridge native worker writes to a caller-supplied async backend."""

    def __init__(self, stream: _AsyncOutput, loop: asyncio.AbstractEventLoop):
        self.stream, self.loop = stream, loop

    def write(self, data: Buffer) -> int:
        return asyncio.run_coroutine_threadsafe(self.stream.write(bytes(data)), self.loop).result()

    def flush(self) -> None:
        # Async streamed backends flush buffered output when closed.
        pass

    def close(self) -> None:
        asyncio.run_coroutine_threadsafe(self.stream.close(), self.loop).result()


class _Publication:
    """Own output closure and publication, including failure cleanup.

    Async streams publish on close and cannot be rolled back. Transactional
    fsspec outputs are closed before commit and discarded on failure.
    """

    def __init__(
        self,
        output: BinaryIO,
        *,
        transactional: bool = False,
    ) -> None:
        self.output = output
        self._commit = getattr(output, "commit", None) if transactional else None
        self._discard = getattr(output, "discard", None) if transactional else None
        self._finished = False

    @classmethod
    def open(
        cls,
        backend: AbstractFileSystem,
        path: str,
        loop: asyncio.AbstractEventLoop,
    ) -> _Publication:
        asynchronous = bool(getattr(backend, "asynchronous", False))
        if asynchronous:
            stream = cast(
                _AsyncOutput,
                asyncio.run_coroutine_threadsafe(backend.open_async(path, "wb"), loop).result(),
            )
            output = cast(BinaryIO, _AsyncSink(stream, loop))
        else:
            output = cast(BinaryIO, backend.open(path, "wb", autocommit=False))

        return cls(output, transactional=not asynchronous)

    def finish(self, *, abort: bool = False, errors: tuple[BaseException, ...] = ()) -> None:
        if self._finished:
            return
        self._finished = True
        failures = list(errors)
        abort = abort or bool(failures)
        if abort and self._discard is not None and isinstance(self.output, AbstractBufferedFile):
            # Prevent close from uploading buffered output before it is discarded.
            self.output.forced = True
        try:
            self.output.close()
        except BaseException as error:
            failures.append(error)
            abort = True
        if not abort and self._commit is not None:
            try:
                self._commit()
            except BaseException as error:
                failures.append(error)
                abort = True
        if abort and self._discard is not None:
            try:
                self._discard()
            except BaseException as error:
                failures.append(error)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise BaseExceptionGroup("output publication failed", failures)


class _WriteSession:
    """Native member state with Python-owned backend publication."""

    def __init__(
        self,
        publication: _Publication,
        spool_memory: int,
        options: WriterOptions,
        external: Callable[[str, bytes, int], int] | None = None,
        external_threshold: int = 16 << 20,
    ) -> None:
        self.publication = publication
        self._session = _native.StagedWriter(
            _native.EntryArchiveWriter(publication.output, **options),
            partial(tempfile.SpooledTemporaryFile, max_size=spool_memory, mode="w+b"),
            external,
            external_threshold,
            options.get("compression_level", 3),
        )

    def mkdir(self, path: str, parents: bool = True, exist_ok: bool = False) -> None:
        self._session.mkdir(path, parents, exist_ok)

    def open(self, path: str) -> _native.StagedMember:
        return self._session.open(path)

    def add_file(self, path: str, body: Reader[bytes], size: int) -> None:
        self._session.add_file(path, body, size)

    def snapshot(self) -> dict[str, TarInfo]:
        entries = {}
        for entry, size in self._session.snapshot():
            info = _from_entry(entry)
            info.size = size
            entries[info.name] = info
        return entries

    def close(self, abort: bool = False) -> None:
        abort, errors = self._session.finish(abort)
        self.publication.finish(abort=abort, errors=tuple(errors))
