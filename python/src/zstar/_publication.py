"""fsspec member spooling and transactional output publication."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable
from functools import partial
from typing import BinaryIO, Protocol, TypeVar, cast

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from typing_extensions import Buffer, Reader

from . import _native
from ._fsspec_options import FileInfo
from ._metadata import EntryKind, Header
from ._ownership import _acleanup, _retain
from ._writer import _writer_call
from .types import AsyncObjectStore

_T = TypeVar("_T")


def _raise_errors(errors: list[BaseException]) -> None:
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise BaseExceptionGroup("output cleanup failed", errors)


class _AsyncOutput(Protocol):
    async def write(self, data: bytes, /) -> int: ...
    async def close(self) -> None: ...


class _AsyncSink:
    def __init__(self, output: _AsyncOutput):
        self.output = output

    async def write(self, data: bytes, /) -> int:
        return await self.output.write(data)

    async def flush(self) -> None:
        # Streamed backends flush when closed.
        pass


class _Publication:
    """Close before commit; discard transactional output on any failure."""

    def __init__(self, output: BinaryIO | _AsyncOutput, *, asynchronous: bool = False):
        self.output = output
        self.asynchronous = asynchronous
        self.sink = (
            _AsyncSink(cast(_AsyncOutput, output))
            if asynchronous
            else _native.BlockingAdapter(output)
        )
        self._finished = False

    @classmethod
    async def open(cls, backend: AbstractFileSystem, path: str) -> _Publication:
        if getattr(backend, "asynchronous", False):
            return cls(await backend.open_async(path, "wb"), asynchronous=True)

        def open_output() -> _Publication:
            return cls(backend.open(path, "wb", autocommit=False))

        return await _writer_call(
            open_output, discard=lambda publication: publication._finish_sync(True, [])
        )

    async def finish(self, *, abort: bool = False, errors: tuple[BaseException, ...] = ()) -> None:
        if self._finished:
            return
        self._finished = True
        failures = list(errors)
        abort = abort or bool(failures)
        if self.asynchronous:
            try:
                await cast(_AsyncOutput, self.output).close()
            except BaseException as error:
                failures.append(error)
            _raise_errors(failures)
        else:
            await _writer_call(partial(self._finish_sync, abort, failures))

    def _finish_sync(self, abort: bool, failures: list[BaseException]) -> None:
        output = cast(BinaryIO, self.output)
        commit, discard = getattr(output, "commit", None), getattr(output, "discard", None)
        if abort and discard is not None and isinstance(output, AbstractBufferedFile):
            output.forced = True
        try:
            output.close()
        except BaseException as error:
            failures.append(error)
            abort = True
        if not abort and commit is not None:
            try:
                commit()
            except BaseException as error:
                failures.append(error)
                abort = True
        if abort and discard is not None:
            try:
                discard()
            except BaseException as error:
                failures.append(error)
        _raise_errors(failures)


class _WriteSession:
    """Serialize staged members and emit them through the shared async writer."""

    def __init__(
        self,
        publication: _Publication,
        writer: _native.WriterSession,
        spool_memory: int,
        external: AsyncObjectStore | None,
        external_threshold: int,
    ):
        self.publication, self._writer = publication, writer
        self._spool_memory = spool_memory
        self._external, self._threshold = external, external_threshold
        self._entries: dict[str, FileInfo] = {}
        self._active: _StagedMember | None = None
        self._failed = False
        self._closed = False
        self._lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        publication: _Publication,
        spool_memory: int,
        options: _native.WriterConfig,
        external: AsyncObjectStore | None = None,
        external_threshold: int = 16 << 20,
    ) -> _WriteSession:
        try:
            writer = await _native.WriterSession.ato_sink(publication.sink, options)
        except BaseException as primary:
            await _acleanup(primary, publication.finish(abort=True))
            raise
        return cls(publication, writer, spool_memory, external, external_threshold)

    def _check(self) -> None:
        if self._failed:
            raise ValueError("archive writer failed; discard this filesystem")
        if self._closed:
            raise ValueError("archive writer closed")

    async def _add(self, header: Header, body: Reader[bytes] | None = None) -> None:
        try:
            source = _native.BlockingAdapter(body) if body is not None else None
            if self._external is not None and header.size > 0 and header.size >= self._threshold:
                await self._writer.aappend_external_body(header, source, self._external)
            else:
                await self._writer.aappend(header, source)
        except BaseException as primary:
            self._failed = True
            await _acleanup(primary, self._writer.aabort())
            raise
        self._entries[header.path] = {
            "name": header.path,
            "size": header.size,
            "type": "directory" if header.kind is EntryKind.DIRECTORY else "file",
        }

    async def _mkdir(self, path: str, parents: bool, exist_ok: bool) -> None:
        self._check()
        existing = self._entries.get(path)
        if not path or existing is not None:
            if exist_ok and (
                not path or (existing is not None and existing["type"] == "directory")
            ):
                return
            raise FileExistsError(path)
        if self._active is not None and self._active.name == path:
            raise FileExistsError(path)
        ancestors = path.split("/")[:-1]
        for index in range(1, len(ancestors) + 1):
            parent = "/".join(ancestors[:index])
            entry = self._entries.get(parent)
            if entry is not None:
                if entry["type"] != "directory":
                    raise NotADirectoryError(parent)
            elif parents:
                await self._add(Header.directory(parent))
            else:
                raise FileNotFoundError(parent)
        await self._add(Header.directory(path))

    async def mkdir(self, path: str, parents: bool = True, exist_ok: bool = False) -> None:
        async with self._lock:
            await self._mkdir(_native.member_name(path), parents, exist_ok)

    async def _prepare(self, path: str) -> None:
        self._check()
        if not path:
            raise IsADirectoryError(path)
        if self._active is not None:
            raise RuntimeError("close the active member before opening another")
        if entry := self._entries.get(path):
            if entry["type"] == "directory":
                raise IsADirectoryError(path)
            raise FileExistsError(path)
        if "/" in path:
            parent = path.rsplit("/", 1)[0]
            if entry := self._entries.get(parent):
                if entry["type"] != "directory":
                    raise NotADirectoryError(parent)
            else:
                await self._mkdir(parent, True, True)

    async def open(self, path: str) -> _StagedMember:
        async with self._lock:
            path = _native.member_name(path)
            await self._prepare(path)
            body = await _writer_call(
                partial(tempfile.SpooledTemporaryFile, max_size=self._spool_memory, mode="w+b"),
                discard=lambda spool: spool.close(),
            )
            self._active = _StagedMember(self, path, cast(BinaryIO, body))
            return self._active

    async def add_file(self, path: str, body: Reader[bytes], size: int) -> None:
        async with self._lock:
            path = _native.member_name(path)
            await self._prepare(path)
            await self._add(Header.file(path, size), body)

    def snapshot(self) -> dict[str, FileInfo]:
        self._check()
        return {path: entry.copy() for path, entry in self._entries.items()}

    async def close(self, abort: bool = False) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            errors: list[BaseException] = []
            if self._active is not None:
                member, self._active = self._active, None
                member._closed = True
                try:
                    await _writer_call(member._body.close)
                except BaseException as error:
                    errors.append(error)
                if not abort:
                    errors.append(RuntimeError("archive closed with an open writable member"))
            if self._failed and not abort and not errors:
                errors.append(ValueError("archive writer failed"))
            abort = abort or self._failed or bool(errors)
            try:
                if abort:
                    await self._writer.aabort()
                else:
                    await self._writer.aclose()
            except BaseException as error:
                errors.append(error)
                abort = True
            await self.publication.finish(abort=abort, errors=tuple(errors))


class _StagedMember:
    mode = "wb"

    def __init__(self, session: _WriteSession, name: str, body: BinaryIO):
        self._session, self.name, self._body = session, name, body
        self._closed = False

    @property
    def path(self) -> str:
        return self.name

    @property
    def closed(self) -> bool:
        return self._closed or self._session._closed

    async def _io(self, operation: Callable[[], _T]) -> _T:
        async with self._session._lock:
            self._session._check()
            if self.closed:
                raise ValueError("I/O operation on closed file")
            return await _writer_call(operation)

    async def write(self, data: Buffer) -> int:
        return await self._io(partial(self._body.write, data))

    async def seek(self, offset: int, whence: int = 0) -> int:
        return await self._io(partial(self._body.seek, offset, whence))

    async def tell(self) -> int:
        return await self._io(self._body.tell)

    async def flush(self) -> None:
        await self._io(self._body.flush)

    async def close(self, abort: bool = False) -> None:
        async with self._session._lock:
            if self.closed:
                return
            self._closed = True
            self._session._active = None
            errors: list[BaseException] = []
            try:
                self._session._check()
                if abort:
                    self._session._failed = True
                else:

                    def rewind() -> int:
                        size = self._body.seek(0, 2)
                        self._body.seek(0)
                        return size

                    size = await _writer_call(rewind)
                    await self._session._add(Header.file(self.name, size), self._body)
            except BaseException as error:
                errors.append(error)
            try:
                await asyncio.shield(_retain(_writer_call(self._body.close)))
            except BaseException as error:
                errors.append(error)
            if errors:
                self._session._failed = True
            _raise_errors(errors)

    async def __aenter__(self) -> _StagedMember:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        return self

    async def __aexit__(self, exc_type: object, *args: object) -> None:
        await self.close(abort=exc_type is not None)
