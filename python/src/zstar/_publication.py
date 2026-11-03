"""Backend output publication shared by archives and external objects."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import BinaryIO, Protocol, cast

from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from typing_extensions import Buffer


class _AsyncOutput(Protocol):
    async def write(self, data: bytes, /) -> int: ...
    async def close(self) -> None: ...


class _SetMetadata(Protocol):
    def __call__(self, path: str, /, **metadata: str) -> object: ...


class _AsyncSetMetadata(Protocol):
    async def __call__(self, path: str, /, **metadata: str) -> object: ...


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
    fsspec outputs are closed before commit and discarded on failure. Metadata
    runs only after publication and its failure never discards published bytes.
    """

    def __init__(
        self,
        output: BinaryIO,
        *,
        transactional: bool = False,
        after_commit: Callable[[], None] | None = None,
    ) -> None:
        self.output = output
        self._commit = getattr(output, "commit", None) if transactional else None
        self._discard = getattr(output, "discard", None) if transactional else None
        self._after_commit = after_commit
        self._finished = False

    @classmethod
    def open(
        cls,
        backend: AbstractFileSystem,
        path: str,
        loop: asyncio.AbstractEventLoop,
        metadata: Mapping[str, str] | None = None,
    ) -> _Publication:
        asynchronous = bool(getattr(backend, "asynchronous", False))
        setxattr = getattr(backend, "_setxattr" if asynchronous else "setxattr", None)
        if metadata and not callable(setxattr):
            raise NotImplementedError("metadata requires a backend with setxattr(), such as s3fs")
        if asynchronous:
            stream = cast(
                _AsyncOutput,
                asyncio.run_coroutine_threadsafe(backend.open_async(path, "wb"), loop).result(),
            )
            output = cast(BinaryIO, _AsyncSink(stream, loop))
        else:
            output = cast(BinaryIO, backend.open(path, "wb", autocommit=False))

        def update_metadata() -> None:
            if asynchronous:
                asyncio.run_coroutine_threadsafe(
                    cast(_AsyncSetMetadata, setxattr)(path, **dict(metadata or {})), loop
                ).result()
            else:
                cast(_SetMetadata, setxattr)(path, **dict(metadata or {}))

        return cls(
            output,
            transactional=not asynchronous,
            after_commit=update_metadata if metadata else None,
        )

    def finish(self, *, abort: bool = False, errors: tuple[BaseException, ...] = ()) -> None:
        if self._finished:
            return
        self._finished = True
        failures = list(errors)
        abort = abort or bool(failures)
        discarded = False
        if abort and self._discard is not None:
            try:
                self._discard()
                discarded = True
            except BaseException as error:
                failures.append(error)
            finally:
                # Prevent buffered close from uploading discarded output.
                if isinstance(self.output, AbstractBufferedFile):
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
        if abort and self._discard is not None and not discarded:
            try:
                self._discard()
            except BaseException as error:
                failures.append(error)
        if not abort and self._after_commit is not None:
            try:
                self._after_commit()
            except BaseException as error:
                failures.append(error)
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise BaseExceptionGroup("output publication failed", failures)
