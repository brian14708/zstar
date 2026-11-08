"""Archive opening with Python's familiar file and mode convention."""

# ruff: noqa: A001 - open is the public archive entry point.

import os
from typing import Literal, TypeAlias, cast, overload

from typing_extensions import Buffer

from ._async_open import _AsyncOpen
from ._reader_api import Archive, AsyncArchive
from ._writer_api import ArchiveWriter, AsyncArchiveWriter
from .types import (
    AsyncBinaryWriter,
    AsyncRangeSource,
    BinaryWriter,
    RangeSource,
    ReaderOptions,
    SeekableReader,
    WriterOptions,
)

_ReadInput: TypeAlias = str | os.PathLike[str] | Buffer | SeekableReader | RangeSource
_AsyncReadInput: TypeAlias = str | os.PathLike[str] | Buffer | SeekableReader | AsyncRangeSource
_WriteInput: TypeAlias = str | os.PathLike[str] | BinaryWriter
_AsyncWriteInput: TypeAlias = str | os.PathLike[str] | AsyncBinaryWriter


@overload
def open(
    file: _ReadInput,
    mode: Literal["r"] = "r",
    *,
    async_: Literal[False] = False,
    atomic: Literal[False] = False,
    options: ReaderOptions | None = None,
) -> Archive: ...


@overload
def open(
    file: _WriteInput,
    mode: Literal["w", "x"],
    *,
    async_: Literal[False] = False,
    atomic: bool = False,
    options: WriterOptions | None = None,
) -> ArchiveWriter: ...


@overload
def open(
    file: _AsyncReadInput,
    mode: Literal["r"] = "r",
    *,
    async_: Literal[True],
    atomic: Literal[False] = False,
    options: ReaderOptions | None = None,
) -> _AsyncOpen[AsyncArchive]: ...


@overload
def open(
    file: _AsyncWriteInput,
    mode: Literal["w", "x"],
    *,
    async_: Literal[True],
    atomic: bool = False,
    options: WriterOptions | None = None,
) -> _AsyncOpen[AsyncArchiveWriter]: ...


def open(
    file: _ReadInput | _AsyncReadInput | _WriteInput | _AsyncWriteInput,
    mode: Literal["r", "w", "x"] = "r",
    *,
    async_: bool = False,
    atomic: bool = False,
    options: ReaderOptions | WriterOptions | None = None,
) -> Archive | ArchiveWriter | _AsyncOpen[AsyncArchive] | _AsyncOpen[AsyncArchiveWriter]:
    """Open an archive for reading (r), writing (w), or exclusive creation (x).

    Use ``with open(file)`` for a blocking reader and ``with open(file, "w")``
    for a writer. With ``async_=True``, the result supports ``async with`` or
    awaiting for explicit lifetime management on the current event loop.

    Paths and URLs own storage. Reader buffers become immutable snapshots.
    Caller-supplied binary files, range sources, and writers remain borrowed.
    Import ReaderOptions and WriterOptions from zstar.types and pass one options
    dictionary. Nested source/sink settings apply to paths and URLs. Atomic local
    output publishes after successful finalization, preserving previous contents
    on failure.
    """
    if not isinstance(mode, str):
        raise TypeError("mode must be a string")
    if mode not in ("r", "w", "x"):
        raise ValueError("mode must be r, w, or x")
    if not isinstance(async_, bool):
        raise TypeError("async_ must be a bool")
    if not isinstance(atomic, bool):
        raise TypeError("atomic must be a bool")
    if mode == "r" and atomic:
        raise ValueError("atomic applies only to writing")
    if async_:
        if mode == "r":
            return _AsyncOpen(
                AsyncArchive._open(
                    cast(_AsyncReadInput, file),
                    options=cast(ReaderOptions | None, options),
                )
            )
        return _AsyncOpen(
            AsyncArchiveWriter._open(
                cast(_AsyncWriteInput, file),
                mode=mode,
                atomic=atomic,
                options=cast(WriterOptions | None, options),
            )
        )
    if mode == "r":
        return Archive._open(
            cast(_ReadInput, file),
            options=cast(ReaderOptions | None, options),
        )
    return ArchiveWriter._open(
        cast(_WriteInput, file),
        mode=mode,
        atomic=atomic,
        options=cast(WriterOptions | None, options),
    )
