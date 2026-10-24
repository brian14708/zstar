"""Idiomatic Python API for Zstar archives."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .fsspec import ZstarFileSystem as ZstarFileSystem
    from .fsspec import ZstarS3FileSystem as ZstarS3FileSystem

try:
    from ._native import (
        Chunking,
        DecodedChunk,
        DecodedMemoryCache,
        ExternalFile,
        PackedCatalog,
        PackedGeneration,
        PackedObjectResolver,
        PackStore,
        PackWriter,
        ZstarError,
        extract,
        materialize,
        pack_directory,
        pack_file,
        publish_packed_generation,
        publish_packed_generation_async,
        put_presigned,
    )
except ModuleNotFoundError as exc:
    if exc.name != "zstar._native":
        raise
    raise ImportError(
        "Zstar's native extension is unavailable. Install zstar-py, or run "
        "just check from the repository root to build and test a wheel."
    ) from exc

from ._archive import Archive as Archive
from ._archive import AsyncArchive as AsyncArchive
from ._info import TarInfo as TarInfo
from ._s3 import s3_archive_metadata as s3_archive_metadata
from ._writer import ArchiveWriter as ArchiveWriter

__all__ = [
    "Archive",
    "ArchiveWriter",
    "AsyncArchive",
    "Chunking",
    "DecodedChunk",
    "DecodedMemoryCache",
    "ExternalFile",
    "PackStore",
    "PackWriter",
    "PackedCatalog",
    "PackedGeneration",
    "PackedObjectResolver",
    "TarInfo",
    "ZstarError",
    "extract",
    "materialize",
    "pack_directory",
    "pack_file",
    "publish_packed_generation",
    "publish_packed_generation_async",
    "put_presigned",
    "s3_archive_metadata",
]


def __getattr__(name: str) -> object:
    if name in ("ZstarFileSystem", "ZstarS3FileSystem"):
        from . import fsspec

        cls = getattr(fsspec, name)
        globals()[name] = cls
        return cls
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"ZstarFileSystem"})
