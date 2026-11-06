"""Idiomatic Python API for Zstar archives."""

try:
    from ._native import ZstarError
except ModuleNotFoundError as _exc:
    if _exc.name != "zstar._native":
        raise
    raise ImportError(
        "Zstar's native extension is unavailable. Install zstar-py, or run "
        "just check from the repository root to build and test a wheel."
    ) from _exc

from ._archive import Archive as Archive
from ._archive import AsyncArchive as AsyncArchive
from ._fsspec import ZstarFileSystem as ZstarFileSystem
from ._writer import ArchiveWriter as ArchiveWriter
from ._writer import AsyncArchiveWriter as AsyncArchiveWriter

__all__ = [
    "Archive",
    "ArchiveWriter",
    "AsyncArchive",
    "AsyncArchiveWriter",
    "ZstarError",
    "ZstarFileSystem",
]
