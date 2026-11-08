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

from ._api import open as open  # noqa: A004 - canonical archive entry point.
from ._file import ArchiveFile as ArchiveFile
from ._file import AsyncArchiveFile as AsyncArchiveFile
from ._metadata import Entry as Entry
from ._metadata import EntryKind as EntryKind
from ._metadata import Extent as Extent
from ._metadata import Header as Header
from ._reader_api import Archive as Archive
from ._reader_api import AsyncArchive as AsyncArchive
from ._request import ReadRequest as ReadRequest
from ._writer_api import ArchiveWriter as ArchiveWriter
from ._writer_api import AsyncArchiveWriter as AsyncArchiveWriter

__all__ = [
    "Archive",
    "ArchiveFile",
    "ArchiveWriter",
    "AsyncArchive",
    "AsyncArchiveFile",
    "AsyncArchiveWriter",
    "Entry",
    "EntryKind",
    "Extent",
    "Header",
    "ReadRequest",
    "ZstarError",
    "open",
]
