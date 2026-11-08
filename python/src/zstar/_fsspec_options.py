"""Metadata and progress types for the optional fsspec integration."""

from typing import Literal, Protocol, TypedDict


class ProgressCallback(Protocol):
    def set_size(self, size: int, /) -> None: ...
    def relative_update(self, inc: int = 1, /) -> None: ...


class FileInfo(TypedDict):
    """File metadata returned by the fsspec filesystem."""

    name: str
    size: int
    type: Literal["file", "directory", "link"]
