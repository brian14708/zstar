"""Optional fsspec integration; install zstar-py[fsspec] to use it."""

from ._fsspec import ZstarFileSystem as ZstarFileSystem
from .types import FileInfo as FileInfo

__all__ = ["FileInfo", "ZstarFileSystem"]
