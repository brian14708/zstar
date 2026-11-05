"""Private wire-format annotations shared by native adapters."""

from typing import Literal, TypeAlias

from .types import WriterOptions

CborValue: TypeAlias = (
    bool | int | str | bytes | list["CborValue"] | tuple["CborValue", ...] | dict[str, "CborValue"]
)
"""Recursive values accepted in archive and entry extension metadata."""
ExtentTuple: TypeAlias = tuple[int, int | str, int] | tuple[int, int | str, int, int]
"""Content extent: decoded size, source, encoded size, and optional decoded offset."""
#: Entry metadata: path, type, mode, uid, gid, nanosecond mtime, and extents.
EntryFields: TypeAlias = tuple[str, Literal[0, 2, 5], int, int, int, int, tuple[ExtentTuple, ...]]
EntryTuple: TypeAlias = (
    EntryFields
    | tuple[
        str, Literal[0, 2, 5], int, int, int, int, tuple[ExtentTuple, ...], dict[str, CborValue]
    ]
)
"""Lossless entry metadata, with an optional final attributes dictionary."""


MemberMetadata: TypeAlias = tuple[str, int, int, int, int, int, int, str, bool]
"""Name, kind, mode, uid, gid, nanosecond mtime, size, and symlink target."""


class NativeWriterOptions(WriterOptions, total=False):
    root_extensions: dict[str, CborValue] | None
