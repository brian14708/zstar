"""Owned immutable metadata and views over native extent storage."""

from collections.abc import Sequence
from typing import overload

from . import _native
from ._kind import EntryKind as EntryKind
from ._native import Entry as Entry
from ._native import Extent as Extent
from ._native import Header as Header


class _Extents(Sequence[Extent]):
    __slots__ = ("_values",)

    def __init__(self, values: _native.Extents) -> None:
        self._values = values

    def __len__(self) -> int:
        return len(self._values)

    @overload
    def __getitem__(self, index: int) -> Extent: ...
    @overload
    def __getitem__(self, index: slice) -> tuple[Extent, ...]: ...
    def __getitem__(self, index: int | slice) -> Extent | tuple[Extent, ...]:
        return self._values[index]
