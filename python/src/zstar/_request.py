"""Immutable member read requests and integer validation."""

from dataclasses import dataclass


def _u64(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")
    if value > (1 << 64) - 1:
        raise OverflowError(f"{name} exceeds uint64")


@dataclass(frozen=True, slots=True)
class ReadRequest:
    """One exact member range; omitted length reads from offset through EOF."""

    path: str
    offset: int = 0
    length: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.path, str):
            raise TypeError("path must be a string")
        _u64(self.offset, "offset")
        if self.length is not None:
            _u64(self.length, "length")
