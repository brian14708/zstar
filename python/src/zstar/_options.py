"""Partial option dictionaries and immutable operation values.

Rust supplies defaults and validates options when opening a reader or writer.
"""

from dataclasses import dataclass
from typing import TypedDict

from .types import AsyncObjectResolver, ExtensionValue, ObjectResolver


class Limits(TypedDict, total=False):
    """Frame, window, entry, extent, and pointer budgets; zero disables a limit."""

    max_frame_encoded_bytes: int
    max_frame_decoded_bytes: int
    max_window_bytes: int
    max_entry_bytes: int
    max_extents: int
    max_pointer_bytes: int


class MetadataReaderOptions(TypedDict, total=False):
    """Per-page metadata size, retained index memory, and entry count limits."""

    max_page_bytes: int
    max_retained_entry_bytes: int
    max_entries: int


class MetadataWriterOptions(TypedDict, total=False):
    """Metadata page encoding and bounded in-memory working buffers."""

    max_page_bytes: int
    max_buffer_bytes: int
    block_target: int
    extension_pages: dict[str, list[ExtensionValue]]


class _ReaderOptions(TypedDict, total=False):
    limits: Limits
    metadata: MetadataReaderOptions
    max_read_bytes: int
    verify_external: bool
    concurrent_chunks: int


class ReaderOptions(_ReaderOptions, total=False):
    """Sync reader policy; user callbacks execute serially on blocking workers."""

    resolver: ObjectResolver | None


class AsyncReaderOptions(_ReaderOptions, total=False):
    """Async reader policy with callbacks on the opening event loop."""

    resolver: AsyncObjectResolver | None


class WriterOptions(TypedDict, total=False):
    """Encoding settings; extensions are copied when the writer opens."""

    limits: Limits
    metadata: MetadataWriterOptions
    frame_size: int
    compression_level: int
    workers: int
    root_extensions: dict[str, ExtensionValue]


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
