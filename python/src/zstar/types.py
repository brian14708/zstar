"""Structural interfaces for custom storage and typed reader options."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from tarfile import TarInfo
from typing import Literal, NamedTuple, NotRequired, TypeAlias, TypedDict

from typing_extensions import Protocol, Reader, Writer

__all__ = [
    "AsyncBinaryWriter",
    "AsyncObjectResolver",
    "AsyncObjectWriter",
    "AsyncRangeSource",
    "AsyncReader",
    "AsyncReaderOptions",
    "BinaryWriter",
    "EntryMetadata",
    "ErrorCategory",
    "ExtensionValue",
    "FileInfo",
    "FileSystemReaderOptions",
    "Limits",
    "ObjectResolver",
    "ObjectWriter",
    "ProgressCallback",
    "RangeSource",
    "ReadReport",
    "ReadRequest",
    "ReaderOptions",
    "RequestProvider",
    "SeekableReader",
    "SignedRequest",
    "SpillStore",
    "WriterOptions",
]

ErrorCategory: TypeAlias = Literal[
    "not-zstar",
    "malformed-footer",
    "invalid-range",
    "missing-entry",
    "unsupported-version",
    "unsupported-feature",
    "unsupported-operation",
    "malformed-frame",
    "malformed-toc",
    "malformed-tar",
    "malformed-pointer",
    "integrity-failure",
    "tar-toc-mismatch",
    "missing-external-content",
    "malformed-external-content",
    "resource-limit",
    "io",
]
"""Stable archive and storage error categories reported by ZstarError.category."""


class SeekableReader(Reader[bytes], Protocol):
    """Borrowed binary input with a cursor; no write or close method required."""

    def seek(self, offset: int, whence: int = 0, /) -> int: ...
    def tell(self) -> int: ...


class BinaryWriter(Writer[bytes], Protocol):
    """Borrowed binary output; only write and flush are required."""

    def flush(self) -> None: ...


class RangeSource(Protocol):
    """Borrowed immutable source; calls may run concurrently on worker threads.

    Return exactly length bytes for a valid offset/length range, or raise an
    exception. Do not share an unprotected seek cursor or reenter this archive.
    The archive never closes the source. Blocking calls cannot be interrupted.
    """

    def read_range(self, offset: int, length: int, /) -> bytes: ...


class ObjectResolver(Protocol):
    """Borrowed, concurrently callable resolver of complete encoded objects.

    address is sha256: followed by 64 lowercase hex digits. hint is advisory;
    enforce max_bytes against actual bytes before unbounded buffering. Raise
    FileNotFoundError for missing objects. Do not reenter the calling archive.
    The archive decodes and applies its hash policy; it never closes the resolver.
    """

    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes: ...


class SpillStore(Reader[bytes], BinaryWriter, Protocol):
    """Exclusive temporary binary store used by bounded metadata spools."""

    def seek(self, offset: int, whence: int = 0, /) -> int: ...


class ProgressCallback(Protocol):
    """Progress reporting used by filesystem uploads and downloads."""

    def set_size(self, size: int, /) -> None: ...
    def relative_update(self, inc: int = 1, /) -> None: ...


class SignedRequest(TypedDict):
    """Signed HTTP URL, optional headers, and expiry returned by a request provider."""

    url: str
    headers: NotRequired[dict[str, str]]
    expires_in: NotRequired[float]


RequestProvider: TypeAlias = Callable[[Literal["GET", "PUT"], bool], Awaitable[SignedRequest]]
"""Async signer receiving an HTTP method and refresh flag, returning a signed request."""


class Limits(TypedDict, total=False):
    """Caller budgets; zero or omission disables each additional limit."""

    max_frame_encoded_bytes: int
    max_frame_decoded_bytes: int
    max_window_bytes: int
    max_entry_bytes: int
    max_extents: int
    max_pointer_bytes: int


class WriterOptions(TypedDict, total=False):
    """Typed archive-writer keyword options for compression, buffering, and storage limits."""

    limits: Limits | None
    workers: int
    root_extensions: dict[str, ExtensionValue] | None
    frame_size: int
    compression_level: int
    memory_bytes: int
    block_target: int
    max_spool_bytes: int
    create_spill_store: Callable[[], SpillStore] | None


class _ReaderBudgets(TypedDict, total=False):
    """Reader budgets and concurrency shared by raw and filesystem APIs."""

    limits: Limits | None
    memory_bytes: int
    max_encoded_bytes: int
    max_decoded_bytes: int
    max_read_bytes: int
    max_retained_entry_bytes: int
    max_entries: int
    max_spool_bytes: int
    create_spill_store: Callable[[], SpillStore] | None

    concurrent_chunks: int


class FileSystemReaderOptions(_ReaderBudgets, total=False):
    """Filesystem reader budgets; use external= and verify_external= for object policy."""


class ReaderOptions(_ReaderBudgets, total=False):
    """Synchronous reader budgets and borrowed synchronous resolver."""

    verify_external: bool
    resolver: ObjectResolver | None


class AsyncReaderOptions(_ReaderBudgets, total=False):
    """Async reader budgets with synchronous or coroutine-based resolution."""

    verify_external: bool
    resolver: ObjectResolver | AsyncObjectResolver | None


class ReadReport(TypedDict):
    """Content and validation checks performed while reading an archive member."""

    metadata_validated: bool
    full_archive_validated: bool
    data_frames_checked: list[int]
    external_objects_sha256_checked: list[str]
    external_objects_unchecked: list[str]


class FileInfo(TypedDict):
    """File metadata returned by the fsspec filesystem."""

    name: str
    size: int
    type: Literal["file", "directory", "link"]


ExtensionValue: TypeAlias = (
    bool
    | int
    | str
    | bytes
    | list["ExtensionValue"]
    | tuple["ExtensionValue", ...]
    | dict[str, "ExtensionValue"]
)
"""Restricted metadata values: no floats, nulls, tags, or non-string map keys."""


class ReadRequest(NamedTuple):
    """One member request using offset and length, not an end position."""

    path: str
    offset: int
    length: int


class AsyncRangeSource(Protocol):
    """Borrowed async source; exact offset/length reads on the opening event loop.

    The loop must stay running for the archive lifetime. Calls may overlap;
    cancellation cancels pending coroutine work. Keep the source immutable.
    """

    async def read_range(self, offset: int, length: int, /) -> bytes: ...


class AsyncObjectResolver(Protocol):
    """Async counterpart of ObjectResolver, using the opening event loop."""

    async def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes: ...


class AsyncReader(Protocol):
    """Borrowed async body. Return at most size bytes; b'' indicates EOF."""

    async def read(self, size: int, /) -> bytes: ...


class AsyncBinaryWriter(Protocol):
    """Borrowed async sink. Writes return a byte count; flush does not close it."""

    async def write(self, data: bytes, /) -> int: ...
    async def flush(self) -> None: ...


class ObjectWriter(Protocol):
    """Store one complete encoded external object; return its stored byte length.

    The SDK computes the address and compresses raw bytes. A deduplicating store
    may retain another valid representation of the same raw bytes. Returned
    lengths must be positive. Objects uploaded before a failure may remain;
    publication/retention belongs to the caller. The SDK never closes this store.
    """

    def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> int: ...


class AsyncObjectWriter(Protocol):
    """Async counterpart of ObjectWriter, on the writer's event loop."""

    async def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> int: ...


@dataclass
class EntryMetadata:
    """Owned tar metadata plus Zstar entry extensions for lossless rewrites.

    Pass info and extensions to writer.addfile(). Extent packing is regenerated;
    root extensions are separate and passed when constructing the output writer.
    """

    info: TarInfo
    extensions: dict[str, ExtensionValue]
