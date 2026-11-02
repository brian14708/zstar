"""Structural interfaces for custom storage and typed reader options."""

from collections.abc import Awaitable, Callable
from typing import Literal, NotRequired, TypeAlias, TypedDict

from typing_extensions import Protocol, Reader, Writer

__all__ = [
    "BinaryWriter",
    "CborValue",
    "EntryFields",
    "EntryTuple",
    "ErrorCategory",
    "ExtentTuple",
    "ManifestOptions",
    "ObjectResolver",
    "ProgressCallback",
    "RangeSource",
    "ReadReport",
    "ReaderOptions",
    "RequestProvider",
    "SeekableReader",
    "SignedRequest",
    "SpillStore",
    "VerificationReport",
    "WriterOptions",
]

CborValue: TypeAlias = (
    bool | int | str | bytes | list["CborValue"] | tuple["CborValue", ...] | dict[str, "CborValue"]
)
ExtentTuple: TypeAlias = tuple[int, int | str, int] | tuple[int, int | str, int, int]
EntryFields: TypeAlias = tuple[str, Literal[0, 2, 5], int, int, int, int, tuple[ExtentTuple, ...]]
EntryTuple: TypeAlias = (
    EntryFields
    | tuple[
        str, Literal[0, 2, 5], int, int, int, int, tuple[ExtentTuple, ...], dict[str, CborValue]
    ]
)

ErrorCategory: TypeAlias = Literal[
    "malformed-catalog",
    "changed-pack",
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


class SeekableReader(Reader[bytes], Protocol):
    """Borrowed binary input with a cursor; no write or close method required."""

    def seek(self, offset: int, whence: int = 0, /) -> int: ...
    def tell(self) -> int: ...


class BinaryWriter(Writer[bytes], Protocol):
    """Borrowed binary output; only write and flush are required."""

    def flush(self) -> None: ...


class RangeSource(Protocol):
    """Immutable, concurrently readable byte-range source."""

    def read_range(self, offset: int, length: int, /) -> bytes: ...


class ObjectResolver(Protocol):
    """Resolve a complete encoded external object by its digest address."""

    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes: ...


class SpillStore(Reader[bytes], BinaryWriter, Protocol):
    """Exclusive temporary binary store used by bounded metadata spools."""

    def seek(self, offset: int, whence: int = 0, /) -> int: ...


class ProgressCallback(Protocol):
    """Progress reporting used by filesystem uploads and downloads."""

    def set_size(self, size: int, /) -> None: ...
    def relative_update(self, inc: int = 1, /) -> None: ...


class SignedRequest(TypedDict):
    url: str
    headers: NotRequired[dict[str, str]]
    expires_in: NotRequired[float]


RequestProvider: TypeAlias = Callable[[Literal["GET", "PUT"], bool], Awaitable[SignedRequest]]


class WriterOptions(TypedDict, total=False):
    workers: int
    root_extensions: dict[str, CborValue] | None
    frame_size: int
    compression_level: int
    memory_bytes: int
    block_target: int
    max_spool_bytes: int
    create_spill_store: Callable[[], SpillStore] | None


class ManifestOptions(TypedDict, total=False):
    memory_bytes: int
    max_encoded_bytes: int
    max_decoded_bytes: int
    max_read_bytes: int
    max_retained_entry_bytes: int
    max_entries: int
    max_spool_bytes: int
    create_spill_store: Callable[[], SpillStore] | None


class ReaderOptions(ManifestOptions, total=False):
    concurrent_chunks: int
    verify_external: bool
    resolver: ObjectResolver | None


class ReadReport(TypedDict):
    metadata_authenticated: bool
    full_archive_validated: bool
    data_frames_checked: list[int]
    external_objects_sha256_checked: list[str]
    external_objects_unchecked: list[str]


class VerificationReport(TypedDict):
    metadata_validated: bool
    full_archive_verified: bool
    tar_members_verified: bool
    pointer_text_verified: bool
    data_checksums_checked: bool
    external_closure_validated: bool
    tar_members: int
    pointer_members: int
    data_frames: int
    external_objects_sha256_checked: list[str]
    external_objects_unchecked: list[str]
