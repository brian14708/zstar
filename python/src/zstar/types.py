"""Structural contracts for borrowed storage objects and callback values."""

from collections.abc import Awaitable, Callable
from typing import Literal, NotRequired, Protocol, TypeAlias, TypedDict

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

ExtensionValue: TypeAlias = (
    bool
    | int
    | str
    | bytes
    | list["ExtensionValue"]
    | tuple["ExtensionValue", ...]
    | dict[str, "ExtensionValue"]
)
"""Restricted extension values: no floats, nulls, tags, or non-string map keys."""


class RangeSource(Protocol):
    """One immutable snapshot. Calls run serially on blocking workers.

    Opening queries size once. Return exactly length bytes from read_at, or
    raise. The archive borrows this object and never closes it.
    """

    def size(self) -> int: ...
    def read_at(self, offset: int, length: int, /) -> bytes: ...


class AsyncRangeSource(Protocol):
    """Snapshot source on the owning loop; independent range calls may overlap."""

    def size(self) -> Awaitable[int]: ...
    def read_at(self, offset: int, length: int, /) -> Awaitable[bytes]: ...


class Reader(Protocol):
    """Borrowed body: return at most size bytes; empty bytes indicate EOF."""

    def read(self, size: int, /) -> bytes: ...


class AsyncReader(Protocol):
    """Borrowed body returning awaitables on the owning loop."""

    def read(self, size: int, /) -> Awaitable[bytes]: ...


class SeekableReader(Reader, Protocol):
    """Borrowed binary file object; adapters preserve its cursor."""

    def seek(self, offset: int, whence: int = 0, /) -> int: ...
    def tell(self) -> int: ...


class BinaryWriter(Protocol):
    """Borrowed sink. Partial writes are valid; flush does not publish or close."""

    def write(self, data: bytes, /) -> int: ...
    def flush(self) -> None: ...


class AsyncBinaryWriter(Protocol):
    """Borrowed sink returning awaitables on the owning loop."""

    def write(self, data: bytes, /) -> Awaitable[int]: ...
    def flush(self) -> Awaitable[None]: ...


class ObjectResolver(Protocol):
    """Fetch complete encoded objects on serial blocking workers.

    Addresses are canonical sha256 strings. hint is advisory. Enforce max_bytes
    during fetching, before unbounded buffering. Raise FileNotFoundError for
    absent objects. The archive performs decoding and optional hash verification.
    """

    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes: ...


class AsyncObjectResolver(Protocol):
    """Object resolver on the owning loop, with the same bounded fetch contract."""

    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> Awaitable[bytes]: ...


class ObjectStore(Protocol):
    """Store encoded content; return its positive stored length.

    A deduplicating store may retain a different valid encoding. Objects already
    uploaded when an operation fails remain subject to the caller's retention
    policy. The writer never closes the store.
    """

    def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> int: ...


class AsyncObjectStore(Protocol):
    """Store encoded content on the owning loop."""

    def put_encoded(self, address: str, encoded: bytes, raw_size: int, /) -> Awaitable[int]: ...


class SignedRequest(TypedDict):
    """Signed URL, optional headers, and validity duration in seconds."""

    url: str
    headers: NotRequired[dict[str, str]]
    expires_in: NotRequired[float]


RequestProvider: TypeAlias = Callable[[Literal["GET", "PUT"], bool], SignedRequest]
AsyncRequestProvider: TypeAlias = Callable[[Literal["GET", "PUT"], bool], Awaitable[SignedRequest]]
