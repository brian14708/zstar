"""Option dictionaries, storage protocols, and callback values.

Options are partial TypedDict dictionaries. Omitted keys use native defaults;
unknown keys and invalid values fail before storage I/O. The same option types
serve synchronous and asynchronous APIs; callbacks must match the selected mode.
"""

from collections.abc import Awaitable, Callable
from typing import Literal, NotRequired, Protocol, Required, TypeAlias, TypedDict

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
    """Maximum encoded or decoded bytes per metadata page; default 8 MiB, positive."""
    max_retained_entry_bytes: int
    """Directory/cache/index and temporary full-scan buffer budget; default 1 GiB."""
    max_entries: int
    """Maximum entry count; default zero disables the additional count limit."""


class MetadataWriterOptions(TypedDict, total=False):
    """Metadata page encoding and bounded in-memory working buffers."""

    max_page_bytes: int
    """Maximum encoded or decoded bytes per metadata page; default 8 MiB, positive."""
    max_buffer_bytes: int
    """Maximum bytes per temporary metadata buffer or sorting index; default 128 MiB."""
    block_target: int
    """Decoded page target; default 64 KiB, from 512 bytes through max_page_bytes."""
    extension_pages: dict[str, list[ExtensionValue]]
    """Optional independently compressed page groups keyed by x-* extension names."""


class Credentials(TypedDict):
    """Explicit S3 credentials."""

    access_key_id: str
    secret_access_key: str
    session_token: NotRequired[str | None]


class S3Options(TypedDict, total=False):
    """Explicit S3 region and credentials or anonymous access; no ambient discovery."""

    region: Required[str]
    """Explicit signing region."""
    endpoint: str | None
    """Optional endpoint for an S3-compatible service."""
    anonymous: bool
    """Enable unsigned access; mutually exclusive with credentials. Default false."""
    credentials: Credentials | None
    """Explicit credentials, required unless anonymous is true."""


class RangeCacheOptions(TypedDict, total=False):
    """Native compressed range cache; an empty dictionary enables defaults."""

    capacity_bytes: int
    """Retained compressed payload budget; default 16 MiB. Zero disables retention."""
    max_entries: int
    """Separate bound on retained intervals/bookkeeping; default 1024, positive."""
    read_ahead_bytes: int
    """Extra forward bytes fetched on a miss; default zero."""
    max_fetch_bytes: int
    """Maximum expanded fetch size; default 8 MiB, positive. Larger reads stay exact."""
    max_in_flight: int
    """Maximum outstanding fetch intervals; default eight, from one through 1024."""


class SourceOptions(TypedDict, total=False):
    """Native transport settings for ReaderOptions.source on paths and URLs.

    Providers return ordinary results in sync mode and awaitables in async mode.
    Omit range_cache or use None to disable the compressed range cache.
    """

    headers: dict[str, str]
    """HTTP headers; cannot be combined with a request provider's own headers."""
    request_provider: RequestProvider | AsyncRequestProvider | None
    """HTTP signed-request provider matching the opening mode; default None."""
    s3: S3Options | None
    """Required configuration for S3 URLs; unavailable on other source kinds."""
    range_cache: RangeCacheOptions | None
    """Compressed interval cache; None or omission disables it, {} enables defaults."""


class SinkOptions(TypedDict, total=False):
    """Native transport settings for WriterOptions.sink on paths and URLs."""

    s3: S3Options | None
    """Required configuration for S3 URLs; unavailable on local output."""


class ReaderOptions(TypedDict, total=False):
    """Reader policy and optional native source transport settings.

    Resolvers must match the opening mode: ObjectResolver for synchronous reads,
    AsyncObjectResolver for async reads. Custom sources manage their own transport
    and reject source settings. fsspec uses fs= or target_options for transport.
    """

    limits: Limits
    """Per-frame and per-entry limits; all additional limits default to disabled."""
    metadata: MetadataReaderOptions
    """Metadata page, retained-index, and entry-count budgets."""
    max_read_bytes: int
    """Maximum returned bytes per allocating read or batch; default 64 MiB, positive."""
    verify_external: bool
    """Check decoded external object SHA-256 in addition to checksums; default false."""
    concurrent_chunks: int
    """Concurrent content fetch/decode work; default eight, from one through 1024."""
    resolver: ObjectResolver | AsyncObjectResolver | None
    """Borrowed external object resolver matching the opening mode; default None."""
    source: SourceOptions | None
    """Transport settings for path/URL inputs; borrowed inputs reject these settings."""


class WriterOptions(TypedDict, total=False):
    """Writer encoding policy and optional native sink transport settings.

    Extensions and options are copied on opening. Custom sinks reject sink
    settings; fsspec uses fs= or target_options for transport.
    """

    limits: Limits
    """Per-frame and per-entry limits; all additional limits default to disabled."""
    metadata: MetadataWriterOptions
    """Metadata page encoding and temporary memory budgets."""
    frame_size: int
    """Decoded data-frame target; default 1 MiB, at least 512 bytes."""
    compression_level: int
    """Zstandard data compression level; default three."""
    workers: int
    """Compression workers, from zero (serial, default) through sixteen."""
    root_extensions: dict[str, ExtensionValue]
    """Archive-level x-* extension values, copied on opening."""
    sink: SinkOptions | None
    """Transport settings for path/URL output; borrowed sinks reject these settings."""


class ProgressCallback(Protocol):
    """Progress notifications accepted by fsspec downloads."""

    def set_size(self, size: int, /) -> None: ...
    def relative_update(self, inc: int = 1, /) -> None: ...


class FileInfo(TypedDict):
    """File metadata returned by the optional fsspec filesystem."""

    name: str
    size: int
    type: Literal["file", "directory", "link"]
