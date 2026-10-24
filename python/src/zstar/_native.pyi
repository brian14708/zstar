from collections.abc import Awaitable, Sequence
from os import PathLike
from types import TracebackType
from typing import BinaryIO, Literal, TypedDict, Unpack

from typing_extensions import Buffer

from .types import (
    CborValue,
    EntryTuple,
    ErrorCategory,
    ManifestOptions,
    ObjectResolver,
    RangeSource,
    ReaderOptions,
    ReadReport,
    RequestProvider,
    VerificationReport,
    WriterOptions,
)

class ZstarError(ValueError):
    category: ErrorCategory

class DecodedChunk:
    @property
    def key(self) -> str: ...
    @property
    def data(self) -> bytes: ...
    @property
    def weight(self) -> int: ...

class DecodedMemoryCache:
    def __init__(self, capacity: int) -> None: ...
    @property
    def used_bytes(self) -> int: ...
    def clear(self) -> None: ...

class MemberCursor: ...

class IndexedArchive:
    def __init__(
        self, source: RangeSource, size: int, **options: Unpack[ReaderOptions]
    ) -> None: ...
    @staticmethod
    def from_bytes(data: Buffer, **options: Unpack[ReaderOptions]) -> IndexedArchive: ...
    @staticmethod
    def from_file(path: str, **options: Unpack[ReaderOptions]) -> IndexedArchive: ...
    @staticmethod
    def from_url(
        url: str, *, headers: dict[str, str] | None = None, **options: Unpack[ReaderOptions]
    ) -> IndexedArchive: ...
    @staticmethod
    def from_s3(
        bucket: str,
        key: str,
        *,
        endpoint: str | None = None,
        region: str | None = None,
        object_prefix: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        **options: Unpack[ReaderOptions],
    ) -> IndexedArchive: ...
    @property
    def closed(self) -> bool: ...
    @property
    def cache_bytes(self) -> int: ...
    @property
    def entry_count(self) -> int: ...
    def entry(self, path: str) -> EntryTuple | None: ...
    def _entry(self, path: str) -> EntryInfo: ...
    def iter_entries(self, prefix: str | None = None) -> IndexedEntries: ...
    def paths(self) -> list[str]: ...
    def read(
        self,
        path: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> bytes: ...
    def read_many(
        self, requests: Sequence[tuple[str, int, int]], *, resolver: ObjectResolver | None = None
    ) -> list[bytes]: ...
    def _open_member(self, path: str) -> MemberCursor: ...
    def _read_member(self, cursor: MemberCursor, offset: int, length: int) -> bytes: ...
    def _read_member_into(self, cursor: MemberCursor, offset: int, buffer: Buffer) -> int: ...
    def _readinto(self, path: str, offset: int, buffer: Buffer) -> int: ...
    def read_with_report(
        self, path: str, offset: int, length: int | None, *, resolver: ObjectResolver | None = None
    ) -> tuple[bytes, ReadReport]: ...
    def verify(self, *, external: bool = False) -> VerificationReport: ...
    def validate_metadata(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> IndexedArchive: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class IndexedEntries:
    def __iter__(self) -> IndexedEntries: ...
    def __next__(self) -> EntryTuple: ...

class EntryInfo(TypedDict):
    path: str
    kind: Literal["file", "directory", "symlink"]
    size: int
    mode: int
    uid: int
    gid: int
    mtime_ns: int

class AsyncArchive:
    @staticmethod
    def from_bytes(data: Buffer, **options: Unpack[ReaderOptions]) -> Awaitable[AsyncArchive]: ...
    @staticmethod
    def from_source(
        source: RangeSource, size: int, **options: Unpack[ReaderOptions]
    ) -> Awaitable[AsyncArchive]: ...
    @staticmethod
    def from_file(path: str, **options: Unpack[ReaderOptions]) -> Awaitable[AsyncArchive]: ...
    @staticmethod
    def from_fileobj(
        fileobj: BinaryIO, **options: Unpack[ReaderOptions]
    ) -> Awaitable[AsyncArchive]: ...
    @staticmethod
    def from_url(
        url: str | None = None,
        *,
        headers: dict[str, str] | None = None,
        provider: RequestProvider | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Awaitable[AsyncArchive]: ...
    @staticmethod
    def from_s3(
        bucket: str,
        key: str,
        *,
        endpoint: str | None = None,
        region: str | None = None,
        object_prefix: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        **options: Unpack[ReaderOptions],
    ) -> Awaitable[AsyncArchive]: ...
    @property
    def closed(self) -> bool: ...
    @property
    def cache_bytes(self) -> int: ...
    @property
    def entry_count(self) -> int: ...
    def entry(self, path: str) -> Awaitable[EntryTuple | None]: ...
    def _entry(self, path: str) -> Awaitable[EntryInfo]: ...
    def paths(self) -> Awaitable[list[str]]: ...
    def read_link(self, path: str) -> Awaitable[str]: ...
    def validate_metadata(self) -> Awaitable[None]: ...
    def iter_entries(self, prefix: str | None = None) -> AsyncEntries: ...
    def read(
        self,
        path: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
    ) -> Awaitable[bytes]: ...
    def read_many(
        self, requests: Sequence[tuple[str, int, int]], *, resolver: ObjectResolver | None = None
    ) -> Awaitable[list[bytes]]: ...
    def _open_member(self, path: str) -> Awaitable[MemberCursor]: ...
    def _read_member(self, cursor: MemberCursor, offset: int, length: int) -> Awaitable[bytes]: ...
    def _read_member_into(
        self, cursor: MemberCursor, offset: int, buffer: Buffer
    ) -> Awaitable[int]: ...
    def _readinto(self, path: str, offset: int, buffer: Buffer) -> Awaitable[int]: ...
    def read_with_report(
        self, path: str, offset: int, length: int | None, *, resolver: ObjectResolver | None = None
    ) -> Awaitable[tuple[bytes, ReadReport]]: ...
    def verify(self, *, external: bool = False) -> Awaitable[VerificationReport]: ...
    def close(self) -> Awaitable[None]: ...
    def __aenter__(self) -> Awaitable[AsyncArchive]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Awaitable[None]: ...

class AsyncEntries:
    def __aiter__(self) -> AsyncEntries: ...
    def __anext__(self) -> Awaitable[EntryTuple]: ...
    def _next_batch(self) -> Awaitable[list[EntryTuple]]: ...

class ManifestArchive:
    def __init__(
        self, source: RangeSource, size: int, **options: Unpack[ManifestOptions]
    ) -> None: ...
    @property
    def closed(self) -> bool: ...
    @property
    def entry_count(self) -> int: ...
    @property
    def spilled(self) -> bool: ...
    @property
    def content_cache_bytes(self) -> int: ...
    def entry(self, path: str) -> EntryTuple | None: ...
    def entries(self) -> list[EntryTuple]: ...
    def iter_entries(self) -> ManifestEntries: ...
    def read(
        self,
        path: str,
        offset: int = 0,
        length: int | None = None,
        *,
        resolver: ObjectResolver | None = None,
        verify_external: bool = True,
    ) -> bytes: ...
    def validate_metadata(self) -> None: ...
    def validate(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> ManifestArchive: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class ManifestEntries:
    def __iter__(self) -> ManifestEntries: ...
    def __next__(self) -> EntryTuple: ...

class EntryArchiveWriter:
    def __init__(self, output: BinaryIO, **options: Unpack[WriterOptions]) -> None: ...
    @property
    def closed(self) -> bool: ...
    def add(self, entry: EntryTuple, body: BinaryIO | None = None, size: int = 0) -> None: ...
    def add_path(
        self,
        path: str | PathLike[str],
        archive_name: str,
        *,
        preserve_metadata: bool = False,
        recursive: bool = True,
    ) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> EntryArchiveWriter: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

def decode_manifest_block(raw: bytes) -> CborValue: ...
def encode_manifest_block(value: CborValue) -> bytes: ...
def decode_object(encoded: bytes, size: int) -> bytes: ...
def pack_file(path: str, data: bytes) -> bytes: ...
def pack_directory(path: str | PathLike[str], *, preserve_metadata: bool = False) -> bytes: ...
def extract(
    data: bytes,
    destination: str | PathLike[str],
    *,
    objects: str | PathLike[str] | None = None,
    allow_special_bits: bool = False,
    verify_external: bool = True,
) -> None: ...
def materialize(
    data: bytes, *, objects: str | PathLike[str] | None = None, verify_external: bool = True
) -> bytes: ...
def put_presigned(
    data: bytes,
    url: str | None = None,
    *,
    headers: dict[str, str] | None = None,
    provider: RequestProvider | None = None,
) -> Awaitable[None]: ...
def _s3_archive_metadata(footer: bytes, source_size: int) -> dict[str, str]: ...

class Chunking:
    def __init__(
        self,
        kind: Literal["fixed", "fastcdc"] = "fixed",
        *,
        target_bytes: int | None = None,
        min_bytes: int = 64 << 10,
        max_bytes: int = 1 << 20,
    ) -> None: ...

class ExternalFile:
    @property
    def size(self) -> int: ...
    @property
    def chunk_count(self) -> int: ...
    def _entry(self, entry: EntryTuple) -> EntryTuple: ...

class PackStore:
    @staticmethod
    def local(root: str | PathLike[str]) -> PackStore: ...
    @staticmethod
    def s3(
        bucket: str,
        *,
        prefix: str = "",
        endpoint: str | None = None,
        region: str | None = None,
        anon: bool | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        part_size: int = 8 << 20,
        concurrency: int = 4,
    ) -> PackStore: ...

class PackedCatalog:
    def __init__(
        self,
        data: bytes | None = None,
        *,
        max_encoded_bytes: int = 16 << 20,
        max_memory_bytes: int = 512 << 20,
        max_packs: int = 65536,
        max_chunks: int = 262144,
    ) -> None: ...
    def encode(self) -> bytes: ...
    @property
    def chunk_count(self) -> int: ...
    @property
    def pack_count(self) -> int: ...

class PackWriter:
    def __init__(
        self,
        store: PackStore,
        *,
        base: PackedCatalog | None = None,
        prefix: str = "objects",
        target_bytes: int = 64 << 20,
        compression_level: int = 3,
        memory_bytes: int = 8 << 20,
        max_spool_bytes: int = 4 << 30,
        max_chunks: int = 262144,
        max_packs: int = 65536,
        max_catalog_bytes: int = 16 << 20,
        max_index_bytes: int = 512 << 20,
    ) -> None: ...
    def __enter__(self) -> PackWriter: ...
    def __exit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def __aenter__(self) -> Awaitable[PackWriter]: ...
    def __aexit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Awaitable[None]: ...
    def externalize(
        self, fileobj: BinaryIO, size: int, *, chunking: Chunking | None = None
    ) -> ExternalFile: ...
    def externalize_async(
        self, fileobj: BinaryIO, size: int, *, chunking: Chunking | None = None
    ) -> Awaitable[ExternalFile]: ...
    def finish(self) -> PackedCatalog: ...
    def finish_async(self) -> Awaitable[PackedCatalog]: ...
    def completed_packs(self) -> list[str]: ...
    def close(self) -> None: ...

class PackedObjectResolver:
    def __init__(
        self,
        store: PackStore,
        catalog: PackedCatalog,
        *,
        max_range_bytes: int = 8 << 20,
        max_overfetch_bytes: int = 0,
        max_batch_bytes: int = 64 << 20,
        cache_bytes: int = 0,
        concurrency: int = 8,
    ) -> None: ...
    def get_encoded(self, address: str, hint: int, max_bytes: int, /) -> bytes: ...

class PackedGeneration:
    def __init__(self, data: bytes) -> None: ...
    def encode(self) -> bytes: ...
    @property
    def archive_key(self) -> str: ...
    @property
    def catalog_key(self) -> str: ...

def publish_packed_generation(
    store: PackStore,
    archive: BinaryIO,
    catalog: PackedCatalog,
    *,
    prefix: str = "objects",
    chunking: Chunking | None = None,
) -> tuple[str, PackedGeneration]: ...
def publish_packed_generation_async(
    store: PackStore,
    archive: BinaryIO,
    catalog: PackedCatalog,
    *,
    prefix: str = "objects",
    chunking: Chunking | None = None,
) -> Awaitable[tuple[str, PackedGeneration]]: ...
