"""Cursor-preserving positional access to borrowed file objects."""

import threading
from weakref import WeakValueDictionary

from . import _native
from ._request import _u64
from .types import RangeSource, SeekableReader


class _FileObjectSource:
    def __init__(self, file: SeekableReader) -> None:
        self._file = file
        self._size: int | None = None
        self._lock = threading.Lock()

    def _size_locked(self) -> int:
        if self._size is None:
            position = self._file.tell()
            try:
                self._file.seek(0, 2)
                size = self._file.tell()
                _u64(size, "source size")
                self._size = size
            finally:
                self._file.seek(position)
        return self._size

    def size(self) -> int:
        with self._lock:
            return self._size_locked()

    def read_at(self, offset: int, length: int) -> bytes:
        _u64(offset, "offset")
        _u64(length, "length")
        with self._lock:
            size = self._size_locked()
            if offset + length > size:
                error = _native.ZstarError("range exceeds source size")
                error.category = "invalid-range"
                raise error
            position = self._file.tell()
            try:
                self._file.seek(offset)
                # Complete reads, the common case, are returned without copying.
                chunks: list[bytes] = []
                remaining = length
                while remaining:
                    chunk = self._file.read(remaining)
                    if not isinstance(chunk, bytes):
                        raise TypeError("file read must return bytes")
                    if not chunk:
                        raise EOFError("short file range")
                    if len(chunk) > remaining:
                        raise ValueError("oversized file read")
                    if len(chunk) == length:
                        return chunk
                    chunks.append(chunk)
                    remaining -= len(chunk)
                return b"".join(chunks)
            finally:
                self._file.seek(position)


_file_sources: WeakValueDictionary[int, _FileObjectSource] = WeakValueDictionary()
_file_sources_gate = threading.Lock()


def fileobj(file: SeekableReader) -> RangeSource:
    """Borrow a seekable binary file, preserving its cursor and ownership.

    Adapters over the same object share a callback identity and serialize their
    seek/read pairs, so callback reentry is rejected before waiting on the file.
    The snapshot's size is captured on first use; keep its contents immutable.
    """
    with _file_sources_gate:
        source = _file_sources.get(id(file))
        if source is None:
            source = _FileObjectSource(file)
            _file_sources[id(file)] = source
        return source
