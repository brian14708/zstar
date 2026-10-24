"""Thread-safe range source over a caller-owned seekable binary file."""

from __future__ import annotations

import os
import threading
from typing import BinaryIO


class _FileSource:
    """Serialize ``seek``/``read`` pairs on a shared file and restore its cursor.

    Raw files may return fewer bytes than requested, so reads loop until
    ``length`` bytes arrive or end of file. The caller keeps ownership of the file.
    """

    def __init__(self, file: BinaryIO) -> None:
        self.file, self.lock = file, threading.Lock()
        position = file.tell()
        try:
            file.seek(0, os.SEEK_END)
            self.size = file.tell()
        finally:
            file.seek(position)

    def read_range(self, offset: int, length: int) -> bytes:
        with self.lock:
            position = self.file.tell()
            try:
                self.file.seek(offset)
                chunks: list[bytes] = []
                remaining = length
                while remaining > 0:
                    chunk = self.file.read(remaining)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                return chunks[0] if len(chunks) == 1 else b"".join(chunks)
            finally:
                self.file.seek(position)
