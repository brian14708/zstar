"""Borrowed async writer streams bridged from bounded native workers."""

import asyncio
import concurrent.futures
import threading
from collections.abc import AsyncIterable, Awaitable
from typing import Any, TypeVar

from .types import AsyncBinaryWriter, AsyncObjectWriter, AsyncReader

_T = TypeVar("_T")


class _AsyncIO:
    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.lock = threading.Lock()
        self.pending: set[concurrent.futures.Future[Any]] = set()
        self.cancelled = False

    def call(self, awaitable: Awaitable[_T]) -> _T:
        async def run() -> _T:
            return await awaitable

        coroutine = run()
        with self.lock:
            if self.cancelled:
                coroutine.close()
                # The body may already have produced a coroutine object.
                if asyncio.iscoroutine(awaitable):
                    awaitable.close()
                raise OSError("async writer operation cancelled")
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
            except BaseException:
                coroutine.close()
                if asyncio.iscoroutine(awaitable):
                    awaitable.close()
                raise
            self.pending.add(future)
        try:
            return future.result()
        finally:
            with self.lock:
                self.pending.discard(future)

    def cancel(self) -> None:
        with self.lock:
            self.cancelled = True
            futures = tuple(self.pending)
        for future in futures:
            future.cancel()


class _AsyncSink:
    def __init__(self, sink: AsyncBinaryWriter, bridge: _AsyncIO) -> None:
        self.sink, self.bridge = sink, bridge

    def write(self, data: bytes) -> int:
        return self.bridge.call(self.sink.write(data))

    def flush(self) -> None:
        self.bridge.call(self.sink.flush())


class _AsyncBody:
    def __init__(self, body: AsyncReader, bridge: _AsyncIO) -> None:
        self.body, self.bridge = body, bridge

    def read(self, size: int = -1) -> bytes:
        return self.bridge.call(self.body.read(size))


class _AsyncStore:
    def __init__(self, store: AsyncObjectWriter, bridge: _AsyncIO) -> None:
        self.store, self.bridge = store, bridge

    def put_encoded(self, address: str, encoded: bytes, raw_size: int) -> int:
        return self.bridge.call(self.store.put_encoded(address, encoded, raw_size))


class _AsyncChunks:
    def __init__(self, chunks: AsyncIterable[bytes], bridge: _AsyncIO) -> None:
        self.iterator = aiter(chunks)
        self.bridge = bridge

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        try:
            return self.bridge.call(anext(self.iterator))
        except StopAsyncIteration:
            raise StopIteration from None
