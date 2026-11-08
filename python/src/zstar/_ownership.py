"""Owned resource cleanup without changing borrowed storage protocols."""

import asyncio
import threading
from collections.abc import Awaitable, Callable
from typing import TypeVar, cast

_T = TypeVar("_T")
_background: set[asyncio.Future[object]] = set()
_background_lock = threading.Lock()


def _release(task: asyncio.Future[object]) -> None:
    with _background_lock:
        _background.discard(task)
    if not task.cancelled():
        task.exception()  # Observe detached failures; joining still raises them.


def _retain(awaitable: Awaitable[_T]) -> asyncio.Future[_T]:
    task = asyncio.ensure_future(awaitable)
    with _background_lock:
        _background.add(cast(asyncio.Future[object], task))
    task.add_done_callback(lambda completed: _release(cast(asyncio.Future[object], completed)))
    return task


async def _acquire(awaitable: Awaitable[_T], dispose: Callable[[_T], Awaitable[None]]) -> _T:
    """Retain acquisition across cancellation and dispose an unclaimed result."""
    task = _retain(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:

        async def abandoned() -> None:
            try:
                value = await task
            except BaseException:
                return
            await dispose(value)

        _retain(abandoned())
        raise


def _cleanup(primary: BaseException, operation: Callable[[], None]) -> None:
    try:
        operation()
    except BaseException as secondary:
        if secondary is primary:
            return
        raise BaseExceptionGroup("operation and cleanup failed", [primary, secondary]) from None


async def _acleanup(primary: BaseException, operation: Awaitable[None]) -> None:
    try:
        await asyncio.shield(_retain(operation))
    except BaseException as secondary:
        if secondary is primary:
            return
        raise BaseExceptionGroup("operation and cleanup failed", [primary, secondary]) from None
