"""Operation context carried into user callbacks by the native dispatcher."""

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from inspect import isawaitable, iscoroutine
from typing import TypeVar

from ._native import ZstarError

_T = TypeVar("_T")
_active: ContextVar[tuple[int, ...]] = ContextVar("zstar_callback_operations", default=())
_objects: ContextVar[tuple[int, ...]] = ContextVar("zstar_callback_objects", default=())
_blocking: ContextVar[bool] = ContextVar("zstar_blocking_callback", default=False)


def in_blocking_callback() -> bool:
    return _blocking.get()


def reject(token: int, objects: tuple[int, ...] = ()) -> None:
    if token in _active.get() or any(identity in _objects.get() for identity in objects):
        error = ZstarError("reentrant use from this object's callback")
        error.category = "unsupported-operation"
        raise error


def invoke(token: int, identity: int, function: Callable[..., _T], args: tuple[object, ...]) -> _T:
    marker = _active.set((*_active.get(), token))
    object_marker = _objects.set((*_objects.get(), identity))
    blocking_marker = _blocking.set(True)
    try:
        result = function(*args)
        if isawaitable(result):
            if iscoroutine(result):
                result.close()
            raise TypeError("sync callbacks must return ordinary results; use the async API")
        return result
    finally:
        _blocking.reset(blocking_marker)
        _objects.reset(object_marker)
        _active.reset(marker)


async def ainvoke(
    token: int,
    identity: int,
    function: Callable[..., Awaitable[_T]],
    args: tuple[object, ...],
    # Holds the native CallbackTicket until the callback finishes.
    _ticket: object,
) -> _T:
    marker = _active.set((*_active.get(), token))
    object_marker = _objects.set((*_objects.get(), identity))
    try:
        return await function(*args)
    finally:
        # A retained exception keeps this frame alive. Release admission even
        # when its traceback is retained by the session's terminal result.
        del _ticket
        _objects.reset(object_marker)
        _active.reset(marker)
