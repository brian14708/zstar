"""Cancellation-safe worker dispatch for fsspec staging/publication."""

import asyncio
from collections.abc import Callable
from functools import partial
from typing import TypeVar

from . import _native

_T = TypeVar("_T")


async def _writer_call(
    operation: Callable[[], _T],
    *,
    discard: Callable[[_T], None] | None = None,
    cancel: Callable[[], None] | None = None,
) -> _T:
    """Join native work before propagating cancellation or releasing ownership."""
    future = asyncio.ensure_future(_native._writer_call(operation))
    cancelled: asyncio.CancelledError | None = None
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError as error:
            cancelled = error
            if cancel is not None:
                cancel()
        except BaseException:
            break
    if cancelled is not None:
        try:
            result = future.result()
        except BaseException:
            pass
        else:
            if discard is not None:
                await _writer_call(partial(discard, result))
        raise cancelled
    return future.result()
