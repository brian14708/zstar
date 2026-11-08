"""Factory results supporting both awaiting and async context management."""

from collections.abc import Callable, Coroutine, Generator
from functools import wraps
from types import TracebackType
from typing import Generic, ParamSpec, Protocol, Self, TypeVar


class _Resource(Protocol):
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


_T = TypeVar("_T", bound=_Resource)
_P = ParamSpec("_P")


class _AsyncOpen(Generic[_T]):
    """A single-use awaitable context manager for acquiring one resource."""

    def __init__(self, opening: Coroutine[object, object, _T]) -> None:
        self._opening = opening
        self._owner: _T | None = None

    def __await__(self) -> Generator[object, None, _T]:
        return self._opening.__await__()

    async def __aenter__(self) -> _T:
        owner = await self
        try:
            result = await owner.__aenter__()
        except BaseException as error:
            await owner.__aexit__(type(error), error, error.__traceback__)
            raise
        self._owner = owner
        return result

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        owner, self._owner = self._owner, None
        assert owner is not None
        await owner.__aexit__(exc_type, exc_value, traceback)


def _async_factory(
    operation: Callable[_P, Coroutine[object, object, _T]],
) -> Callable[_P, _AsyncOpen[_T]]:
    @wraps(operation)
    def opening(*args: _P.args, **kwargs: _P.kwargs) -> _AsyncOpen[_T]:
        return _AsyncOpen(operation(*args, **kwargs))

    return opening
