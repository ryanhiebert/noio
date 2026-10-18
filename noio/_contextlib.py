from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from contextlib import AbstractContextManager
from contextlib import asynccontextmanager
from functools import wraps
from types import TracebackType

from .routine import Routine
from .routine import routine


class ContextManager[T, E: bool | None = bool | None](
    AbstractContextManager[T, E], AbstractAsyncContextManager[T, E]
):
    @routine
    @abstractmethod
    async def __enter__(self) -> T:  # pyright: ignore[reportIncompatibleMethodOverride]
        raise NotImplementedError

    @routine
    @abstractmethod
    async def __exit__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> E:
        raise NotImplementedError

    async def __aenter__(self) -> T:
        return await self.__enter__.a()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> E:
        return await self.__exit__.a(exc_type, exc_value, traceback)

    def _recreate_cm(self) -> ContextManager[T, E]:
        return self

    def __call__[**P, R](self, fn: Routine[P, R]) -> Routine[P, R | None]:
        if not isinstance(fn, Routine):
            raise TypeError(
                "Context decorators require a routine; apply @noio.routine first."
            )

        @routine
        @wraps(fn, updated=())
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R | None:
            async with self._recreate_cm():
                return await fn.a(*args, **kwargs)

        return wrapped


class _GeneratorContextManager[T](ContextManager[T]):
    def __init__(self, factory: Callable[[], AbstractAsyncContextManager[T]]) -> None:
        self.__factory = factory
        self.__manager = factory()

    def _recreate_cm(self) -> ContextManager[T]:
        return _GeneratorContextManager(self.__factory)

    @routine
    async def __enter__(self) -> T:  # pyright: ignore[reportIncompatibleVariableOverride]
        return await self.__manager.__aenter__()

    @routine
    async def __exit__(  # pyright: ignore[reportIncompatibleVariableOverride]
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return await self.__manager.__aexit__(exc_type, exc_value, traceback)


def contextmanager[**P, T](
    fn: Callable[P, AsyncIterator[T]],
) -> Callable[P, ContextManager[T]]:
    factory = asynccontextmanager(fn)

    @wraps(fn)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> ContextManager[T]:
        return _GeneratorContextManager(lambda: factory(*args, **kwargs))

    return wrapped
