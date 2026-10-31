from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import AbstractAsyncContextManager
from contextlib import AbstractContextManager
from contextlib import asynccontextmanager
from contextlib import contextmanager as sync_contextmanager
from copy import copy
from functools import update_wrapper
from functools import wraps
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from types import MethodType
from types import TracebackType
from typing import Any
from typing import Concatenate
from typing import Self
from typing import cast
from typing import overload

from .routine import Routine
from .routine import _CompatibleFunctionDescriptor
from .routine import _Mode
from .routine import _mode
from .routine import cleared_mode
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
    def __init__(
        self,
        factory: Callable[[], AbstractAsyncContextManager[T]],
        ordinary_factory: Callable[[], AbstractContextManager[T]] | None = None,
    ) -> None:
        self.__factory = factory
        self.__ordinary_factory = ordinary_factory
        self.__manager: (
            AbstractAsyncContextManager[T] | AbstractContextManager[T] | None
        ) = None

    def _recreate_cm(self) -> ContextManager[T]:
        return _GeneratorContextManager(self.__factory, self.__ordinary_factory)

    @routine
    async def __enter__(self) -> T:  # pyright: ignore[reportIncompatibleVariableOverride]
        if self.__manager is not None:
            raise RuntimeError("This context manager has already been entered")
        if _mode.get() is _Mode.DRIVEN and self.__ordinary_factory is not None:
            manager = cleared_mode(self.__ordinary_factory)()
            self.__manager = manager
            return cleared_mode(manager.__enter__)()
        manager = self.__factory()
        self.__manager = manager
        return await manager.__aenter__()

    @routine
    async def __exit__(  # pyright: ignore[reportIncompatibleVariableOverride]
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        manager = self.__manager
        if manager is None:
            raise RuntimeError("This context manager has not been entered")
        # Pair exit with the implementation chosen at entry, even if the
        # calling mode has changed in the meantime.
        if isinstance(manager, AbstractContextManager):
            return cleared_mode(manager.__exit__)(exc_type, exc_value, traceback)
        return await manager.__aexit__(exc_type, exc_value, traceback)


class _ContextManagerFactory[**P, T]:
    __name__: str
    __wrapped__: Callable[P, AsyncIterator[T]]

    def __init__(
        self,
        fn: Callable[P, AsyncIterator[T]]
        | classmethod[Any, Any, AsyncIterator[T]]
        | staticmethod[P, AsyncIterator[T]],
    ) -> None:
        self.__class_method = fn if isinstance(fn, classmethod) else None
        self.__static_method = isinstance(fn, staticmethod)
        wrapped = (
            cast(Callable[P, AsyncIterator[T]], fn.__func__)
            if isinstance(fn, classmethod | staticmethod)
            else fn
        )
        self.__factory = asynccontextmanager(wrapped)
        self.__ordinary: Callable[P, Iterator[T]] | None = None
        update_wrapper(self, wrapped)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> ContextManager[T]:
        ordinary = self.__ordinary
        return _GeneratorContextManager(
            lambda: self.__factory(*args, **kwargs),
            None
            if ordinary is None
            else lambda: sync_contextmanager(ordinary)(*args, **kwargs),
        )

    def fn(
        self,
        fn: Callable[P, Iterator[T]] | _CompatibleFunctionDescriptor[P, P, Iterator[T]],
    ) -> Self:
        """Return a new context manager callable with an ordinary implementation."""
        if isinstance(fn, classmethod | staticmethod):
            fn = cast(Callable[P, Iterator[T]], fn.__func__)
        if not callable(fn) or isasyncgenfunction(fn) or iscoroutinefunction(fn):
            raise TypeError(".fn requires an ordinary context-manager implementation")
        result = copy(self)
        result.__ordinary = cast(Callable[P, Iterator[T]], fn)
        return result

    @overload
    def __get__[O, **A](
        self: _ContextManagerFactory[Concatenate[type[O], A], T],
        instance: O | None,
        owner: type[O],
    ) -> _ContextManagerFactory[A, T]: ...

    @overload
    def __get__[O, **A](
        self: _ContextManagerFactory[Concatenate[O, A], T],
        instance: O,
        owner: type[O] | None = None,
    ) -> _ContextManagerFactory[A, T]: ...

    @overload
    def __get__(self, instance: None, owner: type[Any] | None = None) -> Self: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        if self.__class_method is not None:
            result = _ContextManagerFactory(
                self.__class_method.__get__(instance, owner)
            )
            if self.__ordinary is not None:
                cls = owner if owner is not None else type(instance)
                result.__ordinary = MethodType(self.__ordinary, cls)
            return result
        if instance is None or self.__static_method:
            return self
        result = _ContextManagerFactory(MethodType(self.__wrapped__, instance))
        if self.__ordinary is not None:
            result.__ordinary = MethodType(self.__ordinary, instance)
        return result


@overload
def contextmanager[O, **P, T](
    fn: classmethod[O, P, AsyncIterator[T]],
) -> _ContextManagerFactory[Concatenate[type[O], P], T]: ...


@overload
def contextmanager[**P, T](
    fn: Callable[P, AsyncIterator[T]],
) -> _ContextManagerFactory[P, T]: ...


def contextmanager(
    fn: Callable[..., AsyncIterator[Any]]
    | classmethod[Any, Any, AsyncIterator[Any]]
    | staticmethod[Any, AsyncIterator[Any]],
) -> _ContextManagerFactory[..., Any]:
    """Wrap an async generator for ordinary and coroutine context management."""
    return _ContextManagerFactory(fn)
