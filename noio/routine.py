from collections.abc import Callable
from collections.abc import Coroutine
from contextvars import ContextVar
from functools import update_wrapper
from inspect import iscoroutinefunction
from types import MethodType
from typing import Any
from typing import Concatenate
from typing import cast
from typing import overload

_in_coroutine: ContextVar[bool] = ContextVar("noio.in_coroutine", default=False)
_sync_call: ContextVar[bool] = ContextVar("noio.sync_call", default=False)


class Routine[**P, R]:
    __name__: str
    __wrapped__: Callable[P, Coroutine[Any, Any, R]]

    def __init__(
        self,
        fn: Callable[P, Coroutine[Any, Any, R]]
        | classmethod[Any, Any, Coroutine[Any, Any, R]]
        | staticmethod[P, Coroutine[Any, Any, R]],
    ):
        self.__class_method = fn if isinstance(fn, classmethod) else None
        self.__static_method = isinstance(fn, staticmethod)
        self.__fn = (
            cast(Callable[P, Coroutine[Any, Any, R]], fn.__func__)
            if isinstance(fn, classmethod | staticmethod)
            else fn
        )
        if not iscoroutinefunction(self.__fn):
            raise TypeError("@routine requires an async function")
        update_wrapper(self, self.__fn)

    async def a(self, *args: P.args, **kwargs: P.kwargs) -> R:
        token = _in_coroutine.set(True)
        try:
            return await self.__fn(*args, **kwargs)
        finally:
            _in_coroutine.reset(token)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if _in_coroutine.get():
            raise RuntimeError("Use await function.a(...) inside a routine")
        iterator = self.a(*args, **kwargs).__await__()
        token = _sync_call.set(True)
        try:
            iterator.send(None)
        except StopIteration as stop:
            return cast(R, stop.value)
        else:
            raise NotImplementedError(
                "Suspension in ordinary routine calls is not implemented."
            )
        finally:
            try:
                iterator.close()
            finally:
                _sync_call.reset(token)

    @overload
    def __get__[T, **B](
        self: Routine[Concatenate[type[T], B], R],
        instance: T | None,
        owner: type[T],
    ) -> Routine[B, R]: ...

    @overload
    def __get__[T, **B](
        self: Routine[Concatenate[T, B], R],
        instance: T,
        owner: type[T] | None = None,
    ) -> Routine[B, R]: ...

    @overload
    def __get__(
        self, instance: None, owner: type[Any] | None = None
    ) -> Routine[P, R]: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Routine[..., R]:
        if self.__class_method is not None:
            return Routine(self.__class_method.__get__(instance, owner))
        if instance is None or self.__static_method:
            return self
        return Routine(MethodType(self.__fn, instance))


@overload
def routine[T, **P, R](
    fn: classmethod[T, P, Coroutine[Any, Any, R]],
) -> Routine[Concatenate[type[T], P], R]: ...


@overload
def routine[**P, R](
    fn: Callable[P, Coroutine[Any, Any, R]],
) -> Routine[P, R]: ...


def routine(
    fn: Callable[..., Coroutine[Any, Any, Any]]
    | classmethod[Any, Any, Coroutine[Any, Any, Any]]
    | staticmethod[Any, Coroutine[Any, Any, Any]],
) -> Routine[..., Any]:
    """Wrap a coroutine for ordinary calls and lazy awaited calls."""
    return Routine(fn)
