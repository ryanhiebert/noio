from collections.abc import Callable
from collections.abc import Coroutine
from contextvars import ContextVar
from copy import copy
from enum import Enum
from enum import auto
from functools import update_wrapper
from inspect import iscoroutinefunction
from types import MethodType
from typing import Any
from typing import Concatenate
from typing import Protocol
from typing import cast
from typing import overload


class _Mode(Enum):
    """How a routine's coroutine implementation is executing.

    DRIVEN means an ordinary call is driving the coroutine, so nested .a()
    calls use ordinary implementations where provided. AWAITED means an async
    runtime is driving it, so nested .a() calls use coroutine implementations.
    The context variable holds None when no coroutine execution mode is active;
    clearing it lets routines entered by the called function establish their
    own mode.
    """

    DRIVEN = auto()
    AWAITED = auto()


_mode: ContextVar[_Mode | None] = ContextVar("noio.mode", default=None)


# Require compatibility with coroutine parameters P while retaining the
# ordinary implementation's full parameters F.
class _CompatibleFunction[**P, **F, R](Protocol):
    @overload
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...

    @overload
    def __call__(self, *args: F.args, **kwargs: F.kwargs) -> R: ...


class _CompatibleFunctionDescriptor[**P, **F, R](Protocol):
    @property
    def __func__(self) -> _CompatibleFunction[P, F, R]: ...

    @property
    def __wrapped__(self) -> _CompatibleFunction[P, F, R]: ...


class Routine[**P, R, **F = P]:
    __name__: str
    __wrapped__: Callable[P, Coroutine[Any, Any, R]]
    __fn: Callable[P, Coroutine[Any, Any, R]]
    __ordinary: _CompatibleFunction[P, F, R] | None
    __class_method: classmethod[Any, Any, Coroutine[Any, Any, R]] | None

    def __init__[**A, S](
        self: Routine[A, S, A],
        fn: Callable[A, Coroutine[Any, Any, S]]
        | classmethod[Any, Any, Coroutine[Any, Any, S]]
        | staticmethod[A, Coroutine[Any, Any, S]],
    ):
        self.__class_method = fn if isinstance(fn, classmethod) else None
        self.__static_method = isinstance(fn, staticmethod)
        self.__fn = (
            cast(Callable[A, Coroutine[Any, Any, S]], fn.__func__)
            if isinstance(fn, classmethod | staticmethod)
            else fn
        )
        if not iscoroutinefunction(self.__fn):
            raise TypeError("@routine requires an async function")
        self.__ordinary = None
        update_wrapper(self, self.__fn)

    def fn[**Q](
        self, fn: _CompatibleFunction[P, Q, R] | _CompatibleFunctionDescriptor[P, Q, R]
    ) -> Routine[P, R, Q]:
        """Return a new routine with an ordinary-function implementation."""
        if isinstance(fn, classmethod | staticmethod):
            fn = cast(_CompatibleFunction[P, Q, R], fn.__func__)
        if not callable(fn) or iscoroutinefunction(fn):
            raise TypeError(".fn requires an ordinary callable")
        result = cast(Routine[P, R, Q], copy(self))
        result.__ordinary = cast(_CompatibleFunction[P, Q, R], fn)
        return result

    async def a(self, *args: P.args, **kwargs: P.kwargs) -> R:
        mode = _mode.get()
        if mode is _Mode.DRIVEN and self.__ordinary is not None:
            token = _mode.set(None)
            try:
                return self.__ordinary(*args, **kwargs)
            finally:
                _mode.reset(token)
        token = _mode.set(_Mode.DRIVEN if mode is _Mode.DRIVEN else _Mode.AWAITED)
        try:
            return await self.__fn(*args, **kwargs)
        finally:
            _mode.reset(token)

    def __call__(self, *args: F.args, **kwargs: F.kwargs) -> R:
        if _mode.get() is not None:
            raise RuntimeError("Use await function.a(...) inside a routine")
        if self.__ordinary is not None:
            return self.__ordinary(*args, **kwargs)
        # Without an override, F defaults to the coroutine's parameters P.
        awaited = cast(Callable[F, Coroutine[Any, Any, R]], self.a)
        iterator = awaited(*args, **kwargs).__await__()
        token = _mode.set(_Mode.DRIVEN)
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
                _mode.reset(token)

    @overload
    def __get__[T, **B, **C](
        self: Routine[Concatenate[type[T], B], R, Concatenate[type[T], C]],
        instance: T | None,
        owner: type[T],
    ) -> Routine[B, R, C]: ...

    @overload
    def __get__[T, **B, **C](
        self: Routine[Concatenate[T, B], R, Concatenate[T, C]],
        instance: T,
        owner: type[T] | None = None,
    ) -> Routine[B, R, C]: ...

    # If only the ordinary implementation accepts this instance, expose only
    # its ordinary call interface.
    @overload
    def __get__[T, **B](
        self: Routine[P, R, Concatenate[T, B]],
        instance: T,
        owner: type[T] | None = None,
    ) -> Callable[B, R]: ...

    @overload
    def __get__(
        self, instance: None, owner: type[Any] | None = None
    ) -> Routine[P, R, F]: ...

    def __get__(
        self, instance: Any, owner: type[Any] | None = None
    ) -> Routine[..., R, ...] | Callable[..., R]:
        if self.__class_method is not None:
            result = Routine(self.__class_method.__get__(instance, owner))
            if self.__ordinary is not None:
                cls = owner if owner is not None else type(instance)
                result.__ordinary = MethodType(self.__ordinary, cls)
            return result
        if instance is None or self.__static_method:
            return self
        result = Routine(MethodType(self.__fn, instance))
        if self.__ordinary is not None:
            result.__ordinary = MethodType(self.__ordinary, instance)
        return result


class Function[**P, R]:
    """An ordinary callable that clears the coroutine execution mode before calling."""

    __name__: str
    __wrapped__: Callable[P, R]

    def __init__(
        self,
        fn: Callable[P, R] | classmethod[Any, Any, R] | staticmethod[P, R],
    ):
        self.__class_method = fn if isinstance(fn, classmethod) else None
        self.__static_method = isinstance(fn, staticmethod)
        self.__fn = (
            cast(Callable[P, R], fn.__func__)
            if isinstance(fn, classmethod | staticmethod)
            else fn
        )
        if not callable(self.__fn) or iscoroutinefunction(self.__fn):
            raise TypeError("cleared_mode requires an ordinary callable")
        update_wrapper(self, self.__fn)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        token = _mode.set(None)
        try:
            return self.__fn(*args, **kwargs)
        finally:
            _mode.reset(token)

    @overload
    def __get__[T, **B](
        self: Function[Concatenate[type[T], B], R],
        instance: T | None,
        owner: type[T],
    ) -> Function[B, R]: ...

    @overload
    def __get__[T, **B](
        self: Function[Concatenate[T, B], R],
        instance: T,
        owner: type[T] | None = None,
    ) -> Function[B, R]: ...

    @overload
    def __get__(
        self, instance: None, owner: type[Any] | None = None
    ) -> Function[P, R]: ...

    def __get__(
        self, instance: Any, owner: type[Any] | None = None
    ) -> Function[..., R]:
        if self.__class_method is not None:
            return Function(self.__class_method.__get__(instance, owner))
        if instance is None or self.__static_method:
            return self
        return Function(MethodType(self.__fn, instance))


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


@overload
def cleared_mode[T, **P, R](
    fn: classmethod[T, P, R],
) -> Function[Concatenate[type[T], P], R]: ...


@overload
def cleared_mode[**P, R](fn: Callable[P, R]) -> Function[P, R]: ...


def cleared_mode(
    fn: Callable[..., Any] | classmethod[Any, Any, Any] | staticmethod[Any, Any],
) -> Function[..., Any]:
    """Wrap an ordinary callable to clear the mode before calling it."""
    return Function(fn)
