"""Primitives for implementing selectable operations.

Selectable implementations acquire any resource locks before entering
``selectors``. They must never acquire resource locks while holding selectors,
or nest acquisitions of selectors.
Prepare outcomes before completing participants: completion is immediate and
does not roll back if the selectable implementation subsequently raises.
"""

from collections.abc import AsyncIterator
from collections.abc import Callable
from copy import copy
from functools import update_wrapper
from inspect import iscoroutinefunction
from types import MethodType
from typing import Any
from typing import Concatenate
from typing import Protocol
from typing import Self
from typing import cast
from typing import overload

from ._contextlib import ContextManager
from ._contextlib import contextmanager
from ._lock import acquire
from ._selection import SelectorGuard as SelectorGuard
from .routine import Routine
from .routine import routine
from .select import select

__all__ = [
    "SelectionCollisionError",
    "Selector",
    "SelectorGuard",
    "selectable",
    "selectors",
]


class SelectionCollisionError(ValueError):
    """Raised when acquiring multiple guards belonging to one selection."""


class Selector[T]:
    """Permission to complete a choice, used only inside its acquiring context."""

    def __init__(self, guard: SelectorGuard[T]) -> None:
        self._guard = guard
        self._active = True

    def result(self, value: T, /) -> None:
        """Complete with a value, notifying the waiter immediately.

        Raises RuntimeError after completion or outside the acquiring context.
        Exception objects passed here are ordinary result values.
        """
        if not self._active:
            raise RuntimeError("Selector is outside its acquiring context")
        self._guard._selection.result(self._guard._index, value)

    def error(self, error: Exception, /) -> None:
        """Complete with an exception to raise in the waiting selection.

        Raises RuntimeError after completion or outside the acquiring context.
        """
        if not self._active:
            raise RuntimeError("Selector is outside its acquiring context")
        self._guard._selection.error(self._guard._index, error)


@overload
def selectors() -> ContextManager[tuple[()]]: ...


@overload
def selectors[A](
    a: SelectorGuard[A], /
) -> ContextManager[tuple[Selector[A] | None]]: ...


@overload
def selectors[A, B](
    a: SelectorGuard[A], b: SelectorGuard[B], /
) -> ContextManager[tuple[Selector[A] | None, Selector[B] | None]]: ...


@overload
def selectors[A, B, C](
    a: SelectorGuard[A], b: SelectorGuard[B], c: SelectorGuard[C], /
) -> ContextManager[
    tuple[Selector[A] | None, Selector[B] | None, Selector[C] | None]
]: ...


@overload
def selectors(
    a: SelectorGuard[Any],
    b: SelectorGuard[Any],
    c: SelectorGuard[Any],
    d: SelectorGuard[Any],
    /,
    *guards: SelectorGuard[Any],
) -> ContextManager[tuple[Selector[Any] | None, ...]]: ...


@contextmanager
async def selectors(
    *guards: SelectorGuard[Any],
) -> AsyncIterator[tuple[Selector[Any] | None, ...]]:
    """Acquire choices together, preserving argument order and availability.

    Colliding guards raise SelectionCollisionError, even when already inactive.
    Each selector is valid only in this context and its owning execution context.
    Setters record and notify immediately; callers coordinate multi-party
    completion without intervening awaits or other fallible work.
    """
    states = [guard._selection for guard in guards]
    if len({id(state) for state in states}) != len(states):
        raise SelectionCollisionError("Guards from the same selection collide")
    async with acquire(state._lock for state in states):
        acquired = tuple(
            Selector(guard) if guard._selection._pending else None for guard in guards
        )
        try:
            yield acquired
        finally:
            for selector in acquired:
                if selector is not None:
                    selector._active = False


class _Operation[T]:
    def __init__(
        self,
        register: Callable[..., Callable[[], None] | None],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        unbound: bool,
    ) -> None:
        self._register = register
        self._args = args
        self._kwargs = kwargs
        self._unbound = unbound

    def _arguments(self, guard: SelectorGuard[T]) -> tuple[Any, ...]:
        if self._unbound:
            return self._args[:1] + (guard,) + self._args[1:]
        return (guard, *self._args)

    def __select__(self, guard: SelectorGuard[T], /) -> Callable[[], None] | None:
        return self._register(*self._arguments(guard), **self._kwargs)


class _RoutineOperation[T]:
    def __init__(
        self,
        register: Routine[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        unbound: bool,
    ) -> None:
        self._register = register
        self._args = args
        self._kwargs = kwargs
        self._unbound = unbound

    @routine
    async def __select__(self, guard: SelectorGuard[T], /) -> Callable[[], None] | None:
        args = (
            self._args[:1] + (guard,) + self._args[1:]
            if self._unbound
            else (guard, *self._args)
        )
        return await self._register.a(*args, **self._kwargs)


class _UnboundCall[**P, T](Protocol):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    def select(self, *args: P.args, **kwargs: P.kwargs) -> _Operation[T]: ...


class _UnboundRoutineCall[**P, T](Protocol):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    async def a(self, *args: P.args, **kwargs: P.kwargs) -> T: ...
    def select(self, *args: P.args, **kwargs: P.kwargs) -> _RoutineOperation[T]: ...


class _CallOverrides:
    _register: Any
    _unbound: bool
    _call: Callable[..., Any] | None = None
    _operation: type[_Operation[Any]] | type[_RoutineOperation[Any]] = _Operation

    def _runner(self) -> Routine[..., Any]:
        if isinstance(self._call, Routine):
            runner = self._call
        else:

            @routine
            async def runner(*args: Any, **kwargs: Any) -> Any:
                operation = self._operation(self._register, args, kwargs, self._unbound)
                return (await select.a(cast(Any, operation)))[1]

            if self._call is not None:
                runner = runner.fn(self._call)
        return runner

    def _with_call(self, fn: Callable[..., Any]) -> Any:
        if iscoroutinefunction(fn):
            raise TypeError(
                "Wrap coroutine implementations with @routine before applying .call."
            )
        if isinstance(self._register, Routine):
            wrapper = (
                _RoutineDirectSelectableCall
                if isinstance(fn, Routine)
                else _RoutineSelectableCall
            )
        else:
            wrapper = (
                _DirectRoutineSelectableCall
                if isinstance(fn, Routine)
                else _SelectableCall
            )
        result = wrapper(self._register, unbound=self._unbound)
        result._call = fn
        return result

    def _with_call_fn(self, fn: Callable[..., Any]) -> Self:
        assert isinstance(self._call, Routine)
        result = copy(self)
        result._call = self._call.fn(fn)
        return result

    def _bind(self, instance: Any, owner: type[Any] | None) -> Self:
        result = copy(self)
        if instance is None:
            result._unbound = True
        else:
            result._register = self._register.__get__(instance, owner)
            if self._call is not None:
                result._call = (
                    self._call.__get__(instance, owner)
                    if isinstance(self._call, Routine)
                    else MethodType(self._call, instance)
                )
        return result


class _SelectableCall[**P](_CallOverrides):
    def __init__(
        self, register: Callable[P, Callable[[], None] | None], *, unbound: bool = False
    ) -> None:
        self._register = register
        self._unbound = unbound
        update_wrapper(cast(Callable[..., Any], self), register)

    @overload
    def call[**A, T](
        self: _SelectableCall[Concatenate[SelectorGuard[T], A]], fn: Routine[A, T], /
    ) -> _DirectRoutineSelectableCall[P]: ...

    @overload
    def call[O, **A, T](
        self: _SelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Routine[Concatenate[O, A], T],
        /,
    ) -> _DirectRoutineSelectableCall[P]: ...

    @overload
    def call[**A, T](
        self: _SelectableCall[Concatenate[SelectorGuard[T], A]], fn: Callable[A, T], /
    ) -> _SelectableCall[P]: ...

    @overload
    def call[O, **A, T](
        self: _SelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Callable[Concatenate[O, A], T],
        /,
    ) -> _SelectableCall[P]: ...

    def call(self, fn: Callable[..., Any], /) -> Any:
        """Return a copy with a direct callable or routine implementation."""
        return self._with_call(fn)

    def select[**A, T](
        self: _SelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> _Operation[T]:
        return _Operation(self._register, args, kwargs, self._unbound)

    def __call__[**A, T](
        self: _SelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> T:
        return self._runner()(*args, **kwargs)

    @overload
    def __get__[O, **A, T](
        self: _SelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: None,
        owner: type[O],
    ) -> _UnboundCall[Concatenate[O, A], T]: ...

    @overload
    def __get__[O, **A, T](
        self: _SelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: O,
        owner: type[O] | None = None,
    ) -> _SelectableCall[Concatenate[SelectorGuard[T], A]]: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        return self._bind(instance, owner)


class _UnboundDirectRoutineCall[**P, T](_UnboundCall[P, T], Protocol):
    async def a(self, *args: P.args, **kwargs: P.kwargs) -> T: ...


class _DirectRoutineSelectableCall[**P](_SelectableCall[P]):
    @overload
    def call_fn[**A, T](
        self: _DirectRoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        fn: Callable[A, T],
        /,
    ) -> _DirectRoutineSelectableCall[P]: ...

    @overload
    def call_fn[O, **A, T](
        self: _DirectRoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Callable[Concatenate[O, A], T],
        /,
    ) -> _DirectRoutineSelectableCall[P]: ...

    def call_fn(self, fn: Callable[..., Any], /) -> Any:
        """Return a copy with an ordinary direct-call implementation."""
        return self._with_call_fn(fn)

    async def a[**A, T](
        self: _DirectRoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> T:
        return await self._runner().a(*args, **kwargs)

    @overload
    def __get__[O, **A, T](
        self: _DirectRoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: None,
        owner: type[O],
    ) -> _UnboundDirectRoutineCall[Concatenate[O, A], T]: ...

    @overload
    def __get__[O, **A, T](
        self: _DirectRoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: O,
        owner: type[O] | None = None,
    ) -> _DirectRoutineSelectableCall[Concatenate[SelectorGuard[T], A]]: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        return self._bind(instance, owner)


class _RoutineSelectableCall[**P](_CallOverrides):
    _operation = _RoutineOperation

    def __init__(self, register: Routine[P, Any], *, unbound: bool = False) -> None:
        self._register = register
        self._unbound = unbound
        update_wrapper(cast(Callable[..., Any], self), register)

    @overload
    def call[**A, T](
        self: _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        fn: Routine[A, T],
        /,
    ) -> _RoutineDirectSelectableCall[P]: ...

    @overload
    def call[O, **A, T](
        self: _RoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Routine[Concatenate[O, A], T],
        /,
    ) -> _RoutineDirectSelectableCall[P]: ...

    @overload
    def call[**A, T](
        self: _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        fn: Callable[A, T],
        /,
    ) -> _RoutineSelectableCall[P]: ...

    @overload
    def call[O, **A, T](
        self: _RoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Callable[Concatenate[O, A], T],
        /,
    ) -> _RoutineSelectableCall[P]: ...

    def call(self, fn: Callable[..., Any], /) -> Any:
        """Return a copy with a direct callable or routine implementation."""
        return self._with_call(fn)

    def fn(self, fn: Callable[P, Callable[[], None] | None], /) -> Self:
        """Return a copy with an ordinary registration implementation."""
        result = copy(self)
        result._register = self._register.fn(fn)
        return result

    def select[**A, T](
        self: _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> _RoutineOperation[T]:
        return _RoutineOperation(self._register, args, kwargs, self._unbound)

    def __call__[**A, T](
        self: _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> T:
        return self._runner()(*args, **kwargs)

    async def a[**A, T](
        self: _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]],
        *args: A.args,
        **kwargs: A.kwargs,
    ) -> T:
        return await self._runner().a(*args, **kwargs)

    @overload
    def __get__[O, **A, T](
        self: _RoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: None,
        owner: type[O],
    ) -> _UnboundRoutineCall[Concatenate[O, A], T]: ...

    @overload
    def __get__[O, **A, T](
        self: _RoutineSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: O,
        owner: type[O] | None = None,
    ) -> _RoutineSelectableCall[Concatenate[SelectorGuard[T], A]]: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        return self._bind(instance, owner)


class _RoutineDirectSelectableCall[**P](_RoutineSelectableCall[P]):
    @overload
    def call_fn[**A, T](
        self: _RoutineDirectSelectableCall[Concatenate[SelectorGuard[T], A]],
        fn: Callable[A, T],
        /,
    ) -> _RoutineDirectSelectableCall[P]: ...

    @overload
    def call_fn[O, **A, T](
        self: _RoutineDirectSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        fn: Callable[Concatenate[O, A], T],
        /,
    ) -> _RoutineDirectSelectableCall[P]: ...

    def call_fn(self, fn: Callable[..., Any], /) -> Any:
        """Return a copy with an ordinary direct-call implementation."""
        return self._with_call_fn(fn)

    @overload
    def __get__[O, **A, T](
        self: _RoutineDirectSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: None,
        owner: type[O],
    ) -> _UnboundRoutineCall[Concatenate[O, A], T]: ...

    @overload
    def __get__[O, **A, T](
        self: _RoutineDirectSelectableCall[Concatenate[O, SelectorGuard[T], A]],
        instance: O,
        owner: type[O] | None = None,
    ) -> _RoutineDirectSelectableCall[Concatenate[SelectorGuard[T], A]]: ...

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> Any:
        return self._bind(instance, owner)


@overload
# Routine is callable too, but its wrapper additionally exposes .a().
def selectable[**P, C: Callable[[], None] | None](  # pyright: ignore[reportOverlappingOverload]
    register: Routine[P, C],
) -> _RoutineSelectableCall[P]: ...


@overload
def selectable[**P](
    register: Callable[P, Callable[[], None] | None],
) -> _SelectableCall[P]: ...


def selectable(register: Any) -> Any:
    """Make registration callable directly or preparable with .select().

    Wrap a routine to also support .a() and async selection.
    Registration receives the guard before the caller's arguments, after self
    for methods, and returns a cleanup callback or None.
    """
    if iscoroutinefunction(register):
        raise TypeError(
            "Wrap coroutine implementations with @routine before applying @selectable."
        )
    if isinstance(register, Routine):
        return _RoutineSelectableCall(register)
    return _SelectableCall(register)
