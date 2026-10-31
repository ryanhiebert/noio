"""Select one outcome from operations registered in the supplied order."""

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager
from contextlib import AsyncExitStack
from contextlib import ExitStack
from typing import Any
from typing import Literal
from typing import Protocol
from typing import cast
from typing import overload

from ._contextlib import ContextManager
from ._contextlib import contextmanager
from ._selection import SelectorGuard
from ._selection import _Selection
from .routine import Routine
from .routine import cleared_mode

__all__ = ["Selectable", "select"]


class _Selectable[T](Protocol):
    def __select__(self, guard: SelectorGuard[T], /) -> AbstractContextManager[Any]: ...


class _RoutineSelectable[T](Protocol):
    @contextmanager
    async def __select__(self, guard: SelectorGuard[T], /) -> AsyncIterator[Any]:
        yield


class _ClearedSelectable[T](Protocol):
    @cleared_mode
    def __select__(self, guard: SelectorGuard[T], /) -> Any: ...


type Selectable[T] = _Selectable[T] | _RoutineSelectable[T] | _ClearedSelectable[T]


async def _select_async[T](*choices: _RoutineSelectable[T]) -> tuple[int, T]:
    """Select a result, then exit every entered registration normally."""
    if not choices:
        raise ValueError("select requires at least one selectable")
    selection = _Selection[T]()
    async with AsyncExitStack() as cleanups:
        try:
            registrations = [
                choice.__select__(SelectorGuard(selection, index))
                for index, choice in enumerate(choices)
            ]
            if any(
                not isinstance(registration, ContextManager)
                for registration in registrations
            ):
                raise TypeError(
                    "Asynchronous selection requires routine-based context managers"
                )
            for registration in registrations:
                await registration.__aenter__()
                cleanups.push_async_callback(registration.__aexit__, None, None, None)
            return await selection.wait.a()
        finally:
            await selection.invalidate.a()


def _select_fn[T](*choices: Selectable[T]) -> tuple[int, T]:
    if not choices:
        raise ValueError("select requires at least one selectable")
    selection = _Selection[T]()
    with ExitStack() as cleanups:
        try:
            for index, choice in enumerate(choices):
                registration = choice.__select__(SelectorGuard(selection, index))
                registration.__enter__()
                cleanups.callback(registration.__exit__, None, None, None)
            return selection.wait()
        finally:
            selection.invalidate()


class _Select(Routine[..., tuple[int, Any]]):
    def __init__(self) -> None:
        super().__init__(_select_async)
        self.__name__ = "select"
        self.__qualname__ = "select"

    @overload
    def __call__[A](self, a: Selectable[A], /) -> tuple[Literal[0], A]: ...

    @overload
    def __call__[A, B](
        self, a: Selectable[A], b: Selectable[B], /
    ) -> tuple[Literal[0], A] | tuple[Literal[1], B]: ...

    @overload
    def __call__[A, B, C](
        self, a: Selectable[A], b: Selectable[B], c: Selectable[C], /
    ) -> tuple[Literal[0], A] | tuple[Literal[1], B] | tuple[Literal[2], C]: ...

    @overload
    def __call__[A, B, C, D](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
    ): ...

    @overload
    def __call__[A, B, C, D, E](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
    ): ...

    @overload
    def __call__[A, B, C, D, E, F](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        f: Selectable[F],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
    ): ...

    @overload
    def __call__[A, B, C, D, E, F, G](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        f: Selectable[F],
        g: Selectable[G],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
    ): ...

    @overload
    def __call__[A, B, C, D, E, F, G, H](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        f: Selectable[F],
        g: Selectable[G],
        h: Selectable[H],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
    ): ...

    @overload
    def __call__[A, B, C, D, E, F, G, H, I](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        f: Selectable[F],
        g: Selectable[G],
        h: Selectable[H],
        i: Selectable[I],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
        | tuple[Literal[8], I]
    ): ...

    @overload
    def __call__[A, B, C, D, E, F, G, H, I, J](
        self,
        a: Selectable[A],
        b: Selectable[B],
        c: Selectable[C],
        d: Selectable[D],
        e: Selectable[E],
        f: Selectable[F],
        g: Selectable[G],
        h: Selectable[H],
        i: Selectable[I],
        j: Selectable[J],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
        | tuple[Literal[8], I]
        | tuple[Literal[9], J]
    ): ...

    @overload
    def __call__[T](self, *choices: Selectable[T]) -> tuple[int, T]: ...

    @overload
    def __call__(self, *choices: Selectable[Any]) -> tuple[int, Any]: ...

    def __call__(self, *choices: Selectable[Any]) -> tuple[int, Any]:
        return super().__call__(*choices)

    @overload
    async def a[A](self, a: _RoutineSelectable[A], /) -> tuple[Literal[0], A]: ...

    @overload
    async def a[A, B](
        self, a: _RoutineSelectable[A], b: _RoutineSelectable[B], /
    ) -> tuple[Literal[0], A] | tuple[Literal[1], B]: ...

    @overload
    async def a[A, B, C](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        /,
    ) -> tuple[Literal[0], A] | tuple[Literal[1], B] | tuple[Literal[2], C]: ...

    @overload
    async def a[A, B, C, D](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
    ): ...

    @overload
    async def a[A, B, C, D, E](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
    ): ...

    @overload
    async def a[A, B, C, D, E, F](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        f: _RoutineSelectable[F],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
    ): ...

    @overload
    async def a[A, B, C, D, E, F, G](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        f: _RoutineSelectable[F],
        g: _RoutineSelectable[G],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
    ): ...

    @overload
    async def a[A, B, C, D, E, F, G, H](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        f: _RoutineSelectable[F],
        g: _RoutineSelectable[G],
        h: _RoutineSelectable[H],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
    ): ...

    @overload
    async def a[A, B, C, D, E, F, G, H, I](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        f: _RoutineSelectable[F],
        g: _RoutineSelectable[G],
        h: _RoutineSelectable[H],
        i: _RoutineSelectable[I],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
        | tuple[Literal[8], I]
    ): ...

    @overload
    async def a[A, B, C, D, E, F, G, H, I, J](
        self,
        a: _RoutineSelectable[A],
        b: _RoutineSelectable[B],
        c: _RoutineSelectable[C],
        d: _RoutineSelectable[D],
        e: _RoutineSelectable[E],
        f: _RoutineSelectable[F],
        g: _RoutineSelectable[G],
        h: _RoutineSelectable[H],
        i: _RoutineSelectable[I],
        j: _RoutineSelectable[J],
        /,
    ) -> (
        tuple[Literal[0], A]
        | tuple[Literal[1], B]
        | tuple[Literal[2], C]
        | tuple[Literal[3], D]
        | tuple[Literal[4], E]
        | tuple[Literal[5], F]
        | tuple[Literal[6], G]
        | tuple[Literal[7], H]
        | tuple[Literal[8], I]
        | tuple[Literal[9], J]
    ): ...

    @overload
    async def a[T](self, *choices: _RoutineSelectable[T]) -> tuple[int, T]: ...

    @overload
    async def a(self, *choices: _RoutineSelectable[Any]) -> tuple[int, Any]: ...

    async def a(self, *choices: _RoutineSelectable[Any]) -> tuple[int, Any]:
        return await super().a(*choices)


# .fn copies the routine, preserving its concrete class.
select = cast(_Select, _Select().fn(_select_fn))
