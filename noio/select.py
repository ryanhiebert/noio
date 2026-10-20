"""Select one outcome from operations registered in the supplied order."""

from collections.abc import Callable
from contextlib import AsyncExitStack
from contextlib import ExitStack
from typing import Any
from typing import Protocol
from typing import cast

from ._selection import SelectorGuard
from ._selection import _Selection
from .routine import Routine
from .routine import cleared_mode
from .routine import routine

__all__ = ["Selectable", "select"]


class _Selectable[T](Protocol):
    def __select__(self, guard: SelectorGuard[T], /) -> Callable[[], None] | None: ...


class _RoutineSelectable[T](Protocol):
    @routine
    async def __select__(self, guard: SelectorGuard[T], /) -> Any: ...


class _ClearedSelectable[T](Protocol):
    @cleared_mode
    def __select__(self, guard: SelectorGuard[T], /) -> Any: ...


# Keep the descriptors explicit: protocol matching does not bind them
# when comparing it with an ordinary callable signature.
type Selectable[T] = _Selectable[T] | _RoutineSelectable[T] | _ClearedSelectable[T]


@routine
async def select[T](*choices: _RoutineSelectable[T]) -> tuple[int, T]:  # pyright: ignore[reportRedeclaration]
    """Return the winning choice's index and value, cleaning up every registration.

    Ordinary calls accept callable registration and cleanup. Awaited calls
    under an async runtime require routine registration and await cleanup.a().
    Choices register in order, including those after a winner, and clean up
    in reverse order.
    """
    if not choices:
        raise ValueError("select requires at least one selectable")
    registrations = [choice.__select__ for choice in choices]
    if any(not isinstance(register, Routine) for register in registrations):
        raise TypeError(
            "Asynchronous selection requires selectables "
            "to be implemented with routines"
        )

    selection = _Selection[T]()
    async with AsyncExitStack() as cleanups:
        try:
            for index, register in enumerate(registrations):
                cleanup = await register.a(SelectorGuard(selection, index))
                if cleanup is not None:
                    cleanups.push_async_callback(_cleanup, cleanup)
            return await selection.wait.a()
        finally:
            await selection.invalidate.a()


@select.fn
def select[T](*choices: Selectable[T]) -> tuple[int, T]:
    if not choices:
        raise ValueError("select requires at least one selectable")
    registrations = [choice.__select__ for choice in choices]
    selection = _Selection[T]()
    with ExitStack() as cleanups:
        try:
            for index, register in enumerate(registrations):
                cleanup = register(SelectorGuard(selection, index))
                if cleanup is not None:
                    cleanups.callback(cleanup)
            return selection.wait()
        finally:
            selection.invalidate()


async def _cleanup(callback: Callable[[], None]) -> None:
    await cast(Routine[[], None], callback).a()
