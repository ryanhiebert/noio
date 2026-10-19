"""Primitives for implementing selectable operations.

Selectable implementations acquire any resource locks before entering
``selectors``. They must never acquire resource locks while holding selectors,
or nest acquisitions of selectors.
Prepare outcomes before completing participants: completion is immediate and
does not roll back if the selectable implementation subsequently raises.
"""

from collections.abc import AsyncIterator
from typing import Any
from typing import overload

from ._contextlib import ContextManager
from ._contextlib import contextmanager
from ._lock import acquire
from ._selection import _Selection

__all__ = ["SelectionCollisionError", "Selector", "SelectorGuard", "selectors"]


class SelectionCollisionError(ValueError):
    """Raised when acquiring multiple guards belonging to one selection."""


class SelectorGuard[T]:
    """A choice accepting a result of type T, retained while an operation waits.

    Obtain temporary permission to complete it with ``selectors(guard)``.
    Guards are supplied by selection machinery, not constructed by selectables.
    """

    def __init__(self, selection: _Selection[T], index: int) -> None:
        self._selection = selection
        self._index = index


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
