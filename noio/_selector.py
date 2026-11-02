"""Shared choice contracts and guarded completion for selection implementations."""

from collections.abc import AsyncIterator
from contextlib import AbstractContextManager
from typing import Any
from typing import Protocol

from ._contextlib import contextmanager
from ._lock import Lock
from .routine import cleared_mode


class Selector[T](Protocol):
    """One choice bound to its implementation's selection state.

    Inspect pending and complete only while holding the guard's lock.
    Completion consumes pending state, notifies the waiter, and must not yield
    execution.
    """

    def pending(self) -> bool: ...

    def result(self, value: T, /) -> None: ...

    def error(self, error: Exception, /) -> None: ...


class SelectorGuard[T]:
    """Handle supplied to selectables; acquire through selectors()."""

    def __init__(self, selector: Selector[T], lock: Lock) -> None:
        self.selector = selector
        self.lock = lock


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
