"""Persistent completion with selectable waiting."""

from collections.abc import AsyncIterator

from ._contextlib import contextmanager
from ._lock import Lock
from .routine import routine
from .selectable import SelectorGuard
from .selectable import selectable
from .selectable import selectors
from .shield import shield

__all__ = ["Event"]


class Event:
    """A one-shot notification shared by all current and future waiters."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._is_set = False
        self._waiters: dict[SelectorGuard[None], None] = {}

    @shield
    @routine
    async def set(self) -> None:
        """Set the Event permanently; repeated calls are harmless."""
        async with self._lock:
            # The Event lock precedes selection locks. Acquire one selection at
            # a time: multiple registrations can belong to the same selection.
            self._is_set = True
            for guard in self._waiters:
                async with selectors(guard) as (selector,):
                    if selector is not None:
                        selector.result(None)
            self._waiters.clear()

    @selectable
    @contextmanager
    async def wait(self, guard: SelectorGuard[None]) -> AsyncIterator[None]:
        """Wait for completion, returning None; prepare with wait.select()."""
        async with self._lock, selectors(guard) as (selector,):
            if selector is not None:
                if self._is_set:
                    selector.result(None)
                else:
                    self._waiters[guard] = None

        @shield
        @routine
        async def cleanup() -> None:
            async with self._lock:
                self._waiters.pop(guard, None)

        try:
            yield
        finally:
            await cleanup.a()
