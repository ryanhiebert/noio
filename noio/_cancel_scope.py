"""Persistent cancellation shared by explicit scopes and task groups."""

from collections.abc import AsyncIterator
from collections.abc import Callable
from math import inf
from time import monotonic

from ._contextlib import contextmanager
from .event import Event
from .routine import routine
from .selectable import SelectorGuard


class _CancelScope:
    def __init__(
        self, deadline: float = inf, clock: Callable[[], float] = monotonic
    ) -> None:
        self.deadline = deadline
        self.clock = clock
        self.cancelled = Event()

    @routine
    async def cancel(self) -> None:
        """Cancel this scope permanently; repeated calls are harmless."""
        await self.cancelled.set.a()

    @contextmanager
    async def __select__(self, guard: SelectorGuard[None]) -> AsyncIterator[None]:
        # The timer worker may not have run yet, even when IO is ready.
        if self.clock() >= self.deadline:
            await self.cancelled.set.a()
        async with self.cancelled.wait.select().__select__(guard):
            yield
