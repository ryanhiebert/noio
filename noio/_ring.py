"""Internal experiment in atomic, arrival-ordered group exchange."""

from collections.abc import AsyncIterator

from ._contextlib import contextmanager
from ._lock import Lock
from ._shield import shield
from .routine import routine
from .selectable import SelectorGuard
from .selectable import selectable
from .selectable import selectors


class Ring[T]:
    """Exchange values in groups of size, which must be at least two.

    Call exchange(value) to contribute and wait for a group. Each participant
    receives the preceding contribution in registration order, wrapping around.
    Alternative contributions from one selection occupy at most one position;
    the earliest eligible contribution takes precedence.
    Cancellation or choosing another operation withdraws a contribution.
    Prepare with exchange.select(value), or await exchange.a(value).
    """

    def __init__(self, size: int) -> None:
        if size < 2:
            raise ValueError("Ring size must be at least two")
        self._size = size
        self._lock = Lock()
        self._waiters: dict[SelectorGuard[T], T] = {}

    @routine
    async def _match(self) -> None:
        # The caller owns the ring lock. Inspect a candidate group under all
        # its selection locks, then retry if any choices have become inactive.
        # This experiment favors simple rescanning over an indexed queue;
        # performance and starvation under sustained contention are untested.
        while True:
            guards: list[SelectorGuard[T]] = []
            locks: set[Lock] = set()
            # The first size registrations need not be independent participants.
            # Shared lock identity identifies alternatives in one selection:
            # keep its earliest contribution, since only one choice can win.
            for guard in self._waiters:
                if guard.lock not in locks:
                    guards.append(guard)
                    locks.add(guard.lock)
                    if len(guards) == self._size:
                        break
            if len(guards) < self._size:
                return
            # selectors() handles arbitrary group sizes and orders lock
            # acquisition globally, including when competing rings overlap.
            async with selectors(*guards) as choices:
                if any(choice is None for choice in choices):
                    # Another operation can win before registration cleanup
                    # reaches this ring. Queue membership alone is not proof
                    # of readiness; discard stale candidates and form a new group.
                    for guard, choice in zip(guards, choices, strict=True):
                        if choice is None:
                            del self._waiters[guard]
                    continue
                values = [self._waiters[guard] for guard in guards]
                # Prepare everything before the first completion. No awaits
                # may separate these results: the whole group commits together.
                completions = [
                    (choice, values[index - 1])
                    for index, choice in enumerate(choices)
                    if choice is not None
                ]
                for choice, value in completions:
                    choice.result(value)
                for guard in guards:
                    del self._waiters[guard]

    @selectable
    @contextmanager
    async def exchange(self, guard: SelectorGuard[T], value: T) -> AsyncIterator[None]:
        """Contribute a value and receive the preceding group member's value."""

        @shield
        @routine
        async def cleanup() -> None:
            # Withdrawal must finish even if cancellation interrupted matching
            # or the subsequent wait. Registration itself remains cancellable.
            async with self._lock:
                self._waiters.pop(guard, None)

        # Cover partial registration too: matching can be interrupted while
        # acquiring selection locks after the contribution has been inserted.
        # An unsuccessful context entry never gets an exit from its caller,
        # so this finally must also cover everything before yield.
        try:
            async with self._lock:
                self._waiters[guard] = value
                await self._match.a()
            yield
        finally:
            await cleanup.a()
