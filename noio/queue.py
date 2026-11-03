"""Buffered FIFO queues with selectable puts and gets."""

from collections import deque
from collections.abc import AsyncIterator

from ._contextlib import contextmanager
from ._lock import Lock
from ._shield import shield
from .routine import routine
from .selectable import SelectorGuard
from .selectable import selectable
from .selectable import selectors

__all__ = ["Queue", "ShutDown"]


class ShutDown(Exception):
    """The queue no longer accepts the requested operation."""


class Queue[T]:
    """A FIFO buffer; maxsize=0 is unbounded, positive sizes apply backpressure."""

    def __init__(self, maxsize: int = 0) -> None:
        if maxsize < 0:
            raise ValueError("Queue maxsize must be nonnegative")
        self._maxsize = maxsize
        self._lock = Lock()
        self._shutdown = False
        self._queue: deque[T] = deque()
        self._getters: dict[SelectorGuard[T], None] = {}
        self._putters: dict[SelectorGuard[None], T] = {}

    def _has_space(self) -> bool:
        return not self._maxsize or len(self._queue) < self._maxsize

    @shield
    @routine
    async def _notify(self) -> None:
        # The caller owns the queue lock. A committed operation can make other
        # waiters ready, so notification must finish despite cancellation.
        # Acquire one selection at a time: put/get alternatives may share one.
        while True:
            while self._getters and (self._queue or self._shutdown):
                guard = next(iter(self._getters))
                async with selectors(guard) as (selector,):
                    if selector is not None:
                        if self._queue:
                            selector.result(self._queue.popleft())
                        else:
                            selector.error(ShutDown())
                    del self._getters[guard]
            while self._putters and (self._has_space() or self._shutdown):
                guard = next(iter(self._putters))
                async with selectors(guard) as (selector,):
                    if selector is not None:
                        if self._shutdown:
                            selector.error(ShutDown())
                        else:
                            self._queue.append(self._putters[guard])
                            selector.result(None)
                    del self._putters[guard]
            # Newly admitted puts can make getters ready. Stale registrations
            # are discarded without consuming values or filling buffer slots.
            if not (self._getters and self._queue):
                return

    @selectable
    @contextmanager
    async def get(self, guard: SelectorGuard[T]) -> AsyncIterator[None]:
        """Wait for and remove the oldest value, or raise ShutDown after draining."""

        @shield
        @routine
        async def cleanup() -> None:
            async with self._lock:
                self._getters.pop(guard, None)

        try:
            async with self._lock:
                async with selectors(guard) as (selector,):
                    if selector is not None:
                        if self._queue:
                            selector.result(self._queue.popleft())
                        elif self._shutdown:
                            selector.error(ShutDown())
                        else:
                            self._getters[guard] = None
                await self._notify.a()
            yield
        finally:
            await cleanup.a()

    @selectable
    @contextmanager
    async def put(self, guard: SelectorGuard[None], value: T) -> AsyncIterator[None]:
        """Wait for space and store value, or raise ShutDown after shutdown."""

        @shield
        @routine
        async def cleanup() -> None:
            async with self._lock:
                self._putters.pop(guard, None)

        try:
            async with self._lock:
                async with selectors(guard) as (selector,):
                    if selector is not None:
                        if self._shutdown:
                            selector.error(ShutDown())
                        elif self._has_space():
                            self._queue.append(value)
                            selector.result(None)
                        else:
                            self._putters[guard] = value
                await self._notify.a()
            yield
        finally:
            await cleanup.a()

    @shield
    @routine
    async def shutdown(self, *, immediate: bool = False) -> None:
        """Reject puts and allow draining, or discard buffered values immediately.

        Wake blocked operations with ShutDown when they can no longer succeed.
        Repeated calls are harmless; immediate=True can stop an ongoing drain.
        Return after notification without waiting for consumers to finish.
        """
        async with self._lock:
            self._shutdown = True
            if immediate:
                self._queue.clear()
            await self._notify.a()
