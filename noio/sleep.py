"""Suspend the caller using its current IO runtime."""

from collections.abc import AsyncIterator
from math import inf

from aiologic.lowlevel import async_sleep
from aiologic.lowlevel import green_sleep

from ._contextlib import contextmanager
from ._timer import _timer
from .routine import routine
from .selectable import SelectorGuard
from .selectable import selectable
from .selectable import selectors


@selectable
@contextmanager
async def sleep(  # pyright: ignore[reportRedeclaration]
    guard: SelectorGuard[None], seconds: float
) -> AsyncIterator[None]:
    """Wait for a duration; prepare a timeout with sleep.select(seconds)."""
    if not seconds >= 0:
        raise ValueError("seconds must be non-negative and not NaN")
    if seconds == inf:
        yield
        return

    @routine
    async def complete() -> None:
        async with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(None)

    if seconds == 0:
        await sleep.a(0)
        await complete.a()
        yield
        return

    async with _timer(seconds, complete):
        yield


@sleep.call
@routine
async def sleep(seconds: float) -> None:  # pyright: ignore[reportRedeclaration]
    """Wait for seconds, yielding execution to the current runtime.

    The duration must be non-negative and not NaN. Zero yields execution
    without a timed delay. Positive infinity waits indefinitely.
    """
    await async_sleep(seconds)


@sleep.call_fn
def sleep(seconds: float) -> None:
    green_sleep(seconds)
