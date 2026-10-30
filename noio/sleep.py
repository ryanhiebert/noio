"""Suspend the caller using its current IO runtime."""

from math import inf

from aiologic.lowlevel import async_sleep
from aiologic.lowlevel import green_sleep

from ._timer import _timer
from .routine import Routine
from .routine import routine
from .selectable import SelectorGuard
from .selectable import selectable
from .selectable import selectors


@selectable
@routine
async def sleep(  # pyright: ignore[reportRedeclaration]
    guard: SelectorGuard[None], seconds: float
) -> Routine[[], None] | None:
    """Wait for a duration; prepare a timeout with sleep.select(seconds)."""
    if not seconds >= 0:
        raise ValueError("seconds must be non-negative and not NaN")
    if seconds == inf:
        return None

    @routine
    async def complete() -> None:
        async with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(None)

    if seconds == 0:
        await sleep.a(0)
        await complete.a()
        return None

    timer = _timer(seconds, complete)
    await timer.__aenter__()

    @routine
    async def cleanup() -> None:
        # The timer's task group shields worker waits. An outer shield here
        # would nest a Trio cancel scope inside the nursery we must close.
        await timer.__aexit__(None, None, None)

    return cleanup


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
