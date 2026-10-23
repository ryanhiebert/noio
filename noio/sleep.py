"""Suspend the caller using its current IO runtime."""

from aiologic.lowlevel import async_sleep
from aiologic.lowlevel import green_sleep

from .routine import routine


@routine
async def sleep(seconds: float) -> None:  # pyright: ignore[reportRedeclaration]
    """Wait for seconds, yielding execution to the current runtime.

    The duration must be non-negative and not NaN. Zero yields execution
    without a timed delay. Positive infinity waits indefinitely.
    """
    await async_sleep(seconds)


@sleep.fn
def sleep(seconds: float) -> None:
    green_sleep(seconds)
