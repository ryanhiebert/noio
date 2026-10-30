"""Run an expiry routine in an owned worker, stopping its wait on scope exit."""

from collections.abc import AsyncIterator
from math import inf

import aiologic
from aiologic.lowlevel import async_clock
from aiologic.lowlevel import green_clock

from ._contextlib import contextmanager
from ._runtime import current_runtime
from ._task_group import _open_task_group
from .routine import Routine
from .routine import _Mode
from .routine import _mode
from .routine import routine


@contextmanager
async def _timer(seconds: float, on_expiry: Routine[[], None]) -> AsyncIterator[None]:
    if not seconds >= 0:
        raise ValueError("seconds must be non-negative and not NaN")
    if seconds == inf:
        yield
        return

    if await current_runtime.a() == "gevent":
        from gevent import get_hub

        # Patched threads may select gevent before its first hub exists.
        # Establish it before choosing the clock, keeping one clock domain.
        get_hub()
    clock = green_clock if _mode.get() is _Mode.DRIVEN else async_clock
    deadline = clock() + seconds
    stopped = aiologic.Event()
    async with _open_task_group() as group:

        @routine
        async def run() -> None:
            remaining = max(0.0, deadline - clock())
            if _mode.get() is _Mode.DRIVEN:
                was_stopped = stopped.wait(remaining)
            else:
                was_stopped = await stopped.with_(remaining)
            if not was_stopped:
                await on_expiry.a()

        try:
            await group.spawn.a(run)
            yield
        finally:
            stopped.set()
        # The group waits for the worker, including an expiry callback already
        # running. It owns cleanup shielding and Trio's nursery scope ordering.
