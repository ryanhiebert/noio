from collections.abc import Callable
from math import inf

import pytest

from ._event import Event
from ._task_group import _open_task_group
from ._timer import _timer
from .conftest import AsyncRuntime
from .conftest import RunSync
from .routine import routine
from .sleep import sleep


@routine
async def _exercise() -> None:
    expired = Event()

    @routine
    async def notify() -> None:
        expired.set()

    async with _timer(0.001, notify):
        await expired.wait.a()

    @routine
    async def unexpected() -> None:
        pytest.fail("stopped timer must not expire")

    for duration in (60, 1e100, inf):
        async with _timer(duration, unexpected):
            await sleep.a(0)

    entered = Event()
    leaving = Event()
    release = Event()
    finished = Event()
    exited = False

    @routine
    async def callback() -> None:
        entered.set()
        await release.wait.a()
        finished.set()

    @routine
    async def unblock() -> None:
        await leaving.wait.a()
        await sleep.a(0)
        assert not exited
        release.set()

    async with _open_task_group() as group:
        await group.spawn.a(unblock)
        async with _timer(0, callback):
            await entered.wait.a()
            leaving.set()
        exited = True
        await finished.wait.a()


def _check_ordinary(checkpoint: Callable[[], None]) -> None:
    _exercise()


def test_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_ordinary)


def test_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_exercise.a)


def test_timer_cancellation(async_runtime: AsyncRuntime) -> None:
    @routine
    async def unexpected() -> None:
        pytest.fail("stopped timer must not expire")

    async def body() -> None:
        async with _timer(60, unexpected):
            await async_runtime.checkpoint()

    async def main() -> None:
        await async_runtime.cancel(body)

    async_runtime.run(main)


def test_timer_uses_registration_deadline() -> None:
    import trio
    from trio.testing import MockClock

    async def main() -> None:
        clock = trio.lowlevel.current_clock()
        assert isinstance(clock, MockClock)
        clock.jump(100)
        expired = Event()
        times: list[float] = []

        @routine
        async def notify() -> None:
            times.append(trio.current_time())
            expired.set()

        async with _timer(10, notify):
            # The worker has not run yet; these seven seconds still count.
            clock.jump(7)
            await expired.wait.a()
        assert times == [110]

    trio.run(main, clock=MockClock(autojump_threshold=0))
