from collections.abc import Callable
from math import inf
from math import nan
from time import monotonic
from typing import assert_type

import pytest
from aiologic.lowlevel import current_green_library

import noio

from .conftest import AsyncRuntime
from .conftest import RunSync


@noio.routine
async def _composed_sleep(seconds: float) -> None:
    assert assert_type(await noio.sleep.a(seconds), None) is None


def _check_sleep(checkpoint: Callable[[], None]) -> None:
    assert assert_type(noio.sleep(0), None) is None
    start = monotonic()
    noio.sleep(seconds=0.01)
    assert monotonic() - start >= 0.009
    _composed_sleep(0.001)
    for seconds in (-1, -inf, nan):
        with pytest.raises(ValueError):
            noio.sleep(seconds)
    if current_green_library() == "gevent":
        import gevent

        progressed: list[bool] = []
        other = gevent.spawn(progressed.append, True)
        noio.sleep(0)
        assert progressed == [True]
        other.get()
    checkpoint()


def test_sleep(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sleep)


def test_async_sleep(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        assert assert_type(await noio.sleep.a(0), None) is None
        start = monotonic()
        await noio.sleep.a(seconds=0.01)
        assert monotonic() - start >= 0.009
        await _composed_sleep.a(0.001)
        for seconds in (-1, -inf, nan):
            with pytest.raises(ValueError):
                await noio.sleep.a(seconds)

    async_runtime.run(main)


@pytest.mark.parametrize("seconds", [0, 60, inf])
def test_sleep_cancellation(async_runtime: AsyncRuntime, seconds: float) -> None:
    async def cancelled() -> None:
        await noio.sleep.a(seconds)
        pytest.fail("sleep must deliver pending cancellation")

    async def main() -> None:
        await async_runtime.cancel(cancelled)
        # Cancellation must not leave the routine's execution mode active.
        await noio.sleep.a(0)

    async_runtime.run(main)


def test_sleep_yields_to_other_tasks(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        started = async_runtime.event()
        progressed = False

        async def sleeper() -> None:
            started.set()
            await noio.sleep.a(0.02)
            assert progressed

        async def other() -> None:
            nonlocal progressed
            await started.wait()
            progressed = True

        await async_runtime.concurrently(sleeper, other)

    async_runtime.run(main)
