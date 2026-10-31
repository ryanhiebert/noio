from collections.abc import AsyncIterator
from collections.abc import Callable
from math import inf
from math import nan
from time import monotonic
from typing import assert_type

import pytest
from aiologic.lowlevel import current_green_library

import noio
from noio import contextmanager

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


@noio.routine
async def _selectable_sleep() -> None:
    from .event import Event
    from .select import select

    prepared = noio.sleep.select(0.001)
    for _ in range(2):
        start = monotonic()
        assert await select.a(prepared) == (0, None)
        assert monotonic() - start >= 0.0009

    ready = Event()
    await ready.set.a()
    for duration in (0, 60, 1e100, inf):
        assert await select.a(ready.wait.select(), noio.sleep.select(duration)) == (
            0,
            None,
        )
    assert await select.a(noio.sleep.select(0), noio.sleep.select(0)) == (0, None)

    for seconds in (-1, -inf, nan):
        with pytest.raises(ValueError):
            await select.a(noio.sleep.select(seconds))


def _check_selectable_sleep(checkpoint: Callable[[], None]) -> None:
    _selectable_sleep()


def test_selectable_sleep(sync_runtime: RunSync) -> None:
    sync_runtime(_check_selectable_sleep)


def test_selectable_sleep_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_selectable_sleep.a)


@pytest.mark.parametrize("seconds", [0, 60, inf])
@pytest.mark.parametrize("count", [1, 3])
def test_selectable_sleep_cancellation(
    async_runtime: AsyncRuntime, seconds: float, count: int
) -> None:
    from .select import select

    async def body() -> None:
        await select.a(*(noio.sleep.select(seconds) for _ in range(count)))
        pytest.fail("selection must deliver cancellation")

    async def main() -> None:
        await async_runtime.cancel(body)

    async_runtime.run(main)


def test_sleep_registration_timing() -> None:
    import trio
    from trio.testing import MockClock

    from .select import select
    from .selectable import SelectorGuard
    from .selectable import selectable

    @selectable
    @contextmanager
    async def delay_registration(guard: SelectorGuard[None]) -> AsyncIterator[None]:
        await trio.sleep(7)
        yield

    async def main() -> None:
        operation = noio.sleep.select(10)
        await trio.sleep(100)
        assert await select.a(delay_registration.select(), operation) == (1, None)
        assert trio.current_time() == 117
        # Each registration gets a fresh deadline, including worker-start delays.
        assert await select.a(operation, delay_registration.select()) == (0, None)
        assert trio.current_time() == 127

    trio.run(main, clock=MockClock(autojump_threshold=0))


def test_prepared_sleep_shared_across_runtimes() -> None:
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    import trio

    from .select import select

    prepared = noio.sleep.select(0.005)
    barrier = Barrier(3)

    async def asynchronous() -> None:
        assert await select.a(prepared) == (0, None)

    def run_asyncio() -> None:
        barrier.wait(timeout=5)
        asyncio.run(asynchronous())

    def run_trio() -> None:
        barrier.wait(timeout=5)
        trio.run(asynchronous)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_asyncio), pool.submit(run_trio)]
        barrier.wait(timeout=5)
        assert select(prepared) == (0, None)
        for future in futures:
            future.result(timeout=5)


@noio.routine
async def _zero_yields_before_completion() -> None:
    from importlib import import_module

    from .select import select
    from .selectable import SelectorGuard
    from .selectable import selectable
    from .selectable import selectors

    guards: list[SelectorGuard[None]] = []
    yielded: list[bool] = []

    @selectable
    @contextmanager
    async def first(guard: SelectorGuard[None]) -> AsyncIterator[None]:
        guards.append(guard)
        yield

    @noio.routine
    async def checkpoint(seconds: float) -> None:
        assert seconds == 0
        yielded.append(True)
        async with selectors(guards[0]) as (selector,):
            assert selector is not None
            selector.result(None)

    def no_timer(*args: object) -> None:
        pytest.fail("zero and infinite sleeps must not start timers")

    with pytest.MonkeyPatch.context() as patch:
        module = import_module("noio.sleep")
        patch.setattr(module, "async_sleep", checkpoint.a)
        patch.setattr(module, "green_sleep", checkpoint)
        patch.setattr(module, "_timer", no_timer)
        assert await select.a(first.select(), noio.sleep.select(0)) == (0, None)
        assert yielded == [True]
        guards.clear()
        assert await select.a(
            first.select(), noio.sleep.select(inf), noio.sleep.select(0)
        ) == (0, None)


def _check_zero_yield(checkpoint: Callable[[], None]) -> None:
    _zero_yields_before_completion()


def test_zero_yields_before_completion(sync_runtime: RunSync) -> None:
    sync_runtime(_check_zero_yield)


def test_zero_yields_before_completion_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_zero_yields_before_completion.a)


@noio.routine
async def _timer_loses_while_waiting_for_selection_lock() -> None:
    from importlib import import_module

    from ._contextlib import contextmanager
    from ._event import Event
    from ._selection import _Selection
    from .selectable import SelectorGuard
    from .selectable import selectors

    attempting = Event()
    selection = _Selection[None]()
    guard = SelectorGuard(selection, 0)

    @contextmanager
    async def observed_selectors(guard: SelectorGuard[None]):
        attempting.set()
        async with selectors(guard) as selected:
            yield selected

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(import_module("noio.sleep"), "selectors", observed_selectors)
        async with selectors(guard) as (winner,):
            assert winner is not None
            cleanup = noio.sleep.select(0.001).__select__(guard)
            await cleanup.__aenter__()
            # The timer has expired, but its completion must acquire this lock.
            await attempting.wait.a()
            winner.result(None)
        await cleanup.__aexit__(None, None, None)
        assert await selection.wait.a() == (0, None)


def _check_competing_completion(checkpoint: Callable[[], None]) -> None:
    _timer_loses_while_waiting_for_selection_lock()


def test_competing_completion(sync_runtime: RunSync) -> None:
    sync_runtime(_check_competing_completion)


def test_competing_completion_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_timer_loses_while_waiting_for_selection_lock.a)


@pytest.mark.parametrize("import_first", [False, True])
def test_selectable_sleep_in_patched_main_thread(import_first: bool) -> None:
    import subprocess
    import sys
    import sysconfig

    if sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("gevent does not support free-threaded CPython")
    script = "import noio\n" if import_first else ""
    script += (
        "from gevent import monkey\n"
        "monkey.patch_all()\n"
        "from noio.sleep_test import _check_selectable_sleep\n"
        "_check_selectable_sleep(lambda: None)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stdout + result.stderr
