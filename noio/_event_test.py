from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent

import pytest

from noio import routine
from noio._event import Event

from .conftest import AsyncRuntime
from .conftest import RunSync


def _check_sync(checkpoint: Callable[[], None]) -> None:
    event = Event()
    event.set()
    event.set()
    assert event.wait() is None

    @routine
    async def nested() -> None:
        await event.wait.a()
        with pytest.raises(RuntimeError, match="Use await function.a"):
            event.wait()

    nested()
    checkpoint()
    event.wait()


def test_sync_calls(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def _check_green_waiters(checkpoint: Callable[[], None]) -> None:
    import gevent

    event = Event()
    completed: list[bool] = []

    @routine
    async def waiter() -> None:
        await event.wait.a()
        completed.append(True)

    cancelled = gevent.spawn(waiter)
    first = gevent.spawn(waiter)
    second = gevent.spawn(waiter)
    checkpoint()
    assert completed == []
    cancelled.kill()
    event.set()
    first.get(timeout=2)
    second.get(timeout=2)
    assert completed == [True, True]
    event.wait()


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_green_waiters(sync_runtime: RunSync) -> None:
    sync_runtime(_check_green_waiters)


def test_async_waiters(async_runtime: AsyncRuntime) -> None:
    event = Event()
    completed: list[bool] = []

    @routine
    async def waiter() -> None:
        assert await event.wait.a() is None
        completed.append(True)

    async def signal() -> None:
        await async_runtime.checkpoint()
        assert completed == []
        await async_runtime.cancel(waiter.a)
        event.set()
        event.set()

    async def main() -> None:
        await async_runtime.concurrently(waiter.a, waiter.a, signal)
        assert completed == [True, True]
        await waiter.a()

    async_runtime.run(main)


def test_thread_and_async_wakeups(async_runtime: AsyncRuntime) -> None:
    to_thread = Event()
    to_async = Event()
    started = ThreadEvent()

    def worker() -> None:
        started.set()
        to_thread.wait()
        to_async.set()

    async def main() -> None:
        assert started.wait(2)
        to_thread.set()
        await to_async.wait.a()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(worker)
        try:
            async_runtime.run(main)
        finally:
            to_thread.set()
        future.result(timeout=2)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancelled_waiter_leaves_other_waiters_intact(backend: str) -> None:
    import anyio
    from anyio import wait_all_tasks_blocked

    event = Event()

    async def main() -> None:
        scope = anyio.CancelScope()
        cancelled = anyio.Event()
        completed = anyio.Event()

        async def cancelled_waiter() -> None:
            with scope:
                await event.wait.a()
                pytest.fail("cancelled waiter completed")
            cancelled.set()

        async def survivor() -> None:
            await event.wait.a()
            completed.set()

        async with anyio.create_task_group() as group:
            group.start_soon(cancelled_waiter)
            group.start_soon(survivor)
            await wait_all_tasks_blocked()
            scope.cancel()
            await cancelled.wait()
            assert not completed.is_set()
            event.set()
        assert completed.is_set()
        await event.wait.a()

    anyio.run(main, backend=backend)


def test_asyncio_and_trio_share_event() -> None:
    import asyncio

    import trio
    from trio.testing import wait_all_tasks_blocked

    event = Event()
    waiting = ThreadEvent()
    completed = ThreadEvent()

    async def trio_waiter() -> None:
        await event.wait.a()
        completed.set()

    async def trio_main() -> None:
        with trio.fail_after(5):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(trio_waiter)
                await wait_all_tasks_blocked()
                assert not completed.is_set()
                waiting.set()

    async def asyncio_main() -> None:
        async def signal() -> None:
            while not waiting.is_set():
                await asyncio.sleep(0)
            event.set()

        async with asyncio.timeout(5):
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(event.wait.a())
                tasks.create_task(signal())

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(trio.run, trio_main)
        try:
            asyncio.run(asyncio_main())
        finally:
            event.set()
        future.result(timeout=5)
    assert completed.is_set()
