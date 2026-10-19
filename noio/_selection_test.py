from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent
from typing import Self

import anyio
import pytest

from noio import routine
from noio._lock import Lock
from noio._selection import _Selection
from noio.routine import _mode
from noio.selectable import SelectorGuard
from noio.selectable import selectors

from .conftest import AsyncRuntime
from .conftest import RunSync


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_waiter_cannot_observe_partial_exchange(backend: str) -> None:
    async def main() -> None:
        first, second = _Selection[int](), _Selection[int]()
        observed: list[tuple[int, int]] = []

        async def waiter() -> None:
            observed.append(await first.wait.a())

        async with anyio.create_task_group() as group:
            group.start_soon(waiter)
            await anyio.wait_all_tasks_blocked()
            async with selectors(SelectorGuard(first, 0), SelectorGuard(second, 0)) as (
                a,
                b,
            ):
                assert a is not None and b is not None
                a.result(2)
                # Deliberately suspend to prove notification alone cannot expose
                # the outcome. Real matching code must not suspend here.
                await anyio.wait_all_tasks_blocked()
                assert observed == []
                b.result(1)
        assert observed == [(0, 2)]
        assert await second.wait.a() == (0, 1)

    anyio.run(main, backend=backend)


def test_invalidation_waits_for_acquired_selector(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        state = _Selection[int]()
        attempted = async_runtime.event()
        closed = False

        class SignallingLock(Lock):
            async def __aenter__(self) -> Self:
                attempted.set()
                return await super().__aenter__()

        state._lock = SignallingLock()

        async def closer() -> None:
            nonlocal closed
            await state.invalidate.a()
            closed = True

        async def owner() -> None:
            async with selectors(SelectorGuard(state, 0)) as (selector,):
                assert selector is not None
                # Use a fresh event after acquiring the lock ourselves.
                nonlocal attempted
                attempted = async_runtime.event()
                entered.set()
                await attempted.wait()
                assert not closed
                selector.result(4)

        entered = async_runtime.event()

        async def close_after_entry() -> None:
            await entered.wait()
            await closer()

        await async_runtime.concurrently(owner, close_after_entry)
        assert closed
        assert await state.wait.a() == (0, 4)
        async with selectors(SelectorGuard(state, 1)) as (selector,):
            assert selector is None

    async_runtime.run(main)


def test_thread_completes_async_selection(async_runtime: AsyncRuntime) -> None:
    state = _Selection[int]()
    started = ThreadEvent()

    def complete() -> None:
        assert started.wait(5)
        with selectors(SelectorGuard(state, 3)) as (selector,):
            assert selector is not None
            selector.result(42)

    async def main() -> None:
        started.set()
        assert await state.wait.a() == (3, 42)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(complete)
        async_runtime.run(main)
        future.result(timeout=5)


def test_asyncio_and_trio_exchange() -> None:
    import asyncio

    import trio
    from trio.testing import wait_all_tasks_blocked

    first, second = _Selection[str](), _Selection[str]()
    waiting = ThreadEvent()

    async def trio_main() -> None:
        async def waiter() -> None:
            assert await first.wait.a() == (0, "to trio")

        with trio.fail_after(5):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(waiter)
                await wait_all_tasks_blocked()
                waiting.set()

    async def asyncio_main() -> None:
        async with asyncio.timeout(5):
            while not waiting.is_set():
                await asyncio.sleep(0)
            async with selectors(SelectorGuard(second, 1), SelectorGuard(first, 0)) as (
                b,
                a,
            ):
                assert a is not None and b is not None
                a.result("to trio")
                b.result("to asyncio")
            assert await second.wait.a() == (1, "to asyncio")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(trio.run, trio_main)
        asyncio.run(asyncio_main())
        future.result(timeout=6)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancellation_after_notification_does_not_undo_outcome(backend: str) -> None:
    async def main() -> None:
        state = _Selection[int]()
        scope = anyio.CancelScope()
        finished = anyio.Event()

        async def waiter() -> None:
            with scope:
                await state.wait.a()
                pytest.fail("cancelled waiter returned its result")
            finished.set()

        async with (
            anyio.create_task_group() as group,
            selectors(SelectorGuard(state, 0)) as (selector,),
        ):
            assert selector is not None
            selector.result(5)
            group.start_soon(waiter)
            await anyio.wait_all_tasks_blocked()
            scope.cancel()
            await finished.wait()
        # The later select lifecycle will invalidate and clean up on cancellation.
        # The state itself must not undo a committed exchange.
        await state.invalidate.a()
        assert await state.wait.a() == (0, 5)

    anyio.run(main, backend=backend)


def test_invalidation_finishes_despite_repeated_asyncio_cancellation() -> None:
    import asyncio

    async def main() -> None:
        attempted = asyncio.Event()
        state = _Selection[int]()

        class ObservedLock(Lock):
            async def __aenter__(self) -> Self:
                attempted.set()
                return await super().__aenter__()

        state._lock = ObservedLock()
        started = asyncio.Event()

        async def worker() -> None:
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                await state.invalidate.a()

        async with state._lock:
            attempted.clear()
            task = asyncio.create_task(worker())
            await started.wait()
            task.cancel("first")
            await attempted.wait()
            task.cancel("second")
            await asyncio.sleep(0)
            assert not task.done()
            assert state._pending
        with pytest.raises(asyncio.CancelledError):
            await task
        async with selectors(SelectorGuard(state, 0)) as (selector,):
            assert selector is None

    asyncio.run(main())


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_invalidation_finishes_in_cancelled_scope(backend: str) -> None:
    async def main() -> None:
        state = _Selection[int]()
        started, finished = anyio.Event(), anyio.Event()
        scope = anyio.CancelScope()

        async def worker() -> None:
            with scope:
                try:
                    started.set()
                    await anyio.sleep_forever()
                finally:
                    await state.invalidate.a()
            finished.set()

        async with anyio.create_task_group() as group:
            async with state._lock:
                group.start_soon(worker)
                await started.wait()
                scope.cancel()
                await anyio.wait_all_tasks_blocked()
                assert state._pending
                assert not finished.is_set()
            await finished.wait()
        assert scope.cancelled_caught
        async with selectors(SelectorGuard(state, 0)) as (selector,):
            assert selector is None

    anyio.run(main, backend=backend)


def _check_shielded_green_invalidation(checkpoint: Callable[[], None]) -> None:
    import gevent
    from gevent.event import Event

    state = _Selection[int]()
    started = Event()

    def worker() -> None:
        try:
            started.set()
            Event().wait()
        finally:
            state.invalidate()

    with state._lock:
        task = gevent.spawn(worker)
        assert started.wait(timeout=2)
        task.kill(block=False)
        checkpoint()
        task.kill(block=False)
        checkpoint()
        assert not task.dead
        assert state._pending
    task.get(timeout=2)
    with selectors(SelectorGuard(state, 0)) as (selector,):
        assert selector is None


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_shielded_green_invalidation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_shielded_green_invalidation)


def _check_invalidation_preserves_routine_context(
    checkpoint: Callable[[], None],
) -> None:
    state = _Selection[int]()

    @routine
    async def outer() -> None:
        await state.invalidate.a()
        assert _mode.get() is not None
        with pytest.raises(RuntimeError, match="Use await"):
            state.wait()

    outer()
    assert _mode.get() is None
    with selectors(SelectorGuard(state, 0)) as (selector,):
        assert selector is None
    checkpoint()


def test_invalidation_preserves_routine_context(sync_runtime: RunSync) -> None:
    sync_runtime(_check_invalidation_preserves_routine_context)
