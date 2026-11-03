import weakref
from collections.abc import AsyncIterator
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent

import anyio
import pytest

from noio import CancelledError
from noio import Event
from noio import Queue
from noio import ShutDown
from noio import cancel
from noio import contextmanager
from noio.select import _Selection
from noio.select import _Selector
from noio.select import select
from noio.selectable import SelectorGuard
from noio.selectable import selectable

from .conftest import AsyncRuntime
from .conftest import RunSync


def _check_sync(checkpoint: Callable[[], None]) -> None:
    queue = Queue[int]()
    for value in range(20):
        assert queue.put(value) is None
    for value in range(20):
        assert queue.get() == value
    bounded = Queue[int](1)
    assert select(bounded.get.select(), bounded.put.select(1)) == (1, None)
    assert bounded.get() == 1
    bounded.put(2)
    assert select(bounded.put.select(3), bounded.get.select()) == (1, 2)
    assert not bounded._queue
    # Registration continues after winning: neither later operation may act.
    bounded.put(4)
    assert select(bounded.get.select(), bounded.put.select(5)) == (0, 4)
    assert not bounded._queue
    assert select(bounded.put.select(6), bounded.get.select()) == (0, None)
    assert bounded.get() == 6
    assert not bounded._getters and not bounded._putters
    bounded.shutdown()
    bounded.shutdown()
    with pytest.raises(ShutDown):
        bounded.put(7)
    with pytest.raises(ShutDown):
        bounded.get()
    checkpoint()


def test_sync_calls(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_negative_capacity() -> None:
    with pytest.raises(ValueError, match="nonnegative"):
        Queue[int](-1)


def test_async_calls_and_backpressure(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        queue = Queue[int](1)
        values = []

        async def producer() -> None:
            for value in range(30):
                await queue.put.a(value)
            await queue.shutdown.a()

        async def consumer() -> None:
            while True:
                try:
                    values.append(await queue.get.a())
                except ShutDown:
                    return

        await async_runtime.concurrently(producer, consumer)
        assert values == list(range(30))
        assert not queue._getters and not queue._putters

    async_runtime.run(main)


@pytest.mark.parametrize("immediate", [False, True])
def test_shutdown_and_escalation(immediate: bool) -> None:
    queue = Queue[int](2)
    queue.put(1)
    queue.put(2)
    queue.shutdown(immediate=immediate)
    queue.shutdown()
    with pytest.raises(ShutDown):
        queue.put(3)
    if not immediate:
        assert queue.get() == 1
        assert queue.get() == 2
    with pytest.raises(ShutDown):
        queue.get()
    assert not queue._queue

    queue = Queue[int]()
    queue.put(4)
    queue.shutdown()
    queue.shutdown(immediate=True)
    with pytest.raises(ShutDown):
        queue.get()
    assert not queue._queue


def test_immediate_shutdown_releases_buffered_values() -> None:
    class Value:
        pass

    queue = Queue[Value]()
    value = Value()
    reference = weakref.ref(value)
    queue.put(value)
    del value
    queue.shutdown(immediate=True)
    assert reference() is None


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("putting", [False, True])
@pytest.mark.parametrize("immediate", [False, True])
def test_shutdown_wakes_waiters(backend: str, putting: bool, immediate: bool) -> None:
    async def main() -> None:
        queue = Queue[int](1)
        if putting:
            await queue.put.a(1)
        finished = []

        async def waiter() -> None:
            with pytest.raises(ShutDown):
                if putting:
                    await queue.put.a(2)
                else:
                    await queue.get.a()
            finished.append(True)

        async with anyio.create_task_group() as group:
            group.start_soon(waiter)
            group.start_soon(waiter)
            await anyio.wait_all_tasks_blocked()
            assert len(queue._putters if putting else queue._getters) == 2
            await queue.shutdown.a(immediate=immediate)
        assert len(finished) == 2
        assert not queue._getters and not queue._putters
        if putting and not immediate:
            assert await queue.get.a() == 1
        with pytest.raises(ShutDown):
            await queue.get.a()

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("putting", [False, True])
def test_losing_and_cancelled_waiters(backend: str, putting: bool) -> None:
    async def main() -> None:
        queue = Queue[int](1)
        if putting:
            await queue.put.a(1)
        ready = Event()
        operation = queue.put.select(2) if putting else queue.get.select()
        results = []

        async def loser() -> None:
            results.append(await select.a(operation, ready.wait.select()))

        async with anyio.create_task_group() as group:
            group.start_soon(loser)
            await anyio.wait_all_tasks_blocked()
            await ready.set.a()
        assert results == [(1, None)]
        assert not queue._getters and not queue._putters

        async with cancel() as scope:

            async def cancelled() -> None:
                with pytest.raises(CancelledError):
                    await select.a(operation)

            async with anyio.create_task_group() as group:
                group.start_soon(cancelled)
                await anyio.wait_all_tasks_blocked()
                await scope.cancel.a()
        assert not queue._getters and not queue._putters
        if putting:
            assert await queue.get.a() == 1
        await queue.put.a(3)
        assert await queue.get.a() == 3

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("putting", [False, True])
def test_stale_and_duplicate_registrations(putting: bool) -> None:
    queue = Queue[int](1)
    if putting:
        queue.put(1)
    stale, live = _Selection[int | None](), _Selection[int | None]()
    registrations = []
    for state, index in ((stale, 0), (live, 0), (live, 1)):
        operation = queue.put.select(2) if putting else queue.get.select()
        registration = operation.__select__(
            SelectorGuard(_Selector(state, index), state._lock)
        )
        registration.__enter__()
        registrations.append(registration)
    stale.invalidate()
    if putting:
        assert queue.get() == 1
        assert live.wait() == (0, None)
        assert queue.get() == 2
        assert not queue._queue
    else:
        queue.put(3)
        assert live.wait() == (0, 3)
        queue.put(4)
        assert queue.get() == 4
    for registration in reversed(registrations):
        registration.__exit__(None, None, None)
    assert not queue._getters and not queue._putters


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("putting", [False, True])
def test_cancel_registration_before_commit(backend: str, putting: bool) -> None:
    async def main() -> None:
        queue = Queue[int](1)
        if not putting:
            await queue.put.a(1)
        state = _Selection[int | None]()
        scope = anyio.CancelScope()

        async def register() -> None:
            operation = queue.put.select(2) if putting else queue.get.select()
            with scope:
                await operation.__select__(
                    SelectorGuard(_Selector(state, 0), state._lock)
                ).__aenter__()
                pytest.fail("cancelled registration completed")

        async with anyio.create_task_group() as group, state._lock:
            group.start_soon(register)
            await anyio.wait_all_tasks_blocked()
            scope.cancel()
        assert scope.cancelled_caught
        assert state._pending
        assert list(queue._queue) == ([] if putting else [1])
        assert not queue._getters and not queue._putters

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("putting", [False, True])
def test_committed_operation_finishes_notification_despite_cancellation(
    backend: str, putting: bool
) -> None:
    async def main() -> None:
        queue = Queue[int](1)
        if not putting:
            await queue.put.a(1)
        state = _Selection[int | None]()
        waiting = queue.get.select() if putting else queue.put.select(2)
        registration = waiting.__select__(
            SelectorGuard(_Selector(state, 0), state._lock)
        )
        await registration.__aenter__()
        scope = anyio.CancelScope()
        finished = []

        async def commit() -> None:
            with scope:
                if putting:
                    await queue.put.a(3)
                else:
                    assert await queue.get.a() == 1
            finished.append("commit")

        async def shutdown() -> None:
            await queue.shutdown.a()
            finished.append("shutdown")

        async with anyio.create_task_group() as group, state._lock:
            group.start_soon(commit)
            await anyio.wait_all_tasks_blocked()
            assert list(queue._queue) == ([3] if putting else [])
            assert state._pending
            scope.cancel()
            group.start_soon(shutdown)
            await anyio.wait_all_tasks_blocked()
            assert not finished
            # Notification finishes before the competing shutdown acquires the
            # queue lock, even though the caller was cancelled after committing.
        assert set(finished) == {"commit", "shutdown"}
        assert await state.wait.a() == (0, 3 if putting else None)
        if not putting:
            assert await queue.get.a() == 2
        await registration.__aexit__(None, None, None)
        assert not queue._getters and not queue._putters

    anyio.run(main, backend=backend)


def _check_cross_runtime(checkpoint: Callable[[], None]) -> None:
    queue = Queue[int](1)
    registered = ThreadEvent()

    @selectable
    @contextmanager
    async def observed(guard: SelectorGuard[int]) -> AsyncIterator[None]:
        async with queue.get.select().__select__(guard):
            registered.set()
            yield

    def producer() -> None:
        assert registered.wait(2)
        queue.put(42)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(producer)
        assert select(observed.select()) == (0, 42)
        future.result(timeout=2)
    assert not queue._getters
    checkpoint()


def test_cross_runtime(sync_runtime: RunSync) -> None:
    sync_runtime(_check_cross_runtime)


def test_same_queue_threads_asyncio_trio() -> None:
    import asyncio

    import trio

    queue = Queue[int](1)

    async def trio_producer() -> None:
        for value in range(30):
            await queue.put.a(value)

    async def asyncio_producer() -> None:
        for value in range(30, 60):
            await queue.put.a(value)

    def consumer() -> list[int]:
        return [queue.get() for _ in range(60)]

    with ThreadPoolExecutor(max_workers=2) as pool:
        values = pool.submit(consumer)
        trio_result = pool.submit(trio.run, trio_producer)
        asyncio.run(asyncio_producer())
        trio_result.result(timeout=5)
        assert sorted(values.result(timeout=5)) == list(range(60))
    assert not queue._getters and not queue._putters


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("putting", [False, True])
def test_two_queues_compete_for_one_selection(backend: str, putting: bool) -> None:
    async def main() -> None:
        queues = [Queue[int](1), Queue[int](1)]
        state = _Selection[int | None]()
        registrations = []
        for index, queue in enumerate(queues):
            if putting:
                await queue.put.a(index)
            operation = queue.put.select(10 + index) if putting else queue.get.select()
            registration = operation.__select__(
                SelectorGuard(_Selector(state, index), state._lock)
            )
            await registration.__aenter__()
            registrations.append(registration)

        async def complete(index: int) -> None:
            if putting:
                assert await queues[index].get.a() == index
            else:
                await queues[index].put.a(index)

        async with anyio.create_task_group() as group, state._lock:
            group.start_soon(complete, 0)
            group.start_soon(complete, 1)
            await anyio.wait_all_tasks_blocked()
            # Both hold their queue locks and wait for the common selection.
            assert state._pending
            assert [list(queue._queue) for queue in queues] == (
                [[], []] if putting else [[0], [1]]
            )
        winner, result = await state.wait.a()
        assert result == (None if putting else winner)
        remaining = [list(queue._queue) for queue in queues]
        if putting:
            assert remaining[winner] == [10 + winner]
            assert remaining[1 - winner] == []
        else:
            assert remaining[winner] == []
            assert remaining[1 - winner] == [1 - winner]
        for registration in reversed(registrations):
            await registration.__aexit__(None, None, None)
        assert all(not queue._getters and not queue._putters for queue in queues)

    anyio.run(main, backend=backend)
