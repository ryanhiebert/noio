from collections.abc import AsyncIterator
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent
from typing import Self

import anyio
import pytest

from noio import ContextManager
from noio import Event
from noio import contextmanager
from noio import routine
from noio._lock import Lock
from noio._selection import _Selection
from noio.select import select
from noio.selectable import SelectorGuard
from noio.selectable import selectable

from .conftest import AsyncRuntime
from .conftest import RunSync


def _check_sync(checkpoint: Callable[[], None]) -> None:
    event = Event()
    prepared = event.wait.select()
    assert not event._waiters
    assert event.set() is None
    assert event.set() is None
    assert event.wait() is None
    assert select(prepared, prepared) == (0, None)
    assert select(prepared) == (0, None)
    assert not event._waiters

    @routine
    async def nested() -> None:
        await event.set.a()
        await event.wait.a()
        with pytest.raises(RuntimeError, match="Use await"):
            event.set()
        with pytest.raises(RuntimeError, match="Use await"):
            event.wait()

    nested()
    checkpoint()


def test_sync_calls(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_async_calls(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        event = Event()
        assert await event.set.a() is None
        assert await event.set.a() is None
        assert await event.wait.a() is None
        prepared = event.wait.select()
        assert await select.a(prepared, prepared) == (0, None)
        assert await select.a(prepared) == (0, None)
        assert not event._waiters

    async_runtime.run(main)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_broadcast_and_duplicate_registrations(backend: str) -> None:
    async def main() -> None:
        event = Event()
        results: list[object] = []
        prepared = event.wait.select()

        async def direct() -> None:
            results.append(await event.wait.a())

        async def selected() -> None:
            results.append(await select.a(prepared, prepared))

        async with anyio.create_task_group() as group:
            group.start_soon(direct)
            group.start_soon(selected)
            group.start_soon(selected)
            await anyio.wait_all_tasks_blocked()
            assert len(event._waiters) == 5
            assert not results
            await event.set.a()
            await event.set.a()
        assert results.count(None) == 1
        assert results.count((0, None)) == 2
        assert not event._waiters
        assert await select.a(prepared) == (0, None)

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_losing_and_cancelled_waiters_are_removed(backend: str) -> None:
    async def main() -> None:
        pending, ready = Event(), Event()
        await ready.set.a()
        assert await select.a(pending.wait.select(), ready.wait.select()) == (1, None)
        assert not pending._waiters
        # Registration still runs after a winner, but retains no inactive guard.
        assert await select.a(ready.wait.select(), pending.wait.select()) == (0, None)
        assert not pending._waiters
        scope = anyio.CancelScope()
        cancelled = anyio.Event()
        completed = anyio.Event()

        async def cancel_me() -> None:
            with scope:
                await pending.wait.a()
                pytest.fail("cancelled wait completed")
            cancelled.set()

        async def survivor() -> None:
            await pending.wait.a()
            completed.set()

        async with anyio.create_task_group() as group:
            group.start_soon(cancel_me)
            group.start_soon(survivor)
            await anyio.wait_all_tasks_blocked()
            assert len(pending._waiters) == 2
            scope.cancel()
            await cancelled.wait()
            assert len(pending._waiters) == 1
            assert not completed.is_set()
            await pending.set.a()
        assert completed.is_set()
        assert not pending._waiters

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_set_finishes_despite_cancellation_before_event_lock(backend: str) -> None:
    async def main() -> None:
        event = Event()
        scope = anyio.CancelScope()
        finished = anyio.Event()

        async def setter() -> None:
            with scope:
                await event.set.a()
                await anyio.lowlevel.checkpoint()
            finished.set()

        async with anyio.create_task_group() as group, event._lock:
            group.start_soon(setter)
            await anyio.wait_all_tasks_blocked()
            scope.cancel()
            await anyio.wait_all_tasks_blocked()
            assert not finished.is_set()
            assert not event._is_set
        assert scope.cancelled_caught
        assert finished.is_set()
        assert event._is_set
        await event.wait.a()

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("blocked_on", ["event", "selection"])
def test_cancel_partial_registration(backend: str, blocked_on: str) -> None:
    async def main() -> None:
        event = Event()
        state = _Selection[None]()
        scope = anyio.CancelScope()
        lock = event._lock if blocked_on == "event" else state._lock

        async def register() -> None:
            with scope:
                await (
                    event.wait.select().__select__(SelectorGuard(state, 0)).__aenter__()
                )
                pytest.fail("cancelled registration completed")

        async with anyio.create_task_group() as group, lock:
            group.start_soon(register)
            await anyio.wait_all_tasks_blocked()
            scope.cancel()
        assert scope.cancelled_caught
        assert not event._waiters
        await event.set.a()
        await event.wait.a()

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_registration_holds_event_lock_until_visible_to_set(backend: str) -> None:
    async def main() -> None:
        event = Event()
        state = _Selection[None]()
        cleanups: list[ContextManager[None]] = []
        finished = anyio.Event()

        async def register() -> None:
            cleanup = event.wait.select().__select__(SelectorGuard(state, 0))
            await cleanup.__aenter__()
            cleanups.append(cleanup)

        async def setter() -> None:
            await event.set.a()
            finished.set()

        async with anyio.create_task_group() as group, state._lock:
            group.start_soon(register)
            await anyio.wait_all_tasks_blocked()
            # Registration owns the Event lock but cannot yet store its guard.
            assert not event._waiters
            group.start_soon(setter)
            await anyio.wait_all_tasks_blocked()
            assert not finished.is_set()
            assert not event._is_set
        assert await state.wait.a() == (0, None)
        await cleanups[0].__aexit__(None, None, None)
        assert not event._waiters

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_partial_notification_survives_cancellation_and_competing_operations(
    backend: str,
) -> None:
    async def main() -> None:
        event = Event()
        first, second = _Selection[None](), _Selection[None]()
        registrations = []
        for state in (first, second):
            registration = event.wait.select().__select__(SelectorGuard(state, 0))
            await registration.__aenter__()
            registrations.append(registration)
        scope = anyio.CancelScope()
        finished: list[str] = []

        async def setter() -> None:
            with scope:
                await event.set.a()
            finished.append("setter")

        async def another_setter() -> None:
            await event.set.a()
            finished.append("another setter")

        async def late_waiter() -> None:
            await event.wait.a()
            finished.append("late waiter")

        async with anyio.create_task_group() as group, second._lock:
            group.start_soon(setter)
            await anyio.wait_all_tasks_blocked()
            assert event._is_set
            assert not first._pending
            assert second._pending
            scope.cancel()
            group.start_soon(another_setter)
            group.start_soon(late_waiter)
            await anyio.wait_all_tasks_blocked()
            assert not finished
            # Release the blocked selection so shielded notification can finish.
        assert set(finished) == {"setter", "another setter", "late waiter"}
        assert await first.wait.a() == (0, None)
        assert await second.wait.a() == (0, None)
        for registration in reversed(registrations):
            await registration.__aexit__(None, None, None)
        assert not event._waiters

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancelled_cleanup_waits_for_event_lock(backend: str) -> None:
    async def main() -> None:
        event = Event()
        scope = anyio.CancelScope()
        finished = anyio.Event()

        async def waiter() -> None:
            with scope:
                await event.wait.a()
            finished.set()

        async with anyio.create_task_group() as group:
            group.start_soon(waiter)
            await anyio.wait_all_tasks_blocked()
            assert len(event._waiters) == 1
            async with event._lock:
                scope.cancel()
                await anyio.wait_all_tasks_blocked()
                assert not finished.is_set()
                assert len(event._waiters) == 1
            await finished.wait()
        assert not event._waiters
        await event.set.a()

    anyio.run(main, backend=backend)


def _check_green_notification(checkpoint: Callable[[], None]) -> None:
    import gevent
    from gevent.event import Event as GreenEvent

    event = Event()
    state = _Selection[None]()
    cleanup = event.wait.select().__select__(SelectorGuard(state, 0))
    cleanup.__enter__()
    attempted = GreenEvent()

    class ObservedLock(Lock):
        async def __aenter__(self) -> Self:
            attempted.set()
            return await super().__aenter__()

    state._lock = ObservedLock()
    with state._lock:
        setter = gevent.spawn(event.set)
        assert attempted.wait(timeout=2)
        assert event._is_set
        assert state._pending
        setter.kill(block=False)
        checkpoint()
        setter.kill(block=False)
        checkpoint()
        assert not setter.dead
    setter.get(timeout=2)
    assert state.wait() == (0, None)
    cleanup.__exit__(None, None, None)
    assert not event._waiters
    event.wait()


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_green_notification_survives_cancellation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_green_notification)


def test_same_event_and_prepared_wait_across_threads_asyncio_and_trio() -> None:
    import asyncio

    import trio

    event = Event()
    prepared = event.wait.select()
    thread_ready, trio_ready, asyncio_ready = (
        ThreadEvent(),
        ThreadEvent(),
        ThreadEvent(),
    )

    @selectable
    @contextmanager
    async def observed(
        guard: SelectorGuard[None], ready: ThreadEvent
    ) -> AsyncIterator[None]:
        async with prepared.__select__(guard):
            ready.set()
            yield

    async def trio_main() -> tuple[int, None]:
        with trio.fail_after(5):
            return await select.a(observed.select(trio_ready))

    async def asyncio_main() -> None:
        async def signal() -> None:
            while not all(
                flag.is_set() for flag in (thread_ready, trio_ready, asyncio_ready)
            ):
                await asyncio.sleep(0)
            assert len(event._waiters) == 3
            await event.set.a()

        async with asyncio.timeout(5):
            result, _ = await asyncio.gather(
                select.a(observed.select(asyncio_ready)), signal()
            )
        assert result == (0, None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        thread_result = pool.submit(select, observed.select(thread_ready))
        trio_result = pool.submit(trio.run, trio_main)
        try:
            asyncio.run(asyncio_main())
        finally:
            event.set()
        assert thread_result.result(timeout=5) == (0, None)
        assert trio_result.result(timeout=5) == (0, None)
    assert not event._waiters
    assert select(prepared) == (0, None)


def _check_cross_runtime_wakeup(checkpoint: Callable[[], None]) -> None:
    event = Event()
    registered = ThreadEvent()

    @selectable
    @contextmanager
    async def observed(guard: SelectorGuard[None]) -> AsyncIterator[None]:
        async with event.wait.select().__select__(guard):
            registered.set()
            yield

    def setter() -> None:
        assert registered.wait(2)
        event.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(setter)
        assert select(observed.select()) == (0, None)
        future.result(timeout=2)
    assert not event._waiters
    checkpoint()


def test_cross_runtime_wakeup(sync_runtime: RunSync) -> None:
    sync_runtime(_check_cross_runtime_wakeup)


@pytest.mark.parametrize("operation", ["set", "set-lock", "cleanup"])
def test_repeated_asyncio_cancellation_during_shielded_work(operation: str) -> None:
    import asyncio

    async def main() -> None:
        event = Event()
        attempted = asyncio.Event()

        class ObservedLock(Lock):
            async def __aenter__(self) -> Self:
                attempted.set()
                return await super().__aenter__()

        state = _Selection[None]()
        cleanup = event.wait.select().__select__(SelectorGuard(state, 0))
        await cleanup.__aenter__()
        lock = ObservedLock()
        if operation == "set":
            state._lock = lock
        else:
            event._lock = lock

        async def worker() -> None:
            if operation != "cleanup":
                await event.set.a()
            else:
                await cleanup.__aexit__(None, None, None)

        async with lock:
            attempted.clear()
            task = asyncio.create_task(worker())
            await attempted.wait()
            task.cancel("first")
            await asyncio.sleep(0)
            task.cancel("second")
            await asyncio.sleep(0)
            assert not task.done()
            assert event._waiters
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not event._waiters
        if operation != "cleanup":
            await cleanup.__aexit__(None, None, None)
            assert await state.wait.a() == (0, None)
        else:
            assert not event._is_set
            await state.invalidate.a()

    asyncio.run(main())


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_two_events_compete_for_one_selection(backend: str) -> None:
    async def main() -> None:
        first, second = Event(), Event()
        state = _Selection[None]()
        registrations = []
        for index, event in enumerate((first, second)):
            registration = event.wait.select().__select__(SelectorGuard(state, index))
            await registration.__aenter__()
            registrations.append(registration)
        async with anyio.create_task_group() as group, state._lock:
            group.start_soon(first.set.a)
            group.start_soon(second.set.a)
            await anyio.wait_all_tasks_blocked()
            # Both setters own their Event lock and are blocked on the same
            # selection. Releasing it must allow exactly one to commit.
            assert first._is_set and second._is_set
            assert state._pending
        assert await state.wait.a() in ((0, None), (1, None))
        for registration in reversed(registrations):
            await registration.__aexit__(None, None, None)
        assert not first._waiters and not second._waiters
        await first.wait.a()
        await second.wait.a()

    anyio.run(main, backend=backend)
