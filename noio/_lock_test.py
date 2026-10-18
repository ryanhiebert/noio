from collections.abc import Awaitable
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack
from contextlib import ExitStack
from threading import Event
from types import TracebackType
from typing import Self
from typing import assert_type

import pytest

from noio import routine
from noio._lock import Lock
from noio._lock import acquire

from .conftest import AsyncRuntime
from .conftest import RunSync


def _check_sync(checkpoint: Callable[[], None]) -> None:
    lock = Lock()
    with lock as entered:
        assert assert_type(entered, Lock) is lock
        with pytest.raises(RuntimeError), lock:
            pass
        checkpoint()
    with pytest.raises(RuntimeError):
        lock.__exit__(None, None, None)

    error = ValueError("body failed")
    with pytest.raises(ValueError) as raised, lock as entered:
        assert assert_type(entered, Lock) is lock
        raise error
    assert raised.value is error

    @routine
    async def inner() -> None:
        async with lock as entered:
            assert entered is lock
            checkpoint()
            with pytest.raises(RuntimeError, match="Use await function.a"), lock:
                pass
            with pytest.raises(RuntimeError, match="Use await function.a"):
                lock.__exit__(None, None, None)

    @routine
    async def outer() -> None:
        await inner.a()
        async with AsyncExitStack() as stack:
            assert await stack.enter_async_context(lock) is lock

    outer()
    with ExitStack() as stack:
        # Pyright does not bind routine descriptors during protocol matching.
        assert stack.enter_context(lock) is lock  # pyright: ignore[reportArgumentType]
        checkpoint()


def test_sync_calls(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_thread_contention_and_ownership() -> None:
    lock = Lock()
    started = Event()
    entered = Event()

    @routine
    async def nested() -> None:
        async with lock:
            entered.set()

    def worker() -> None:
        with pytest.raises(RuntimeError):
            lock.__exit__(None, None, None)
        started.set()
        nested()

    with ThreadPoolExecutor(max_workers=1) as pool:
        with lock:
            future = pool.submit(worker)
            assert started.wait(2)
            assert not entered.is_set()
        future.result(timeout=2)
    assert entered.is_set()
    with lock:
        pass


def _check_green_contention(checkpoint: Callable[[], None]) -> None:
    import gevent

    lock = Lock()
    entered = []

    @routine
    async def contender() -> None:
        async with lock:
            entered.append(True)
            checkpoint()

    with lock:
        task = gevent.spawn(contender)
        checkpoint()
        assert not entered
    task.get(timeout=2)
    assert entered == [True]

    with lock:
        cancelled = gevent.spawn(contender)
        checkpoint()
        cancelled.kill()
    contender()
    assert entered == [True, True]


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_green_contention_and_cancellation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_green_contention)


def test_async_calls(async_runtime: AsyncRuntime) -> None:
    lock = Lock()

    @routine
    async def inner() -> None:
        async with lock as entered:
            assert assert_type(entered, Lock) is lock
            await async_runtime.checkpoint()
            with pytest.raises(RuntimeError, match="Use await function.a"), lock:
                pass
            with pytest.raises(RuntimeError, match="Use await function.a"):
                lock.__exit__(None, None, None)
            with pytest.raises(RuntimeError):
                async with lock:
                    pass

    @routine
    async def outer() -> None:
        await inner.a()

    async def main() -> None:
        # Creating the acquisition coroutine must not acquire the lock.
        call = lock.__aenter__()
        async with lock:
            pass
        assert assert_type(await call, Lock) is lock
        await lock.__aexit__(None, None, None)
        with pytest.raises(RuntimeError):
            await lock.__aexit__(None, None, None)
        await outer.a()
        with pytest.raises(ValueError, match="body failed"):
            async with lock:
                raise ValueError("body failed")
        async with lock:
            pass
        # Ordinary ownership is also valid inside an async function.
        with lock:
            pass

        # A failed ordinary routine must not leak ordinary dispatch into async work.
        @routine
        async def fails() -> None:
            raise ValueError("routine failed")

        with pytest.raises(ValueError):
            fails()
        await outer.a()

    async_runtime.run(main)


def test_async_contention_and_ownership(async_runtime: AsyncRuntime) -> None:
    lock = Lock()

    async def main() -> None:
        held = async_runtime.event()
        attempted = async_runtime.event()
        order = []

        async def owner() -> None:
            async with lock:
                held.set()
                await attempted.wait()
                await async_runtime.checkpoint()
                assert order == []
                order.append("owner")

        @routine
        async def nested() -> None:
            async with lock:
                order.append("waiter")

        async def waiter() -> None:
            await held.wait()
            with pytest.raises(RuntimeError):
                await lock.__aexit__(None, None, None)
            attempted.set()
            await nested.a()

        await async_runtime.concurrently(owner, waiter)
        assert order == ["owner", "waiter"]

    async_runtime.run(main)


def test_cancelled_acquisition_does_not_take_lock(async_runtime: AsyncRuntime) -> None:
    lock = Lock()

    async def waiter() -> None:
        async with lock:
            pytest.fail("cancelled waiter acquired the lock")

    async def main() -> None:
        held = async_runtime.event()
        cancelled = async_runtime.event()

        async def owner() -> None:
            async with lock:
                held.set()
                await cancelled.wait()

        async def cancel_waiter() -> None:
            await held.wait()
            try:
                await async_runtime.cancel(waiter)
            finally:
                cancelled.set()

        await async_runtime.concurrently(owner, cancel_waiter)
        async with lock:
            pass

    async_runtime.run(main)


def test_thread_and_async_handoff(async_runtime: AsyncRuntime) -> None:
    lock = Lock()
    held = Event()
    release = Event()
    attempted = Event()
    acquired = Event()

    def worker() -> None:
        with lock:
            held.set()
            assert release.wait(2)
        assert attempted.wait(2)
        with lock:
            acquired.set()

    async def main() -> None:
        while not held.is_set():
            await async_runtime.checkpoint()

        async def signal() -> None:
            await async_runtime.checkpoint()
            release.set()

        async def waiter() -> None:
            async with lock:
                attempted.set()
                await async_runtime.checkpoint()
                assert not acquired.is_set()

        await async_runtime.concurrently(waiter, signal)
        while not acquired.is_set():
            await async_runtime.checkpoint()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(worker)
        try:
            async_runtime.run(main)
        finally:
            release.set()
            attempted.set()
        future.result(timeout=2)


def test_asyncio_and_trio_share_lock() -> None:
    import asyncio

    import trio
    from trio.testing import wait_all_tasks_blocked

    lock = Lock()
    waiting = Event()
    acquired = Event()

    @routine
    async def waiter() -> None:
        async with lock:
            acquired.set()

    async def trio_main() -> None:
        with trio.fail_after(5):
            async with trio.open_nursery() as nursery:
                nursery.start_soon(waiter.a)
                # Confirm acquisition has suspended before asyncio releases the lock.
                await wait_all_tasks_blocked()
                assert not acquired.is_set()
                waiting.set()

    with ThreadPoolExecutor(max_workers=1) as pool:

        @routine
        async def owner() -> None:
            async with lock:
                future = pool.submit(trio.run, trio_main)
                async with asyncio.timeout(5):
                    while not waiting.is_set():
                        await asyncio.sleep(0)
                assert not acquired.is_set()
            await asyncio.wrap_future(future)
            assert acquired.is_set()
            async with lock:
                pass

        asyncio.run(owner.a())


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancellation_while_waiting_and_holding(backend: str) -> None:
    import anyio

    lock = Lock()

    async def main() -> None:
        started = anyio.Event()
        finished = anyio.Event()
        scope = anyio.CancelScope()

        async def waiter() -> None:
            with scope:
                started.set()
                async with lock:
                    pytest.fail("cancelled waiter acquired the lock")
            finished.set()

        async with anyio.create_task_group() as group:
            async with lock:
                group.start_soon(waiter)
                await started.wait()
                scope.cancel()
                await finished.wait()
            async with lock:
                pass

        with anyio.CancelScope() as owner_scope:
            async with lock:
                owner_scope.cancel()
                await anyio.lowlevel.checkpoint()
        assert owner_scope.cancelled_caught
        async with lock:
            pass

    anyio.run(main, backend=backend)


class RecordedLock(Lock):
    def __init__(self, calls: list[tuple[str, Lock]]) -> None:
        super().__init__()
        self.__calls = calls

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        self.__calls.append(("enter", self))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await super().__aexit__(exc_type, exc_value, traceback)
        self.__calls.append(("exit", self))


@routine
async def exercise_acquire(checkpoint: Callable[[], Awaitable[None]]) -> None:
    calls: list[tuple[str, Lock]] = []
    first, second = sorted([RecordedLock(calls), RecordedLock(calls)], key=id)
    async with acquire(iter([second, first, second])) as value:
        assert assert_type(value, None) is None
        assert calls == [("enter", first), ("enter", second)]
        await checkpoint()
    assert calls == [
        ("enter", first),
        ("enter", second),
        ("exit", second),
        ("exit", first),
    ]
    async with acquire([]):
        pass

    error = ValueError("body")
    with pytest.raises(ValueError) as raised:
        async with acquire([second, first]):
            raise error
    assert raised.value is error
    assert calls[-2:] == [("exit", second), ("exit", first)]

    # Failing on the second lock must release the first, but not the outer lock.
    async with second:
        calls.clear()
        with pytest.raises(RuntimeError):
            async with acquire([second, first]):
                pytest.fail("reacquisition must fail")
        assert calls == [("enter", first), ("exit", first)]
        async with first:
            pass
        with pytest.raises(RuntimeError):
            async with second:
                pass
    async with acquire([first, second]):
        pass


def _check_acquire_sync(checkpoint: Callable[[], None]) -> None:
    async def green_checkpoint() -> None:
        checkpoint()

    exercise_acquire(green_checkpoint)
    locks = [Lock(), Lock()]
    with acquire(locks) as value:
        assert assert_type(value, None) is None
        checkpoint()


def test_acquire_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_acquire_sync)


def test_acquire_async(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        await exercise_acquire.a(async_runtime.checkpoint)

    async_runtime.run(main)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_acquire_cancel_partial_acquisition(backend: str) -> None:
    import anyio

    async def main() -> None:
        acquired = anyio.Event()
        finished = anyio.Event()
        calls: list[tuple[str, Lock]] = []

        class SignallingLock(RecordedLock):
            async def __aenter__(self) -> Self:
                await super().__aenter__()
                acquired.set()
                return self

        first, second = sorted([SignallingLock(calls), SignallingLock(calls)], key=id)
        scope = anyio.CancelScope()

        async def waiter() -> None:
            with scope:
                async with acquire([second, first]):
                    pytest.fail("second lock is held")
            finished.set()

        async with anyio.create_task_group() as group, second:
            acquired = anyio.Event()
            calls.clear()
            group.start_soon(waiter)
            await acquired.wait()
            assert calls == [("enter", first)]
            scope.cancel()
            await finished.wait()
            assert calls == [("enter", first), ("exit", first)]
            async with first:
                pass
        async with acquire([first, second]):
            pass

    anyio.run(main, backend=backend)


def test_acquire_thread_and_async_handoff(async_runtime: AsyncRuntime) -> None:
    locks = [Lock(), Lock()]
    held = Event()
    release = Event()
    calls: list[str] = []

    def worker() -> None:
        with acquire(locks):
            held.set()
            assert release.wait(5)
            calls.append("thread")

    async def main() -> None:
        while not held.is_set():
            await async_runtime.checkpoint()

        async def signal() -> None:
            await async_runtime.checkpoint()
            release.set()

        async def waiter() -> None:
            async with acquire(reversed(locks)):
                calls.append("async")
                await async_runtime.checkpoint()

        await async_runtime.concurrently(waiter, signal)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(worker)
        try:
            async_runtime.run(main)
        finally:
            release.set()
        future.result(timeout=5)
    assert calls == ["thread", "async"]


def test_acquire_competing_orders(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        paused = async_runtime.event()
        resume = async_runtime.event()
        owner_waiting = False
        completed: list[str] = []

        class InterleavedLock(Lock):
            async def __aenter__(self) -> Self:
                nonlocal owner_waiting
                if owner_waiting:
                    owner_waiting = False
                    resume.set()
                    # The contender must wait on the first lock, not take the second.
                    assert self is first
                elif self is second and not completed:
                    owner_waiting = True
                    paused.set()
                    await resume.wait()
                return await super().__aenter__()

        first, second = sorted([InterleavedLock(), InterleavedLock()], key=id)

        async def owner() -> None:
            async with acquire([first, second]):
                completed.append("owner")

        async def contender() -> None:
            await paused.wait()
            async with acquire([second, first]):
                completed.append("contender")

        await async_runtime.concurrently(owner, contender)
        assert completed == ["owner", "contender"]

    async_runtime.run(main)


def _check_acquire_green_cancellation(checkpoint: Callable[[], None]) -> None:
    import gevent

    calls: list[tuple[str, Lock]] = []
    first, second = sorted([RecordedLock(calls), RecordedLock(calls)], key=id)

    def waiter() -> None:
        with acquire([second, first]):
            pytest.fail("second lock is held")

    with second:
        task = gevent.spawn(waiter)
        try:
            checkpoint()
            assert not task.dead
            assert calls == [("enter", first)]
        finally:
            task.kill()
        assert calls == [("enter", first), ("exit", first)]
        with first:
            pass
        with pytest.raises(RuntimeError), second:
            pass
    with acquire([first, second]):
        pass


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_acquire_green_cancellation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_acquire_green_cancellation)
