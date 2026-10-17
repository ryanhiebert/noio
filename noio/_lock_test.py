from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack
from contextlib import ExitStack
from threading import Event
from typing import assert_type

import pytest

from noio import routine
from noio._lock import Lock

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
