from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from inspect import unwrap
from threading import Event as ThreadEvent
from typing import assert_type

import pytest

from noio import routine
from noio import shield
from noio._event import Event
from noio.routine import Routine
from noio.routine import _mode

from .conftest import AsyncRuntime
from .conftest import RunSync


def _check_sync(checkpoint: Callable[[], None]) -> None:
    @routine
    async def operation(value: int, *, suffix: str = "!") -> str:
        """An operation with metadata and keyword arguments."""
        checkpoint()
        return f"{value}{suffix}"

    protected = shield(operation)
    assert isinstance(protected, Routine)
    assert protected is not operation
    assert protected.__name__ == operation.__name__
    assert protected.__doc__ == operation.__doc__
    assert unwrap(protected) is unwrap(operation)
    assert signature(protected) == signature(operation)
    assert_type(protected(42, suffix="?"), str)
    assert protected(42, suffix="?") == "42?"

    @routine
    async def nested() -> str:
        result = await protected.a(7)
        assert _mode.get() is not None
        with pytest.raises(RuntimeError, match="Use await"):
            protected(7)
        return result

    assert nested() == "7!"
    assert _mode.get() is None

    class Worker:
        @shield
        @routine
        async def run(self, value: int) -> int:
            checkpoint()
            return value + 1

    worker = Worker()
    assert worker.run(3) == 4
    assert Worker.run(worker, 4) == 5
    assert shield(worker.run)(5) == 6

    @shield
    def ordinary(value: int, *, suffix: str) -> str:
        checkpoint()
        return f"{value}{suffix}"

    assert not isinstance(ordinary, Routine)
    assert_type(ordinary(1, suffix="!"), str)
    assert ordinary(1, suffix="!") == "1!"

    class CallableObject:
        def __call__(self, value: int) -> int:
            return value + 1

    assert shield(CallableObject())(4) == 5

    error = ValueError("failure")

    @shield
    @routine
    async def fails() -> None:
        raise error

    with pytest.raises(ValueError) as caught:
        fails()
    assert caught.value is error
    assert _mode.get() is None


def test_sync_calls(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_async_calls(async_runtime: AsyncRuntime) -> None:
    class Worker:
        @shield
        @routine
        async def run(self, value: int, *, suffix: str) -> str:
            await async_runtime.checkpoint()
            return f"{value}{suffix}"

    @shield
    async def coroutine(value: int) -> int:
        await async_runtime.checkpoint()
        return value + 1

    error = ValueError("failure")

    @shield
    @routine
    async def fails() -> None:
        await async_runtime.checkpoint()
        raise error

    async def main() -> None:
        assert_type(await Worker().run.a(4, suffix="!"), str)
        assert await Worker().run.a(4, suffix="!") == "4!"
        assert not isinstance(coroutine, Routine)
        assert_type(await coroutine(4), int)
        assert await coroutine(4) == 5
        with pytest.raises(ValueError) as caught:
            await fails.a()
        assert caught.value is error
        assert _mode.get() is None

    async_runtime.run(main)


def test_preserves_ordinary_override(async_runtime: AsyncRuntime) -> None:
    calls: list[str] = []

    @routine
    async def operation(value: int) -> str:  # pyright: ignore[reportRedeclaration]
        calls.append("async")
        await async_runtime.checkpoint()
        return str(value)

    @operation.fn
    def operation(value: int | str) -> str:
        calls.append("ordinary")
        return str(value)

    protected = shield(operation)
    assert_type(protected("ordinary-only input"), str)
    assert protected("ordinary-only input") == "ordinary-only input"

    @routine
    async def nested() -> str:
        return await protected.a(2)

    assert nested() == "2"
    assert calls == ["ordinary", "ordinary", "ordinary"]

    async def main() -> None:
        assert_type(await protected.a(3), str)
        assert await nested.a() == "2"

    async_runtime.run(main)
    assert calls == ["ordinary", "ordinary", "ordinary", "async", "async"]


@pytest.mark.parametrize("as_routine", [False, True])
def test_repeated_asyncio_cancellation_finishes_body(as_routine: bool) -> None:
    import asyncio

    async def main() -> None:
        started, release = asyncio.Event(), asyncio.Event()
        finished: list[str] = []

        async def body(value: str) -> str:
            started.set()
            await release.wait()
            finished.append(value)
            return value

        protected = shield(routine(body)).a if as_routine else shield(body)
        task = asyncio.create_task(protected("finished"))
        await started.wait()
        task.cancel("first")
        await asyncio.sleep(0)
        task.cancel("second")
        await asyncio.sleep(0)
        assert not task.done()
        assert not finished
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished == ["finished"]

    asyncio.run(main())


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
@pytest.mark.parametrize("as_routine", [False, True])
def test_cancelled_scope_finishes_body(backend: str, as_routine: bool) -> None:
    import anyio

    async def main() -> None:
        started, release, finished = anyio.Event(), anyio.Event(), anyio.Event()
        scope = anyio.CancelScope()

        async def body() -> None:
            started.set()
            await release.wait()
            finished.set()

        protected = shield(routine(body)).a if as_routine else shield(body)

        async def caller() -> None:
            with scope:
                await protected()
                await anyio.lowlevel.checkpoint()

        async with anyio.create_task_group() as group:
            group.start_soon(caller)
            await started.wait()
            scope.cancel()
            await anyio.wait_all_tasks_blocked()
            assert not finished.is_set()
            release.set()
        assert finished.is_set()
        assert scope.cancelled_caught

    anyio.run(main, backend=backend)


def _check_green_cancellation(checkpoint: Callable[[], None]) -> None:
    import gevent
    from gevent.event import Event as GreenEvent

    def check(as_routine: bool) -> None:
        started, release = GreenEvent(), Event()
        finished: list[str] = []

        def ordinary() -> None:
            started.set()
            release.wait()
            finished.append("ordinary")

        @routine
        async def operation() -> None:
            started.set()
            await release.wait.a()
            finished.append("routine")

        protected = shield(operation) if as_routine else shield(ordinary)
        task = gevent.spawn(protected)
        assert started.wait(timeout=2)
        task.kill(block=False)
        checkpoint()
        task.kill(block=False)
        checkpoint()
        assert not task.dead
        assert not finished
        release.set()
        task.get(timeout=2)
        assert finished == ["routine" if as_routine else "ordinary"]

    check(False)
    check(True)


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_green_cancellation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_green_cancellation)


def test_same_shielded_routine_across_threads_asyncio_and_trio() -> None:
    import asyncio

    import trio

    release = Event()
    thread_started, trio_started, asyncio_started = (
        ThreadEvent(),
        ThreadEvent(),
        ThreadEvent(),
    )

    @shield
    @routine
    async def operation(started: ThreadEvent, value: str) -> str:
        started.set()
        await release.wait.a()
        return value

    async def trio_main() -> str:
        with trio.fail_after(5):
            return await operation.a(trio_started, "trio")

    async def asyncio_main() -> None:
        async def signal() -> None:
            while not all(
                flag.is_set()
                for flag in (thread_started, trio_started, asyncio_started)
            ):
                await asyncio.sleep(0)
            release.set()

        async with asyncio.timeout(5):
            result, _ = await asyncio.gather(
                operation.a(asyncio_started, "asyncio"), signal()
            )
            assert result == "asyncio"

    with ThreadPoolExecutor(max_workers=2) as pool:
        thread_result = pool.submit(operation, thread_started, "thread")
        trio_result = pool.submit(trio.run, trio_main)
        try:
            asyncio.run(asyncio_main())
        finally:
            release.set()
        assert thread_result.result(timeout=5) == "thread"
        assert trio_result.result(timeout=5) == "trio"
