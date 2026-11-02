from collections.abc import Callable
from contextvars import ContextVar
from math import inf

import pytest

from noio import CancelledError
from noio import Event
from noio import cancel
from noio import cleared_mode
from noio import routine
from noio import sleep
from noio._shield import shield
from noio._task_group import _open_task_group
from noio.select import select

from .conftest import AsyncRuntime
from .conftest import RunSync


@routine
async def exercise_timeout() -> None:
    ready = Event()
    await ready.set.a()
    async with cancel(timeout=1):
        await ready.wait.a()
        await sleep.a(0)
    async with cancel(timeout=inf):
        await ready.wait.a()
    # Expiry is sticky, wins against ready IO, and creates fresh exceptions.
    errors: list[CancelledError] = []
    async with cancel(timeout=0):
        for _ in range(2):
            with pytest.raises(CancelledError) as raised:
                await ready.wait.a()
            errors.append(raised.value)
        with pytest.raises(CancelledError):
            await select.a(sleep.select(inf), ready.wait.select())
    assert errors[0] is not errors[1]
    assert errors[0]._scope is errors[1]._scope
    assert not ready._waiters
    await ready.wait.a()
    # Successful exit never checks expiration.
    async with cancel(timeout=0):
        pass
    with pytest.raises(CancelledError):
        async with cancel(timeout=0.01):
            await sleep.a(inf)
    pending = Event()
    with pytest.raises(CancelledError):
        async with cancel(timeout=0.01):
            await pending.wait.a()
    assert not pending._waiters
    with pytest.raises(CancelledError):
        async with cancel(timeout=0), cancel(timeout=10):
            await ready.wait.a()
    # An inner deadline ending does not poison its still-active outer scope.
    async with cancel(timeout=10):
        with pytest.raises(CancelledError):
            async with cancel(timeout=0):
                await ready.wait.a()
        await ready.wait.a()
    for seconds in (-1, float("nan")):
        with pytest.raises(ValueError):
            async with cancel(timeout=seconds):
                pytest.fail("invalid timeout entered")
    error = ValueError("body")
    with pytest.raises(ValueError) as raised:
        async with cancel(timeout=10):
            raise error
    assert raised.value is error


def _check_timeout(checkpoint: Callable[[], None]) -> None:
    exercise_timeout()
    checkpoint()


def test_ordinary_timeout(sync_runtime: RunSync) -> None:
    sync_runtime(_check_timeout)


def test_async_timeout(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_timeout.a)


@routine
async def exercise_shield() -> None:
    @shield
    @routine
    async def protected() -> None:
        await sleep.a(0)
        # Shielding excludes outer deadlines, not locally introduced ones.
        with pytest.raises(CancelledError):
            async with cancel(timeout=0):
                await sleep.a(0)
        await sleep.a(0)

    async with cancel(timeout=0):
        await protected.a()
        with pytest.raises(CancelledError):
            await sleep.a(0)


def _check_shield(checkpoint: Callable[[], None]) -> None:
    exercise_shield()
    with cancel(timeout=0):
        shield(lambda: sleep(0))()
        with pytest.raises(CancelledError):
            sleep(0)
    checkpoint()


def test_ordinary_shield(sync_runtime: RunSync) -> None:
    sync_runtime(_check_shield)


def test_async_shield(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_shield.a)


@routine
async def exercise_group_inheritance() -> None:
    marker = ContextVar("spawn context", default="before")
    seen: list[str] = []

    @routine
    async def child() -> None:
        seen.append(marker.get())
        await sleep.a(0)

    # The timeout around spawn does not become the child's deadline.
    async with _open_task_group() as group, cancel(timeout=0):
        marker.set("spawn")
        await group.spawn.a(child)
    assert seen == ["spawn"]
    seen.clear()
    # A group opened within a timeout passes it to every child.
    with pytest.raises(BaseExceptionGroup) as raised:
        async with cancel(timeout=0), _open_task_group() as group:
            await group.spawn.a(child)
            await group.spawn.a(child)
    assert len(raised.value.exceptions) == 2
    assert all(isinstance(error, CancelledError) for error in raised.value.exceptions)
    assert raised.value.exceptions[0] is not raised.value.exceptions[1]
    assert seen == ["spawn", "spawn"]


def _check_inheritance(checkpoint: Callable[[], None]) -> None:
    exercise_group_inheritance()
    checkpoint()


def test_ordinary_group_inheritance(sync_runtime: RunSync) -> None:
    sync_runtime(_check_inheritance)


def test_async_group_inheritance(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_group_inheritance.a)


def test_runtime_clock_and_shared_deadline() -> None:
    import trio
    from trio.testing import MockClock

    async def main() -> None:
        start = trio.current_time()
        async with cancel(timeout=10):
            await sleep.a(4)
            with pytest.raises(CancelledError):
                await sleep.a(7)
            assert trio.current_time() == start + 10
            with pytest.raises(CancelledError):
                await sleep.a(0)
        # A deadline that passed without giving the worker a turn still wins.
        async with cancel(timeout=1):
            clock.jump(2)
            with pytest.raises(CancelledError):
                await sleep.a(0)

    clock = MockClock(autojump_threshold=0)
    trio.run(main, clock=clock)


def test_native_task_context_copy_limitation(async_runtime: AsyncRuntime) -> None:
    async def child() -> None:
        # Native launchers can copy a scope without owning its lifetime.
        with pytest.raises(CancelledError):
            await sleep.a(0)

    async def main() -> None:
        async with cancel(timeout=0):
            await async_runtime.concurrently(child, child)
            with pytest.raises(CancelledError):
                await sleep.a(0)

    async_runtime.run(main)


def test_runtime_cancellation_cleans_up(async_runtime: AsyncRuntime) -> None:
    event = Event()

    async def worker() -> None:
        async with cancel(timeout=10):
            await event.wait.a()

    async def main() -> None:
        await async_runtime.cancel(worker)

    async_runtime.run(main)
    assert not event._waiters


def test_direct_override_only_outside_timeout(async_runtime: AsyncRuntime) -> None:
    from collections.abc import AsyncIterator

    from noio import contextmanager
    from noio.selectable import SelectorGuard
    from noio.selectable import selectable
    from noio.selectable import selectors

    calls: list[str] = []

    @selectable
    @contextmanager
    async def operation(guard: SelectorGuard[int]) -> AsyncIterator[None]:  # pyright: ignore[reportRedeclaration]
        calls.append("register")
        async with selectors(guard) as (choice,):
            if choice is not None:
                choice.result(42)
        yield
        calls.append("cleanup")

    @operation.call
    @routine
    async def operation() -> int:
        calls.append("direct")
        return 42

    assert operation() == 42
    with cancel(timeout=1):
        assert operation() == 42
    assert calls == ["direct", "register", "cleanup"]
    calls.clear()

    async def main() -> None:
        assert await operation.a() == 42
        async with cancel(timeout=1):
            assert await operation.a() == 42
        assert calls == ["direct", "register", "cleanup"]

    async_runtime.run(main)


def test_timeout_cleanup_waits_for_resource_lock(async_runtime: AsyncRuntime) -> None:
    from noio import contextmanager
    from noio.selectable import SelectorGuard
    from noio.selectable import selectable

    event = Event()
    registered = async_runtime.event()
    release = async_runtime.event()
    exiting = async_runtime.event()

    @selectable
    @contextmanager
    async def observed(guard: SelectorGuard[None]):
        async with event.wait.select().__select__(guard):
            registered.set()
            yield
            exiting.set()

    async def holder() -> None:
        await registered.wait()
        async with event._lock:
            await exiting.wait()
            assert event._waiters
        release.set()

    async def wait() -> None:
        async with cancel(timeout=0.01):
            with pytest.raises(CancelledError):
                await observed.a()
            await release.wait()
            assert not event._waiters

    async def main() -> None:
        await async_runtime.concurrently(wait, holder)

    async_runtime.run(main)


def test_one_timeout_shared_with_owned_thread(async_runtime: AsyncRuntime) -> None:
    errors: list[CancelledError] = []
    event = Event()

    def worker() -> None:
        with pytest.raises(CancelledError) as raised:
            event.wait()
        errors.append(raised.value)

    async def main() -> None:
        async with cancel(timeout=0.02):
            # Ordinary entry deliberately owns threads, even inside a coroutine.
            with _open_task_group() as threads:
                threads.spawn(worker)
                with pytest.raises(CancelledError) as raised:
                    await event.wait.a()
                errors.append(raised.value)
        assert len(errors) == 2
        assert errors[0] is not errors[1]
        assert errors[0]._scope is errors[1]._scope
        assert not event._waiters

    async_runtime.run(main)


@routine
async def exercise_shielded_timeout() -> None:
    async with cancel(timeout=0):
        for seconds in (inf, 10):
            async with cancel(timeout=seconds, shield=True):
                await sleep.a(0)
                with pytest.raises(CancelledError):
                    async with cancel(timeout=0):
                        await sleep.a(0)
                await sleep.a(0)
            with pytest.raises(CancelledError):
                await sleep.a(0)
        with pytest.raises(CancelledError):
            async with cancel(timeout=0, shield=True):
                await sleep.a(0)
        # Expiry of the enclosing timeout is not raised by shield exit.
        async with cancel(timeout=inf, shield=True):
            pass


def _check_shielded_timeout(checkpoint: Callable[[], None]) -> None:
    exercise_shielded_timeout()
    checkpoint()


def test_ordinary_shielded_timeout(sync_runtime: RunSync) -> None:
    sync_runtime(_check_shielded_timeout)


def test_async_shielded_timeout(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_shielded_timeout.a)


def test_shielded_timeout_native_trio() -> None:
    import trio
    from trio.testing import MockClock

    async def main() -> None:
        task = trio.lowlevel.current_task()
        with trio.CancelScope() as outer:
            outer.cancel()
            async with cancel(timeout=1, shield=True):
                assert trio.lowlevel.current_task() is task
                await trio.sleep(0)
                with trio.CancelScope() as inner:
                    inner.cancel()
                    await trio.sleep(0)
                    pytest.fail("inner cancellation was shielded")
                assert inner.cancelled_caught
                with pytest.raises(CancelledError):
                    await sleep.a(inf)
            await trio.sleep(0)
            pytest.fail("outer cancellation was lost")
        assert outer.cancelled_caught

    trio.run(main, clock=MockClock(autojump_threshold=0))


def test_shielded_timeout_native_asyncio_limitation() -> None:
    import asyncio

    async def main() -> None:
        task = asyncio.current_task()
        assert task is not None
        with pytest.raises(asyncio.CancelledError):
            async with cancel(timeout=inf, shield=True):
                asyncio.get_running_loop().call_soon(task.cancel)
                await asyncio.sleep(0)
        task.uncancel()

    asyncio.run(main())


def _check_native_gevent_limitation(checkpoint: Callable[[], None]) -> None:
    from ._runtime import current_runtime

    if current_runtime() != "gevent":
        return
    import gevent

    current = gevent.getcurrent()
    with pytest.raises(gevent.GreenletExit), cancel(timeout=inf, shield=True):
        killer = gevent.spawn(gevent.kill, current)
        try:
            checkpoint()
            checkpoint()
            pytest.fail("native cancellation unexpectedly shielded")
        finally:
            killer.join()


def test_shielded_timeout_native_gevent_limitation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_native_gevent_limitation)


def test_shielded_timeout_anyio_asyncio_limitation() -> None:
    import anyio

    async def main() -> None:
        with anyio.CancelScope() as outer:
            async with cancel(timeout=inf, shield=True):
                outer.cancel()
                await anyio.sleep(0)
                pytest.fail("AnyIO cancellation unexpectedly shielded")
        assert outer.cancelled_caught

    anyio.run(main, backend="asyncio")


def test_shielded_deadline_native_wait_limitation() -> None:
    import trio
    from trio.testing import MockClock

    async def main() -> None:
        start = trio.current_time()
        async with cancel(timeout=1, shield=True):
            await trio.sleep(2)
            assert trio.current_time() == start + 2
            with pytest.raises(CancelledError):
                await sleep.a(0)

    trio.run(main, clock=MockClock(autojump_threshold=0))


@routine
async def exercise_registration_cancellation() -> None:
    from noio import contextmanager
    from noio.selectable import SelectorGuard
    from noio.selectable import selectable

    reached: list[str] = []

    @selectable
    @contextmanager
    async def operation(guard: SelectorGuard[None]):
        reached.append("entered")
        await sleep.a(0)
        reached.append("continued")
        yield

    with pytest.raises(CancelledError):
        async with cancel(timeout=0):
            await operation.a()
    assert reached == ["entered"]


def _check_registration_cancellation(checkpoint: Callable[[], None]) -> None:
    exercise_registration_cancellation()
    checkpoint()


def test_registration_cancellation_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_registration_cancellation)


def test_registration_cancellation_async(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_registration_cancellation.a)


@pytest.mark.parametrize("protected", [False, True])
def test_committed_result_cleanup_cancellation(protected: bool) -> None:
    import trio
    from trio.testing import MockClock

    from noio import contextmanager
    from noio.selectable import SelectorGuard
    from noio.selectable import selectable
    from noio.selectable import selectors

    clock = MockClock(autojump_threshold=0)
    values = [42]
    finished: list[bool] = []

    @selectable
    @contextmanager
    async def receive(guard: SelectorGuard[int]):
        async with selectors(guard) as (choice,):
            assert choice is not None
            choice.result(values.pop())
        yield
        clock.jump(2)
        async with cancel(timeout=inf, shield=protected):
            await sleep.a(0)
            finished.append(True)

    async def main() -> None:
        async with cancel(timeout=1):
            if protected:
                assert await receive.a() == 42
            else:
                with pytest.raises(CancelledError):
                    await receive.a()
        assert values == []  # Cancellation cannot roll back the committed receive.
        assert finished == ([True] if protected else [])

    trio.run(main, clock=clock)


@routine
async def exercise_cancellation_exception() -> None:
    with pytest.raises(CancelledError):
        async with cancel(timeout=0):
            try:
                await sleep.a(0)
            except Exception:
                pytest.fail("ordinary error handler swallowed cancellation")
    async with cancel():
        await sleep.a(0)


def _check_cancellation_exception(checkpoint: Callable[[], None]) -> None:
    exercise_cancellation_exception()
    checkpoint()


def test_ordinary_cancellation_exception(sync_runtime: RunSync) -> None:
    sync_runtime(_check_cancellation_exception)


def test_async_cancellation_exception(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_cancellation_exception.a)


@routine
async def exercise_explicit_cancellation() -> None:
    ready = Event()
    await ready.set.a()
    async with cancel() as scope:
        assert await scope.cancel.a() is None
        assert await scope.cancel.a() is None
        for _ in range(2):
            with pytest.raises(CancelledError):
                await ready.wait.a()
        async with cancel(shield=True) as inner:
            await ready.wait.a()
            await inner.cancel.a()
            with pytest.raises(CancelledError):
                await ready.wait.a()
    await ready.wait.a()


def _check_explicit_cancellation(checkpoint: Callable[[], None]) -> None:
    exercise_explicit_cancellation()
    checkpoint()


def test_explicit_cancellation_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_explicit_cancellation)


def test_explicit_cancellation_async(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_explicit_cancellation.a)


def test_explicit_cancellation_from_thread(async_runtime: AsyncRuntime) -> None:
    from collections.abc import AsyncIterator

    from noio import contextmanager
    from noio.selectable import SelectorGuard
    from noio.selectable import selectable

    async def main() -> None:
        pending = Event()
        async with cancel() as scope:

            @selectable
            @contextmanager
            async def trigger(guard: SelectorGuard[None]) -> AsyncIterator[None]:
                # The first choice and the cancellation event are registered.
                assert pending._waiters

                @cleared_mode
                def cancel_from_thread() -> None:
                    with _open_task_group() as threads:
                        threads.spawn(scope.cancel)

                cancel_from_thread()
                yield

            with pytest.raises(CancelledError):
                await select.a(pending.wait.select(), trigger.select())
        assert not pending._waiters

    async_runtime.run(main)
