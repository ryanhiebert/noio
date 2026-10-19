from collections.abc import Awaitable
from collections.abc import Callable
from typing import Self
from typing import assert_type

import anyio
import pytest

from noio import routine
from noio._lock import Lock
from noio._selection import _Selection
from noio.selectable import SelectionCollisionError
from noio.selectable import Selector
from noio.selectable import SelectorGuard
from noio.selectable import selectors

from .conftest import AsyncRuntime
from .conftest import RunSync


@routine
async def exercise_selectors(checkpoint: Callable[[], Awaitable[None]]) -> None:
    left = _Selection[str]()
    right = _Selection[int]()
    left_guard = SelectorGuard(left, 2)
    right_guard = SelectorGuard(right, 7)
    async with selectors() as empty:
        assert empty == ()
    async with selectors(left_guard, right_guard) as (a, b):
        assert_type(a, Selector[str] | None)
        assert_type(b, Selector[int] | None)
        assert a is not None and b is not None
        await checkpoint()
        a.result("received")
        b.result(42)
        with pytest.raises(RuntimeError, match="no longer pending"):
            a.result("again")
    with pytest.raises(RuntimeError, match="outside"):
        a.result("escaped")
    with pytest.raises(RuntimeError, match="outside"):
        b.error(ValueError())
    assert await left.wait.a() == (2, "received")
    assert await right.wait.a() == (7, 42)
    async with selectors(SelectorGuard(left, 9), right_guard) as pair:
        assert pair == (None, None)
    # Closing a completed selection preserves its outcome.
    await left.invalidate.a()
    assert await left.wait.a() == (2, "received")

    pending = _Selection[None]()
    async with selectors(left_guard, SelectorGuard(pending, 0)) as (a, c):
        assert a is None and c is not None
    # Exiting without completion does not retire the choice.
    async with selectors(SelectorGuard(pending, 1)) as (c,):
        assert c is not None
        c.result(None)
    assert await pending.wait.a() == (1, None)

    error = ValueError("delivered")
    failed = _Selection[str]()
    value = _Selection[Exception]()
    async with selectors(SelectorGuard(failed, 0), SelectorGuard(value, 0)) as (a, d):
        assert a is not None and d is not None
        a.error(error)
        d.result(error)
    with pytest.raises(ValueError) as caught:
        await failed.wait.a()
    assert caught.value is error
    assert (await value.wait.a())[1] is error

    inactive = _Selection[str]()
    await inactive.invalidate.a()
    await inactive.invalidate.a()
    async with selectors(SelectorGuard(inactive, 0)) as (a,):
        assert a is None

    for state in (inactive, _Selection[str]()):
        # Both distinct guards and a repeated identical guard must fail.
        guard = SelectorGuard(state, 0)
        for other in (guard, SelectorGuard(state, 1)):
            with pytest.raises(SelectionCollisionError, match="collide"):
                async with selectors(guard, other):
                    pytest.fail("colliding guards entered")
        async with state._lock:
            pass

    # A faulty selectable can complete one participant before raising. No rollback.
    first, second = _Selection[int](), _Selection[int]()
    y: Selector[int] | None = None
    with pytest.raises(ValueError, match="selectable bug"):
        async with selectors(SelectorGuard(first, 0), SelectorGuard(second, 0)) as (
            x,
            y,
        ):
            assert x is not None and y is not None
            x.result(1)
            raise ValueError("selectable bug")
    assert y is not None
    with pytest.raises(RuntimeError, match="outside"):
        y.result(2)
    assert await first.wait.a() == (0, 1)
    async with selectors(SelectorGuard(second, 0)) as (y,):
        assert y is not None
        y.result(2)
    assert await second.wait.a() == (0, 2)


def _check_sync(checkpoint: Callable[[], None]) -> None:
    async def green_checkpoint() -> None:
        checkpoint()

    exercise_selectors(green_checkpoint)
    state = _Selection[int]()
    with selectors(SelectorGuard(state, 0)) as (selector,):
        assert selector is not None
        selector.result(1)
    assert state.wait() == (0, 1)


def test_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_async(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        await exercise_selectors.a(async_runtime.checkpoint)

    async_runtime.run(main)


def test_competing_matches_in_opposite_order(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        paused, resume = async_runtime.event(), async_runtime.event()
        owner_waiting = False
        completed: list[str] = []

        class InterleavedLock(Lock):
            async def __aenter__(self) -> Self:
                nonlocal owner_waiting
                if owner_waiting:
                    owner_waiting = False
                    assert self is first_lock
                    resume.set()
                elif self is second_lock and not completed:
                    owner_waiting = True
                    paused.set()
                    await resume.wait()
                return await super().__aenter__()

        first_lock, second_lock = sorted([InterleavedLock(), InterleavedLock()], key=id)
        first, second = _Selection[int](), _Selection[int]()
        first._lock, second._lock = first_lock, second_lock

        async def owner() -> None:
            async with selectors(SelectorGuard(first, 0), SelectorGuard(second, 0)) as (
                a,
                b,
            ):
                assert a is not None and b is not None
                a.result(1)
                b.result(2)
                completed.append("owner")

        async def contender() -> None:
            await paused.wait()
            async with selectors(
                SelectorGuard(second, 1), SelectorGuard(first, 1)
            ) as pair:
                assert pair == (None, None)
                completed.append("contender")

        await async_runtime.concurrently(owner, contender)
        assert completed == ["owner", "contender"]
        assert await first.wait.a() == (0, 1)
        assert await second.wait.a() == (0, 2)

    async_runtime.run(main)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancellation_during_partial_acquisition(backend: str) -> None:
    async def main() -> None:
        acquisitions: list[Lock] = []

        class RecordedLock(Lock):
            async def __aenter__(self) -> Self:
                await super().__aenter__()
                acquisitions.append(self)
                return self

        first, second = _Selection[int](), _Selection[int]()
        first._lock, second._lock = RecordedLock(), RecordedLock()
        first, second = sorted([first, second], key=lambda state: id(state._lock))
        finished = anyio.Event()
        scope = anyio.CancelScope()

        async def contender() -> None:
            with scope:
                async with selectors(SelectorGuard(second, 0), SelectorGuard(first, 0)):
                    pytest.fail("second lock is held")
            finished.set()

        async with anyio.create_task_group() as group, second._lock:
            acquisitions.clear()
            group.start_soon(contender)
            await anyio.wait_all_tasks_blocked()
            assert acquisitions == [first._lock]
            scope.cancel()
            await finished.wait()
            # Partial acquisition released the first lock.
            async with first._lock:
                pass
        async with selectors(SelectorGuard(first, 0), SelectorGuard(second, 0)) as (
            a,
            b,
        ):
            assert a is not None and b is not None

        with anyio.CancelScope() as scope:
            async with selectors(SelectorGuard(first, 0)) as (a,):
                assert a is not None
                scope.cancel()
                await anyio.lowlevel.checkpoint()
        assert a is not None
        with pytest.raises(RuntimeError, match="outside"):
            a.result(1)
        # The guard context is not the selection lifecycle owner.
        await first.invalidate.a()
        async with selectors(SelectorGuard(first, 0)) as (a,):
            assert a is None

    anyio.run(main, backend=backend)


def test_many_guards_preserve_argument_order() -> None:
    states = [_Selection[int]() for _ in range(5)]
    states[2].invalidate()
    guards = [SelectorGuard(state, index) for index, state in enumerate(states)]
    with selectors(*reversed(guards)) as acquired:
        assert len(acquired) == 5
        for index, selector in zip(reversed(range(5)), acquired, strict=True):
            if index == 2:
                assert selector is None
            else:
                assert selector is not None
                selector.result(index + 10)
    for index, state in enumerate(states):
        if index != 2:
            assert state.wait() == (index, index + 10)


def _check_green_cancellation(checkpoint: Callable[[], None]) -> None:
    import gevent

    first, second = _Selection[int](), _Selection[int]()
    first, second = sorted([first, second], key=lambda state: id(state._lock))
    leaked: list[Selector[int]] = []

    def partial() -> None:
        with selectors(SelectorGuard(second, 0), SelectorGuard(first, 0)):
            pytest.fail("second lock is held")

    with second._lock:
        task = gevent.spawn(partial)
        checkpoint()
        assert not task.dead
        task.kill()
        with first._lock:
            pass

    def holding() -> None:
        with selectors(SelectorGuard(first, 0), SelectorGuard(second, 0)) as (a, b):
            assert a is not None and b is not None
            leaked.extend((a, b))
            gevent.sleep(10)

    task = gevent.spawn(holding)
    checkpoint()
    task.kill()
    assert len(leaked) == 2
    for selector in leaked:
        with pytest.raises(RuntimeError, match="outside"):
            selector.result(1)
    with selectors(SelectorGuard(first, 1), SelectorGuard(second, 1)) as (a, b):
        assert a is not None and b is not None
        a.result(2)
        b.result(3)
    assert first.wait() == (1, 2)
    assert second.wait() == (1, 3)


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_green_cancellation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_green_cancellation)
