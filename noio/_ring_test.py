from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import anyio
import pytest

from noio import CancelledError
from noio import Event
from noio import cancel
from noio._ring import Ring
from noio.select import _Selection
from noio.select import _Selector
from noio.select import select
from noio.selectable import SelectorGuard

from .conftest import AsyncRuntime
from .conftest import RunSync


@pytest.mark.parametrize("size", [-1, 0, 1])
def test_invalid_size(size: int) -> None:
    with pytest.raises(ValueError, match="at least two"):
        Ring[int](size)


@pytest.mark.parametrize("size", [2, 3, 5])
def test_ordered_groups(size: int, async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        ring = Ring[int](size)
        for offset in (0, 10):
            states = [_Selection[int]() for _ in range(size)]
            registrations = []
            for index, state in enumerate(states):
                registration = ring.exchange.select(offset + index).__select__(
                    SelectorGuard(_Selector(state, 0), state._lock)
                )
                await registration.__aenter__()
                registrations.append(registration)
                if index < size - 1:
                    assert all(state._pending for state in states)
            for index, state in enumerate(states):
                assert await state.wait.a() == (0, offset + (index - 1) % size)
            for registration in reversed(registrations):
                await registration.__aexit__(None, None, None)
            assert not ring._waiters

    async_runtime.run(main)


def _check_sync(checkpoint: Callable[[], None]) -> None:
    ring = Ring[str](3)
    states = [_Selection[str](), _Selection[str]()]
    registrations = []
    for value, state in zip(("A", "B"), states, strict=True):
        registration = ring.exchange.select(value).__select__(
            SelectorGuard(_Selector(state, 0), state._lock)
        )
        registration.__enter__()
        registrations.append(registration)
    # The same instance crosses from an ordinary/green caller to another thread.
    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(ring.exchange, "C").result(timeout=2) == "B"
    assert states[0].wait() == (0, "C")
    assert states[1].wait() == (0, "A")
    for registration in reversed(registrations):
        registration.__exit__(None, None, None)
    assert not ring._waiters
    checkpoint()


def test_sync_and_cross_runtime(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_alternatives_and_withdrawal(backend: str) -> None:
    async def main() -> None:
        ring = Ring[str](3)
        results = {}
        ready = Event()

        async def alternatives() -> None:
            results["A"] = await select.a(
                ring.exchange.select("A"), ring.exchange.select("unused")
            )

        async def withdrawing() -> None:
            results["withdrawn"] = await select.a(
                ring.exchange.select("withdrawn"), ready.wait.select()
            )

        async def second() -> None:
            results["B"] = await ring.exchange.a("B")

        async with anyio.create_task_group() as group:
            group.start_soon(alternatives)
            await anyio.wait_all_tasks_blocked()
            group.start_soon(withdrawing)
            await anyio.wait_all_tasks_blocked()
            assert len(ring._waiters) == 3
            assert not results  # Duplicate choices cannot fill the group.
            await ready.set.a()
            await anyio.wait_all_tasks_blocked()
            assert results == {"withdrawn": (1, None)}
            group.start_soon(second)
            await anyio.wait_all_tasks_blocked()
            assert await ring.exchange.a("C") == "B"
        assert results == {"withdrawn": (1, None), "A": (0, "C"), "B": "A"}
        assert not ring._waiters
        # Registration after a winner cannot contribute to a later group.
        assert await select.a(ready.wait.select(), ring.exchange.select("late")) == (
            0,
            None,
        )
        assert not ring._waiters
        async with cancel() as scope:
            await scope.cancel.a()
            with pytest.raises(CancelledError):
                await ring.exchange.a("cancelled")
        assert not ring._waiters

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_competing_groups_and_cancelled_partial_acquisition(backend: str) -> None:
    async def main() -> None:
        rings = [Ring[str](3), Ring[str](3)]
        states = sorted(
            [_Selection[str](), _Selection[str]()], key=lambda s: id(s._lock)
        )
        registrations = []
        for ring_index, ring in enumerate(rings):
            for value, state in zip(("A", "B"), states, strict=True):
                registration = ring.exchange.select(value).__select__(
                    SelectorGuard(_Selector(state, ring_index), state._lock)
                )
                await registration.__aenter__()
                registrations.append(registration)
        interrupted = anyio.CancelScope()
        finished = anyio.Event()
        results = []

        async def attempt() -> None:
            with interrupted:
                await rings[0].exchange.a("cancelled")
                pytest.fail("partial group completed")
            finished.set()

        async def competitor() -> None:
            results.append(await rings[1].exchange.a("C"))

        async with anyio.create_task_group() as group, states[1]._lock:
            group.start_soon(attempt)
            await anyio.wait_all_tasks_blocked()
            assert len(rings[0]._waiters) == 3
            group.start_soon(competitor)
            await anyio.wait_all_tasks_blocked()
            assert len(rings[1]._waiters) == 3
            assert all(state._pending for state in states)
            interrupted.cancel()
            await finished.wait()
            assert len(rings[0]._waiters) == 2
            assert all(state._pending for state in states)
            # The competing ring can finish once the final selection is unlocked.
        assert results == ["B"]
        assert await states[0].wait.a() == (1, "C")
        assert await states[1].wait.a() == (1, "A")
        # Stale losing registrations must be skipped without partial completion.
        async with cancel(timeout=0):
            with pytest.raises(CancelledError):
                await rings[0].exchange.a("late")
        for registration in reversed(registrations):
            await registration.__aexit__(None, None, None)
        assert all(not ring._waiters for ring in rings)

    anyio.run(main, backend=backend)


def test_same_ring_threads_asyncio_trio() -> None:
    import asyncio

    import trio

    ring = Ring[str](3)

    async def asyncio_main() -> str:
        return await ring.exchange.a("asyncio")

    async def trio_main() -> str:
        return await ring.exchange.a("trio")

    with ThreadPoolExecutor(max_workers=2) as pool:
        ordinary = pool.submit(ring.exchange, "thread")
        trio_result = pool.submit(trio.run, trio_main)
        result = asyncio.run(asyncio_main())
        assert {ordinary.result(timeout=2), trio_result.result(timeout=2), result} == {
            "thread",
            "asyncio",
            "trio",
        }
    assert not ring._waiters


def test_inactive_registrations_are_skipped() -> None:
    ring = Ring[str](3)
    stale = _Selection[str]()
    live = [_Selection[str](), _Selection[str](), _Selection[str]()]
    registrations = []
    for state, index, value in [
        (stale, 0, "stale"),
        (stale, 1, "stale alternative"),
        (live[0], 0, "A"),
    ]:
        registration = ring.exchange.select(value).__select__(
            SelectorGuard(_Selector(state, index), state._lock)
        )
        registration.__enter__()
        registrations.append(registration)
    stale.invalidate()
    for state, value in zip(live[1:], ("B", "C"), strict=True):
        registration = ring.exchange.select(value).__select__(
            SelectorGuard(_Selector(state, 0), state._lock)
        )
        registration.__enter__()
        registrations.append(registration)
    assert [state.wait() for state in live] == [(0, "C"), (0, "A"), (0, "B")]
    for registration in reversed(registrations):
        registration.__exit__(None, None, None)
    assert not ring._waiters


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancellation_withdraws_waiting_contribution(backend: str) -> None:
    async def main() -> None:
        ring = Ring[str](3)
        async with cancel() as scope:

            async def waiter() -> None:
                with pytest.raises(CancelledError):
                    await ring.exchange.a("withdrawn")

            async with anyio.create_task_group() as group:
                group.start_soon(waiter)
                await anyio.wait_all_tasks_blocked()
                assert len(ring._waiters) == 1
                await scope.cancel.a()
        assert not ring._waiters

    anyio.run(main, backend=backend)


def test_exchange_does_not_decorate_functions(
    function_metadata_writes: list[str],
) -> None:
    ring = Ring[str](2)
    states = [_Selection[str](), _Selection[str]()]
    registrations = [
        ring.exchange.select(value).__select__(
            SelectorGuard(_Selector(state, 0), state._lock)
        )
        for value, state in zip("AB", states, strict=True)
    ]
    for registration in registrations:
        registration.__enter__()
    assert [state.wait() for state in states] == [(0, "B"), (0, "A")]
    for registration in reversed(registrations):
        registration.__exit__(None, None, None)
    assert function_metadata_writes == []
