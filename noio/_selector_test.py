from collections.abc import Callable

import pytest

from noio import routine
from noio._lock import Lock
from noio.select import _Selection
from noio.select import _Selector
from noio.selectable import SelectionCollisionError
from noio.selectable import SelectorGuard
from noio.selectable import selectors

from .conftest import AsyncRuntime
from .conftest import RunSync


class Choice:
    def __init__(self) -> None:
        self._pending = True
        self.outcome: int | Exception | None = None

    def pending(self) -> bool:
        return self._pending

    def result(self, value: int, /) -> None:
        self.outcome = value
        self._pending = False

    def error(self, error: Exception, /) -> None:
        self.outcome = error
        self._pending = False


@routine
async def exercise_guards() -> None:
    choice = Choice()
    lock = Lock()
    guard = SelectorGuard(choice, lock)
    assert guard.lock is lock
    async with selectors(guard) as (first,):
        assert first is choice
    async with selectors(guard) as (second,):
        assert second is not None and second is choice
        second.result(42)
    assert choice.outcome == 42
    async with selectors(guard) as (inactive,):
        assert inactive is None

    # Independent selection implementations participate in the same acquisition.
    alternate = Choice()
    state = _Selection[int]()
    async with selectors(
        SelectorGuard(alternate, Lock()),
        SelectorGuard(_Selector(state, 7), state._lock),
    ) as (left, right):
        assert left is not None and right is not None
        left.result(10)
        right.result(20)
    assert alternate.outcome == 10
    assert await state.wait.a() == (7, 20)

    # Shared locks conflict regardless of the choice's implementation or state.
    with pytest.raises(SelectionCollisionError):
        async with selectors(
            SelectorGuard(Choice(), state._lock),
            SelectorGuard(_Selector(state, 8), state._lock),
        ):
            pytest.fail("conflicting choices acquired")
    failed = Choice()
    error = ValueError("choice failed")
    async with selectors(SelectorGuard(failed, Lock())) as (selector,):
        assert selector is not None
        selector.error(error)
    assert failed.outcome is error


def _check_guards(checkpoint: Callable[[], None]) -> None:
    exercise_guards()
    checkpoint()


def test_ordinary_guards(sync_runtime: RunSync) -> None:
    sync_runtime(_check_guards)


def test_async_guards(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(exercise_guards.a)


def test_failed_entry_releases_lock() -> None:
    class BrokenChoice(Choice):
        def pending(self) -> bool:
            raise ValueError("availability failed")

    choice = BrokenChoice()
    lock = Lock()
    with (
        pytest.raises(ValueError, match="availability"),
        selectors(SelectorGuard(choice, lock)),
    ):
        pytest.fail("broken choice acquired")
    with lock:
        pass
