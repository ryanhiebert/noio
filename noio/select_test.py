from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager as sync_contextmanager
from contextvars import ContextVar
from threading import Event as ThreadEvent
from typing import Any
from typing import Literal
from typing import Self
from typing import assert_type

import anyio
import pytest

from noio import cleared_mode
from noio import contextmanager
from noio import routine
from noio._lock import Lock
from noio.routine import Routine
from noio.routine import _mode
from noio.select import Selectable
from noio.select import _Selection
from noio.select import _Selector
from noio.select import select
from noio.selectable import SelectorGuard
from noio.selectable import selectors

from .conftest import AsyncRuntime
from .conftest import RunSync


class OrdinaryChoice:
    def __init__(self, value: str | None, log: list[str], name: str) -> None:
        self.value = value
        self.log = log
        self.name = name
        self.guard: SelectorGuard[str] | None = None

    @sync_contextmanager
    def __select__(self, guard: SelectorGuard[str]) -> Iterator[None]:
        self.guard = guard
        self.log.append(f"register {self.name}")
        if self.value is not None:
            with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(self.value)
        try:
            yield
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        assert self.guard is not None
        with selectors(self.guard) as (selector,):
            assert selector is None
        self.log.append(f"cleanup {self.name}")


class DualChoice:
    def __init__(self, value: str | None, log: list[str], name: str) -> None:
        self.value = value
        self.log = log
        self.name = name
        self.guard: SelectorGuard[str] | None = None

    @contextmanager
    async def __select__(self, guard: SelectorGuard[str]) -> AsyncIterator[None]:
        await self._register(guard)
        try:
            yield
        finally:
            await self.cleanup.a()

    @routine
    async def cleanup(self) -> None:
        await self._cleanup()

    async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
        self.guard = guard
        self.log.append(f"register {self.name}")
        if self.value is not None:
            async with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(self.value)
        return self.cleanup

    async def _cleanup(self) -> None:
        assert self.guard is not None
        async with selectors(self.guard) as (selector,):
            assert selector is None
        self.log.append(f"cleanup {self.name}")


def _check_sync(checkpoint: Callable[[], None]) -> None:
    for choice_type in (OrdinaryChoice, DualChoice):
        log: list[str] = []
        choices = [
            choice_type(value, log, str(i)) for i, value in enumerate(("a", "b"))
        ]
        assert assert_type(select(*choices), tuple[int, str]) == (0, "a")
        assert log == ["register 0", "register 1", "cleanup 1", "cleanup 0"]
        checkpoint()
    with pytest.raises(ValueError):
        select()

    # Ordinary callbacks may acquire/release noio locks and retain caller context.
    marker = ContextVar("selection callback", default="before")
    lock = Lock()

    class OwnedChoice(OrdinaryChoice):
        @sync_contextmanager
        def __select__(self, guard: SelectorGuard[str]) -> Iterator[None]:
            lock.__enter__()
            marker.set("registered")
            with super().__select__(guard):
                yield

        def cleanup(self) -> None:
            assert marker.get() == "registered"
            lock.__exit__(None, None, None)
            marker.set("cleaned")
            super().cleanup()

    assert select(OwnedChoice("value", [], "owned")) == (0, "value")
    assert marker.get() == "cleaned"


def test_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_async_registration_and_cleanup(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        log: list[str] = []
        choices = [DualChoice(value, log, str(i)) for i, value in enumerate(("a", "b"))]
        assert assert_type(await select.a(*choices), tuple[int, str]) == (0, "a")
        assert log == ["register 0", "register 1", "cleanup 1", "cleanup 0"]
        with pytest.raises(ValueError):
            await select.a()
        log.clear()
        with pytest.raises(TypeError, match="requires routine-based context managers"):
            await select.a(  # pyright: ignore[reportCallIssue]
                choices[0],
                OrdinaryChoice("bad", log, "ordinary"),  # pyright: ignore[reportArgumentType]
            )
        assert not log

        marker = ContextVar("async selection callback", default="before")
        lock = Lock()

        class OwnedChoice(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                await lock.__enter__.a()
                marker.set("registered")
                return await super()._register(guard)

            async def _cleanup(self) -> None:
                assert marker.get() == "registered"
                await lock.__exit__.a(None, None, None)
                marker.set("cleaned")
                await super()._cleanup()

        assert await select.a(OwnedChoice("value", [], "owned")) == (0, "value")
        assert marker.get() == "cleaned"

    async_runtime.run(main)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_completion_while_later_registration_is_running(backend: str) -> None:
    async def main() -> None:
        log: list[str] = []
        first = DualChoice(None, log, "first")
        registering, resume = anyio.Event(), anyio.Event()

        class Later(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                registering.set()
                await resume.wait()
                return await super()._register(guard)

        async def complete() -> None:
            await registering.wait()
            assert first.guard is not None
            async with selectors(first.guard) as (selector,):
                assert selector is not None
                selector.result("external")
            assert log == ["register first"]
            resume.set()

        async with anyio.create_task_group() as group:
            group.start_soon(complete)
            assert await select.a(first, Later("too late", log, "later")) == (
                0,
                "external",
            )
        assert log == [
            "register first",
            "register later",
            "cleanup later",
            "cleanup first",
        ]

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cancel_registration_then_attempt_every_cleanup(backend: str) -> None:
    async def main() -> None:
        entered = anyio.Event()
        log: list[str] = []
        guards: list[SelectorGuard[str]] = []
        lock = Lock()
        marker = ContextVar("identity", default=False)
        scope = anyio.CancelScope()

        class First(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                await lock.__enter__.a()
                marker.set(True)
                return await super()._register(guard)

            async def _cleanup(self) -> None:
                assert marker.get()
                await lock.__exit__.a(None, None, None)
                log.append("first cleanup")

        class Second(DualChoice):
            async def _cleanup(self) -> None:
                log.append("second cleanup attempted")
                await anyio.lowlevel.checkpoint()
                pytest.fail("cancelled scope should interrupt unshielded cleanup")

        class Interrupted(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                guards.append(guard)
                entered.set()
                await anyio.sleep_forever()
                raise AssertionError("unreachable")

        async def worker() -> None:
            with scope:
                await select.a(
                    First(None, log, "first"),
                    Second(None, log, "second"),
                    Interrupted(None, log, "interrupted"),
                )
            log.append("cancelled")

        async with anyio.create_task_group() as group:
            group.start_soon(worker)
            await entered.wait()
            scope.cancel()
        assert scope.cancelled_caught
        assert log == [
            "register first",
            "register second",
            "second cleanup attempted",
            "first cleanup",
            "cancelled",
        ]
        async with selectors(*guards) as acquired:
            assert acquired == (None,)
        async with lock:
            pass

    anyio.run(main, backend=backend)


@pytest.mark.parametrize("registration_fails", [True, False])
def test_registration_and_cleanup_failure(
    async_runtime: AsyncRuntime, registration_fails: bool
) -> None:
    async def main() -> None:
        log: list[str] = []
        guards: list[SelectorGuard[str]] = []

        class Broken:
            @contextmanager
            async def __select__(
                self, guard: SelectorGuard[str]
            ) -> AsyncIterator[None]:
                guards.append(guard)
                if registration_fails:
                    raise ValueError("registration")
                async with selectors(guard) as (selector,):
                    assert selector is not None
                    selector.result("complete")
                try:
                    yield
                finally:
                    raise ValueError("cleanup")

        expected = ValueError
        with pytest.raises(expected):
            await select.a(
                DualChoice(None, log, "first"), Broken(), DualChoice(None, log, "last")
            )
        expected_log = ["register first"]
        if not registration_fails:
            expected_log += ["register last", "cleanup last"]
        assert log == expected_log + ["cleanup first"]
        async with selectors(*guards) as acquired:
            assert acquired == (None,)

    async_runtime.run(main)


def test_cleanup_errors_do_not_stop_unwinding(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        log: list[str] = []

        class Fails(DualChoice):
            async def _cleanup(self) -> None:
                await super()._cleanup()
                raise ValueError(self.name)

        with pytest.raises(ValueError, match="first"):
            await select.a(Fails("winner", log, "first"), Fails(None, log, "second"))
        assert log == [
            "register first",
            "register second",
            "cleanup second",
            "cleanup first",
        ]

    async_runtime.run(main)


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_cleanup_error_replaces_cancellation(backend: str) -> None:
    async def main() -> None:
        scope = anyio.CancelScope()
        registered = anyio.Event()
        log: list[str] = []

        class Fails(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                cleanup = await super()._register(guard)
                registered.set()
                return cleanup

            async def _cleanup(self) -> None:
                log.append("cleanup attempted")
                raise ValueError("cleanup failed")

        async def cancel() -> None:
            await registered.wait()
            scope.cancel()

        async with anyio.create_task_group() as group:
            group.start_soon(cancel)
            with pytest.raises(ValueError, match="cleanup failed"), scope:
                await select.a(Fails(None, log, "choice"))
        assert log == ["register choice", "cleanup attempted"]
        assert not scope.cancelled_caught

    anyio.run(main, backend=backend)


def test_repeated_asyncio_cancellation_invalidates_before_local_cleanup() -> None:
    import asyncio

    async def main() -> None:
        log: list[str] = []
        registered = asyncio.Event()
        entered_invalidation = asyncio.Event()
        owned = Lock()
        state_lock = Lock()
        marker = ContextVar("cleanup identity", default=False)

        class ObservedLock(Lock):
            async def __aenter__(self):
                entered_invalidation.set()
                return await super().__aenter__()

        class Choice(DualChoice):
            async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
                nonlocal state_lock
                await owned.__enter__.a()
                marker.set(True)
                assert isinstance(guard.selector, _Selector)
                guard.selector._selection._lock = state_lock = ObservedLock()
                guard.lock = state_lock
                cleanup = await super()._register(guard)
                registered.set()
                return cleanup

            async def _cleanup(self) -> None:
                assert marker.get()
                await owned.__exit__.a(None, None, None)
                await super()._cleanup()

        choice = Choice(None, log, "choice")
        task = asyncio.create_task(select.a(choice))
        await registered.wait()
        async with state_lock:
            entered_invalidation.clear()
            task.cancel("first")
            await entered_invalidation.wait()
            task.cancel("second")
            await asyncio.sleep(0)
            assert log == ["register choice"]
            assert not task.done()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert log == ["register choice", "cleanup choice"]
        assert choice.guard is not None
        async with selectors(choice.guard) as (selector,):
            assert selector is None
        async with owned:
            pass

    asyncio.run(main())


def test_select_completed_from_another_thread(async_runtime: AsyncRuntime) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    ready = Event()
    log: list[str] = []

    class Choice(DualChoice):
        async def _register(self, guard: SelectorGuard[str]) -> Routine[[], None]:
            cleanup = await super()._register(guard)
            ready.set()
            return cleanup

    choice = Choice(None, log, "choice")

    def complete() -> None:
        assert ready.wait(5)
        assert choice.guard is not None
        with selectors(choice.guard) as (selector,):
            assert selector is not None
            selector.result("from thread")

    async def main() -> None:
        assert await select.a(choice) == (0, "from thread")

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(complete)
        async_runtime.run(main)
        future.result(timeout=5)
    assert log == ["register choice", "cleanup choice"]


def test_routine_selectable_with_local_cleanup() -> None:
    log: list[str] = []

    class Choice:
        @contextmanager
        async def __select__(self, guard: SelectorGuard[int]) -> AsyncIterator[None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)

            @routine
            async def cleanup() -> None:
                log.append("cleanup")

            try:
                yield
            finally:
                await cleanup.a()

    choice: Selectable[int] = Choice()
    assert assert_type(select(choice), tuple[Literal[0], int]) == (0, 42)
    assert log == ["cleanup"]


def _check_select_composes_with_routines(checkpoint: Callable[[], None]) -> None:
    @routine
    async def outer() -> tuple[int, str]:
        with pytest.raises(RuntimeError, match="Use await function.a"):
            select(DualChoice("wrong call", [], "choice"))
        return await select.a(DualChoice("value", [], "choice"))

    assert outer() == (0, "value")
    checkpoint()


def test_select_composes_with_routines(sync_runtime: RunSync) -> None:
    sync_runtime(_check_select_composes_with_routines)


def _check_sync_registration_failure_invalidates_before_cleanup(
    checkpoint: Callable[[], None],
) -> None:
    log: list[str] = []
    guards: list[SelectorGuard[str]] = []

    class Broken(OrdinaryChoice):
        @sync_contextmanager
        def __select__(self, guard: SelectorGuard[str]) -> Iterator[None]:
            guards.append(guard)
            checkpoint()
            raise ValueError("registration failed")
            yield

    with pytest.raises(ValueError, match="registration failed"):
        select(OrdinaryChoice(None, log, "first"), Broken(None, log, "broken"))
    assert log == ["register first", "cleanup first"]
    with selectors(*guards) as acquired:
        assert acquired == (None,)


def test_sync_registration_failure_invalidates_before_cleanup(
    sync_runtime: RunSync,
) -> None:
    sync_runtime(_check_sync_registration_failure_invalidates_before_cleanup)


def _check_sync_cleanup_errors_do_not_stop_unwinding(
    checkpoint: Callable[[], None],
) -> None:
    log: list[str] = []

    class Fails(OrdinaryChoice):
        def cleanup(self) -> None:
            super().cleanup()
            checkpoint()
            raise ValueError(self.name)

    with pytest.raises(ValueError, match="first"):
        select(Fails("winner", log, "first"), Fails(None, log, "second"))
    assert log == [
        "register first",
        "register second",
        "cleanup second",
        "cleanup first",
    ]


def test_sync_cleanup_errors_do_not_stop_unwinding(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync_cleanup_errors_do_not_stop_unwinding)


def _check_sync_cleanup_accepts_either_callable(checkpoint: Callable[[], None]) -> None:
    log: list[str] = []

    def cleanup() -> None:
        checkpoint()
        log.append("ordinary")

    @routine
    async def routine_cleanup() -> None:
        checkpoint()
        log.append("routine")

    class Ordinary:
        def __init__(self, callback: Callable[[], None]) -> None:
            self.callback = callback

        @sync_contextmanager
        def __select__(self, guard: SelectorGuard[int]) -> Iterator[None]:
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            try:
                yield
            finally:
                self.callback()

    class Dual:
        def __init__(self, callback: Callable[[], None]) -> None:
            self.callback = callback

        @contextmanager
        async def __select__(self, guard: SelectorGuard[int]) -> AsyncIterator[None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            try:
                yield
            finally:
                cleared_mode(self.callback)()

    for choice_type in (Ordinary, Dual):
        for callback in (cleanup, routine_cleanup):
            assert assert_type(
                select(choice_type(callback)), tuple[Literal[0], int]
            ) == (
                0,
                42,
            )
    assert log == ["ordinary", "routine", "ordinary", "routine"]


def test_sync_cleanup_accepts_either_callable(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync_cleanup_accepts_either_callable)


def test_cleared_mode_registration_is_ordinary_only(
    async_runtime: AsyncRuntime,
) -> None:
    log: list[str] = []

    class Choice:
        @cleared_mode
        @sync_contextmanager
        def __select__(self, guard: SelectorGuard[int]) -> Iterator[None]:
            log.append("register")
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            try:
                yield
            finally:
                self.cleanup()

        @cleared_mode
        def cleanup(self) -> None:
            log.append("cleanup")

    choice = Choice()
    assert assert_type(select(choice), tuple[Literal[0], int]) == (0, 42)
    assert log == ["register", "cleanup"]
    log.clear()

    async def main() -> None:
        with pytest.raises(TypeError, match="requires routine-based context managers"):
            await select.a(choice)  # pyright: ignore[reportArgumentType, reportCallIssue]
        assert not log

    async_runtime.run(main)


def test_invalid_context_manager_unwinds_earlier_registrations() -> None:
    log: list[str] = []

    class Broken:
        def __select__(self, guard: SelectorGuard[str]) -> Any:
            return 42

    with pytest.raises(AttributeError, match="__enter__"):
        select(
            OrdinaryChoice(None, log, "first"),
            Broken(),
            OrdinaryChoice(None, log, "last"),
        )
    assert log == ["register first", "cleanup first"]


def _check_registration_without_cleanup(checkpoint: Callable[[], None]) -> None:
    guards: list[SelectorGuard[int]] = []

    class Ready:
        @sync_contextmanager
        def __select__(self, guard: SelectorGuard[int]) -> Iterator[None]:
            guards.append(guard)
            with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(42)
            yield

    assert assert_type(
        select(Ready(), Ready()), tuple[Literal[0], int] | tuple[Literal[1], int]
    ) == (0, 42)
    for guard in guards:
        with selectors(guard) as (selector,):
            assert selector is None
    checkpoint()


def test_registration_without_cleanup(sync_runtime: RunSync) -> None:
    sync_runtime(_check_registration_without_cleanup)


def test_routine_registration_without_cleanup(async_runtime: AsyncRuntime) -> None:
    guards: list[SelectorGuard[int]] = []

    class Ready:
        @contextmanager
        async def __select__(self, guard: SelectorGuard[int]) -> AsyncIterator[None]:
            guards.append(guard)
            async with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(42)
            yield

    async def main() -> None:
        assert assert_type(
            await select.a(Ready(), Ready()),
            tuple[Literal[0], int] | tuple[Literal[1], int],
        ) == (0, 42)
        for guard in guards:
            async with selectors(guard) as (selector,):
                assert selector is None

    async_runtime.run(main)


class _TypedChoice[T]:
    def __init__(self, value: T, ready: bool = True) -> None:
        self.value = value
        self.ready = ready

    @contextmanager
    async def __select__(self, guard: SelectorGuard[T]) -> AsyncIterator[None]:
        if self.ready:
            async with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(self.value)
        yield


class _OrdinaryTypedChoice[T]:
    def __init__(self, value: T) -> None:
        self.value = value

    @sync_contextmanager
    def __select__(self, guard: SelectorGuard[T]) -> Iterator[None]:
        with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(self.value)
        yield


def test_select_correlates_index_and_value() -> None:
    number = _OrdinaryTypedChoice[int](42)
    text = _OrdinaryTypedChoice[str]("hello")
    data = _OrdinaryTypedChoice[bytes](b"data")
    assert_type(select(number), tuple[Literal[0], int])
    assert_type(select(number, text), tuple[Literal[0], int] | tuple[Literal[1], str])
    assert_type(
        select(number, text, data),
        tuple[Literal[0], int] | tuple[Literal[1], str] | tuple[Literal[2], bytes],
    )
    # Both protocol forms can participate in the same ordinary call.
    assert_type(
        select(number, _TypedChoice[str]("hello")),
        tuple[Literal[0], int] | tuple[Literal[1], str],
    )
    assert isinstance(select, Routine)


@pytest.mark.parametrize("winner", [0, 1, 2])
def test_select_result_narrows_by_index(
    async_runtime: AsyncRuntime, winner: int
) -> None:
    number = _TypedChoice[int](42, winner == 0)
    text = _TypedChoice[str]("hello", winner == 1)
    data = _TypedChoice[bytes](b"data", winner == 2)

    @routine
    async def choose() -> None:
        result = await select.a(number, text, data)
        assert_type(
            result,
            tuple[Literal[0], int] | tuple[Literal[1], str] | tuple[Literal[2], bytes],
        )
        assert result[0] == winner
        if result[0] == 0:
            assert assert_type(result[1], int) == 42
        elif result[0] == 1:
            assert assert_type(result[1], str) == "hello"
        else:
            assert assert_type(result[1], bytes) == b"data"

    choose()
    async_runtime.run(choose.a)


def test_select_typing_fallbacks(async_runtime: AsyncRuntime) -> None:
    numbers = [_TypedChoice[int](value) for value in range(11)]
    mixed = (
        _TypedChoice[int](1),
        _TypedChoice[str]("a"),
        _TypedChoice[bytes](b"b"),
        _TypedChoice[float](2.5),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
        _TypedChoice[int](1),
    )
    assert_type(select(*numbers), tuple[int, int])
    assert_type(select(*mixed), tuple[int, Any])

    async def main() -> None:
        assert_type(await select.a(numbers[0]), tuple[Literal[0], int])
        assert_type(
            await select.a(mixed[0], mixed[1]),
            tuple[Literal[0], int] | tuple[Literal[1], str],
        )
        assert_type(await select.a(*numbers), tuple[int, int])
        assert_type(await select.a(*mixed), tuple[int, Any])

    async_runtime.run(main)


def test_select_extended_result_types(async_runtime: AsyncRuntime) -> None:
    choice0 = _TypedChoice[int](0)
    choice1 = _TypedChoice[str]("one", ready=False)
    choice2 = _TypedChoice[bytes](b"two", ready=False)
    choice3 = _TypedChoice[float](3.0, ready=False)
    choice4 = _TypedChoice[bool](True, ready=False)
    choice5 = _TypedChoice[complex](5j, ready=False)
    choice6 = _TypedChoice[list[int]]([6], ready=False)
    choice7 = _TypedChoice[dict[str, int]]({"seven": 7}, ready=False)
    choice8 = _TypedChoice[tuple[int, ...]]((8,), ready=False)
    choice9 = _TypedChoice[set[int]]({9}, ready=True)

    async def main() -> None:
        assert_type(
            await select.a(choice0, choice1, choice2, choice3),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float],
        )
        assert_type(
            await select.a(choice0, choice1, choice2, choice3, choice4),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool],
        )
        assert_type(
            await select.a(choice0, choice1, choice2, choice3, choice4, choice5),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool]
            | tuple[Literal[5], complex],
        )
        assert_type(
            await select.a(
                choice0, choice1, choice2, choice3, choice4, choice5, choice6
            ),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool]
            | tuple[Literal[5], complex]
            | tuple[Literal[6], list[int]],
        )
        assert_type(
            await select.a(
                choice0, choice1, choice2, choice3, choice4, choice5, choice6, choice7
            ),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool]
            | tuple[Literal[5], complex]
            | tuple[Literal[6], list[int]]
            | tuple[Literal[7], dict[str, int]],
        )
        assert_type(
            await select.a(
                choice0,
                choice1,
                choice2,
                choice3,
                choice4,
                choice5,
                choice6,
                choice7,
                choice8,
            ),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool]
            | tuple[Literal[5], complex]
            | tuple[Literal[6], list[int]]
            | tuple[Literal[7], dict[str, int]]
            | tuple[Literal[8], tuple[int, ...]],
        )
        assert_type(
            await select.a(
                choice0,
                choice1,
                choice2,
                choice3,
                choice4,
                choice5,
                choice6,
                choice7,
                choice8,
                choice9,
            ),
            tuple[Literal[0], int]
            | tuple[Literal[1], str]
            | tuple[Literal[2], bytes]
            | tuple[Literal[3], float]
            | tuple[Literal[4], bool]
            | tuple[Literal[5], complex]
            | tuple[Literal[6], list[int]]
            | tuple[Literal[7], dict[str, int]]
            | tuple[Literal[8], tuple[int, ...]]
            | tuple[Literal[9], set[int]],
        )

    async_runtime.run(main)
    assert_type(
        select(choice0, choice1, choice2, choice3),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float],
    )
    assert_type(
        select(choice0, choice1, choice2, choice3, choice4),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool],
    )
    assert_type(
        select(choice0, choice1, choice2, choice3, choice4, choice5),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool]
        | tuple[Literal[5], complex],
    )
    assert_type(
        select(choice0, choice1, choice2, choice3, choice4, choice5, choice6),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool]
        | tuple[Literal[5], complex]
        | tuple[Literal[6], list[int]],
    )
    assert_type(
        select(choice0, choice1, choice2, choice3, choice4, choice5, choice6, choice7),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool]
        | tuple[Literal[5], complex]
        | tuple[Literal[6], list[int]]
        | tuple[Literal[7], dict[str, int]],
    )
    assert_type(
        select(
            choice0,
            choice1,
            choice2,
            choice3,
            choice4,
            choice5,
            choice6,
            choice7,
            choice8,
        ),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool]
        | tuple[Literal[5], complex]
        | tuple[Literal[6], list[int]]
        | tuple[Literal[7], dict[str, int]]
        | tuple[Literal[8], tuple[int, ...]],
    )
    assert_type(
        select(
            choice0,
            choice1,
            choice2,
            choice3,
            choice4,
            choice5,
            choice6,
            choice7,
            choice8,
            choice9,
        ),
        tuple[Literal[0], int]
        | tuple[Literal[1], str]
        | tuple[Literal[2], bytes]
        | tuple[Literal[3], float]
        | tuple[Literal[4], bool]
        | tuple[Literal[5], complex]
        | tuple[Literal[6], list[int]]
        | tuple[Literal[7], dict[str, int]]
        | tuple[Literal[8], tuple[int, ...]]
        | tuple[Literal[9], set[int]],
    )

    choice0.ready = False

    async def last() -> None:
        result = await select.a(
            choice0,
            choice1,
            choice2,
            choice3,
            choice4,
            choice5,
            choice6,
            choice7,
            choice8,
            choice9,
        )
        assert result[0] == 9
        assert assert_type(result[1], set[int]) == {9}

    async_runtime.run(last)


@routine
async def exercise_registration_isolation(failure: str) -> None:
    log: list[str] = []
    error = ValueError(failure)

    class Registration:
        def __init__(self, name: str, guard: SelectorGuard[int]) -> None:
            self.name = name
            self.guard = guard

        @routine
        async def enter(self) -> str:
            log.append(f"enter {self.name}")
            if self.name == "second":
                if failure == "registration":
                    raise error
                async with selectors(self.guard) as (selector,):
                    assert selector is not None
                    if failure == "result":
                        selector.error(error)
                    else:
                        selector.result(42)
            return "ignored entry value"

        @routine
        async def exit(self, *exc: object) -> bool:
            assert exc == (None, None, None)
            async with selectors(self.guard) as (selector,):
                assert selector is None
            log.append(f"exit {self.name}")
            if self.name == "second" and failure == "cleanup":
                raise error
            # This must not suppress a different registration's exception.
            return True

        def __enter__(self) -> str:
            return self.enter()

        def __exit__(self, *exc: object) -> bool:
            return self.exit(*exc)

        async def __aenter__(self) -> str:
            return await self.enter.a()

        async def __aexit__(self, *exc: object) -> bool:
            return await self.exit.a(*exc)

    class Choice:
        def __init__(self, name: str) -> None:
            self.name = name

        @contextmanager
        async def __select__(self, guard: SelectorGuard[int]) -> AsyncIterator[str]:
            async with Registration(self.name, guard) as value:
                yield value

    if failure == "none":
        assert await select.a(Choice("first"), Choice("second")) == (1, 42)
    else:
        with pytest.raises(ValueError) as raised:
            await select.a(Choice("first"), Choice("second"))
        assert raised.value is error
    assert log == ["enter first", "enter second"] + (
        [] if failure == "registration" else ["exit second"]
    ) + ["exit first"]


def _check_registration_isolation(checkpoint: Callable[[], None]) -> None:
    for failure in ("none", "registration", "result", "cleanup"):
        exercise_registration_isolation(failure)
    checkpoint()


def test_ordinary_registration_isolation(sync_runtime: RunSync) -> None:
    sync_runtime(_check_registration_isolation)


@pytest.mark.parametrize("failure", ["none", "registration", "result", "cleanup"])
def test_async_registration_isolation(
    async_runtime: AsyncRuntime, failure: str
) -> None:
    async_runtime.run(lambda: exercise_registration_isolation.a(failure))


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
            async with selectors(
                SelectorGuard(_Selector(first, 0), first._lock),
                SelectorGuard(_Selector(second, 0), second._lock),
            ) as (
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
            async with selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (
                selector,
            ):
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
        async with selectors(SelectorGuard(_Selector(state, 1), state._lock)) as (
            selector,
        ):
            assert selector is None

    async_runtime.run(main)


def test_thread_completes_async_selection(async_runtime: AsyncRuntime) -> None:
    state = _Selection[int]()
    started = ThreadEvent()

    def complete() -> None:
        assert started.wait(5)
        with selectors(SelectorGuard(_Selector(state, 3), state._lock)) as (selector,):
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
            async with selectors(
                SelectorGuard(_Selector(second, 1), second._lock),
                SelectorGuard(_Selector(first, 0), first._lock),
            ) as (
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
            selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (selector,),
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
        async with selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (
            selector,
        ):
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
        async with selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (
            selector,
        ):
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
    with selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (selector,):
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
    with selectors(SelectorGuard(_Selector(state, 0), state._lock)) as (selector,):
        assert selector is None
    checkpoint()


def test_invalidation_preserves_routine_context(sync_runtime: RunSync) -> None:
    sync_runtime(_check_invalidation_preserves_routine_context)
