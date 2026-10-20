from collections.abc import Callable
from contextvars import ContextVar
from typing import Any
from typing import assert_type

import anyio
import pytest

from noio import cleared_mode
from noio import routine
from noio._lock import Lock
from noio.routine import Routine
from noio.select import Selectable
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

    def __select__(self, guard: SelectorGuard[str]) -> Callable[[], None]:
        self.guard = guard
        self.log.append(f"register {self.name}")
        if self.value is not None:
            with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(self.value)
        return self.cleanup

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

    @routine
    async def __select__(self, guard: SelectorGuard[str]) -> Routine[[], None]:
        return await self._register(guard)

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
        def __select__(self, guard: SelectorGuard[str]) -> Callable[[], None]:
            lock.__enter__()
            marker.set("registered")
            return super().__select__(guard)

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
        with pytest.raises(
            TypeError, match="requires selectables to be implemented with routines"
        ):
            await select.a(
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
def test_registration_failure_and_bad_cleanup(
    async_runtime: AsyncRuntime, registration_fails: bool
) -> None:
    async def main() -> None:
        log: list[str] = []
        guards: list[SelectorGuard[str]] = []

        class Broken:
            @routine
            async def __select__(self, guard: SelectorGuard[str]) -> Any:
                guards.append(guard)
                if registration_fails:
                    raise ValueError("registration")
                async with selectors(guard) as (selector,):
                    assert selector is not None
                    selector.result("complete")
                return lambda: pytest.fail("ordinary cleanup must not be invoked")

        expected = ValueError if registration_fails else AttributeError
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
                guard._selection._lock = state_lock = ObservedLock()
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
        @routine
        async def __select__(self, guard: SelectorGuard[int]) -> Routine[[], None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)

            @routine
            async def cleanup() -> None:
                log.append("cleanup")

            return cleanup

    choice: Selectable[int] = Choice()
    assert assert_type(select(choice), tuple[int, int]) == (0, 42)
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
        def __select__(self, guard: SelectorGuard[str]) -> Callable[[], None]:
            guards.append(guard)
            checkpoint()
            raise ValueError("registration failed")

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

        def __select__(self, guard: SelectorGuard[int]) -> Callable[[], None]:
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            return self.callback

    class Dual:
        def __init__(self, callback: Callable[[], None]) -> None:
            self.callback = callback

        @routine
        async def __select__(self, guard: SelectorGuard[int]) -> Callable[[], None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            return self.callback

    for choice_type in (Ordinary, Dual):
        for callback in (cleanup, routine_cleanup):
            assert assert_type(select(choice_type(callback)), tuple[int, int]) == (
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
        def __select__(self, guard: SelectorGuard[int]) -> Callable[[], None]:
            log.append("register")
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            return self.cleanup

        @cleared_mode
        def cleanup(self) -> None:
            log.append("cleanup")

    choice = Choice()
    assert assert_type(select(choice), tuple[int, int]) == (0, 42)
    assert log == ["register", "cleanup"]
    log.clear()

    async def main() -> None:
        with pytest.raises(
            TypeError, match="requires selectables to be implemented with routines"
        ):
            await select.a(choice)  # pyright: ignore[reportArgumentType]
        assert not log

    async_runtime.run(main)


def test_invalid_ordinary_cleanup_fails_during_cleanup() -> None:
    log: list[str] = []

    class Broken(OrdinaryChoice):
        def __select__(self, guard: SelectorGuard[str]) -> Any:
            super().__select__(guard)
            return 42

    with pytest.raises(TypeError):
        select(
            OrdinaryChoice(None, log, "first"),
            Broken("winner", log, "broken"),
            OrdinaryChoice(None, log, "last"),
        )
    assert log == [
        "register first",
        "register broken",
        "register last",
        "cleanup last",
        "cleanup first",
    ]


def test_async_cleanup_uses_duck_typing(async_runtime: AsyncRuntime) -> None:
    log: list[str] = []

    class Cleanup:
        async def a(self) -> None:
            await async_runtime.checkpoint()
            log.append("cleanup")

    class Choice:
        @routine
        async def __select__(self, guard: SelectorGuard[int]) -> Any:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(42)
            return Cleanup()

    async def main() -> None:
        assert await select.a(Choice()) == (0, 42)
        assert log == ["cleanup"]

    async_runtime.run(main)


def _check_registration_without_cleanup(checkpoint: Callable[[], None]) -> None:
    guards: list[SelectorGuard[int]] = []

    class Ready:
        def __select__(self, guard: SelectorGuard[int]) -> None:
            guards.append(guard)
            with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(42)

    assert assert_type(select(Ready(), Ready()), tuple[int, int]) == (0, 42)
    for guard in guards:
        with selectors(guard) as (selector,):
            assert selector is None
    checkpoint()


def test_registration_without_cleanup(sync_runtime: RunSync) -> None:
    sync_runtime(_check_registration_without_cleanup)


def test_routine_registration_without_cleanup(async_runtime: AsyncRuntime) -> None:
    guards: list[SelectorGuard[int]] = []

    class Ready:
        @routine
        async def __select__(self, guard: SelectorGuard[int]) -> None:
            guards.append(guard)
            async with selectors(guard) as (selector,):
                if selector is not None:
                    selector.result(42)

    async def main() -> None:
        assert assert_type(await select.a(Ready(), Ready()), tuple[int, int]) == (0, 42)
        for guard in guards:
            async with selectors(guard) as (selector,):
                assert selector is None

    async_runtime.run(main)
