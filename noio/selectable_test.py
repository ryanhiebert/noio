from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager as sync_contextmanager
from typing import Any
from typing import Literal
from typing import Self
from typing import assert_type

import anyio
import pytest

from noio import contextmanager
from noio import routine
from noio._lock import Lock
from noio._selection import _Selection
from noio.select import select
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


def _check_selectable_functions_and_methods(checkpoint: Callable[[], None]) -> None:
    from .selectable import selectable

    log: list[str] = []

    @selectable
    @sync_contextmanager
    def operation(
        guard: SelectorGuard[int], value: int, *, scale: int = 2
    ) -> Iterator[None]:
        log.append("register")
        with selectors(guard) as (selector,):
            assert selector is not None
            selector.result(value * scale)
        try:
            yield
        finally:
            log.append("cleanup")

    prepared = operation.select(3, scale=4)
    assert not log
    assert assert_type(select(prepared), tuple[Literal[0], int]) == (0, 12)
    assert assert_type(operation(3), int) == 6
    assert log == ["register", "cleanup"] * 2
    assert not hasattr(operation, "a")

    class Counter:
        def __init__(self, offset: int):
            self.offset = offset

        @selectable
        @sync_contextmanager
        def calculate(
            self, guard: SelectorGuard[int], value: int, *, scale: int = 2
        ) -> Iterator[None]:
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(self.offset + value * scale)
            yield

    class Child(Counter):
        pass

    first, second = Counter(10), Child(20)
    assert assert_type(first.calculate(3), int) == 16
    assert assert_type(second.calculate(3, scale=4), int) == 32
    assert assert_type(Counter.calculate(first, 3), int) == 16
    assert assert_type(select(second.calculate.select(3)), tuple[Literal[0], int]) == (
        0,
        26,
    )
    assert select(Counter.calculate.select(second, 3)) == (0, 26)
    checkpoint()


def test_selectable_functions_and_methods(sync_runtime: RunSync) -> None:
    sync_runtime(_check_selectable_functions_and_methods)


def test_selectable_routine_functions_and_methods(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    log: list[str] = []

    @routine
    async def cleanup() -> None:
        log.append("cleanup")

    @selectable
    @contextmanager
    async def operation(
        guard: SelectorGuard[int], value: int, *, scale: int = 2
    ) -> AsyncIterator[None]:
        log.append("register")
        async with selectors(guard) as (selector,):
            assert selector is not None
            selector.result(value * scale)
        try:
            yield
        finally:
            await cleanup.a()

    class Counter:
        def __init__(self, offset: int):
            self.offset = offset

        @selectable
        @contextmanager
        async def calculate(
            self, guard: SelectorGuard[int], value: int, *, scale: int = 2
        ) -> AsyncIterator[None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(self.offset + value * scale)
            try:
                yield
            finally:
                await cleanup.a()

    class Child(Counter):
        pass

    first, second = Counter(10), Child(20)
    assert assert_type(operation(3), int) == 6
    assert assert_type(first.calculate(3), int) == 16
    assert assert_type(Counter.calculate(first, 3), int) == 16
    log.clear()

    @routine
    async def main() -> None:
        prepared = operation.select(3, scale=4)
        call = operation.a(3)
        assert not log
        assert assert_type(await call, int) == 6
        assert assert_type(await select.a(prepared), tuple[Literal[0], int]) == (0, 12)
        assert log == ["register", "cleanup"] * 2
        assert assert_type(await first.calculate.a(3), int) == 16
        assert assert_type(await second.calculate.a(3, scale=4), int) == 32
        assert assert_type(await Counter.calculate.a(first, 3), int) == 16
        assert assert_type(
            await select.a(second.calculate.select(3)), tuple[Literal[0], int]
        ) == (0, 26)
        assert await select.a(Counter.calculate.select(second, 3)) == (0, 26)
        with pytest.raises(RuntimeError, match="Use await"):
            operation(3)

    async_runtime.run(main.a)


def test_selectable_rejects_bare_coroutine() -> None:
    from .selectable import selectable

    async def registration(guard: SelectorGuard[int]) -> Callable[[], None]:
        return lambda: None

    invalid: Any = registration
    with pytest.raises(
        TypeError,
        match="requires a context manager callable",
    ):
        selectable(invalid)


def test_selectable_prepared_operation_shared_across_runtimes(
    async_runtime: AsyncRuntime,
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from ._event import Event
    from .selectable import selectable

    entered, release = Event(), Event()
    lock = Lock()
    guards: list[SelectorGuard[int]] = []
    cleaned: list[SelectorGuard[int]] = []

    @selectable
    @contextmanager
    async def operation(guard: SelectorGuard[int], value: int) -> AsyncIterator[None]:
        async with lock:
            guards.append(guard)
            first = len(guards) == 1
        if first:
            entered.set()
            await release.wait.a()
        async with selectors(guard) as (selector,):
            assert selector is not None
            selector.result(value)

        @routine
        async def cleanup() -> None:
            async with selectors(guard) as (selector,):
                assert selector is None
            async with lock:
                cleaned.append(guard)

        try:
            yield
        finally:
            await cleanup.a()

    prepared = operation.select(42)
    assert not guards
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(select, prepared)
        entered.wait()
        try:

            async def main() -> None:
                assert await select.a(prepared) == (0, 42)
                assert len(cleaned) == 1

            async_runtime.run(main)
        finally:
            release.set()
        assert future.result(timeout=5) == (0, 42)
    assert len(guards) == len(cleaned) == 2
    assert guards[0] is not guards[1]


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_selectable_direct_call_cancellation_cleans_up(backend: str) -> None:
    from .selectable import selectable

    async def main() -> None:
        registered = anyio.Event()
        cleaned: list[bool] = []

        @selectable
        @contextmanager
        async def operation(
            guard: SelectorGuard[int], value: int
        ) -> AsyncIterator[None]:
            @routine
            async def cleanup() -> None:
                with anyio.CancelScope(shield=True):
                    async with selectors(guard) as (selector,):
                        assert selector is None
                cleaned.append(True)

            registered.set()
            try:
                yield
            finally:
                await cleanup.a()

        async with anyio.create_task_group() as group:
            group.start_soon(operation.a, 42)
            await registered.wait()
            group.cancel_scope.cancel()
        assert cleaned == [True]

    anyio.run(main, backend=backend)


def test_selectable_direct_calls_raise_delivered_errors() -> None:
    from .selectable import selectable

    error = ValueError("delivered")
    cleaned: list[bool] = []

    @selectable
    @sync_contextmanager
    def operation(guard: SelectorGuard[int]) -> Iterator[None]:
        with selectors(guard) as (selector,):
            assert selector is not None
            selector.error(error)
        try:
            yield
        finally:
            cleaned.append(True)

    with pytest.raises(ValueError) as raised:
        operation()
    assert raised.value is error
    assert cleaned == [True]


def test_selectable_typing_rejects_invalid_calls(tmp_path: Any) -> None:
    import json
    import subprocess
    import sys
    from pathlib import Path

    source = """from collections.abc import Iterator, AsyncIterator
from contextlib import contextmanager as sync_contextmanager
from noio import contextmanager
from noio import routine
from noio.select import select
from noio.selectable import selectable
from noio.selectable import SelectorGuard

@selectable
@sync_contextmanager
def ordinary(
    guard: SelectorGuard[int], value: str, *, count: int = 1
) -> Iterator[None]:
    yield

@selectable
@contextmanager
async def asynchronous(guard: SelectorGuard[int], value: str) -> AsyncIterator[None]:
    yield

class Example:
    @selectable
    @sync_contextmanager
    def method(self, guard: SelectorGuard[int], value: str) -> Iterator[None]:
        yield

@routine
async def invalid_cleanup(guard: SelectorGuard[int]) -> int:
    return 42

selectable(invalid_cleanup)  # reject

ordinary(1)  # reject
ordinary.select("x", count="bad")  # reject
ordinary.a("x")  # reject
Example().method(1)  # reject
Example.method.select(Example(), 1)  # reject

def wrong_argument(value: int) -> int:
    return value

def wrong_result(value: str) -> str:
    return value

def wrong_registration(guard: SelectorGuard[int], value: int) -> None:
    pass

def wrong_cleanup(guard: SelectorGuard[int], value: str) -> int:
    return 42

asynchronous.fn(wrong_registration)  # reject
asynchronous.fn(wrong_cleanup)  # reject
asynchronous.call(wrong_argument)  # reject
asynchronous.call(wrong_result)  # reject
asynchronous.call_fn(wrong_argument)  # reject
asynchronous.call_fn(wrong_result)  # reject
ordinary.call(wrong_argument)  # reject
ordinary.call_fn(wrong_result)  # reject
ordinary.fn(wrong_registration)  # reject

@routine
async def direct(value: str) -> int:
    return len(value)

configured = asynchronous.call(direct)
configured.call_fn(wrong_argument)  # reject
configured.call_fn(wrong_result)  # reject
configured.call(lambda value: len(value)).call_fn(len)  # reject
ordinary.call(len).call_fn(len)  # reject

async def caller() -> None:
    await asynchronous.a(1)  # reject
    await select.a(ordinary.select("x"))  # reject
"""
    (tmp_path / "example.py").write_text(source)
    (tmp_path / "pyrightconfig.json").write_text(
        json.dumps(
            {
                "include": ["example.py"],
                "extraPaths": [str(Path(__file__).resolve().parent.parent)],
                "pythonVersion": "3.15",
                "typeCheckingMode": "standard",
            }
        )
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "basedpyright",
            "--project",
            str(tmp_path),
            "--outputjson",
        ],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    diagnostics = json.loads(result.stdout)["generalDiagnostics"]
    expected = {i for i, line in enumerate(source.splitlines()) if "# reject" in line}
    assert {item["range"]["start"]["line"] for item in diagnostics} == expected, (
        result.stdout
    )
    assert all(item["severity"] == "error" for item in diagnostics), result.stdout


def test_decorated_registration_without_cleanup(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    @selectable
    @sync_contextmanager
    def ordinary(guard: SelectorGuard[int], value: int) -> Iterator[None]:
        with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(value)
        yield

    @selectable
    @contextmanager
    async def asynchronous(
        guard: SelectorGuard[int], value: int
    ) -> AsyncIterator[None]:
        async with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(value)
        yield

    assert assert_type(ordinary(42), int) == 42
    assert assert_type(asynchronous(42), int) == 42
    assert select(ordinary.select(42)) == (0, 42)

    async def main() -> None:
        assert assert_type(await asynchronous.a(42), int) == 42
        assert await select.a(asynchronous.select(42)) == (0, 42)

    async_runtime.run(main)


@pytest.mark.parametrize("registration_first", [False, True])
def test_selectable_implementation_overrides(
    async_runtime: AsyncRuntime, registration_first: bool
) -> None:
    from .selectable import selectable

    log: list[str] = []

    @selectable
    @contextmanager
    async def operation(  # pyright: ignore[reportRedeclaration]
        guard: SelectorGuard[int], value: int, *, offset: int = 0
    ) -> AsyncIterator[None]:
        log.append("async registration")
        async with selectors(guard) as (selector,):
            assert selector is not None
            selector.result(value + offset)
        yield

    original = operation

    def register(
        guard: SelectorGuard[int], value: int, *, offset: int = 0
    ) -> Iterator[None]:
        log.append("ordinary registration")
        with selectors(guard) as (selector,):
            assert selector is not None
            selector.result(value + offset)
        yield

    @routine
    async def direct(value: int, *, offset: int = 0) -> int:
        log.append("async direct")
        return value + offset

    if registration_first:
        configured = operation.fn(register).call(direct)
    else:
        configured = operation.call(direct).fn(register)

    @configured.call_fn
    def operation(value: int, *, offset: int = 0) -> int:
        log.append("ordinary direct")
        return value + offset

    assert assert_type(operation(40, offset=2), int) == 42
    assert select(operation.select(40, offset=2)) == (0, 42)
    assert log == ["ordinary direct", "ordinary registration"]
    log.clear()

    @routine
    async def main() -> None:
        assert assert_type(await operation.a(40, offset=2), int) == 42
        assert await select.a(operation.select(40, offset=2)) == (0, 42)

    main()
    assert log == ["ordinary direct", "ordinary registration"]
    log.clear()
    async_runtime.run(main.a)
    assert log == ["async direct", "async registration"]
    log.clear()
    assert original(42) == 42
    assert log == ["async registration"]


def test_selectable_method_overrides(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    class Example:
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix

        @selectable
        @contextmanager
        async def operation(  # pyright: ignore[reportRedeclaration]
            self, guard: SelectorGuard[str], value: int
        ) -> AsyncIterator[None]:
            async with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(f"{self.prefix} async registration {value}")
            yield

        @operation.fn
        def operation(self, guard: SelectorGuard[str], value: int) -> Iterator[None]:  # pyright: ignore[reportRedeclaration]
            with selectors(guard) as (selector,):
                assert selector is not None
                selector.result(f"{self.prefix} ordinary registration {value}")
            yield

        @operation.call
        @routine
        async def operation(self, value: int) -> str:  # pyright: ignore[reportRedeclaration]
            return f"{self.prefix} async direct {value}"

        @operation.call_fn
        def operation(self, value: int) -> str:
            return f"{self.prefix} ordinary direct {value}"

    class Child(Example):
        pass

    for example in (Example("first"), Child("second")):
        assert (
            assert_type(example.operation(42), str)
            == f"{example.prefix} ordinary direct 42"
        )
        assert Example.operation(example, 42) == f"{example.prefix} ordinary direct 42"
        assert (
            select(example.operation.select(42))[1]
            == f"{example.prefix} ordinary registration 42"
        )
        assert (
            select(Example.operation.select(example, 42))[1]
            == f"{example.prefix} ordinary registration 42"
        )

        async def main(example: Example = example) -> None:
            assert (
                assert_type(await example.operation.a(42), str)
                == f"{example.prefix} async direct 42"
            )
            assert (
                await Example.operation.a(example, 42)
                == f"{example.prefix} async direct 42"
            )
            assert (await select.a(example.operation.select(42)))[
                1
            ] == f"{example.prefix} async registration 42"
            assert (await select.a(Example.operation.select(example, 42)))[
                1
            ] == f"{example.prefix} async registration 42"

        async_runtime.run(main)


def test_ordinary_selectable_direct_overrides(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    @selectable
    @sync_contextmanager
    def operation(guard: SelectorGuard[int], value: int) -> Iterator[None]:
        raise AssertionError("direct calls must not register")
        yield

    def ordinary(value: int) -> int:
        return value + 1

    plain = operation.call(ordinary)
    assert assert_type(plain(41), int) == 42
    assert not hasattr(plain, "a")

    @routine
    async def direct(value: int) -> int:
        await async_runtime.checkpoint()
        return value + 2

    first = operation.call(direct).call_fn(ordinary)
    assert assert_type(first(41), int) == 42

    async def main() -> None:
        assert assert_type(await first.a(40), int) == 42
        # A direct coroutine implementation does not make registration async.
        with pytest.raises(TypeError, match="requires routine-based context managers"):
            await select.a(first.select(42))  # pyright: ignore[reportCallIssue, reportArgumentType]

    async_runtime.run(main)


def test_ordinary_selectable_direct_routine_method(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    class Example:
        def __init__(self, offset: int) -> None:
            self.offset = offset

        @selectable
        @sync_contextmanager
        def operation(self, guard: SelectorGuard[int], value: int) -> Iterator[None]:  # pyright: ignore[reportRedeclaration]
            raise AssertionError("direct calls must not register")
            yield

        @operation.call
        @routine
        async def operation(self, value: int) -> int:  # pyright: ignore[reportRedeclaration]
            return value + self.offset

        @operation.call_fn
        def operation(self, value: int) -> int:
            return value + self.offset + 1

    example = Example(2)
    assert assert_type(example.operation(39), int) == 42
    assert assert_type(Example.operation(example, 39), int) == 42

    async def main() -> None:
        assert assert_type(await example.operation.a(40), int) == 42
        assert assert_type(await Example.operation.a(example, 40), int) == 42

    async_runtime.run(main)


def test_selectable_call_preserves_routine_fn(async_runtime: AsyncRuntime) -> None:
    from .selectable import selectable

    @selectable
    @contextmanager
    async def operation(guard: SelectorGuard[str]) -> AsyncIterator[None]:
        raise AssertionError("direct calls must not register")
        yield

    @routine
    async def direct() -> str:  # pyright: ignore[reportRedeclaration]
        return "async"

    @direct.fn
    def direct() -> str:
        return "ordinary"

    configured = operation.call(direct)
    replaced = configured.call_fn(lambda: "replacement")
    assert configured() == "ordinary"
    assert replaced() == "replacement"

    @routine
    async def main() -> None:
        assert await configured.a() == "async"
        assert await replaced.a() == "async"
        with pytest.raises(RuntimeError, match="await function.a"):
            configured()

    async_runtime.run(main.a)


def test_selectable_overrides_reject_raw_coroutines() -> None:
    from .selectable import selectable

    @selectable
    @contextmanager
    async def operation(guard: SelectorGuard[int]) -> AsyncIterator[None]:
        raise AssertionError("not called")
        yield

    async def direct() -> int:
        return 42

    with pytest.raises(TypeError, match="@routine"):
        operation.call(direct)  # pyright: ignore[reportCallIssue, reportArgumentType]
    with pytest.raises(TypeError, match="ordinary callable"):
        operation.call(routine(direct)).call_fn(direct)  # pyright: ignore[reportCallIssue, reportArgumentType]


def test_selectable_ordinary_overrides_require_routines() -> None:
    from .selectable import selectable

    @selectable
    @sync_contextmanager
    def ordinary(guard: SelectorGuard[int], value: int) -> Iterator[None]:
        pass
        yield

    @selectable
    @contextmanager
    async def asynchronous(
        guard: SelectorGuard[int], value: int
    ) -> AsyncIterator[None]:
        pass
        yield

    def direct(value: int) -> int:
        return value

    @routine
    async def direct_routine(value: int) -> int:
        return value

    assert not hasattr(ordinary, "fn")
    assert hasattr(asynchronous, "fn")
    for operation in (ordinary, asynchronous):
        assert not hasattr(operation, "call_fn")
        plain = operation.call(direct)
        assert not hasattr(plain, "call_fn")
        customized = operation.call(direct_routine).call_fn(lambda value: value + 1)
        assert customized(41) == 42
        replaced = customized.call(direct)
        assert not hasattr(replaced, "call_fn")
        assert replaced(42) == 42
        assert customized(41) == 42
