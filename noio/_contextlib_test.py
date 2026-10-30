from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import assert_type

import pytest

from noio import ContextManager
from noio import contextmanager
from noio import routine
from noio._lock import Lock

from .conftest import AsyncRuntime
from .conftest import RunSync


class StubManager(ContextManager[int, bool]):
    def __init__(
        self,
        suppress: bool = False,
        checkpoint: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.__suppress = suppress
        self.__checkpoint = checkpoint
        self.exits: list[
            tuple[
                type[BaseException] | None, BaseException | None, TracebackType | None
            ]
        ] = []

    @routine
    async def __enter__(self) -> int:  # pyright: ignore[reportIncompatibleVariableOverride]
        if self.__checkpoint is not None:
            await self.__checkpoint()
        return 42

    @routine
    async def __exit__(  # pyright: ignore[reportIncompatibleVariableOverride]
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self.__checkpoint is not None:
            await self.__checkpoint()
        self.exits.append((exc_type, exc_value, traceback))
        return self.__suppress


class ChildManager(StubManager):
    pass


class ChildLock(Lock):
    pass


def test_context_manager_remains_abstract() -> None:
    assert ContextManager.__abstractmethods__ == {"__enter__", "__exit__"}
    with pytest.raises(TypeError, match="abstract"):
        ContextManager[int]()  # pyright: ignore[reportAbstractUsage]


def _check_sync(checkpoint: Callable[[], None]) -> None:
    stub = ChildManager()
    manager: ContextManager[int, bool] = stub
    with manager as value:
        assert assert_type(value, int) == 42
        checkpoint()
    assert stub.exits == [(None, None, None)]
    error = ValueError("body failed")
    with pytest.raises(ValueError) as raised, manager:
        raise error
    assert raised.value is error
    assert stub.exits[-1][0] is ValueError
    assert stub.exits[-1][1] is error
    assert stub.exits[-1][2] is not None
    with ChildManager(suppress=True):
        raise error
    assert assert_type(stub.__exit__(None, None, None), bool) is False

    @routine
    async def nested() -> None:
        async with manager as value:
            assert assert_type(value, int) == 42

    nested()
    lock = ChildLock()
    with lock as entered:
        assert assert_type(entered, ChildLock) is lock

    @routine
    async def nested_lock() -> None:
        async with lock as entered:
            assert assert_type(entered, ChildLock) is lock

    nested_lock()


def test_context_manager_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_context_manager_async(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        stub = ChildManager(checkpoint=async_runtime.checkpoint)
        manager: ContextManager[int, bool] = stub
        async with manager as value:
            assert assert_type(value, int) == 42
        assert stub.exits == [(None, None, None)]
        error = ValueError("body failed")
        with pytest.raises(ValueError) as raised:
            async with manager:
                raise error
        assert raised.value is error
        assert stub.exits[-1][0] is ValueError
        assert stub.exits[-1][1] is error
        assert stub.exits[-1][2] is not None
        async with ChildManager(True, async_runtime.checkpoint):
            raise error
        assert assert_type(await stub.__aexit__(None, None, None), bool) is False
        lock = ChildLock()
        async with lock as entered:
            assert assert_type(entered, ChildLock) is lock
        await lock.__aenter__()
        assert assert_type(await lock.__aexit__(None, None, None), None) is None

    async_runtime.run(main)


@routine
async def exercise_generator(checkpoint: Callable[[], Awaitable[None]]) -> None:
    calls: list[str] = []

    @contextmanager
    async def resource(value: int, *, scale: int = 1) -> AsyncIterator[int]:
        calls.append("enter")
        await checkpoint()
        try:
            yield value * scale
        finally:
            await checkpoint()
            calls.append("exit")

    manager = resource(3, scale=2)
    assert calls == []
    assert resource.__name__ == "resource"
    async with manager as value:
        assert assert_type(value, int) == 6
        assert calls == ["enter"]
    assert calls == ["enter", "exit"]

    for error in (
        ValueError("body"),
        StopIteration("body"),
        StopAsyncIteration("body"),
    ):
        with pytest.raises(type(error)) as raised:
            async with resource(1):
                raise error
        assert raised.value is error
        assert calls[-2:] == ["enter", "exit"]

    @contextmanager
    async def suppress() -> AsyncIterator[None]:
        try:
            yield
        except ValueError:
            calls.append("suppressed")

    async with suppress():
        raise ValueError("body")
    assert calls[-1] == "suppressed"

    @contextmanager
    async def too_many() -> AsyncIterator[None]:
        try:
            yield
            yield
        finally:
            calls.append("closed")

    with pytest.raises(RuntimeError, match="didn't stop"):
        async with too_many():
            pass
    assert calls[-1] == "closed"

    @contextmanager
    async def too_few() -> AsyncIterator[int]:
        for value in ():
            yield value

    with pytest.raises(RuntimeError, match="didn't yield"):
        async with too_few():
            pytest.fail("body must not run")

    error = ValueError("cleanup")

    @contextmanager
    async def bad_cleanup() -> AsyncIterator[None]:
        try:
            yield
        finally:
            raise error

    with pytest.raises(ValueError) as raised:
        async with bad_cleanup():
            raise KeyError("body")
    assert raised.value is error
    assert isinstance(error.__context__, KeyError)


def _check_generator_sync(checkpoint: Callable[[], None]) -> None:
    async def green_checkpoint() -> None:
        checkpoint()

    exercise_generator(green_checkpoint)
    lock = Lock()
    calls: list[str] = []

    @contextmanager
    async def resource() -> AsyncIterator[Lock]:
        async with lock:
            try:
                yield lock
            finally:
                calls.append("exit")

    with resource() as entered:
        assert assert_type(entered, Lock) is lock
        checkpoint()
    with pytest.raises(ValueError), resource():
        raise ValueError("body")
    assert calls == ["exit", "exit"]
    with lock:
        pass


def test_generator_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_generator_sync)


def test_generator_async(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        await exercise_generator.a(async_runtime.checkpoint)

    async_runtime.run(main)


@pytest.mark.parametrize("decorated", [False, True])
def test_generator_cancellation_runs_cleanup(
    async_runtime: AsyncRuntime, decorated: bool
) -> None:
    calls: list[str] = []

    @contextmanager
    async def resource() -> AsyncIterator[None]:
        try:
            yield
        finally:
            calls.append("exit")

    @routine
    async def body() -> None:
        await async_runtime.checkpoint()

    async def cancelled() -> None:
        if decorated:
            await resource()(body).a()
        else:
            async with resource():
                await body.a()

    async def main() -> None:
        await async_runtime.cancel(cancelled)
        assert calls == ["exit"]

    async_runtime.run(main)


@routine
async def exercise_decorator(checkpoint: Callable[[], Awaitable[None]]) -> None:
    calls: list[str] = []

    @contextmanager
    async def resource() -> AsyncIterator[None]:
        await checkpoint()
        calls.append("enter")
        try:
            yield
        finally:
            calls.append("exit")
            await checkpoint()

    manager = resource()

    @manager
    @routine
    async def scaled(value: int, *, scale: int = 1) -> int:
        calls.append("body")
        return value * scale

    @manager
    @routine
    async def asynchronous(value: int) -> int:
        await checkpoint()
        calls.append("body")
        return value

    @manager
    @manager
    @routine
    async def nested() -> int:
        return 42

    assert calls == []
    assert scaled.__name__ == "scaled"
    for _ in range(2):
        assert assert_type(await scaled.a(3, scale=2), int | None) == 6
        assert calls[-3:] == ["enter", "body", "exit"]
        assert assert_type(await asynchronous.a(7), int | None) == 7
        assert calls[-3:] == ["enter", "body", "exit"]
        assert await nested.a() == 42
        assert calls[-4:] == ["enter", "enter", "exit", "exit"]

    error = ValueError("body")

    @manager
    @routine
    async def failing() -> None:
        raise error

    with pytest.raises(ValueError) as raised:
        await failing.a()
    assert raised.value is error
    assert calls[-2:] == ["enter", "exit"]

    @contextmanager
    async def suppress() -> AsyncIterator[None]:
        try:
            yield
        except ValueError:
            calls.append("suppressed")

    assert await suppress()(failing).a() is None
    assert calls[-1] == "suppressed"


def _check_decorator_sync(checkpoint: Callable[[], None]) -> None:
    async def green_checkpoint() -> None:
        checkpoint()

    exercise_decorator(green_checkpoint)
    lock = Lock()

    @contextmanager
    async def locked() -> AsyncIterator[None]:
        async with lock:
            yield

    @locked()
    @routine
    async def ordinary(value: int) -> int:
        checkpoint()
        return value

    @locked()
    @routine
    async def asynchronous(value: int) -> int:
        await green_checkpoint()
        return value

    for _ in range(2):
        assert assert_type(ordinary(3), int | None) == 3
        assert assert_type(asynchronous(7), int | None) == 7
    with lock:
        pass


def test_decorator_sync(sync_runtime: RunSync) -> None:
    sync_runtime(_check_decorator_sync)


def test_decorator_async(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        await exercise_decorator.a(async_runtime.checkpoint)

    async_runtime.run(main)


def test_decorator_rejects_non_routines() -> None:
    @contextmanager
    async def resource() -> AsyncIterator[None]:
        pytest.fail("context must not be entered")
        yield

    def ordinary() -> None:
        pytest.fail("body must not run")

    async def asynchronous() -> None:
        pytest.fail("body must not run")

    for fn in (ordinary, asynchronous, object()):
        with pytest.raises(TypeError, match="require a routine"):
            resource()(fn)  # pyright: ignore[reportArgumentType]


@routine
async def exercise_override(expected: str) -> None:
    from collections.abc import Iterator

    from noio import sleep

    log: list[str] = []

    @contextmanager
    async def resource(  # pyright: ignore[reportRedeclaration]
        value: int, *, scale: int = 1
    ) -> AsyncIterator[int]:
        log.append("async enter")
        await sleep.a(0)
        try:
            yield value * scale
        except ValueError:
            log.append("async suppress")
        finally:
            await sleep.a(0)
            log.append("async exit")

    original = resource

    @resource.fn
    def resource(value: int, *, scale: int = 1) -> Iterator[int]:
        log.append("ordinary enter")
        # The override's body must run with routine mode cleared.
        sleep(0)
        try:
            yield value * scale
        except ValueError:
            log.append("ordinary suppress")
        finally:
            sleep(0)
            log.append("ordinary exit")

    manager = resource(3, scale=2)
    assert log == []
    assert resource.__name__ == "resource"
    async with manager as value:
        assert assert_type(value, int) == 6
    assert log == [f"{expected} enter", f"{expected} exit"]
    with pytest.raises(RuntimeError, match="already been entered"):
        async with manager:
            pytest.fail("a generator context manager is single-use")

    async with resource(1):
        raise ValueError("suppressed")
    assert log[-3:] == [f"{expected} enter", f"{expected} suppress", f"{expected} exit"]
    error = KeyError("body")
    with pytest.raises(KeyError) as raised:
        async with resource(1):
            raise error
    assert raised.value is error
    assert log[-2:] == [f"{expected} enter", f"{expected} exit"]

    @resource(4, scale=2)
    @routine
    async def decorated(value: int) -> int:
        return value

    for _ in range(2):
        assert await decorated.a(42) == 42
        assert log[-2:] == [f"{expected} enter", f"{expected} exit"]
    # Adding an override produces a new factory.
    async with original(1):
        pass
    assert log[-2:] == ["async enter", "async exit"]


def _check_override(checkpoint: Callable[[], None]) -> None:
    exercise_override("ordinary")


def test_contextmanager_override_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_override)


def test_contextmanager_override_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(lambda: exercise_override.a("async"))


def test_override_selected_at_entry_and_retained_for_exit(
    async_runtime: AsyncRuntime,
) -> None:
    from collections.abc import Iterator

    from noio import sleep

    log: list[str] = []

    @contextmanager
    async def resource() -> AsyncIterator[int]:  # pyright: ignore[reportRedeclaration]
        log.append("async enter")
        yield 1
        log.append("async exit")

    @resource.fn
    def resource() -> Iterator[int]:
        log.append("ordinary enter")
        sleep(0)
        yield 2
        sleep(0)
        log.append("ordinary exit")

    # Construction in ordinary code must not select the ordinary override.
    prepared = resource()

    async def main() -> None:
        async with prepared as value:
            assert value == 1
        assert log == ["async enter", "async exit"]
        # Likewise, construction in a coroutine permits ordinary entry.
        with resource() as value:
            assert value == 2
        assert log[-2:] == ["ordinary enter", "ordinary exit"]

        ordinary = resource()
        assert ordinary.__enter__() == 2
        await ordinary.__aexit__(None, None, None)
        assert log[-2:] == ["ordinary enter", "ordinary exit"]

        asynchronous = resource()
        assert await asynchronous.__aenter__() == 1
        asynchronous.__exit__(None, None, None)
        assert log[-2:] == ["async enter", "async exit"]

    async_runtime.run(main)


def test_override_methods(async_runtime: AsyncRuntime) -> None:
    from collections.abc import Iterator

    log: list[str] = []

    class Resource:
        def __init__(self, value: int) -> None:
            self.value = value

        @contextmanager
        async def open(  # pyright: ignore[reportRedeclaration]
            self, *, offset: int = 0
        ) -> AsyncIterator[int]:
            log.append("async")
            yield self.value + offset

        @open.fn
        def open(self, *, offset: int = 0) -> Iterator[int]:
            log.append("ordinary")
            yield self.value + offset

    class Child(Resource):
        @classmethod
        @contextmanager
        async def named(cls) -> AsyncIterator[str]:
            yield cls.__name__

        @staticmethod
        @contextmanager
        async def fixed() -> AsyncIterator[int]:
            yield 7

    first = Child(10)
    second = Child(20)
    with first.open(offset=1) as value:
        assert assert_type(value, int) == 11
    with Resource.open(second, offset=2) as value:
        assert value == 22
    assert log == ["ordinary", "ordinary"]

    async def main() -> None:
        async with first.open(offset=3) as value:
            assert assert_type(value, int) == 13
        async with second.open() as value:
            assert value == 20
        async with Child.named() as name:
            assert assert_type(name, str) == "Child"
        async with Child.fixed() as value:
            assert assert_type(value, int) == 7
        assert log[-2:] == ["async", "async"]

    async_runtime.run(main)


def test_inner_method_descriptors(async_runtime: AsyncRuntime) -> None:
    from collections.abc import Iterator
    from inspect import signature
    from inspect import unwrap
    from typing import Any

    class Resource:
        @contextmanager
        @classmethod
        async def named(cls, suffix: str = "") -> AsyncIterator[str]:  # pyright: ignore[reportRedeclaration]
            yield f"async {cls.__name__}{suffix}"

        original_named = named

        @named.fn
        @classmethod
        def named(cls, suffix: str = "") -> Iterator[str]:
            yield f"ordinary {cls.__name__}{suffix}"

        @contextmanager
        @staticmethod
        async def fixed(value: int) -> AsyncIterator[int]:  # pyright: ignore[reportRedeclaration]
            yield value

        @fixed.fn
        @staticmethod
        def fixed(value: int) -> Iterator[int]:
            yield value * 2

    class Child(Resource):
        pass

    instance = Child()
    with Child.named("!") as name:
        assert assert_type(name, str) == "ordinary Child!"
    with instance.named() as name:
        assert assert_type(name, str) == "ordinary Child"
    with Child.original_named() as name:
        assert name == "async Child"
    with Child.fixed(3) as value:
        assert assert_type(value, int) == 6
    # Like Routine, the factory type does not retain the staticmethod marker.
    dynamic_instance: Any = instance
    with dynamic_instance.fixed(4) as value:
        assert value == 8
    assert Child.named.__name__ == "named"
    assert list(signature(Child.named).parameters) == ["suffix"]
    assert list(signature(Child.fixed).parameters) == ["value"]
    assert unwrap(Child.fixed) is unwrap(Resource.fixed)

    async def main() -> None:
        async with Child.named("!") as name:
            assert assert_type(name, str) == "async Child!"
        async with instance.named() as name:
            assert assert_type(name, str) == "async Child"
        async with Child.fixed(3) as value:
            assert assert_type(value, int) == 3
        async with dynamic_instance.fixed(4) as value:
            assert value == 4

    async_runtime.run(main)


def test_override_rejects_async_implementations() -> None:
    @contextmanager
    async def resource() -> AsyncIterator[None]:
        yield

    async def generator() -> AsyncIterator[None]:
        yield

    async def coroutine() -> None:
        pass

    for invalid in (generator, coroutine, object()):
        with pytest.raises(TypeError, match="ordinary context-manager implementation"):
            resource.fn(invalid)  # pyright: ignore[reportArgumentType]


def test_override_typing_rejects_incompatible_implementations(tmp_path: Path) -> None:
    import json
    import subprocess
    import sys

    source = """from collections.abc import AsyncIterator, Iterator
from typing import assert_type
from noio import contextmanager

@contextmanager
async def resource(value: int, *, scale: int = 1) -> AsyncIterator[int]:
    yield value * scale

def broader(value: object, *, scale: object = 1) -> Iterator[int]:
    yield 1

def wrong_argument(value: str, *, scale: int = 1) -> Iterator[int]:
    yield 1

def wrong_result(value: int, *, scale: int = 1) -> Iterator[str]:
    yield "wrong"

configured = resource.fn(broader)
with configured(2, scale=3) as value:
    assert_type(value, int)
resource.fn(wrong_argument)  # reject
resource.fn(wrong_result)  # reject
configured("wrong")  # reject
configured(1, unknown=True)  # reject
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
