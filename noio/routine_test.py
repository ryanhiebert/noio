import asyncio
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from functools import partial
from inspect import signature
from inspect import unwrap
from threading import Event
from threading import get_ident
from types import CoroutineType
from typing import Any
from typing import Self
from typing import assert_type

import pytest

from noio import cleared_mode
from noio import routine

from .conftest import AsyncRuntime
from .conftest import RunSync
from .conftest import SyncCheck
from .routine import _Mode
from .routine import _mode


def test_abstract_routine(async_runtime: AsyncRuntime):
    class Base(ABC):
        @routine
        @abstractmethod
        async def calculate(self, value: int) -> int:
            return value * 2

    class Incomplete(Base):
        pass

    for cls in (Base, Incomplete):
        with pytest.raises(TypeError, match="abstract"):
            cls()  # pyright: ignore[reportAbstractUsage]

    class Concrete(Base):
        @routine
        async def calculate(self, value: int) -> int:  # pyright: ignore[reportIncompatibleVariableOverride]
            return await super().calculate.a(value) + 1

    class Child(Concrete):
        pass

    def ordinary(instance: Base) -> int:
        return assert_type(instance.calculate(3), int)

    async def awaited(instance: Base) -> int:
        return assert_type(await instance.calculate.a(3), int)

    for instance in (Concrete(), Child()):
        assert ordinary(instance) == 7
        assert async_runtime.run(partial(awaited, instance)) == 7


@pytest.mark.parametrize("descriptor", [classmethod, staticmethod])
def test_abstract_routine_descriptors(descriptor: Any):
    async def abstract(*args: object) -> int:
        raise NotImplementedError

    class Base(ABC):  # noqa: B024
        method = routine(descriptor(abstractmethod(abstract)))

    class Incomplete(Base):
        pass

    for cls in (Base, Incomplete):
        assert cls.__abstractmethods__ == {"method"}
        with pytest.raises(TypeError, match="abstract"):
            cls()

    async def concrete(*args: object) -> int:
        return 42

    class Concrete(Base):
        method = routine(descriptor(concrete))

    assert Concrete().method() == 42
    assert asyncio.run(Concrete.method.a()) == 42


def test_routine_rejects_regular_functions():
    calls = []

    async def number() -> int:
        return 42

    def ordinary() -> int:
        calls.append(True)
        return 42

    def make_coroutine() -> Coroutine[Any, Any, int]:
        calls.append(True)
        return number()

    for function in [ordinary, make_coroutine]:
        invalid: Any = function
        with pytest.raises(TypeError, match="@routine requires an async function"):
            routine(invalid)
    assert calls == []


def test_routine_methods_bind_both_call_forms():
    class Calculator:
        def __init__(self, offset: int):
            self.__offset = offset

        @routine
        async def calculate(self, value: int, /, *, scale: int = 2) -> int:
            return (value + self.__offset) * scale

    class Child(Calculator):
        pass

    first = Child(3)
    second = Calculator(5)
    assert assert_type(first.calculate(4, scale=3), int) == 21
    assert second.calculate(4) == 18
    assert Calculator.calculate(first, 4) == 14
    assert list(signature(first.calculate).parameters) == ["value", "scale"]
    assert first.calculate.__name__ == "calculate"

    @routine
    async def caller() -> int:
        first_result = await assert_type(
            first.calculate.a(4), CoroutineType[Any, Any, int]
        )
        second_result = await second.calculate.a(4, scale=3)
        unbound_result = await Calculator.calculate.a(first, 4)
        return first_result + second_result + unbound_result

    assert caller() == 55


def test_routine_metadata_and_argument_signature_are_preserved():
    async def original(value: int, /, *, scale: int = 2) -> int:
        """Multiply a value."""
        return value * scale

    wrapped = routine(original)
    assert unwrap(wrapped) is original
    assert signature(wrapped) == signature(original)
    assert wrapped.__name__ == "original"
    assert wrapped.__doc__ == original.__doc__
    assert assert_type(wrapped(3, scale=4), int) == 12


def test_classmethods_preserve_class_binding_and_self_return_types():
    class Factory:
        def __init__(self, value: int):
            self.value = value

        @routine
        @classmethod
        async def create(cls, value: int, /, *, scale: int = 2) -> Self:
            """Create an instance."""
            return cls(value * scale)

    class Child(Factory):
        pass

    instance = Child(0)
    assert assert_type(Factory.create(3), Factory).value == 6
    assert type(assert_type(Child.create(3), Child)) is Child
    assert assert_type(instance.create(3, scale=4), Child).value == 12
    call = assert_type(Child.create.a(3), CoroutineType[Any, Any, Child])

    @routine
    async def caller() -> Child:
        result = assert_type(await call, Child)
        assert type(result) is Child
        assert result.value == 6
        result = assert_type(await instance.create.a(3, scale=4), Child)
        assert result.value == 12
        base = assert_type(await Factory.create.a(3), Factory)
        assert type(base) is Factory
        return result

    assert caller().value == 12
    assert list(signature(Child.create).parameters) == ["value", "scale"]
    assert list(signature(instance.create).parameters) == ["value", "scale"]
    assert Child.create.__name__ == "create"
    assert Child.create.__doc__ == "Create an instance."
    assert unwrap(Child.create).__self__ is Child


def test_classmethod_async_calls_are_lazy(async_runtime: AsyncRuntime):
    calls = []

    class Factory:
        @routine
        @classmethod
        async def identify(cls) -> str:
            calls.append(cls)
            await async_runtime.checkpoint()
            return cls.__name__

    class Child(Factory):
        pass

    call = Child.identify.a()
    assert calls == []
    assert async_runtime.run(lambda: call) == "Child"
    assert calls == [Child]


def test_staticmethods_preserve_arguments_types_and_metadata():
    class Calculator:
        @routine
        @staticmethod
        async def calculate(value: int, /, *, scale: int = 2) -> int:
            """Multiply a value."""
            return value * scale

    class Child(Calculator):
        pass

    # Type checkers lose the staticmethod marker through the outer decorator.
    instance: Any = Child()
    assert assert_type(Calculator.calculate(3), int) == 6
    assert assert_type(Child.calculate(3, scale=4), int) == 12
    assert instance.calculate(3) == 6

    @routine
    async def caller() -> int:
        with pytest.raises(RuntimeError, match="Use await function.a"):
            instance.calculate(3)
        call = assert_type(Child.calculate.a(3), CoroutineType[Any, Any, int])
        return (
            assert_type(await call, int)
            + await Calculator.calculate.a(3)
            + await instance.calculate.a(3, scale=4)
        )

    assert caller() == 24
    assert Calculator.calculate is instance.calculate
    assert list(signature(instance.calculate).parameters) == ["value", "scale"]
    assert instance.calculate.__name__ == "calculate"
    assert instance.calculate.__doc__ == "Multiply a value."
    assert unwrap(instance.calculate) is Calculator.calculate.__wrapped__


def test_staticmethod_async_calls_are_lazy(async_runtime: AsyncRuntime):
    calls = []

    class Calculator:
        @routine
        @staticmethod
        async def calculate(value: int) -> int:
            calls.append(value)
            await async_runtime.checkpoint()
            return value * 2

    calculator: Any = Calculator()
    call = calculator.calculate.a(3)
    assert calls == []
    assert async_runtime.run(lambda: call) == 6
    assert calls == [3]


@pytest.mark.parametrize("descriptor", [classmethod, staticmethod])
def test_routine_rejects_regular_methods(descriptor: Any):
    def ordinary(cls: type) -> None:
        pass

    invalid: Any = descriptor(ordinary)
    with pytest.raises(TypeError, match="@routine requires an async function"):
        routine(invalid)


def test_classmethod_calls_follow_routine_context_rules():
    calls = []

    class Factory:
        @classmethod
        def ordinary(cls) -> None:
            calls.append("ordinary")

        @routine
        @classmethod
        async def asynchronous(cls) -> None:
            calls.append("asynchronous")

    @routine
    async def caller() -> None:
        Factory.ordinary()
        with pytest.raises(RuntimeError, match="Use await function.a"):
            Factory.asynchronous()
        await Factory.asynchronous.a()

    caller()
    assert calls == ["ordinary", "asynchronous"]


def test_concurrent_await_of_one_coroutine_is_rejected(async_runtime: AsyncRuntime):
    async def main() -> None:
        entered = async_runtime.event()
        release = async_runtime.event()

        @routine
        async def blocked() -> int:
            entered.set()
            await release.wait()
            return 42

        call = blocked.a()

        async def first() -> None:
            assert await call == 42

        async def second() -> None:
            await entered.wait()
            try:
                with pytest.raises(
                    RuntimeError, match="coroutine is being awaited already"
                ):
                    await call
            finally:
                release.set()

        await async_runtime.concurrently(first, second)

    async_runtime.run(main)


def _check_values(checkpoint: Callable[[], None]) -> None:
    caller = get_ident()

    def ordinary(value: int, /, *, scale: int = 2) -> int:
        checkpoint()
        assert get_ident() == caller
        return value * scale

    @routine
    async def inner(value: int) -> int:
        checkpoint()
        assert get_ident() == caller
        return value + 1

    @routine
    async def outer(value: int) -> int:
        return await inner.a(value)

    assert ordinary(3, scale=4) == 12
    assert outer(3) == 4
    assert _mode.get() is None


def _check_lazy_calls(checkpoint: Callable[[], None]) -> None:
    calls = []

    @routine
    async def number(value: int) -> int:
        checkpoint()
        calls.append(value)
        return value

    call = assert_type(number.a(3), CoroutineType[Any, Any, int])
    assert isinstance(call, CoroutineType)
    assert calls == []

    @routine
    async def caller() -> int:
        value = assert_type(await call, int)
        with pytest.raises(
            RuntimeError, match="cannot reuse already awaited coroutine"
        ):
            await call
        return value

    assert caller() == 3
    assert number(4) == 4
    assert calls == [3, 4]


def _check_context_and_call_rules(checkpoint: Callable[[], None]) -> None:
    context = ContextVar("context", default="caller")
    calls = []

    def ordinary() -> None:
        calls.append("ordinary")

    @routine
    async def inner() -> int:
        assert context.get() == "caller"
        checkpoint()
        assert _mode.get() is not None
        context.set("changed")
        ordinary()
        return 42

    def helper() -> int:
        return inner()

    @routine
    async def outer() -> int:
        with pytest.raises(RuntimeError, match="Use await function.a"):
            helper()
        return await inner.a()

    assert outer() == 42
    assert calls == ["ordinary"]
    assert context.get() == "changed"
    assert _mode.get() is None
    ordinary()
    assert calls == ["ordinary", "ordinary"]


def _check_exceptions(checkpoint: Callable[[], None]) -> None:
    error = ValueError("broken")
    finalized = []

    def ordinary() -> None:
        checkpoint()
        raise error

    @routine
    async def inner() -> None:
        ordinary()

    @routine
    async def outer() -> None:
        try:
            await inner.a()
        finally:
            finalized.append(True)

    with pytest.raises(ValueError) as raised:
        outer()
    assert raised.value is error
    assert _mode.get() is None
    assert finalized == [True]


def _check_suspension_not_implemented(checkpoint: Callable[[], None]) -> None:
    finalized = []

    @routine
    async def asynchronous() -> None:
        try:
            checkpoint()
            await asyncio.sleep(0)
        finally:
            finalized.append(True)

    with pytest.raises(
        NotImplementedError,
        match="Suspension in ordinary routine calls is not implemented",
    ):
        asynchronous()
    assert finalized == [True]
    assert _mode.get() is None


@pytest.mark.parametrize(
    "check",
    [
        _check_values,
        _check_lazy_calls,
        _check_context_and_call_rules,
        _check_exceptions,
        _check_suspension_not_implemented,
    ],
    ids=lambda check: check.__name__.removeprefix("_check_"),
)
def test_normal_calls(sync_runtime: RunSync, check: SyncCheck):
    # Checks must be module-level so gevent subprocesses can import them by name.
    sync_runtime(check)


def test_awaited_calls(async_runtime: AsyncRuntime):
    calls = []
    context = ContextVar("context", default="caller")

    def ordinary(value: int) -> int:
        calls.append("ordinary")
        return value + 1

    @routine
    async def inner(value: int) -> int:
        calls.append("inner")
        assert context.get() == "outer"
        with pytest.raises(RuntimeError, match="Use await function.a"):
            inner(value)
        await async_runtime.checkpoint()
        assert _mode.get() is not None
        context.set("inner")
        return ordinary(value)

    @routine
    async def outer(value: int) -> int:
        context.set("outer")
        return await inner.a(value)

    async def main() -> None:
        call = outer.a(3)
        assert calls == []
        assert await call == 4
        assert calls == ["inner", "ordinary"]
        assert context.get() == "inner"
        assert _mode.get() is None
        assert ordinary(4) == 5
        with pytest.raises(
            RuntimeError, match="cannot reuse already awaited coroutine"
        ):
            await call

    async_runtime.run(main)
    assert context.get() == "caller"
    assert _mode.get() is None
    assert calls == ["inner", "ordinary", "ordinary"]


def test_awaited_exceptions(async_runtime: AsyncRuntime):
    error = ValueError("broken")
    finalized = []

    @routine
    async def inner() -> None:
        await async_runtime.checkpoint()
        raise error

    @routine
    async def outer() -> None:
        try:
            await inner.a()
        finally:
            finalized.append(True)

    async def main() -> None:
        with pytest.raises(ValueError) as raised:
            await outer.a()
        assert raised.value is error
        assert finalized == [True]
        assert _mode.get() is None

    async_runtime.run(main)


def test_cancellation(async_runtime: AsyncRuntime):
    finalized = []

    @routine
    async def blocked() -> None:
        try:
            await async_runtime.checkpoint()
            pytest.fail("Cancellation was not delivered")
        finally:
            finalized.append(True)

    async def caller() -> None:
        try:
            await blocked.a()
        finally:
            assert _mode.get() is None

    async def main() -> None:
        await async_runtime.cancel(caller)
        assert finalized == [True]
        assert _mode.get() is None

    async_runtime.run(main)
    assert _mode.get() is None


def test_context_is_task_local(async_runtime: AsyncRuntime):
    context = ContextVar("task_context", default=0)

    @routine
    async def child(value: int) -> None:
        context.set(value)
        await async_runtime.checkpoint()
        assert context.get() == value
        assert _mode.get() is not None

    async def observer() -> None:
        await async_runtime.checkpoint()
        assert context.get() == 0
        assert _mode.get() is None

    async def main() -> None:
        await async_runtime.concurrently(
            lambda: child.a(3), lambda: child.a(5), observer
        )
        assert context.get() == 0
        assert _mode.get() is None

    async_runtime.run(main)


def _check_gevent_contexts(checkpoint: Callable[[], None]) -> None:
    import gevent

    context = ContextVar("greenlet_context", default=0)

    def ordinary(value: int) -> int:
        assert _mode.get() is None
        checkpoint()
        assert context.get() == value
        assert _mode.get() is None
        return value

    @routine
    async def asynchronous(value: int) -> int:
        context.set(value)
        checkpoint()
        assert context.get() == value
        assert _mode.get() is not None
        return value

    def caller(value: int) -> int:
        assert asynchronous(value) == value
        assert _mode.get() is None
        return ordinary(value)

    first = gevent.spawn(caller, 3)
    second = gevent.spawn(caller, 5)
    assert first.get(timeout=2) == 3
    assert second.get(timeout=2) == 5
    assert context.get() == 0
    assert _mode.get() is None


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_context_is_greenlet_local(sync_runtime: RunSync):
    sync_runtime(_check_gevent_contexts)


def test_fn_returns_new_routine_preserving_original() -> None:
    async def original(value: int, /, *, scale: int = 2) -> int:
        """Calculate a value."""
        return value * scale

    def ordinary(value: int, /, *, scale: int = 2) -> int:
        return value * scale + 1

    initial = routine(original)
    changed = initial.fn(ordinary)
    assert changed is not initial
    assert initial(3) == 6
    assert assert_type(changed(3, scale=4), int) == 13
    assert asyncio.run(changed.a(3, scale=4)) == 12
    assert unwrap(changed) is original
    assert signature(changed) == signature(initial)
    assert changed.__name__ == initial.__name__
    assert changed.__doc__ == initial.__doc__

    replaced = changed.fn(lambda value, *, scale=2: value * scale + 2)
    assert replaced is not changed
    assert replaced(3) == 8
    assert changed(3) == 7


def test_fn_rejects_coroutine_function() -> None:
    @routine
    async def operation() -> None:
        pass

    async def invalid() -> None:
        pass

    bad: Any = invalid
    with pytest.raises(TypeError, match="ordinary callable"):
        operation.fn(bad)


def _check_fn_composition(checkpoint: Callable[[], None]) -> None:
    calls: list[str] = []
    marker = ContextVar("fn context", default="before")

    @routine
    async def inner() -> int:
        calls.append("inner")
        checkpoint()
        return 42

    @routine
    async def operation(fail: bool = False) -> int:  # pyright: ignore[reportRedeclaration]
        pytest.fail("ordinary execution must use the override")

    @operation.fn
    def operation(fail: bool = False) -> int:
        assert _mode.get() is None
        value = inner()
        marker.set("ordinary")
        if fail:
            raise ValueError("override failed")
        return value

    @routine
    async def outer() -> int:
        with pytest.raises(RuntimeError, match="Use await"):
            operation()
        with pytest.raises(ValueError, match="override failed"):
            await operation.a(True)
        assert _mode.get() is not None
        assert marker.get() == "ordinary"
        result = await operation.a()
        assert _mode.get() is not None
        return result

    assert operation() == 42
    assert outer() == 42
    assert calls == ["inner", "inner", "inner"]
    assert _mode.get() is None
    assert marker.get() == "ordinary"


def test_fn_composition(sync_runtime: RunSync) -> None:
    sync_runtime(_check_fn_composition)


def test_fn_async_mode_is_lazy_and_uses_coroutine(
    async_runtime: AsyncRuntime,
) -> None:
    calls: list[str] = []

    @routine
    async def operation() -> int:  # pyright: ignore[reportRedeclaration]
        calls.append("coroutine")
        await async_runtime.checkpoint()
        return 42

    @operation.fn
    def operation() -> int:
        calls.append("ordinary")
        return 7

    call = operation.a()
    assert not calls

    async def main() -> None:
        assert assert_type(await call, int) == 42
        assert operation() == 7
        assert await operation.a() == 42

    async_runtime.run(main)
    assert calls == ["coroutine", "ordinary", "coroutine"]


def test_fn_method_binding_and_inheritance(async_runtime: AsyncRuntime) -> None:
    class Base:
        def __init__(self, value: int) -> None:
            self.value = value

        @routine
        async def calculate(self, scale: int = 2) -> int:  # pyright: ignore[reportRedeclaration]
            return self.value * scale

        @calculate.fn
        def calculate(self, scale: int = 2) -> int:
            return self.value * scale + 1

    class Child(Base):
        @Base.calculate.fn
        def calculate(self: Base, scale: int = 2) -> int:
            return self.value * scale + 2

    base, child = Base(3), Child(5)
    assert assert_type(base.calculate(), int) == 7
    assert assert_type(child.calculate(), int) == 12
    assert Base.calculate(child, 3) == 16
    assert Child.calculate(child, 3) == 17
    assert base.calculate() == 7

    async def main() -> None:
        assert await base.calculate.a() == 6
        assert await child.calculate.a() == 10

    async_runtime.run(main)


def test_fn_classmethod_and_staticmethod_binding(async_runtime: AsyncRuntime) -> None:
    class Base:
        @routine
        @classmethod
        async def identify(cls, value: int) -> tuple[type, int]:  # pyright: ignore[reportRedeclaration]
            return cls, value

        @identify.fn
        @classmethod
        def identify(cls, value: int) -> tuple[type, int]:
            return cls, value + 1

        @routine
        @staticmethod
        async def double(value: int) -> int:  # pyright: ignore[reportRedeclaration]
            return value * 2

        @double.fn
        @staticmethod
        def double(value: int) -> int:
            return value * 2 + 1

    class Child(Base):
        pass

    instance: Any = Child()
    assert Base.identify(2) == (Base, 3)
    assert Child.identify(2) == (Child, 3)
    assert instance.identify(2) == (Child, 3)
    assert Child.double(2) == instance.double(2) == 5

    async def main() -> None:
        assert await Child.identify.a(2) == (Child, 2)
        assert await instance.identify.a(2) == (Child, 2)
        assert await instance.double.a(2) == 4

    async_runtime.run(main)


def test_fn_same_routine_across_thread_and_async_runtime(
    async_runtime: AsyncRuntime,
) -> None:
    entered, release = Event(), Event()

    @routine
    async def operation() -> str:  # pyright: ignore[reportRedeclaration]
        assert entered.is_set()
        await async_runtime.checkpoint()
        return "coroutine"

    @operation.fn
    def operation() -> str:
        entered.set()
        assert release.wait(5)
        return "ordinary"

    async def main() -> None:
        assert await operation.a() == "coroutine"

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(operation)
        try:
            assert entered.wait(5)
            async_runtime.run(main)
        finally:
            release.set()
        assert future.result(timeout=5) == "ordinary"


def test_fn_preserves_broader_arguments(async_runtime: AsyncRuntime) -> None:
    @routine
    async def calculate(value: int, /, *, scale: int = 2) -> int:  # pyright: ignore[reportRedeclaration]
        return value * scale

    @calculate.fn
    def calculate(value: int | str, /, *, scale: int = 2, offset: int = 0) -> int:
        return int(value) * scale + offset

    assert assert_type(calculate("3", scale=4, offset=1), int) == 13

    @routine
    async def caller() -> int:
        return await calculate.a(3, scale=4)

    assert caller() == 12

    async def main() -> None:
        assert assert_type(await calculate.a(3, scale=4), int) == 12
        assert await caller.a() == 12

    async_runtime.run(main)


def test_fn_broader_method_arguments_bind_both_signatures(
    async_runtime: AsyncRuntime,
) -> None:
    class Base:
        def __init__(self, offset: int) -> None:
            self.offset = offset

        @routine
        async def calculate(self, value: int) -> int:  # pyright: ignore[reportRedeclaration]
            return value + self.offset

        @calculate.fn
        def calculate(self, value: int | str, *, scale: int = 1) -> int:
            return (int(value) + self.offset) * scale

    class Child(Base):
        pass

    first, second = Base(2), Child(3)
    assert assert_type(first.calculate("4", scale=2), int) == 12
    assert assert_type(second.calculate("4", scale=2), int) == 14
    assert Base.calculate(first, "4", scale=2) == 12
    assert Child.calculate(second, "4", scale=2) == 14

    async def main() -> None:
        assert assert_type(await first.calculate.a(4), int) == 6
        assert assert_type(await second.calculate.a(4), int) == 7
        assert await Base.calculate.a(first, 4) == 6

    async_runtime.run(main)


def test_fn_broader_classmethod_and_staticmethod_arguments(
    async_runtime: AsyncRuntime,
) -> None:
    class Base:
        @routine
        @classmethod
        async def create(cls, value: int) -> tuple[type, int]:  # pyright: ignore[reportRedeclaration]
            return cls, value

        @create.fn
        @classmethod
        def create(cls, value: int | str, *, scale: int = 1) -> tuple[type, int]:
            return cls, int(value) * scale

        @routine
        @staticmethod
        async def double(value: int) -> int:  # pyright: ignore[reportRedeclaration]
            return value * 2

        @double.fn
        @staticmethod
        def double(value: int | str, *, offset: int = 0) -> int:
            return int(value) * 2 + offset

    class Child(Base):
        pass

    instance: Any = Child()
    assert assert_type(Base.create("3", scale=2), tuple[type, int]) == (Base, 6)
    assert assert_type(Child.create("3", scale=2), tuple[type, int]) == (Child, 6)
    assert instance.create("3", scale=2) == (Child, 6)
    assert assert_type(Child.double("3", offset=1), int) == 7
    assert instance.double("3", offset=1) == 7

    async def main() -> None:
        assert assert_type(await Child.create.a(3), tuple[type, int]) == (Child, 3)
        assert assert_type(await Child.double.a(3), int) == 6

    async_runtime.run(main)


def test_typing_rejects_incompatible_routines(tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path

    source = """from noio import cleared_mode
from noio import routine
from noio.routine import Routine

@routine
async def calculate(value: int) -> int:
    return value

def wrong_argument(value: str) -> int:
    return len(value)

def wrong_result(value: int) -> str:
    return str(value)

def required_extra(value: int, extra: int) -> int:
    return value + extra

def broader(value: int | str, *, extra: int = 0) -> int:
    return int(value) + extra

calculate.fn(wrong_argument)  # reject
calculate.fn(wrong_result)  # reject
calculate.fn(required_extra)  # reject
changed = calculate.fn(broader)
changed("3", extra=1)

async def check() -> None:
    await changed.a("3")  # reject
    await changed.a(3, extra=1)  # reject

async def original(value: int) -> int:
    return value

forged: Routine[[int], int, [str]] = Routine(original)  # reject

class Choice:
    @routine
    @classmethod
    async def calculate(cls, value: int) -> int:
        return value

    @classmethod
    def bad(cls, value: str) -> int:
        return len(value)

    invalid = calculate.fn(bad)  # reject

class Bound:
    def method(self, value: int) -> int:
        return value

@routine
async def unbound(obj: Bound, value: int) -> int:
    return value

unbound.fn(Bound().method)  # reject

@cleared_mode
def callback(value: int) -> int:
    return value

not_a_routine: Routine[[int], int] = callback  # reject
callback("wrong")  # reject

async def invalid_adapter_call() -> None:
    await callback.a("wrong")  # reject
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
    actual = {item["range"]["start"]["line"] for item in diagnostics}
    assert actual == expected, result.stdout
    assert all(item["severity"] == "error" for item in diagnostics), result.stdout


def test_fn_can_start_async_runtime_from_driven_coroutine(
    async_runtime: AsyncRuntime,
) -> None:
    @routine
    async def inner() -> str:  # pyright: ignore[reportRedeclaration]
        assert _mode.get() is _Mode.AWAITED
        await async_runtime.checkpoint()
        assert _mode.get() is _Mode.AWAITED
        return "coroutine"

    @inner.fn
    def inner() -> str:
        assert _mode.get() is None
        return "ordinary"

    @routine
    async def bridge(fail: bool = False) -> str:  # pyright: ignore[reportRedeclaration]
        return await inner.a()

    @bridge.fn
    def bridge(fail: bool = False) -> str:
        assert inner() == "ordinary"
        assert _mode.get() is None
        result = async_runtime.run(inner.a)
        assert _mode.get() is None
        assert result == "coroutine"
        if fail:
            raise ValueError("bridge failed")
        return result

    @routine
    async def outer() -> None:
        assert _mode.get() is _Mode.DRIVEN
        assert await bridge.a() == "coroutine"
        assert _mode.get() is _Mode.DRIVEN
        assert await inner.a() == "ordinary"
        with pytest.raises(ValueError, match="bridge failed"):
            await bridge.a(True)
        assert _mode.get() is _Mode.DRIVEN
        assert await inner.a() == "ordinary"
        with pytest.raises(RuntimeError, match="Use await"):
            inner()

    assert bridge() == "coroutine"
    outer()
    assert bridge() == "coroutine"


def test_fn_can_run_its_coroutine_in_an_async_runtime(
    async_runtime: AsyncRuntime,
) -> None:
    calls: list[str] = []

    @routine
    async def operation() -> int:  # pyright: ignore[reportRedeclaration]
        calls.append("coroutine")
        await async_runtime.checkpoint()
        return 42

    @operation.fn
    def operation() -> int:
        calls.append("ordinary")
        return async_runtime.run(operation.a)

    @routine
    async def outer() -> int:
        return await operation.a()

    assert operation() == 42
    assert outer() == 42
    assert calls == ["ordinary", "coroutine", "ordinary", "coroutine"]


def _check_cleared_mode_composition(checkpoint: Callable[[], None]) -> None:
    from noio.routine import Function
    from noio.routine import Routine

    @routine
    async def inner(value: int) -> int:
        assert _mode.get() is _Mode.DRIVEN
        checkpoint()
        return value * 2

    @cleared_mode
    def callback(value: int, /, *, offset: int = 0) -> int:
        assert _mode.get() is None
        result = inner(value) + offset
        assert _mode.get() is None
        return result

    assert isinstance(callback, Function)
    assert not isinstance(callback, Routine)
    assert assert_type(callback(3, offset=1), int) == 7

    @routine
    async def outer() -> int:
        result = callback(3, offset=1)
        assert _mode.get() is _Mode.DRIVEN
        return result

    assert outer() == 7
    assert _mode.get() is None


def test_cleared_mode_composition(sync_runtime: RunSync) -> None:
    sync_runtime(_check_cleared_mode_composition)


def test_cleared_mode_call_restores_awaited_context(
    async_runtime: AsyncRuntime,
) -> None:
    marker = ContextVar("cleared_mode marker", default="before")
    calls: list[str] = []
    caller_thread = get_ident()

    def callback(fail: bool = False) -> int:
        assert get_ident() == caller_thread
        assert _mode.get() is None
        marker.set("inside")
        calls.append("called")
        if fail:
            raise ValueError("callback failed")
        return 42

    @routine
    async def outer() -> None:
        adapter = cleared_mode(callback)
        assert not calls
        assert assert_type(adapter(), int) == 42
        assert marker.get() == "inside"
        assert _mode.get() is _Mode.AWAITED
        with pytest.raises(ValueError, match="callback failed"):
            cleared_mode(callback)(True)
        assert marker.get() == "inside"
        assert _mode.get() is _Mode.AWAITED
        await async_runtime.checkpoint()

    async def main() -> None:
        await outer.a()
        assert marker.get() == "inside"
        assert _mode.get() is None

    async_runtime.run(main)
    assert calls == ["called", "called"]
    assert marker.get() == "before"


def test_cleared_mode_call_site_can_start_async_runtime(
    async_runtime: AsyncRuntime,
) -> None:
    @routine
    async def inner() -> str:  # pyright: ignore[reportRedeclaration]
        assert _mode.get() is _Mode.AWAITED
        await async_runtime.checkpoint()
        return "coroutine"

    @inner.fn
    def inner() -> str:
        return "ordinary"

    def launch() -> str:
        assert _mode.get() is None
        return async_runtime.run(inner.a)

    @routine
    async def outer() -> str:
        result = cleared_mode(launch)()
        assert _mode.get() is _Mode.DRIVEN
        return result

    assert outer() == "coroutine"
    assert _mode.get() is None


def test_cleared_mode_metadata_and_rejection() -> None:
    def original(value: int, /, *, scale: int = 2) -> int:
        """Calculate a value."""
        return value * scale

    adapted = cleared_mode(original)
    assert unwrap(adapted) is original
    assert signature(adapted) == signature(original)
    assert adapted.__name__ == original.__name__
    assert adapted.__doc__ == original.__doc__
    assert not hasattr(adapted, "a")

    async def coroutine(*args: object) -> None:
        pass

    for invalid in (coroutine, classmethod(coroutine), staticmethod(coroutine), 42):
        bad: Any = invalid
        with pytest.raises(TypeError, match="ordinary callable"):
            cleared_mode(bad)


def test_cleared_mode_methods_bind_inside_and_outside_routines(
    async_runtime: AsyncRuntime,
) -> None:
    class Base:
        def __init__(self, value: int) -> None:
            self.value = value

        @cleared_mode
        def calculate(self, scale: int = 2) -> int:
            return self.value * scale

        @cleared_mode
        @classmethod
        def create(cls, value: int) -> Self:
            return cls(value)

        @cleared_mode
        @staticmethod
        def double(value: int) -> int:
            return value * 2

    class Child(Base):
        pass

    first, second = Base(3), Child(4)
    assert assert_type(first.calculate(), int) == 6
    assert assert_type(second.calculate(), int) == 8
    assert Base.calculate(second, 3) == 12
    assert type(assert_type(Child.create(5), Child)) is Child
    assert first.create(5).value == 5
    assert assert_type(Child.double(3), int) == 6
    # As with Routine, staticmethod instance access loses its marker in typing.
    instance: Any = second
    assert instance.double(3) == 6

    @routine
    async def main() -> None:
        assert assert_type(second.calculate(3), int) == 12
        assert type(assert_type(Child.create(5), Child)) is Child
        assert Base.calculate(first, 3) == 9
        assert assert_type(Child.double(3), int) == 6
        assert instance.double(3) == 6

    async_runtime.run(main.a)


def test_cleared_mode_same_instance_across_thread_and_async_runtime(
    async_runtime: AsyncRuntime,
) -> None:
    entered, release = Event(), Event()

    @cleared_mode
    def callback(block: bool) -> int:
        assert _mode.get() is None
        if block:
            entered.set()
            assert release.wait(5)
        assert _mode.get() is None
        return 42

    async def main() -> None:
        assert callback(False) == 42
        await async_runtime.checkpoint()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(callback, True)
        try:
            assert entered.wait(5)
            async_runtime.run(main)
        finally:
            release.set()
        assert future.result(timeout=5) == 42


def test_routine_with_different_self_types(async_runtime: AsyncRuntime) -> None:
    from .routine import Routine

    class Worker[F: Callable[[], int]]:
        def __init__(self, fn: F) -> None:
            self.fn = fn

        @routine
        async def run(  # pyright: ignore[reportRedeclaration]
            self: Worker[Routine[[], int]],
        ) -> int:
            return await self.fn.a()

        @run.fn
        def run(self: Worker[Callable[[], int]]) -> int:
            return self.fn()

    @routine
    async def value() -> int:
        return 42

    ordinary: Worker[Callable[[], int]] = Worker(lambda: 7)
    asynchronous = Worker(value)
    assert_type(ordinary.run, Callable[[], int])
    assert_type(asynchronous.run, Routine[[], int])
    assert ordinary.run() == 7
    assert asynchronous.run() == 42

    async def main() -> None:
        assert await asynchronous.run.a() == 42

    async_runtime.run(main)
