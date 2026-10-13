import asyncio
from collections.abc import Callable
from collections.abc import Coroutine
from contextvars import ContextVar
from inspect import signature
from inspect import unwrap
from threading import get_ident
from types import CoroutineType
from typing import Any
from typing import Self
from typing import assert_type

import pytest

from noio import routine

from .conftest import AsyncRuntime
from .conftest import RunSync
from .conftest import SyncCheck
from .routine import _in_coroutine as in_coroutine


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
    assert not in_coroutine.get()


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
        assert in_coroutine.get()
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
    assert not in_coroutine.get()
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
    assert not in_coroutine.get()
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
    assert not in_coroutine.get()


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
        assert in_coroutine.get()
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
        assert not in_coroutine.get()
        assert ordinary(4) == 5
        with pytest.raises(
            RuntimeError, match="cannot reuse already awaited coroutine"
        ):
            await call

    async_runtime.run(main)
    assert context.get() == "caller"
    assert not in_coroutine.get()
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
        assert not in_coroutine.get()

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
            assert not in_coroutine.get()

    async def main() -> None:
        await async_runtime.cancel(caller)
        assert finalized == [True]
        assert not in_coroutine.get()

    async_runtime.run(main)
    assert not in_coroutine.get()


def test_context_is_task_local(async_runtime: AsyncRuntime):
    context = ContextVar("task_context", default=0)

    @routine
    async def child(value: int) -> None:
        context.set(value)
        await async_runtime.checkpoint()
        assert context.get() == value
        assert in_coroutine.get()

    async def observer() -> None:
        await async_runtime.checkpoint()
        assert context.get() == 0
        assert not in_coroutine.get()

    async def main() -> None:
        await async_runtime.concurrently(
            lambda: child.a(3), lambda: child.a(5), observer
        )
        assert context.get() == 0
        assert not in_coroutine.get()

    async_runtime.run(main)


def _check_gevent_contexts(checkpoint: Callable[[], None]) -> None:
    import gevent

    context = ContextVar("greenlet_context", default=0)

    def ordinary(value: int) -> int:
        assert not in_coroutine.get()
        checkpoint()
        assert context.get() == value
        assert not in_coroutine.get()
        return value

    @routine
    async def asynchronous(value: int) -> int:
        context.set(value)
        checkpoint()
        assert context.get() == value
        assert in_coroutine.get()
        return value

    def caller(value: int) -> int:
        assert asynchronous(value) == value
        assert not in_coroutine.get()
        return ordinary(value)

    first = gevent.spawn(caller, 3)
    second = gevent.spawn(caller, 5)
    assert first.get(timeout=2) == 3
    assert second.get(timeout=2) == 5
    assert context.get() == 0
    assert not in_coroutine.get()


@pytest.mark.parametrize(
    "sync_runtime",
    ["gevent", "gevent-patched-before-import", "gevent-patched-after-import"],
    indirect=True,
)
def test_context_is_greenlet_local(sync_runtime: RunSync):
    sync_runtime(_check_gevent_contexts)
