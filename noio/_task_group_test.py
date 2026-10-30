import asyncio
import subprocess
import sys
import sysconfig
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from threading import Event as ThreadEvent
from typing import Any
from typing import assert_type

import pytest

from noio import routine
from noio import sleep
from noio._event import Event
from noio._task_group import _open_task_group

from .conftest import AsyncRuntime
from .conftest import RunSync


@routine
async def _exercise() -> None:
    entered = Event()
    release = Event()
    calls: list[str] = []
    context = ContextVar("child context", default="unset")
    context.set("parent")

    @routine
    async def grandchild() -> None:
        assert context.get() == "child"
        await sleep.a(0)
        calls.append("grandchild")

    @routine
    async def child() -> None:
        assert context.get() == "parent"
        context.set("child")
        entered.set()
        await release.wait.a()
        await group.spawn.a(grandchild)
        async with _open_task_group() as nested:
            await nested.spawn.a(grandchild)
        calls.append("child")

    async with _open_task_group() as group:
        assert await group.spawn.a(child) is None
        await entered.wait.a()
        assert context.get() == "parent"
        release.set()
    assert calls.count("grandchild") == 2
    assert calls.count("child") == 1
    with pytest.raises(RuntimeError, match="closed"):
        await group.spawn.a(child)


def _check_sync(checkpoint: Callable[[], None]) -> None:
    _exercise()
    calls: list[int] = []

    def ordinary(value: int, *, factor: int) -> None:
        checkpoint()
        # Child entry must clear an inherited routine execution mode.
        sleep(0)
        calls.append(value * factor)

    with _open_task_group() as group:
        assert group.spawn(ordinary, 3, factor=4) is None
    assert calls == [12]
    with _open_task_group():
        pass


def test_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_sync)


def test_coroutine(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_exercise.a)


@routine
async def _failures() -> None:
    failed = Event()
    first = ValueError("first")
    second = KeyError("second")
    body = RuntimeError("body")
    calls: list[str] = []

    @routine
    async def failing() -> None:
        failed.set()
        raise first

    @routine
    async def sibling() -> None:
        await failed.wait.a()
        await sleep.a(0)
        calls.append("sibling")
        raise second

    with pytest.raises(ExceptionGroup) as raised:
        async with _open_task_group() as group:
            await group.spawn.a(failing)
            await group.spawn.a(sibling)
            await failed.wait.a()
            await sleep.a(0)
            calls.append("parent")
            raise body
    assert set(raised.value.exceptions) == {first, second, body}
    assert sorted(calls) == ["parent", "sibling"]

    with pytest.raises(RuntimeError) as raised_body:
        async with _open_task_group():
            raise body
    assert raised_body.value is body


def _check_failures(checkpoint: Callable[[], None]) -> None:
    _failures()


def test_ordinary_failures(sync_runtime: RunSync) -> None:
    sync_runtime(_check_failures)


def test_coroutine_failures(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_failures.a)


def test_coroutine_requires_routine(async_runtime: AsyncRuntime) -> None:
    def ordinary() -> None:
        pytest.fail("must not run")

    async def coroutine() -> None:
        pytest.fail("must not run")

    async def main() -> None:
        async with _open_task_group() as group:
            for fn in (ordinary, coroutine):
                with pytest.raises(
                    TypeError, match=r"This (asyncio|Trio) task group needs a routine"
                ):
                    await group.spawn.a(fn)  # pyright: ignore[reportArgumentType]

    async_runtime.run(main)


def test_coroutine_group_rejects_ordinary_spawn(async_runtime: AsyncRuntime) -> None:
    @routine
    async def child() -> None:
        pytest.fail("must not run")

    async def main() -> None:
        async with _open_task_group() as group:
            with pytest.raises(
                RuntimeError,
                match=r"This task group is running in (Trio|asyncio)\. "
                r"To start a task, use `await group\.spawn\.a",
            ):
                group.spawn(child)

    async_runtime.run(main)


def _check_reject_coroutine(checkpoint: Callable[[], None]) -> None:
    async def coroutine() -> None:
        pytest.fail("must not run")

    with (
        _open_task_group() as group,
        pytest.raises(
            TypeError,
            match=r"This (thread|gevent) task group needs "
            r"an ordinary function or routine",
        ),
    ):
        group.spawn(coroutine)


def test_ordinary_rejects_coroutine(sync_runtime: RunSync) -> None:
    sync_runtime(_check_reject_coroutine)


@routine
async def _exit_additions() -> None:
    from noio._task_group import _Children

    exiting = Event()
    finished: list[bool] = []
    original = _Children.wait

    @routine
    async def finish(
        group: _Children[Any],
    ) -> tuple[list[BaseException], BaseException | None]:
        exiting.set()
        return await original.a(group)

    @routine
    async def grandchild() -> None:
        await sleep.a(0)
        finished.append(True)

    @routine
    async def child() -> None:
        await exiting.wait.a()
        await group.spawn.a(grandchild)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_Children, "wait", finish)
        async with _open_task_group() as group:
            await group.spawn.a(child)
            await group.spawn.a(child)
    assert finished == [True, True]


def _check_exit_additions(checkpoint: Callable[[], None]) -> None:
    _exit_additions()


def test_ordinary_add_child_during_exit(sync_runtime: RunSync) -> None:
    sync_runtime(_check_exit_additions)


def test_coroutine_add_child_during_exit(async_runtime: AsyncRuntime) -> None:
    async_runtime.run(_exit_additions.a)


@pytest.mark.parametrize("during_exit", [False, True])
def test_external_cancellation_joins(
    async_runtime: AsyncRuntime,
    during_exit: bool,
) -> None:
    finished: list[bool] = []

    @routine
    async def child() -> None:
        await sleep.a(0)
        await sleep.a(0)
        finished.append(True)

    async def body() -> None:
        async with _open_task_group() as group:
            await group.spawn.a(child)
            if not during_exit:
                await async_runtime.checkpoint()

    async def main() -> None:
        await async_runtime.cancel(body)
        assert finished == [True]

    async_runtime.run(main)


def test_asyncio_child_cancelled_before_start() -> None:
    async def main() -> None:
        @routine
        async def child() -> None:
            pytest.fail("cancelled child must not start")

        with pytest.raises(BaseExceptionGroup) as raised:
            async with _open_task_group() as group:
                await group.spawn.a(child)
                task = group._children.items[0].handle
                assert isinstance(task, asyncio.Task)
                task.cancel()
        assert len(raised.value.exceptions) == 1
        assert isinstance(raised.value.exceptions[0], asyncio.CancelledError)

    asyncio.run(main())


def test_launch_failure_does_not_hang(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading

    error = RuntimeError("cannot launch")

    def fail_start(self: threading.Thread) -> None:
        raise error

    monkeypatch.setattr(threading.Thread, "start", fail_start)
    with pytest.raises(RuntimeError) as raised, _open_task_group() as group:
        group.spawn(lambda: None)
    assert raised.value is error


def test_groups_in_different_runtimes_share_events() -> None:
    import trio

    entered = ThreadEvent()
    release = Event()
    complete = Event()
    group_ref: list[Any] = []

    @routine
    async def worker() -> None:
        await release.wait.a()
        complete.set()

    async def trio_main() -> None:
        async with _open_task_group() as group:
            group_ref.append(group)
            await group.spawn.a(worker)
            entered.set()

    async def asyncio_main() -> None:
        while not entered.is_set():
            await asyncio.sleep(0)
        async with _open_task_group() as group:
            with pytest.raises(
                RuntimeError, match="from the runtime where it was opened"
            ):
                await group_ref[0].spawn.a(worker)
            await group.spawn.a(complete.wait)
            release.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(trio.run, trio_main)
        try:
            asyncio.run(asyncio_main())
        finally:
            release.set()
        future.result(timeout=5)


@pytest.mark.skipif(
    bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
    reason="gevent does not support free-threaded CPython",
)
@pytest.mark.parametrize("import_first", [False, True])
def test_monkeypatched_main_thread(import_first: bool) -> None:
    script = "import noio\n" if import_first else ""
    script += (
        "from gevent import monkey\n"
        "monkey.patch_all()\n"
        "import gevent\n"
        "from noio._task_group_test import _check_sync\n"
        "_check_sync(lambda: gevent.sleep(0))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_child_preserves_callable_type(async_runtime: AsyncRuntime) -> None:
    from ._task_group import _Child
    from .routine import Routine

    calls: list[int] = []

    def ordinary(value: int) -> None:
        calls.append(value)

    @routine
    async def asynchronous(value: int) -> None:
        calls.append(value)

    ordinary_child = _Child[Callable[[int], None]](ordinary, (1,), {})
    routine_child = _Child[Routine[[int], None]](asynchronous, (2,), {})
    assert_type(ordinary_child.fn, Callable[[int], None])
    assert_type(routine_child.fn, Routine[[int], None])
    assert_type(ordinary_child.run, Callable[[], None])
    assert_type(routine_child.run, Routine[[], None])
    ordinary_child.run()
    routine_child.run()
    async_runtime.run(routine_child.run.a)
    assert calls == [1, 2, 2]
    assert ordinary_child.error is None
    assert routine_child.error is None


def test_asyncio_launch_failure_closes_coroutine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from collections.abc import Coroutine
    from inspect import CORO_CLOSED
    from inspect import getcoroutinestate

    error = RuntimeError("cannot launch")
    captured: list[Coroutine[Any, Any, None]] = []
    calls: list[bool] = []

    @routine
    async def child() -> None:
        calls.append(True)

    def fail_start(coroutine: Coroutine[Any, Any, None]) -> None:
        captured.append(coroutine)
        raise error

    async def main() -> None:
        async with _open_task_group() as group:
            with monkeypatch.context() as patch:
                patch.setattr(asyncio.get_running_loop(), "create_task", fail_start)
                with pytest.raises(RuntimeError) as raised:
                    await group.spawn.a(child)
                assert raised.value is error
            assert getcoroutinestate(captured[0]) == CORO_CLOSED
            await group.spawn.a(child)
        assert calls == [True]

    asyncio.run(main())
