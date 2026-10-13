import subprocess
import sys
import sysconfig
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from time import sleep
from typing import Any
from typing import Protocol

import pytest


class RunAsync(Protocol):
    def __call__[R](self, fn: Callable[[], Coroutine[Any, Any, R]]) -> R: ...


type AsyncCall = Callable[[], Coroutine[Any, Any, None]]


class RunConcurrently(Protocol):
    async def __call__(self, *functions: AsyncCall) -> None: ...


class AsyncEvent(Protocol):
    def set(self) -> None: ...

    async def wait(self) -> object: ...


@dataclass(frozen=True)
class AsyncRuntime:
    run: RunAsync
    checkpoint: Callable[[], Awaitable[None]]
    event: Callable[[], AsyncEvent]
    concurrently: RunConcurrently
    cancel: Callable[[AsyncCall], Awaitable[None]]


@pytest.fixture(params=["asyncio", "trio", "anyio-asyncio", "anyio-trio"])
def async_runtime(request: pytest.FixtureRequest) -> AsyncRuntime:
    import asyncio

    import anyio
    import trio

    backend: str = request.param

    def run[R](fn: Callable[[], Coroutine[Any, Any, R]]) -> R:
        if backend == "asyncio":
            return asyncio.run(fn())
        if backend == "trio":
            return trio.run(fn)
        return anyio.run(fn, backend=backend.removeprefix("anyio-"))

    async def checkpoint() -> None:
        if backend == "asyncio":
            await asyncio.sleep(0)
        elif backend == "trio":
            await trio.lowlevel.checkpoint()
        else:
            await anyio.lowlevel.checkpoint()

    def event() -> AsyncEvent:
        if backend == "asyncio":
            return asyncio.Event()
        if backend == "trio":
            return trio.Event()
        return anyio.Event()

    async def concurrently(*functions: AsyncCall) -> None:
        if backend == "asyncio":
            async with asyncio.TaskGroup() as tasks:
                for fn in functions:
                    tasks.create_task(fn())
        elif backend == "trio":
            async with trio.open_nursery() as nursery:
                for fn in functions:
                    nursery.start_soon(fn)
        else:
            async with anyio.create_task_group() as group:
                for fn in functions:
                    group.start_soon(fn)

    async def cancel(fn: AsyncCall) -> None:
        if backend == "asyncio":

            async def cancelled() -> None:
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                await fn()

            with pytest.raises(asyncio.CancelledError):
                await asyncio.create_task(cancelled())
        elif backend == "trio":
            with trio.CancelScope() as scope:
                scope.cancel()
                await fn()
            assert scope.cancelled_caught
        else:
            with anyio.CancelScope() as anyio_scope:
                anyio_scope.cancel()
                await fn()
            assert anyio_scope.cancelled_caught

    return AsyncRuntime(run, checkpoint, event, concurrently, cancel)


type SyncCheck = Callable[[Callable[[], None]], None]
type RunSync = Callable[[SyncCheck], None]


@pytest.fixture(
    params=[
        "main-thread",
        "worker-thread",
        "gevent",
        "gevent-patched-before-import",
        "gevent-patched-after-import",
    ]
)
def sync_runtime(request: pytest.FixtureRequest) -> RunSync:
    """Run a module-level check, passing the runtime's scheduling checkpoint.

    Gevent subprocesses import checks by name, so they must be module-level functions.
    """
    mode: str = request.param
    if mode.startswith("gevent") and sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("gevent does not support free-threaded CPython")

    def run(check: SyncCheck) -> None:
        if mode == "main-thread":
            check(partial(sleep, 0))
        elif mode == "worker-thread":
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(check, partial(sleep, 0)).result(timeout=2)
        else:
            bootstrap = ""
            if mode == "gevent-patched-after-import":
                bootstrap += "import noio\n"
            if mode != "gevent":
                bootstrap += "from gevent import monkey\nmonkey.patch_all()\n"
            bootstrap += (
                "import gevent, importlib, sys\n"
                "check = getattr(importlib.import_module(sys.argv[1]), sys.argv[2])\n"
                "gevent.spawn(check, lambda: gevent.sleep(0)).get(timeout=2)\n"
            )
            result = subprocess.run(
                [sys.executable, "-c", bootstrap, check.__module__, check.__name__],
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert result.returncode == 0, result.stdout + result.stderr

    return run
