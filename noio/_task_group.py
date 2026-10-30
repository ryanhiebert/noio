"""Owned child execution; errors are reported after every child finishes.

Completion means the worker function and its cleanup have finished.
There is deliberately no group cancellation policy yet. A child that does not
finish prevents scope exit, including when the body or another child fails.
"""

import asyncio
import threading
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterator
from contextlib import contextmanager as sync_contextmanager
from contextvars import copy_context
from inspect import iscoroutinefunction
from typing import Any
from typing import Protocol
from typing import Self

from aiologic.lowlevel import async_checkpoint
from aiologic.lowlevel import current_async_library
from aiologic.lowlevel import current_green_library

from ._contextlib import contextmanager
from ._event import Event
from ._runtime import current_runtime
from .routine import Routine
from .routine import _Mode
from .routine import _mode
from .routine import routine
from .shield import shield


class _Child[F: Callable[..., Any]]:
    def __init__(
        self,
        fn: F,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.error: BaseException | None = None
        self.done = Event()
        self.handle: object | None = None

    @routine
    async def run(  # pyright: ignore[reportRedeclaration]
        self: _Child[Routine[..., Any]],
    ) -> None:
        try:
            await self.fn.a(*self.args, **self.kwargs)
        except BaseException as error:
            self.error = error

    @run.fn
    def run(self: _Child[Callable[..., Any]]) -> None:
        try:
            self.fn(*self.args, **self.kwargs)
        except BaseException as error:
            self.error = error

    @routine
    async def wait(self) -> None:
        await self.done.wait.a()


class _Children[F: Callable[..., Any]]:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.items: list[_Child[F]] = []
        self._closed = False

    @sync_contextmanager
    def register(
        self, fn: F, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Iterator[_Child[F]]:
        child = _Child(fn, args, kwargs)
        # No user code, runtime calls, or waits under this lock.
        with self._lock:
            if self._closed:
                raise RuntimeError("Task group is closed")
            self.items.append(child)
        try:
            yield child
        except BaseException:
            child.done.set()
            raise

    @routine
    async def wait(
        self,
    ) -> tuple[list[BaseException], BaseException | None]:
        errors: list[BaseException] = []
        interrupted: BaseException | None = None
        index = 0
        while True:
            with self._lock:
                if index == len(self.items):
                    self._closed = True
                    return errors, interrupted
                child = self.items[index]
            try:
                await shield(child.wait).a()
            except BaseException as error:
                # Shielding defers cancellation until the child finishes.
                # Wait for the remaining children before delivering it.
                interrupted = interrupted or error
            if child.error is not None:
                errors.append(child.error)
            index += 1


class _ThreadTaskGroup:
    def __init__(self, children: _Children[Callable[..., Any]]) -> None:
        self._children = children

    @classmethod
    @contextmanager
    async def open(cls) -> AsyncIterator[Self]:
        children = _Children[Callable[..., Any]]()
        group = cls(children)
        body_error: BaseException | None = None
        try:
            yield group
        except BaseException as error:
            body_error = error
        finally:
            errors, interrupted = await children.wait.a()
        await _report_errors.a(body_error, errors, interrupted)

    @routine
    async def spawn(  # pyright: ignore[reportRedeclaration]
        self,
        fn: Routine[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        raise RuntimeError(
            "This task group is running in threads. "
            "To start a task, call `group.spawn(...)` from an ordinary function."
        )

    @spawn.fn
    def spawn(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not callable(fn) or iscoroutinefunction(fn):
            raise TypeError(
                "This thread task group needs an ordinary function or routine. "
                "A bare async function cannot run here."
            )
        if current_green_library() != "threading":
            raise RuntimeError(
                "This task group runs in threads. Call spawn from an ordinary thread."
            )
        with self._children.register(fn, args, kwargs) as child:

            def run() -> None:
                try:
                    child.run()
                finally:
                    child.done.set()

            threading.Thread(target=copy_context().run, args=(run,)).start()


class _GeventTaskGroup:
    def __init__(self, children: _Children[Callable[..., Any]]) -> None:
        import gevent

        self._children = children
        self._hub = gevent.get_hub()

    @classmethod
    @contextmanager
    async def open(cls) -> AsyncIterator[Self]:
        children = _Children[Callable[..., Any]]()
        group = cls(children)
        body_error: BaseException | None = None
        try:
            yield group
        except BaseException as error:
            body_error = error
        finally:
            errors, interrupted = await children.wait.a()
        await _report_errors.a(body_error, errors, interrupted)

    @routine
    async def spawn(  # pyright: ignore[reportRedeclaration]
        self,
        fn: Routine[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        raise RuntimeError(
            "This task group is running in gevent. "
            "To start a task, call `group.spawn(...)` from an ordinary function."
        )

    @spawn.fn
    def spawn(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        import gevent

        if not callable(fn) or iscoroutinefunction(fn):
            raise TypeError(
                "This gevent task group needs an ordinary function or routine. "
                "A bare async function cannot run here."
            )
        if current_green_library() != "gevent" or gevent.get_hub() is not self._hub:
            raise RuntimeError(
                "This task group can only start tasks from the runtime "
                "where it was opened."
            )
        with self._children.register(fn, args, kwargs) as child:
            worker = gevent.spawn(copy_context().run, child.run)
            worker.rawlink(lambda _: child.done.set())


class _AsyncioTaskGroup:
    def __init__(self, children: _Children[Routine[..., Any]]) -> None:
        self._children = children
        self._loop = asyncio.get_running_loop()

    @classmethod
    @contextmanager
    async def open(cls) -> AsyncIterator[Self]:
        children = _Children[Routine[..., Any]]()
        group = cls(children)
        body_error: BaseException | None = None
        try:
            yield group
        except BaseException as error:
            body_error = error
        finally:
            errors, interrupted = await children.wait.a()
        await _report_errors.a(body_error, errors, interrupted, checkpoint=True)

    @routine
    async def spawn(  # pyright: ignore[reportRedeclaration]
        self,
        fn: Routine[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if not isinstance(fn, Routine):
            raise TypeError(
                "This asyncio task group needs a routine. "
                "Pass a function decorated with `@noio.routine`."
            )
        if (
            _mode.get() is not _Mode.AWAITED
            or current_async_library(failsafe=True) != "asyncio"
            or asyncio.get_running_loop() is not self._loop
        ):
            raise RuntimeError(
                "This task group can only start tasks from the runtime "
                "where it was opened."
            )
        with self._children.register(fn, args, kwargs) as child:
            coroutine = child.run.a()
            try:
                task = self._loop.create_task(coroutine)
            except BaseException:
                coroutine.close()
                raise
            child.handle = task

            def finished(task: asyncio.Task[None]) -> None:
                try:
                    task.result()
                except BaseException as error:
                    child.error = error
                child.done.set()

            task.add_done_callback(finished)

    @spawn.fn
    def spawn(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        raise RuntimeError(
            "This task group is running in asyncio. "
            "To start a task, use `await group.spawn.a(...)`."
        )


class _Nursery(Protocol):
    def start_soon(
        self, async_fn: Callable[[], Coroutine[Any, Any, None]], /
    ) -> None: ...


class _TrioTaskGroup:
    def __init__(
        self, children: _Children[Routine[..., Any]], nursery: _Nursery
    ) -> None:
        import trio

        self._children = children
        self._token = trio.lowlevel.current_trio_token()
        self._nursery = nursery

    @classmethod
    @contextmanager
    async def open(cls) -> AsyncIterator[Self]:
        import trio

        body_error: BaseException | None = None
        async with trio.open_nursery() as nursery:
            children = _Children[Routine[..., Any]]()
            group = cls(children, nursery)
            try:
                yield group
            except BaseException as error:
                body_error = error
            finally:
                nursery.cancel_scope.shield = True
                errors, interrupted = await children.wait.a()
        # Report after leaving the nursery so it does not regroup our failures.
        await _report_errors.a(body_error, errors, interrupted, checkpoint=True)

    @routine
    async def spawn(  # pyright: ignore[reportRedeclaration]
        self,
        fn: Routine[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        import trio

        if not isinstance(fn, Routine):
            raise TypeError(
                "This Trio task group needs a routine. "
                "Pass a function decorated with `@noio.routine`."
            )
        if (
            _mode.get() is not _Mode.AWAITED
            or current_async_library(failsafe=True) != "trio"
            or trio.lowlevel.current_trio_token() is not self._token
        ):
            raise RuntimeError(
                "This task group can only start tasks from the runtime "
                "where it was opened."
            )
        with self._children.register(fn, args, kwargs) as child:

            async def run() -> None:
                # Parent cancellation does not abandon or cancel owned work.
                with trio.CancelScope(shield=True):
                    try:
                        await child.run.a()
                    finally:
                        child.done.set()

            self._nursery.start_soon(run)

    @spawn.fn
    def spawn(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        raise RuntimeError(
            "This task group is running in Trio. "
            "To start a task, use `await group.spawn.a(...)`."
        )


@routine
async def _report_errors(
    body_error: BaseException | None,
    errors: list[BaseException],
    interrupted: BaseException | None,
    *,
    checkpoint: bool = False,
) -> None:
    if body_error is None:
        body_error = interrupted
    elif interrupted is not None and type(interrupted) is not type(body_error):
        errors.append(interrupted)
    if checkpoint and body_error is None:
        try:
            # Deliver cancellation left pending by shielded child waits.
            await async_checkpoint(force=True)
        except BaseException as error:
            body_error = error
    if body_error is not None:
        if not errors:
            raise body_error
        errors.insert(0, body_error)
    if errors:
        raise BaseExceptionGroup("Task group failures", errors)


@contextmanager
async def _open_task_group() -> AsyncIterator[
    _ThreadTaskGroup | _GeventTaskGroup | _AsyncioTaskGroup | _TrioTaskGroup
]:
    """Choose execution at entry and wait for children, including their additions."""
    match await current_runtime.a():
        case "threading":
            scope = _ThreadTaskGroup.open()
        case "gevent":
            scope = _GeventTaskGroup.open()
        case "asyncio":
            scope = _AsyncioTaskGroup.open()
        case "trio":
            scope = _TrioTaskGroup.open()
        case library:
            raise RuntimeError(f"Unsupported task group runtime: {library}")

    async with scope as group:
        yield group
