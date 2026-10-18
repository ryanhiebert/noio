from collections.abc import AsyncIterator
from collections.abc import Iterable
from contextlib import AsyncExitStack
from types import TracebackType
from typing import Self

import aiologic
from aiologic.lowlevel import current_async_library

from ._contextlib import ContextManager
from ._contextlib import contextmanager
from .routine import _sync_call
from .routine import routine


class Lock(ContextManager["Lock", None]):
    """An owned, non-reentrant lock shared by threads, greenlets, and async tasks."""

    def __init__(self) -> None:
        self.__lock = aiologic.Lock()

    @routine
    async def __enter__(self) -> Self:  # pyright: ignore[reportIncompatibleVariableOverride]
        """Wait for ownership; reacquiring raises RuntimeError."""
        if _sync_call.get():
            self.__lock.green_acquire()
        else:
            await self.__lock.async_acquire()
        return self

    @routine
    async def __exit__(  # pyright: ignore[reportIncompatibleVariableOverride]
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release from the owning thread, greenlet, or async task."""
        # An ordinary acquisition inside an async function still has green ownership.
        if self.__lock.green_owned() or current_async_library(failsafe=True) is None:
            self.__lock.green_release()
        else:
            self.__lock.async_release()

    async def __aenter__(self) -> Self:
        return await self.__enter__.a()


@contextmanager
async def acquire(locks: Iterable[Lock]) -> AsyncIterator[None]:
    """Acquire distinct locks in identity order and release in reverse order."""
    ordered = sorted(set(locks), key=id)
    async with AsyncExitStack() as stack:
        for lock in ordered:
            await stack.enter_async_context(lock)
        yield
