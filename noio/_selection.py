"""Private selection state shared by selection and selectable APIs."""

from typing import cast

from aiologic.lowlevel import shield

from ._event import Event
from ._lock import Lock
from .routine import routine


class _Selection[T]:
    """Shared state for choices in one select call."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending = True
        self._event = Event()
        # An index is recorded only on completion, not on invalidation.
        self._index: int | None = None
        self._value: T | None = None
        self._error: Exception | None = None

    def result(self, index: int, value: T) -> None:
        """Record a value while the caller holds _lock."""
        if not self._pending:
            raise RuntimeError("Selection is no longer pending")
        self._value = value
        self._index = index
        self._pending = False
        self._event.set()

    def error(self, index: int, error: Exception) -> None:
        """Record an exception while the caller holds _lock."""
        if not self._pending:
            raise RuntimeError("Selection is no longer pending")
        self._error = error
        self._index = index
        self._pending = False
        self._event.set()

    @routine
    async def invalidate(self) -> None:  # pyright: ignore[reportRedeclaration]
        """Make every choice ineligible, preserving any committed outcome.

        It does not notify waiters: invalidation is not an outcome.
        """
        await shield(self._invalidate_async())

    @invalidate.fn
    def invalidate(self) -> None:
        shield(self._invalidate_sync)()

    def _invalidate_sync(self) -> None:
        with self._lock:
            self._pending = False

    async def _invalidate_async(self) -> None:
        async with self._lock:
            self._pending = False

    @routine
    async def wait(self) -> tuple[int, T]:
        await self._event.wait.a()
        # Notification may arrive while a selectable is still completing other
        # participants. Do not expose the outcome until its locks are released.
        async with self._lock:
            assert self._index is not None
            index = self._index
            value = self._value
            error = self._error
        if error is not None:
            raise error
        return index, cast(T, value)


class SelectorGuard[T]:
    """A choice accepting a result of type T, retained while an operation waits.

    Obtain temporary permission to complete it with ``selectors(guard)``.
    Guards are supplied by selection machinery, not constructed by selectables.
    """

    def __init__(self, selection: _Selection[T], index: int) -> None:
        self._selection = selection
        self._index = index
