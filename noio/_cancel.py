"""Cancellation context shared by selection, shielding, and owned child execution."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from ._selector import _RoutineSelectable


class CancelledError(BaseException):
    """Cancellation delivered at IO; ordinary exception handlers do not catch it."""

    def __init__(self, scope: _RoutineSelectable[Any]) -> None:
        super().__init__("Scope cancelled")
        self._scope = scope


_context: ContextVar[tuple[_RoutineSelectable[Any], ...]] = ContextVar(
    "noio.cancellation", default=()
)


def current_cancellations() -> tuple[_RoutineSelectable[Any], ...]:
    return _context.get()


@contextmanager
def _using_cancellations(
    scopes: tuple[_RoutineSelectable[Any], ...],
) -> Iterator[None]:
    token = _context.set(scopes)
    try:
        yield
    finally:
        _context.reset(token)


@contextmanager
def enter_cancel_scope(scope: _RoutineSelectable[Any]) -> Iterator[None]:
    with _using_cancellations((*_context.get(), scope)):
        yield


@contextmanager
def shield_cancellation() -> Iterator[None]:
    with _using_cancellations(()):
        yield


class _CancellationContext:
    """Capture a group's effective scopes for installation in its children."""

    def __init__(self) -> None:
        self._scopes = _context.get()

    @contextmanager
    def enter(self) -> Iterator[None]:
        with _using_cancellations(self._scopes):
            yield
