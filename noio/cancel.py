"""Establish a cancellation scope for selectable IO."""

from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import AbstractContextManager
from contextlib import nullcontext
from math import inf
from time import monotonic

from ._cancel import CancelledError as CancelledError
from ._cancel import enter_cancel_scope
from ._cancel import shield_cancellation
from ._cancel_scope import _CancelScope
from ._contextlib import contextmanager
from ._runtime import current_runtime
from ._timer import _timer


@contextmanager
async def cancel(
    *, timeout: float = inf, shield: bool = False
) -> AsyncIterator[_CancelScope]:
    """Cancel selectable IO when the scope deadline expires."""
    if not timeout >= 0:
        raise ValueError("timeout must be non-negative and not NaN")
    runtime = await current_runtime.a()
    protection: AbstractContextManager[object] = nullcontext()
    if shield and runtime == "trio":
        import trio

        protection = trio.CancelScope(shield=True)
    with protection, shield_cancellation() if shield else nullcontext():
        clock: Callable[[], float]
        match runtime:
            case "trio":
                import trio

                clock = trio.lowlevel.current_clock().current_time
            case "asyncio":
                import asyncio

                clock = asyncio.get_running_loop().time
            case _:
                clock = monotonic
        scope = _CancelScope(clock() + timeout, clock)
        async with _timer(max(0.0, scope.deadline - clock()), scope.cancelled.set):
            with enter_cancel_scope(scope):
                yield scope
