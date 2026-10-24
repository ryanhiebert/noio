"""Shield ordinary callables, coroutine functions, and routines."""

from collections.abc import Callable
from functools import wraps
from typing import Any
from typing import overload

from aiologic.lowlevel import shield as _shield

from .routine import Routine
from .routine import routine

__all__ = ["shield"]


@overload
def shield[**P, T, **F](fn: Routine[P, T, F], /) -> Routine[P, T, F]: ...


@overload
def shield[**P, T](fn: Callable[P, T], /) -> Callable[P, T]: ...


def shield(fn: Any, /) -> Any:
    """Shield a callable from cancellation, preserving routine call interfaces."""
    if not isinstance(fn, Routine):
        return _shield(fn)

    @routine
    @wraps(fn, updated=())
    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        return await _shield(fn.a(*args, **kwargs))

    return wrapped.fn(_shield(fn))
