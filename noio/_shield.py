"""Internal callable shielding for cleanup requiring native cancellation protection."""

from collections.abc import Callable
from functools import wraps
from inspect import iscoroutinefunction
from typing import Any
from typing import overload

from aiologic.lowlevel import shield as _shield

from ._cancel import shield_cancellation
from .routine import Routine
from .routine import routine


@overload
def shield[**P, T, **F](fn: Routine[P, T, F], /) -> Routine[P, T, F]: ...


@overload
def shield[**P, T](fn: Callable[P, T], /) -> Callable[P, T]: ...


def shield(fn: Any, /) -> Any:
    """Shield a callable from cancellation, preserving routine call interfaces."""

    @wraps(fn)
    def ordinary(*args: Any, **kwargs: Any) -> Any:
        with shield_cancellation():
            return fn(*args, **kwargs)

    @wraps(fn)
    async def asynchronous(*args: Any, **kwargs: Any) -> Any:
        with shield_cancellation():
            if isinstance(fn, Routine):
                return await fn.a(*args, **kwargs)
            return await fn(*args, **kwargs)

    if not isinstance(fn, Routine):
        return _shield(asynchronous if iscoroutinefunction(fn) else ordinary)

    @routine
    @wraps(fn, updated=())
    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        return await _shield(asynchronous(*args, **kwargs))

    return wrapped.fn(_shield(ordinary))
