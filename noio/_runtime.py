"""Select the runtime for an ordinary or coroutine operation."""

import sys
from typing import Literal

from aiologic.lowlevel import current_async_library
from aiologic.lowlevel import current_green_library

from .routine import routine

type Runtime = Literal["threading", "eventlet", "gevent", "asyncio", "curio", "trio"]


@routine
async def current_runtime() -> Runtime:  # pyright: ignore[reportRedeclaration]
    library = current_async_library()
    if library in ("asyncio", "curio", "trio"):
        return library
    raise RuntimeError(f"Unrecognized coroutine runtime: {library!r}")


@current_runtime.fn
def current_runtime() -> Runtime:
    library = current_green_library()
    # Patched threads use gevent even before its first hub is created.
    if library == "threading" and "gevent.monkey" in sys.modules:
        from gevent.monkey import is_object_patched

        if is_object_patched("threading", "Thread"):
            return "gevent"
    if library in ("threading", "eventlet", "gevent"):
        return library
    raise RuntimeError(f"Unrecognized ordinary runtime: {library!r}")
