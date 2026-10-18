Context managers
================

`noio.contextmanager` is like
[`contextlib.contextmanager`](https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager),
but takes an async generator
and produces a context manager supporting both `with` and `async with`.

When you want your normal context manager to also work in async loops,
decorate it with `noio.contextmanager`,
make its generator function async,
and convert calls to routines to their awaited `.a()` versions.

```python
from collections.abc import AsyncIterator

import noio

@noio.contextmanager
async def temporary_items() -> AsyncIterator[list[str]]:
    items: list[str] = []
    try:
        yield items
    finally:
        items.clear()
```

Context managers can decorate routines.
Place the context decorator above `@noio.routine`;
decorating a non-routine raises `TypeError`.

```python
@temporary_items()
@noio.routine
async def operation() -> None:
    ...
```
