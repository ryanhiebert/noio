noio
====

`noio` brings together two big ideas:
that many IO operations can support callers waiting on multiple things at once without compromise,
and that IO can be abstracted across ordinary functions and coroutines.

Use `select` to wait for the first of several selectable operations to complete.
Use `routine` to support ordinary and coroutine calls with one interface.

Wait for the first operation to complete
---------------------------------------

Queues provide selectable `get` and `put` methods.
Ordinary calls wait for values from each queue in turn:

```python
from noio import Queue

primary = Queue[str]()
secondary = Queue[str]()

# Producers in other threads or tasks put values into these queues.
primary_value = primary.get()
secondary_value = secondary.get()
```

The first call waits for `primary`, even if `secondary` already has data available.
Use `select` to get a value from whichever queue can deliver first:

```python
from noio.select import select

index, value = select(
    primary.get.select(),
    secondary.get.select(),
)
```

If `primary` wins, `index` is `0` and `value` comes from `primary`.
If `secondary` wins, `index` is `1` and `value` comes from `secondary`.
The other queue keeps its values for a later get.
See [selection](noio/select.md) and [queues](noio/queue.md) for usage.

Support ordinary and coroutine calls
------------------------------------

Coroutines are an advanced feature
that should be avoided wherever reasonable.
When writing a new library, start with regular functions,
which are able to support threads and green threads.
It might look something like this:

```python
import noio

def greeting(name: str) -> str:
    noio.sleep(0.1)
    return f"Hello, {name}!"

assert greeting("world") == "Hello, world!"
```

When it's important for your function to also support coroutine libraries
like `asyncio`, `trio` and `anyio`,
update it to look like something like this:

```python
import asyncio
import noio

@noio.routine
async def greeting(name: str) -> str:
    await noio.sleep.a(0.1)
    return f"Hello, {name}!"

@noio.routine
async def introduction(name: str) -> str:
    return await greeting.a(name)

assert greeting("world") == "Hello, world!"
assert introduction("world") == "Hello, world!"
assert asyncio.run(introduction.a("world")) == "Hello, world!"
```

When existing callers call your function the same way they used to,
it will work without modification
because it will drive the coroutine implementation to completion directly.
Other routines will await a new `.a()` version
that allows the run loop to handle scheduling.

Finding all the places to convert can be tricky,
so `noio` will raise an error
if a routine implementation calls another routine with a regular function call.

See [routines](noio/routine.md) for usage and customization.

Contributing
------------

Before making changes, read [CONTRIBUTING.md](CONTRIBUTING.md).
