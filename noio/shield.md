Shielding calls
===============

`shield` lets a call finish despite cancellation of its caller.
Apply it to an ordinary callable:

```python
from noio import shield, sleep

@shield
def finish() -> str:
    sleep(0.1)
    return "finished"

result = finish()
```

Acquire and release owned resources inside the shielded function.
Some runtimes execute it in a separate task or greenlet.
Cancellation may still reach the caller after the protected work finishes.

Supporting coroutine callers
----------------------------

To support both ordinary and coroutine calls,
wrap a routine with `@shield`:

```python
from noio import routine

@shield
@routine
async def finish() -> str:
    await sleep.a(0.1)
    return "finished"

result = finish()
```

Coroutine callers use `.a()`:

```python
result = await finish.a()
```

