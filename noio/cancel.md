Cancellation scopes
===================

Use `cancel` to give selectable IO a shared time budget:

```python
import noio

event = noio.Event()
try:
    with noio.cancel(timeout=5):
        event.wait()
except noio.CancelledError:
    print("The wait was cancelled")
```

The countdown starts on context entry.
Every call to `select` or a selectable operation within the scope participates,
including direct calls such as `event.wait()` and `sleep()`.
When the timeout expires, those calls raise `CancelledError`.

Cancellation remains active after an error is caught;
subsequent IO raises again, even when the operation is immediately ready.
A block that finishes normally succeeds even if its timeout has elapsed.

Nested scopes both remain active; an inner timeout cannot extend an outer deadline.
Zero cancels at the first selectable IO operation.
Omitting `timeout`, or using positive infinity, sets no deadline.
Negative durations and NaN are rejected.

Use the scope's `cancel()` method to cancel explicitly:

```python
with noio.cancel() as scope:
    scope.cancel()
    # Subsequent selectable IO raises CancelledError.
```

Another worker can call `scope.cancel()` to interrupt waits in the scope.
Repeated calls are harmless.
In coroutine code, use `await scope.cancel.a()`.
Explicit cancellation and timeout expiry have the same effect.

Use `shield=True` to give recovery work an independent time budget:

```python
with noio.cancel(timeout=5):
    try:
        event.wait()
    except noio.CancelledError:
        with noio.cancel(timeout=1, shield=True):
            recovery_complete.wait()
```

A shield excludes enclosing noio cancellation scopes.
Its own timeout and scopes entered inside it still apply.
Use `cancel(shield=True)` for a shield without a deadline.
Leaving a shield restores enclosing cancellation without itself raising an error.

Use `async with` in coroutine code:

```python
async def wait_for_event(event):
    async with noio.cancel(timeout=5):
        await event.wait.a()
```

Cancellation delivery currently occurs only through noio selectable IO.
It does not interrupt arbitrary code or other libraries' blocking calls.
Cancellation can be delayed while an operation finishes necessary cleanup.

Under Trio, shielding also blocks enclosing native cancellation scopes;
inner native cancellation scopes remain effective.
Native integration is incomplete elsewhere:
asyncio task cancellation, AnyIO cancellation on asyncio,
and gevent kills and native timeouts can still interrupt shielded blocks.

Native thread and task launchers do not provide supported noio cancellation inheritance.
Do not rely on them to give child tasks or threads the enclosing cancellation behavior.
