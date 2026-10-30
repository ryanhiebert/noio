Sleeping
========

Use `noio.sleep` to wait for a duration in seconds:

```python
import noio

noio.sleep(0.1)
```

It suspends the caller using the current runtime and returns `None`.

Use `sleep.select()` to put a timeout alongside another operation:

```python
from noio.select import select

ready = noio.Event()
index, _ = select(ready.wait.select(), noio.sleep.select(5))
if index == 1:
    print("Still waiting after five seconds")
```

The countdown starts when the sleep is registered by `select`.
A prepared sleep can be reused; each registration starts a fresh countdown.

In coroutine code, use `await noio.sleep.a(0.1)` for a direct wait,
or `await select.a(ready.wait.select(), noio.sleep.select(5))`
to wait with a timeout.

The duration must be non-negative and not NaN.
Zero yields execution without a timed delay.
Positive infinity waits indefinitely, until interrupted or cancelled.
Scheduling may delay the caller's resumption beyond the requested duration.
