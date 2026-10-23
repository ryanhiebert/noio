Sleeping
========

Use `noio.sleep` to wait for a duration in seconds:

```python
import noio

noio.sleep(0.1)
```

`sleep` is a [routine](routine.md),
so it can also be used as `await noio.sleep.a(0.1)`.
It suspends the caller using the current runtime and returns `None`.

The duration must be non-negative and not NaN.
Zero yields execution without a timed delay.
Positive infinity waits indefinitely, until interrupted or cancelled.
Scheduling may delay the caller's resumption beyond the requested duration.
