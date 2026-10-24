Events
======

An `Event` records completion for all current and future waiters.
Create a new Event for each completion.

```python
from noio import Event
from noio.select import select

finished = Event()
stopped = Event()
```

Wait for completion
-------------------

Another thread or task signals completion with `finished.set()`.
Choose how to wait:

### Ordinary call

```python
finished.wait()
```

### Ordinary select

```python
index, value = select(finished.wait.select(), stopped.wait.select())
```

### Coroutine call

Coroutine callers signal completion with `await finished.set.a()`.

```python
await finished.wait.a()
```

### Coroutine select

```python
index, value = await select.a(finished.wait.select(), stopped.wait.select())
```

Completion persists
-------------------

`set` may be called many times.
Subsequent calls will succeed, effectively doing nothing.

`wait` may be called many times, even after completion.
Before completion, it will wait for the event.
After completion, it will respond immediately.

```python
finished.set()
finished.set()

assert finished.wait() is None
assert finished.wait() is None
```
