Queues
======

A `Queue` stores values in first-in, first-out order.
`put(value)` waits for space and returns `None` after storing the value.
`get()` waits for a value and removes it.

```python
from noio import Queue

queue = Queue[str](maxsize=2)
queue.put("first")
queue.put("second")
assert queue.get() == "first"
assert queue.get() == "second"
```

The default `maxsize=0` allows an unbounded buffer.
A positive `maxsize` limits the number of buffered values;
puts wait while the buffer is full.
Negative capacities raise `ValueError`.
Capacity zero does not request an unbuffered handoff.

Selection
---------

Both puts and gets are selectable.
For example, wait for the first available value from two queues:

```python
from noio.select import select

primary = Queue[str]()
secondary = Queue[str]()
secondary.put("ready")

assert select(primary.get.select(), secondary.get.select()) == (1, "ready")
```

Only the winning operation changes its queue.
Cancellation or choosing another operation removes a pending registration
without consuming a value or adding one.
A completed put stores its value independently of when a getter receives it.

A selection may offer both a get and a put on the same queue.
They remain alternatives: they cannot exchange with each other.
On an empty queue, the put can win and leave its value for a later get.

```python
queue = Queue[str](maxsize=1)
assert select(queue.get.select(), queue.put.select("ready")) == (1, None)
assert queue.get() == "ready"
```

Shutdown
--------

`shutdown()` rejects new puts and wakes blocked putters with `ShutDown`.
Buffered values remain available to getters.
Once the buffer is empty, gets raise `ShutDown` too.
Waiting getters on an empty queue wake with `ShutDown`.

```python
from noio import ShutDown

queue.put("last")
queue.shutdown()
assert queue.get() == "last"

try:
    queue.get()
except ShutDown:
    pass
```

`shutdown(immediate=True)` also discards buffered values,
so gets immediately raise `ShutDown`.
Repeated shutdown calls are harmless.
An immediate shutdown can stop a drain already in progress;
a later ordinary shutdown does not reopen the queue.

Shutdown returns after updating the queue and notifying affected waiters.
It does not wait for consumers to drain the buffer or finish their work.

Coroutine calls
---------------

Use `.a()` for coroutine calls to `put`, `get`, and `shutdown`.
Use the same prepared operations with `select.a()`.

```python
queue = Queue[str](maxsize=2)
await queue.put.a("ready")
assert await queue.get.a() == "ready"
await queue.shutdown.a()
```
