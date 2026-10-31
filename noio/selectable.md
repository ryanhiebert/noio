Implementing selectables
=======================

A selectable implements an operation offered as a choice in a selection.
Only one choice can win.
The `noio.selectable` module lets implementations check whether their choices
can still win and complete them with a value or an exception.

Decorating a function or method
-------------------------------

Use `@selectable` to make a registration context manager callable usable as an operation:

```python
from collections.abc import Iterator
from contextlib import contextmanager

from noio.select import select
from noio.selectable import SelectorGuard, selectable, selectors

@selectable
@contextmanager
def ready(guard: SelectorGuard[str], value: str) -> Iterator[None]:
    with selectors(guard) as (selector,):
        if selector is not None:
            selector.result(value)
    yield

assert ready("hello") == "hello"
assert select(ready.select("first"), ready.select("second")) == (0, "first")
```

A direct call selects that one operation and returns its value.
`.select(...)` captures the arguments without registering the operation.

For a method that also supports coroutine calls, use [noio.contextmanager](contextlib.md).
`.fn` forwards to that callable to customize its ordinary generator implementation:

```python
from collections.abc import AsyncIterator
import noio

class Greeter:
    @selectable
    @noio.contextmanager
    async def greet(self, guard: SelectorGuard[str], name: str) -> AsyncIterator[None]:
        async with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(f"Hello, {name}!")
        yield

    @greet.fn
    def greet(self, guard: SelectorGuard[str], name: str) -> Iterator[None]:
        with selectors(guard) as (selector,):
            if selector is not None:
                selector.result(f"Hello, {name}!")
        yield

greeter = Greeter()
assert greeter.greet("world") == "Hello, world!"
```

This also supports `await greeter.greet.a("world")`
and `await select.a(greeter.greet.select("world"), other)`.
Both implementations receive the guard and register before yielding.
Put deregistration after the yield, typically in a `finally` block.

Registration and cleanup
------------------------

`Selectable[T]` from `noio.select` describes an operation that can complete with a value of type `T`.
Implement `__select__(self, guard)` to return a context manager for the supplied `SelectorGuard[T]`.
Entering registers the choice; exiting removes the registration.
Registration may complete the choice immediately or retain its guard for later.
A generator registration without cleanup still yields once.

Ordinary selection uses ordinary context-manager entry and exit.
Async selection requires routine-based `noio.ContextManager` instances;
it constructs and checks all registration contexts before entering any of them.
`@noio.contextmanager` supports both forms.

Contexts are entered in order, including those after a winner,
and exited in reverse order, including the winner.
Before exiting any successfully entered context, selection makes all its guards unavailable
and finishes any registration that is still running or unwinding.
If entry raises or is cancelled, that context must undo its own partial registration.

Every registration exits normally: its exit method receives `(None, None, None)`.
It does not receive selection errors, cancellation, or another registration's errors.
Entry values and exit return values are ignored.
An exit error propagates, but does not prevent the remaining contexts from exiting.

Runtime cancellation propagates even if a choice has already completed.
Completion is not undone, but the caller does not receive its result.
Invalidating the guards is protected from cancellation;
context exits run in the caller's context and handle their own cancellation needs.

Completing a choice
-------------------

The selection machinery supplies a `SelectorGuard[T]` for each choice.
It is an opaque handle that a selectable can retain while its operation waits.
`T` describes the value the choice receives when it wins.
Selectables use the supplied guards rather than constructing their own.

Enter `selectors(guard)` to obtain temporary permission to complete the choice:

```python
def complete(guard: SelectorGuard[str], message: str) -> bool:
    with selectors(guard) as (selector,):
        if selector is None:
            return False
        selector.result(message)
        return True
```

The context yields a tuple containing a `Selector[T]` or `None` for each guard.
`None` means that choice can no longer win:
its selection has already completed or has been closed, for example by cancellation.

`selector.result(value)` completes the choice with a value.
`selector.error(exception)` completes it with an exception for the consumer to raise.
Passing an exception to `result()` instead delivers the exception object as a value.
Both methods return `None`, and either makes all other choices in that selection unavailable.

Acquisition does not oblige the selectable to complete the choice.
It may leave the context without calling either method and try again later.
The guard remains usable for another attempt,
but each acquired selector is valid only inside the context that yielded it
and in the task, thread, or greenlet that entered that context.
Completing a selector twice or using it after context exit raises `RuntimeError`.

If the selectable protects a resource with a lock,
acquire that lock before entering `selectors()`.
Keep it held while checking the resource and completing the operation.

Exchanging between selections
-----------------------------

An operation may need several participants from distinct selections.
Pass their guards to one `selectors(*guards)` call.
It accepts any number of guards and yields selectors or `None`
in the same order as the arguments.
Availability is checked independently for each choice.

For a two-way exchange, each participant receives what the other sends:

```python
async def exchange(
    left_guard: SelectorGuard[str],
    left_value: int,
    right_guard: SelectorGuard[int],
    right_value: str,
) -> bool:
    # The caller holds any locks protecting the resource.
    async with selectors(left_guard, right_guard) as (left, right):
        if left is None or right is None:
            return False
        left.result(right_value)
        right.result(left_value)
        return True
```

Use `with` for ordinary execution and `async with` for asynchronous execution.
The example handles only completion;
the selectable also manages its resource's bookkeeping.
For example, a queue removes matched or stale entries.
An unavailable participant does not imply that its peer is also unavailable:
never discard a waiting peer just because the incoming choice can no longer win.

Each participant can receive a value or an exception independently.
Prepare the outcomes and perform potentially failing work
before completing the first participant.
Then complete all participants without intervening awaits or other fallible work.

Completion takes effect immediately, including notification of the consumer.
Consumers wait for the selection locks to be released before reading their outcomes.
If an implementation completes one participant and then raises,
that participant remains completed; there is no rollback.
Context exit releases the locks and ends the temporary selectors' lifetimes,
even after an exception or cancellation.

Locking and collisions
----------------------

`selectors()` acquires all participating selection locks in a consistent order
before checking availability, and releases them in reverse order.
A failed or cancelled acquisition releases any locks it has already acquired.

Acquire resource locks before selection locks.
Never acquire a resource lock while holding selectors,
and do not nest `selectors()` contexts.
Pass all guards needed for an attempt to one call.
Larger groups are supported, but hold more selections' locks
than trying one candidate pair at a time.

Two guards from the same selection cannot participate together:
that would allow two choices in one selection to win.
Passing them to `selectors()` raises `SelectionCollisionError`,
a `ValueError` subclass exported by `noio.selectable`.
The check occurs before any locks are acquired
and also rejects repeated guards and guards whose selection is already unavailable.
Let this error propagate to the consumer.

Typing and empty acquisition
----------------------------

For one, two, or three guards,
the tuple annotations preserve each guard's individual value type.
Larger groups have a fallback annotation of `tuple[Selector[Any] | None, ...]`.
Calling `selectors()` without arguments yields an empty tuple.

Customizing direct execution
----------------------------

Use `.call` to supply a direct implementation that avoids selection.
It receives the operation's arguments without a guard
and returns the operation's result:

```python
@ready.call
def ready(value: str) -> str:
    return value
```

`.select(...)` continues to use the registration implementation.
The direct implementation may also be a routine:

```python
@ready.call
@noio.routine
async def ready(value: str) -> str:
    return value
```

When the direct implementation is a [routine](routine.md),
use `.call_fn` to supply its ordinary implementation:

```python
@ready.call_fn
def ready(value: str) -> str:
    return value
```
