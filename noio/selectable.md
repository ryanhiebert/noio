Implementing selectables
=======================

A selectable implements an operation offered as a choice in a selection.
Only one choice can win.
The `noio.selectable` module lets implementations check whether their choices
can still win and complete them with a value or an exception.

```python
from noio.selectable import SelectorGuard, selectors
```

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
