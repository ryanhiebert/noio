Selecting an operation
======================

Use `select` to wait for one of several selectable operations:

```python
from noio.select import select

index, value = select(first, second)
```

The result contains the winning selectable's zero-based index and its value.
If that selectable completes with an exception, `select` raises it instead.
At least one selectable is required.

Selectables register in the supplied order.
If the first selectable has a result ready,
it can deliver it before `select` even asks the second one.
Then it wins, with no race against later selectables.

`select` is a [routine](routine.md),
so it can be used as `await select.a(first, second)`.

See [implementing selectables](selectable.md) for the selectable interface
and its registration and cleanup contract.


Result types
------------

For up to ten statically known selectables,
the result type keeps each index paired with its value type.
For example, selecting between `Selectable[str]` and `Selectable[int]`
returns `tuple[Literal[0], str] | tuple[Literal[1], int]`.
Keep the result as a tuple and check `result[0]`
to let the type checker narrow `result[1]`.
Unpacking into separate `index` and `value` variables loses that relationship.
This applies to both `select(...)` and `await select.a(...)`.

Larger or dynamically sized selections return `tuple[int, T]`
when the selectables share a value type `T`,
and fall back to `tuple[int, Any]` otherwise.
