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
