Routine
========

Start with regular functions, which need no decorator.
When a function also needs to support coroutine libraries,
write its implementation as an async function decorated with `@noio.routine`.
Existing callers can continue calling it normally.

Inside a routine, use `await function.a(...)` to call other routines.

Methods
-------

The `@routine` decorator works on methods.
If you're using it on a classmethod or staticmethod,
be sure to put classmethod or staticmethod directly above the function,
and this decorator above that.

```python
import asyncio
from typing import Self
import noio

class Greeter:
    def __init__(self, name: str):
        self.name = name

    @noio.routine
    @classmethod
    async def create(cls, name: str) -> Self:
        return cls(name)

assert Greeter.create("world").name == "world"
assert asyncio.run(Greeter.create.a("world")).name == "world"
```
