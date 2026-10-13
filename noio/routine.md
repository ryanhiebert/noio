Routines
========

Use `@noio.routine` when your implementation is an async function. Call it normally from regular functions, or await its `.a()` version from async functions. Regular functions need no decorator.

Inside an async function, use `await function.a(...)` to call `noio` routines.

Methods
-------

The `@routine` decorator works on methods. If you're using it on a classmethod or staticmethod, be sure to put classmethod or staticmethod directly above the function, and this decorator above that.

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
