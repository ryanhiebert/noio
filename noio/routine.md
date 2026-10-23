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


Calling ordinary functions
--------------------------

A routine sets the coroutine execution mode when entering its coroutine implementation.
This is why noio can require the implementation to use `await function.a(...)`
to call other routines, whether an ordinary call or an async runtime is driving it.
The mode tells those routines which implementation to use.

Sometimes a routine needs to clear the mode for a call,
so the function runs as if no coroutine execution mode had been activated.
For example, a thread making an ordinary call to a noio routine
might need to start an `asyncio` event loop inside that routine.
Without clearing the mode,
noio routines awaited in that event loop would still use ordinary execution,
even though they should use their coroutine implementations under the event loop.
Use `noio.cleared_mode` to clear the surrounding mode before calling the function:

```python
import asyncio
import noio

@noio.routine
async def run_delay(seconds: float) -> None:
    noio.cleared_mode(asyncio.run)(noio.sleep.a(seconds))

run_delay(0.01)
```

The ordinary call to `run_delay` drives its coroutine implementation.
Clearing the mode before calling `asyncio.run` lets the event loop
use `noio.sleep`'s coroutine implementation, which yields to the loop.
Without clearing it, `noio.sleep.a()` would use the ordinary implementation
and block the event loop with `time.sleep`.

You can also decorate a function that starts the event loop:

```python
@noio.cleared_mode
def run_sleep(seconds: float) -> None:
    asyncio.run(noio.sleep.a(seconds))

@noio.routine
async def run_delay(seconds: float) -> None:
    run_sleep(seconds)

run_delay(0.01)
```

Both forms return a callable that clears the mode before invoking the function
and restores the previous mode when it returns or raises an exception.
Call it directly, including inside a routine's coroutine implementation.
Routines called by the function can then establish their own mode.
This also allows the function to call into another IO runtime,
subject to that runtime's rules about nesting.

The function runs in the caller's thread and task.
If it blocks, the caller blocks too.
The adapted function has no coroutine implementation.
APIs that require a `Routine` do not accept it.


Providing an ordinary implementation
------------------------------------

By default, an ordinary call to a routine drives its coroutine implementation directly.
When ordinary calls need a different implementation,
use the routine's `.fn` decorator:

```python
@noio.routine
async def greeting(name: str) -> str:
    return f"Hello, {name}!"

@greeting.fn
def greeting(name: str | bytes) -> str:
    if isinstance(name, bytes):
        name = name.decode()
    return f"Hello, {name}!"

assert greeting(b"world") == "Hello, world!"
```

Ordinary calls use the supplied function.
Awaited `.a()` calls also use it when an outer routine is driven by an ordinary call.
Under an async runtime, `.a()` uses the coroutine implementation.
This lets routines compose through `.a()` while using the implementation
appropriate to their caller.

Like `cleared_mode`, the routine clears the mode before calling the supplied function,
then restores the previous mode when the function returns or raises.
It can call other routines normally or call into another IO runtime.
The coroutine implementation continues to use `await other.a(...)`.

The function must accept the coroutine implementation's arguments
and return its result type.
It may also accept additional argument types or optional parameters.
In the example, ordinary calls accept `str` or `bytes`,
while `.a()` accepts `str`.

`.fn` returns a new routine, leaving the original unchanged.
The new routine keeps the coroutine's name, documentation, and signature.
Methods preserve their binding;
for classmethods and staticmethods, put the corresponding decorator
below `.fn`, just as with `@routine`.
Pyright currently reports a redeclaration for same-name definitions;
a targeted `# pyright: ignore[reportRedeclaration]` on the coroutine definition
suppresses that diagnostic.

`Routine[P, R, F]` describes coroutine parameters `P`, result type `R`,
and ordinary-function parameters `F`.
`F` defaults to `P`, so `Routine[P, R]` describes matching signatures.
The `.fn` decorator infers the ordinary-function parameters from its argument.
