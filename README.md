noio
====

There are many opinions on the right way to do IO. There are so many libraries. Some use regular functions, while some use coroutines. Some use threads. Some use green threads. Some existing abstractions only work with coroutines on single threads. You just want people to be able to use your library.

`noio` is a minimal IO interface that provides primitives that work no matter what IO the user chooses, with clear documentation about compatibility for each implemented interface.

Getting Started
---------------

`noio` believes that coroutines are an advanced feature that should be avoided wherever reasonable. That includes getting started writing a library with `noio`. When writing a new library, start with regular functions, which are able to support threads and green threads. It might look something like this:

```python
def greeting(name: str) -> str:
    return f"Hello, {name}!"

assert greeting("world") == "Hello, world!"
```

When it's important for your function to also support coroutine libraries like `asyncio`, `trio` and `anyio`, just update it to look like something like this:

```python
import asyncio
import noio

@noio.routine
async def greeting(name: str) -> str:
    return f"Hello, {name}!"

@noio.routine
async def introduction(name: str) -> str:
    return await greeting.a(name)

assert greeting("world") == "Hello, world!"
assert introduction("world") == "Hello, world!"
assert asyncio.run(introduction.a("world")) == "Hello, world!"
```

When existing callers call your function the same way they used to, it will work without modification because it will drive the coroutine implementation to completion directly. Callers from async functions will await a new `.a()` version that allows the run loop to handle scheduling.

Finding all the places to convert can be tricky, so `noio` will raise an error if a routine implementation calls another routine with a regular function call.

Contributing
------------

Before making changes, read [CONTRIBUTING.md](CONTRIBUTING.md).
