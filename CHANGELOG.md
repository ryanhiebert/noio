Changelog
=========

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com).

Unreleased
----------

### Fixed

- Reject invalid routine cleanup return types in `@selectable` annotations.

### Added

- `noio.shield` to shield routines and other callables from cancellation.

- Custom registration and direct-call implementations for `@selectable`.

- Correlated index and value types for `select` results.

- Selectable registration may omit cleanup when none is needed.

- `@noio.selectable.selectable` to make a function or method usable directly or as a selectable.

- `noio.select.select` to wait for one of several selectable operations.

- `noio.cleared_mode` to adapt an ordinary callable
  to clear the coroutine execution mode before calling it,
  then restore the previous mode when the function completes.
  The mode is set automatically when entering a routine,
  so coroutine implementations can call the `.a()` versions
  without needing to know whether they are being driven by an ordinary call or an async runtime.
  Clearing this allows the callable to establish a new mode,
  such as when calling into another IO runtime.
  It supports both decorator and call-site forms.

- `Routine.fn` to provide an ordinary-function implementation
  while retaining the coroutine implementation for async runtimes.
  Ordinary calls use this implementation,
  as do awaited `.a()` calls within a coroutine driven by an ordinary call.
  Like `cleared_mode`, it clears the mode before calling the function,
  then restores the previous mode when the function completes.
  The override may accept broader arguments while preserving `.a()`'s signature.

- `noio.selectable.Selector`, `SelectorGuard`, and `selectors()`
  for acquiring selection guards together and completing selectable operations.
  `SelectionCollisionError` rejects guards belonging to the same selection.
- `@noio.contextmanager` to create context managers supporting both
  `with` and `async with` from an async generator.
  Context managers can also decorate existing routines.
- `noio.ContextManager` as an abstract base class
  for context managers with routine entry and exit methods.

- `noio.sleep` to pause execution using the current IO runtime.

[0.1] - 2026-10-17
-----------------

Initial experimental release.
Requires Python 3.15 or later.

### Added

- `@noio.routine` to support ordinary calls and awaited `.a()` calls
  from a single coroutine implementation.
- A guard against ordinary routine calls inside routine implementations.

[0.1]: https://github.com/ryanhiebert/noio/releases/tag/0.1
