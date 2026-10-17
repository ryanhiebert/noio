Changelog
=========

All notable changes to this project will be documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com).

[0.1] - 2026-10-17
-----------------

Initial experimental release.
Requires Python 3.15 or later.

### Added

- `@noio.routine` to support ordinary calls and awaited `.a()` calls
  from a single coroutine implementation.
- A guard against ordinary routine calls inside routine implementations.

[0.1]: https://github.com/ryanhiebert/noio/releases/tag/0.1
