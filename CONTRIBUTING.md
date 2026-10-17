Contributing
============

Choose APIs that best serve noio's goals.
Compatibility with existing libraries is valuable,
but we are free to make different choices when they improve the API.
Trio has a history of thoughtful API design
and is a useful reference when evaluating those choices.
Consider the reasoning behind its decisions,
without treating them as requirements for noio.

Do not attempt to retain backward compatibility between versions,
even for features that have already been released.
Document incompatible changes under Breaking Changes in the changelog.

Small releases are welcome as long as the included features are ready for people to use,
and we have reasonable confidence that any breaking changes are documented in the changelog.

Before adding a dependency, check that its license is compatible with
distributing noio under the MIT license.
Check any attribution or license notice requirements as well.

Design primitives so the same instance can be shared across supported runtimes,
including runtimes running concurrently in different threads.
Where applicable, test interaction between runtimes,
not just each runtime independently.
Treat compatibility across supported runtimes as the default.
Individual primitive docs only need to describe compatibility limitations.

Do not rely on any kind of monkeypatching being in place.
When monkeypatching is predictable, such as with gevent,
make sure the implementation will work whether it is patched or not.

Every bug fix must include a regression test
that fails without the fix and passes with it.

Record user-facing changes in `CHANGELOG.md` under an Unreleased section,
creating that section when needed.
At release time, turn it into a dated version entry.

Use semantic line breaks in Markdown prose.
Start each sentence on a new line,
and break longer sentences at natural clause or phrase boundaries
rather than wrapping to a fixed width.
