import asyncio
import subprocess
import sys
import sysconfig
from collections.abc import Callable
from typing import assert_type

import pytest
from aiologic.lowlevel import current_async_library
from aiologic.lowlevel import current_green_library

from . import _runtime
from ._runtime import Runtime
from ._runtime import current_runtime
from .conftest import AsyncRuntime
from .conftest import RunSync
from .routine import routine


@routine
async def _runtime_through_routine() -> Runtime:
    return await current_runtime.a()


def _check_ordinary(checkpoint: Callable[[], None]) -> None:
    expected = current_green_library()
    assert assert_type(current_runtime(), Runtime) == expected
    assert _runtime_through_routine() == expected


def test_ordinary(sync_runtime: RunSync) -> None:
    sync_runtime(_check_ordinary)


def test_coroutine(async_runtime: AsyncRuntime) -> None:
    async def main() -> None:
        assert (
            assert_type(await current_runtime.a(), Runtime) == current_async_library()
        )
        assert await _runtime_through_routine.a() == current_async_library()
        # Ordinary calls inside a coroutine still select ordinary execution.
        assert current_runtime() == "threading"
        assert _runtime_through_routine() == "threading"

    async_runtime.run(main)


@pytest.mark.skipif(
    bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
    reason="gevent does not support free-threaded CPython",
)
@pytest.mark.parametrize("import_first", [False, True])
def test_patched_threads_before_hub_creation(import_first: bool) -> None:
    script = "import noio._runtime\n" if import_first else ""
    script += (
        "from gevent import monkey\n"
        "monkey.patch_all()\n"
        "from noio._runtime import current_runtime\n"
        "assert current_runtime() == 'gevent'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("library", ["eventlet", "gevent"])
def test_recognized_ordinary_names(
    monkeypatch: pytest.MonkeyPatch, library: str
) -> None:
    monkeypatch.setattr(_runtime, "current_green_library", lambda: library)
    assert current_runtime() == library


def test_recognized_curio_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_runtime, "current_async_library", lambda: "curio")
    assert asyncio.run(current_runtime.a()) == "curio"


def test_unknown_runtime_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_runtime, "current_green_library", lambda: "unknown")
    with pytest.raises(RuntimeError, match="Unrecognized ordinary runtime: 'unknown'"):
        current_runtime()
    monkeypatch.setattr(_runtime, "current_async_library", lambda: "unknown")
    with pytest.raises(RuntimeError, match="Unrecognized coroutine runtime: 'unknown'"):
        asyncio.run(current_runtime.a())
