import subprocess
import sys
import sysconfig

import pytest


@pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded CPython build",
)
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gevent requires the GIL; revisit sync_runtime skips when this passes",
)
def test_gevent_preserves_disabled_gil() -> None:
    # Ignore PYTHON_GIL overrides so this probes gevent's default behavior.
    # Keep imports isolated from the rest of the test suite.
    result = subprocess.run(
        [
            sys.executable,
            "-E",
            "-c",
            "import sys\n"
            "assert not sys._is_gil_enabled()\n"
            "import gevent\n"
            "print(sys._is_gil_enabled())\n",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    assert result.stdout.strip() == "False", result.stderr
