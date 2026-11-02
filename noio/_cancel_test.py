from math import inf
from time import monotonic

import pytest

from ._cancel import _CancellationContext
from ._cancel import current_cancellations
from ._cancel import enter_cancel_scope
from ._cancel import shield_cancellation
from .cancel import _CancelScope


def test_nested_shields_restore_enclosing_timeouts() -> None:
    outer, inner = _CancelScope(inf, monotonic), _CancelScope(inf, monotonic)
    with enter_cancel_scope(outer):
        with shield_cancellation():
            assert current_cancellations() == ()
            with enter_cancel_scope(inner):
                assert current_cancellations() == (inner,)
                with shield_cancellation():
                    assert current_cancellations() == ()
                assert current_cancellations() == (inner,)
        assert current_cancellations() == (outer,)
    assert current_cancellations() == ()


def test_child_installs_captured_scopes_and_restores_caller() -> None:
    outer, caller = _CancelScope(inf, monotonic), _CancelScope(inf, monotonic)
    with enter_cancel_scope(outer):
        inherited = _CancellationContext()
        with shield_cancellation():
            shielded = _CancellationContext()
        with enter_cancel_scope(caller):
            assert current_cancellations() == (outer, caller)
            with inherited.enter():
                assert current_cancellations() == (outer,)
            with pytest.raises(ValueError), shielded.enter():
                assert current_cancellations() == ()
                raise ValueError("child failed")
            assert current_cancellations() == (outer, caller)
    assert current_cancellations() == ()
