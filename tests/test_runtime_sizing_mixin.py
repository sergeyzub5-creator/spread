from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.workers.runtime_sizing_mixin import WorkerRuntimeSizingMixin


class _FakeSizingRuntime(WorkerRuntimeSizingMixin):
    def __init__(self) -> None:
        self.position = SimpleNamespace(left_side="SELL", right_side="BUY")
        self.left_leg_state = SimpleNamespace(filled_qty=Decimal("0.4"))
        self.right_leg_state = SimpleNamespace(filled_qty=Decimal("0.7"))
        self._left_instrument = object()
        self._right_instrument = object()
        self._latest_quotes = {
            self._left_instrument: SimpleNamespace(bid=Decimal("86.19"), ask=Decimal("86.20")),
            self._right_instrument: SimpleNamespace(bid=Decimal("86.24"), ask=Decimal("86.25")),
        }


class _FakeHardCapRuntime(WorkerRuntimeSizingMixin):
    def __init__(self) -> None:
        self.position = SimpleNamespace(left_side="SELL", right_side="BUY")
        self.left_leg_state = SimpleNamespace(filled_qty=Decimal("1.0"))
        self.right_leg_state = SimpleNamespace(filled_qty=Decimal("0.8"))
        self._left_instrument = object()
        self._right_instrument = object()
        self._latest_quotes = {
            self._left_instrument: SimpleNamespace(bid=Decimal("49"), ask=Decimal("50")),
            self._right_instrument: SimpleNamespace(bid=Decimal("49"), ask=Decimal("50")),
        }
        self.task = SimpleNamespace(
            target_notional=Decimal("100"),
            step_notional=Decimal("20"),
            runtime_params={"entry_cycle_growth_factor": "1.3"},
        )
        # Simulate stale/incomplete notional accounting while quantity is still reserved in-flight.
        self.active_entry_cycle = SimpleNamespace(
            target_notional_usdt=Decimal("0"),
            left_target_qty=Decimal("1.0"),
            right_target_qty=Decimal("1.0"),
        )
        self.prefetch_entry_cycle = None
        self._entry_growth_limited = False
        self._entry_growth_limit_qty = None
        self._entry_growth_limit_notional_usdt = None
        self._entry_cycle_success_streak = 0

    def _maybe_log_entry_cycle_clamp(self, **kwargs) -> None:
        return None


class RuntimeSizingMixinTests(unittest.TestCase):
    def test_current_position_notional_uses_worst_leg_qty(self) -> None:
        runtime = _FakeSizingRuntime()
        edge_result = SimpleNamespace(left_action="SELL", right_action="BUY")

        value = runtime._current_position_notional_usdt(edge_result)

        # Worst leg qty is 0.7; expensive reference price is right ask=86.25.
        self.assertEqual(value, Decimal("60.375"))

    def test_effective_cycle_notional_blocks_when_inflight_qty_already_consumes_cap(self) -> None:
        runtime = _FakeHardCapRuntime()
        edge_result = SimpleNamespace(left_action="SELL", right_action="BUY", best_edge=Decimal("0.3"), direction="EDGE_1")

        value = runtime._effective_entry_cycle_notional_usdt(edge_result)

        self.assertEqual(value, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
