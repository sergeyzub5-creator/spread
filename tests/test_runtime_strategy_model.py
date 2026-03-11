from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.workers.runtime_execution_mixin import WorkerRuntimeExecutionMixin
from app.core.workers.runtime_exit_orchestrator import current_exit_edge, exit_sides_for_position
from app.core.workers.runtime_sizing_mixin import WorkerRuntimeSizingMixin
from app.core.workers.runtime_state_guards import exit_signal_active


class _FakeSizingRuntime(WorkerRuntimeSizingMixin):
    def __init__(self) -> None:
        self.position = SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY")
        self._entry_growth_limited = False
        self._exit_active = False

    def _exit_signal_active(self) -> bool:
        return self._exit_active

    def _effective_entry_cycle_notional_usdt(self, _edge_result) -> Decimal:
        return Decimal("1")


class _FakeExecutionRuntime(WorkerRuntimeExecutionMixin):
    def __init__(self, *, left_side: str | None, left_qty: Decimal) -> None:
        self.left_leg_state = SimpleNamespace(side=left_side, filled_qty=left_qty)
        self.right_leg_state = SimpleNamespace(side=None, filled_qty=Decimal("0"))


class RuntimeStrategyModelTests(unittest.TestCase):
    def test_entry_capacity_does_not_block_on_direction_flip(self) -> None:
        runtime = _FakeSizingRuntime()
        edge_result = SimpleNamespace(left_action="BUY", right_action="SELL")

        reason = runtime._entry_capacity_block_reason(edge_result)

        self.assertIsNone(reason)

    def test_entry_capacity_does_not_block_when_exit_signal_active(self) -> None:
        runtime = _FakeSizingRuntime()
        runtime._exit_active = True
        edge_result = SimpleNamespace(left_action="SELL", right_action="BUY")

        reason = runtime._entry_capacity_block_reason(edge_result)

        self.assertIsNone(reason)

    def test_exit_sides_prefer_live_leg_sides(self) -> None:
        runtime = SimpleNamespace(
            position=SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY", left_side="SELL", right_side="BUY"),
            left_leg_state=SimpleNamespace(side="BUY"),
            right_leg_state=SimpleNamespace(side="SELL"),
        )

        left_side, right_side = exit_sides_for_position(runtime)

        self.assertEqual((left_side, right_side), ("SELL", "BUY"))

    def test_position_effect_reduces_on_opposite_non_reduce_side(self) -> None:
        runtime = _FakeExecutionRuntime(left_side="BUY", left_qty=Decimal("0.7"))
        request = SimpleNamespace(side="SELL", reduce_only=False)

        effect = runtime._resolve_leg_request_position_effect(leg_name="left", request=request)

        self.assertEqual(effect, Decimal("-1"))

    def test_exit_sides_require_known_leg_sides(self) -> None:
        runtime = SimpleNamespace(
            position=SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY", left_side=None, right_side=None),
            left_leg_state=SimpleNamespace(side=None),
            right_leg_state=SimpleNamespace(side=None),
        )

        left_side, right_side = exit_sides_for_position(runtime)

        self.assertEqual((left_side, right_side), (None, None))

    def test_current_exit_edge_works_without_position_object(self) -> None:
        runtime = SimpleNamespace(
            position=None,
            _left_instrument="L",
            _right_instrument="R",
            _latest_quotes={
                "L": SimpleNamespace(bid=Decimal("100"), ask=Decimal("101")),
                "R": SimpleNamespace(bid=Decimal("102"), ask=Decimal("103")),
            },
            left_leg_state=SimpleNamespace(side="BUY"),
            right_leg_state=SimpleNamespace(side="SELL"),
            _safe_edge=lambda a, b: (a - b) / b if b > Decimal("0") else None,
        )

        edge = current_exit_edge(runtime)

        self.assertIsNotNone(edge)

    def test_exit_signal_active_uses_threshold_as_convergence_ceiling(self) -> None:
        # Выход когда спред *сузился*: abs(edge) <= порог (не пол как у входа).
        runtime = SimpleNamespace(
            _is_spread_entry_runtime=True,
            position=SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY"),
            _is_simulated_signal_mode=lambda: False,
            _simulated_exit_window_open=False,
            _decimal_or_zero=lambda value: Decimal(str(value)),
            task=SimpleNamespace(exit_threshold=Decimal("0.1"), runtime_params={}),
            _current_exit_edge=lambda: Decimal("0.05"),
        )

        self.assertTrue(exit_signal_active(runtime))

    def test_exit_signal_active_false_when_spread_still_wide(self) -> None:
        runtime = SimpleNamespace(
            _is_spread_entry_runtime=True,
            position=SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY"),
            _is_simulated_signal_mode=lambda: False,
            _simulated_exit_window_open=False,
            _decimal_or_zero=lambda value: Decimal(str(value)),
            task=SimpleNamespace(exit_threshold=Decimal("0.1"), runtime_params={}),
            _current_exit_edge=lambda: Decimal("0.25"),
        )

        self.assertFalse(exit_signal_active(runtime))

    def test_exit_signal_active_uses_abs_edge(self) -> None:
        runtime = SimpleNamespace(
            _is_spread_entry_runtime=True,
            position=SimpleNamespace(direction="LEFT_SELL_RIGHT_BUY"),
            _is_simulated_signal_mode=lambda: False,
            _simulated_exit_window_open=False,
            _decimal_or_zero=lambda value: Decimal(str(value)),
            task=SimpleNamespace(exit_threshold=Decimal("0.1"), runtime_params={}),
            _current_exit_edge=lambda: Decimal("-0.05"),
        )

        self.assertTrue(exit_signal_active(runtime))


if __name__ == "__main__":
    unittest.main()
