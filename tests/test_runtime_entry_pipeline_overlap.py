from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.models.workers import StrategyCycleState
from app.core.workers.runtime_entry_pipeline_helpers import enforce_entry_pipeline_inflight_invariant
from app.core.workers.runtime_entry_pipeline_overlap import submit_overlap_entry
from app.core.workers.runtime_state_guards import entry_pipeline_busy_reason


class _FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        return

    def warning(self, *_args, **_kwargs) -> None:
        return


class _FakeCycle:
    def __init__(self, cycle_id: int, state: StrategyCycleState) -> None:
        self.cycle_id = cycle_id
        self.state = state


class _FakeOverlapRuntime:
    def __init__(self, *, active_state: StrategyCycleState = StrategyCycleState.PLANNED) -> None:
        self.active_entry_cycle: _FakeCycle | None = _FakeCycle(7, active_state)
        self.prefetch_entry_cycle: _FakeCycle | None = None
        self.created_cycles: list[tuple[bool, int]] = []
        self.submits: list[dict] = []
        self.task = SimpleNamespace(runtime_params={})
        self.logger = _FakeLogger()

    def _start_entry_cycle(self, decision, *, prefetch: bool = False):
        del decision
        cycle = _FakeCycle(99, StrategyCycleState.PLANNED)
        self.created_cycles.append((prefetch, cycle.cycle_id))
        if prefetch:
            self.prefetch_entry_cycle = cycle
        else:
            self.active_entry_cycle = cycle
        return cycle

    def _set_strategy_state(self, _state) -> None:
        return

    def _set_entry_cycle_state(self, _state) -> None:
        return

    def _format_order_size(self, value: Decimal) -> str:
        return format(value, "f")

    def _sync_active_entry_cycle_metrics(self) -> None:
        return

    def submit_dual_test_orders(self, **kwargs) -> None:
        self.submits.append(kwargs)


class _FakeBusyRuntime:
    def __init__(
        self,
        *,
        state: StrategyCycleState | None,
        overlap_enabled: bool,
        ack_ready: bool,
        has_mismatch: bool = False,
        global_guard_enabled: bool = True,
    ) -> None:
        self.active_entry_cycle = _FakeCycle(1, state) if state is not None else None
        self.prefetch_entry_cycle = None
        self.active_exit_cycle = None
        self._entry_pipeline_frozen = False
        self._is_spread_entry_runtime = True
        self.position = None
        self._overlap_enabled = overlap_enabled
        self._ack_ready = ack_ready
        self._has_mismatch = has_mismatch
        self._global_guard_enabled = global_guard_enabled
        self._entry_recovery = False
        self._exit_recovery = False
        self._hedge_active = False
        self._live_orders = False
        self._exit_signal = False

    def _entry_pipeline_maybe_thaw(self) -> bool:
        return False

    def _runtime_reconcile_active(self) -> bool:
        return False

    def _position_has_qty_mismatch(self) -> bool:
        return self._has_mismatch

    def _exit_signal_active(self) -> bool:
        return self._exit_signal

    def _entry_pipeline_overlap_enabled(self) -> bool:
        return self._overlap_enabled

    def _entry_cycle_ack_ready(self, _cycle) -> bool:
        return self._ack_ready

    def _entry_recovery_active(self) -> bool:
        return self._entry_recovery

    def _exit_recovery_active(self) -> bool:
        return self._exit_recovery

    def _hedge_protection_active(self) -> bool:
        return self._hedge_active

    def _has_live_leg_orders(self) -> bool:
        return self._live_orders

    def _global_hedge_guard_enabled(self) -> bool:
        return self._global_guard_enabled


class _FakeInvariantRuntime:
    def __init__(self, *, mode_requested: str = "overlap_1") -> None:
        self._entry_pipeline_mode_requested = mode_requested
        self._entry_pipeline_mode = mode_requested
        self._entry_pipeline_frozen = False
        self._entry_pipeline_freeze_reason = None
        self._entry_pipeline_freeze_ts = None
        self._entry_pipeline_mode_fallback_reason = None
        self.active_entry_cycle = _FakeCycle(10, StrategyCycleState.ACTIVE)
        self.prefetch_entry_cycle = _FakeCycle(11, StrategyCycleState.PLANNED)
        self._entry_cycle_order_keys = {"left": {}, "right": {}}
        self.logger = _FakeLogger()
        self.state = SimpleNamespace(metrics={})

    def _entry_pipeline_freeze(self, *, reason: str) -> None:
        self._entry_pipeline_frozen = True
        self._entry_pipeline_freeze_reason = reason
        self.state.metrics["entry_pipeline_mode"] = self._entry_pipeline_mode
        self.state.metrics["entry_pipeline_frozen"] = True
        self.state.metrics["entry_pipeline_freeze_reason"] = reason


class RuntimeEntryPipelineOverlapTests(unittest.TestCase):
    def test_submit_overlap_reuses_promoted_planned_cycle(self) -> None:
        runtime = _FakeOverlapRuntime()
        decision = SimpleNamespace(
            planned_size={"left_qty": Decimal("0.2"), "right_qty": Decimal("0.2"), "cycle_notional_usdt": Decimal("20")},
            edge_name="edge_1",
            direction="LEFT_SELL_RIGHT_BUY",
            left_action="SELL",
            right_action="BUY",
        )

        submit_overlap_entry(runtime, decision)

        self.assertEqual(runtime.created_cycles, [])
        self.assertIsNotNone(runtime.active_entry_cycle)
        self.assertEqual(runtime.active_entry_cycle.cycle_id, 7)
        self.assertEqual(runtime.active_entry_cycle.state, StrategyCycleState.SUBMITTING)
        self.assertEqual(len(runtime.submits), 1)
        self.assertEqual(runtime.submits[0].get("entry_cycle_id"), 7)

    def test_submit_overlap_sends_prefetch_immediately(self) -> None:
        runtime = _FakeOverlapRuntime(
            active_state=StrategyCycleState.SUBMITTING,
        )
        decision = SimpleNamespace(
            planned_size={"left_qty": Decimal("0.2"), "right_qty": Decimal("0.2"), "cycle_notional_usdt": Decimal("20")},
            edge_name="edge_1",
            direction="LEFT_SELL_RIGHT_BUY",
            left_action="SELL",
            right_action="BUY",
        )

        submit_overlap_entry(runtime, decision)

        self.assertEqual(runtime.created_cycles, [(True, 99)])
        self.assertIsNotNone(runtime.prefetch_entry_cycle)
        self.assertEqual(runtime.prefetch_entry_cycle.state, StrategyCycleState.SUBMITTING)
        self.assertEqual(len(runtime.submits), 1)
        self.assertEqual(runtime.submits[0].get("entry_cycle_id"), 99)

    def test_busy_reason_allows_planned_active_cycle_in_overlap(self) -> None:
        runtime = _FakeBusyRuntime(
            state=StrategyCycleState.PLANNED,
            overlap_enabled=True,
            ack_ready=False,
        )

        reason = entry_pipeline_busy_reason(runtime)

        self.assertIsNone(reason)

    def test_busy_reason_waits_for_ack_when_submitting(self) -> None:
        runtime = _FakeBusyRuntime(
            state=StrategyCycleState.SUBMITTING,
            overlap_enabled=True,
            ack_ready=False,
        )

        reason = entry_pipeline_busy_reason(runtime)

        self.assertEqual(reason, "ENTRY_PIPELINE_WAIT_ACK")

    def test_strict_mode_busy_reason_remains_active_cycle(self) -> None:
        runtime = _FakeBusyRuntime(
            state=StrategyCycleState.SUBMITTING,
            overlap_enabled=False,
            ack_ready=True,
        )
        reason = entry_pipeline_busy_reason(runtime)
        self.assertEqual(reason, "ENTRY_CYCLE_ACTIVE")

    def test_busy_reason_allows_overlap_chain_when_ack_ready_despite_transient_mismatch(self) -> None:
        runtime = _FakeBusyRuntime(
            state=StrategyCycleState.SUBMITTING,
            overlap_enabled=True,
            ack_ready=True,
            has_mismatch=True,
        )

        reason = entry_pipeline_busy_reason(runtime)

        self.assertIsNone(reason)

    def test_busy_reason_blocks_when_pipeline_frozen(self) -> None:
        runtime = _FakeBusyRuntime(
            state=StrategyCycleState.SUBMITTING,
            overlap_enabled=True,
            ack_ready=True,
        )
        runtime._entry_pipeline_frozen = True

        reason = entry_pipeline_busy_reason(runtime)

        self.assertEqual(reason, "ENTRY_PIPELINE_FROZEN")

    def test_busy_reason_defers_global_mismatch_guard_during_cycle_cooldown(self) -> None:
        runtime = _FakeBusyRuntime(
            state=None,
            overlap_enabled=True,
            ack_ready=True,
            has_mismatch=True,
            global_guard_enabled=False,
        )

        reason = entry_pipeline_busy_reason(runtime)

        self.assertIsNone(reason)

    def test_busy_reason_blocks_on_real_global_mismatch(self) -> None:
        runtime = _FakeBusyRuntime(
            state=None,
            overlap_enabled=True,
            ack_ready=True,
            has_mismatch=True,
            global_guard_enabled=True,
        )
        reason = entry_pipeline_busy_reason(runtime)
        self.assertEqual(reason, "HEDGE_MISMATCH_ACTIVE")

    def test_busy_reason_does_not_block_by_exit_priority_flag(self) -> None:
        runtime = _FakeBusyRuntime(
            state=None,
            overlap_enabled=True,
            ack_ready=True,
        )
        runtime._exit_signal = True

        reason = entry_pipeline_busy_reason(runtime)

        self.assertIsNone(reason)

    def test_inflight_invariant_keeps_overlap_when_max_two(self) -> None:
        runtime = _FakeInvariantRuntime()
        runtime._entry_cycle_order_keys = {
            "left": {"order:a": 10},
            "right": {"order:b": 11},
        }

        enforce_entry_pipeline_inflight_invariant(runtime)

        self.assertFalse(runtime._entry_pipeline_frozen)
        self.assertEqual(runtime.state.metrics.get("entry_inflight_cycles"), 2)
        self.assertEqual(runtime.state.metrics.get("entry_inflight_cycle_ids"), "10,11")

    def test_inflight_invariant_freezes_on_overflow(self) -> None:
        runtime = _FakeInvariantRuntime()
        runtime._entry_cycle_order_keys = {
            "left": {"order:a": 10, "order:b": 12},
            "right": {"order:c": 11, "order:d": 13},
        }

        enforce_entry_pipeline_inflight_invariant(runtime)

        self.assertTrue(runtime._entry_pipeline_frozen)
        self.assertEqual(runtime._entry_pipeline_mode, "overlap_1")
        self.assertEqual(runtime._entry_pipeline_freeze_reason, "ENTRY_PIPELINE_INFLIGHT_OVERFLOW")
        self.assertEqual(runtime.state.metrics.get("entry_inflight_cycles"), 4)


if __name__ == "__main__":
    unittest.main()
