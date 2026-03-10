from __future__ import annotations

import threading
import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.models.workers import StrategyCycleState
from app.core.workers.runtime_exit_attempt_flow import update_exit_attempt_state


class _FakeCycle:
    def __init__(self, cycle_id: int, state: StrategyCycleState = StrategyCycleState.ACTIVE) -> None:
        self.cycle_id = cycle_id
        self.state = state
        self.started_at = 1
        self.left_filled_qty = Decimal("0")
        self.right_filled_qty = Decimal("0")


class _FakeRuntime:
    def __init__(self) -> None:
        self._state_lock = threading.RLock()
        self._is_spread_entry_runtime = True
        self.active_exit_cycle = _FakeCycle(15)
        self.active_exit_cycle.started_at = 1
        self.left_leg_state = SimpleNamespace(actual_position_qty=Decimal("0.7"))
        self.right_leg_state = SimpleNamespace(actual_position_qty=Decimal("0.7"))
        self.state = SimpleNamespace(metrics={"left_order_status": "FILLED", "right_order_status": "FILLED", "last_result": None})
        self.logger = SimpleNamespace(
            warning=lambda *_args, **_kwargs: None,
            info=lambda *_args, **_kwargs: None,
        )
        self.finalized: list[int] = []
        self.reconcile_calls = 0
        self.commit_calls = 0
        self.deferred_exit_chain_calls = 0
        self.force_flat_calls = 0
        self.EXIT_CYCLE_SETTLE_TIMEOUT_MS = 5000
        self.EXIT_LIVE_ORDER_STALE_MS = 2000
        self._has_live_orders = False
        self._has_stale_live_orders = False
        self.exit_recovery_plan = None
        self._cycle_committed_success = False

    def _is_exit_full_success(self, *_args, **_kwargs) -> bool:
        return False

    def _is_exit_cycle_committed_success(self) -> bool:
        return self._cycle_committed_success

    def _is_exit_full_fail(self, *_args, **_kwargs) -> bool:
        return False

    def _is_exit_partial(self, *_args, **_kwargs) -> bool:
        return True

    def _sync_active_exit_cycle_from_legs(self) -> None:
        return

    def _set_strategy_state(self, *_args, **_kwargs) -> None:
        return

    def _clear_recovery_status(self, *, context: str) -> None:
        self.last_cleared_context = context

    def _commit_exit_cycle(self) -> None:
        self.commit_calls += 1
        self.active_exit_cycle = None

    def _settle_dual_execution_state(self, *, reason: str) -> None:
        self.last_settle_reason = reason

    def _exit_tail_resync_in_progress(self) -> bool:
        return False

    def _exit_recovery_blocked_by_grace(self, *_args, **_kwargs) -> bool:
        return False

    def _rebalance_grace_remaining_ms(self) -> int:
        return 0

    def _has_live_leg_orders(self) -> bool:
        return self._has_live_orders

    def _exit_has_stale_live_orders(self) -> bool:
        return self._has_stale_live_orders

    def _is_entry_attempt_active(self, *_args, **_kwargs) -> bool:
        return False

    def _reconcile_exit_remainder_from_exchange(self, *, reason: str) -> None:
        self.reconcile_calls += 1
        self.state.metrics["left_order_status"] = "FILLED"
        self.state.metrics["right_order_status"] = "FILLED"
        # Simulate race: another path already advanced active cycle.
        self.active_exit_cycle = _FakeCycle(16)
        self.last_reconcile_reason = reason

    def _finalize_exit_cycle(self, *, state, error: str | None = None) -> None:
        del state, error
        if self.active_exit_cycle is not None:
            self.finalized.append(int(self.active_exit_cycle.cycle_id))

    def _reset_position_state(self) -> None:
        return

    def _sync_position_from_legs(self) -> None:
        return

    def _position_has_qty_mismatch(self) -> bool:
        return False

    def _request_hedge_protection_check(self, *, reason: str) -> None:
        self.last_hedge_reason = reason

    def _publish_state(self) -> None:
        return

    def _update_strategy_state_from_exit_attempt(self, *_args, **_kwargs) -> None:
        return

    def _is_simulated_signal_mode(self) -> bool:
        return False

    def _request_deferred_exit_chain(self) -> None:
        self.deferred_exit_chain_calls += 1

    def _force_flatten_all_positions_from_hedge(self) -> None:
        self.force_flat_calls += 1


class RuntimeExitAttemptFlowTests(unittest.TestCase):
    def test_stale_reconcile_does_not_abort_next_cycle(self) -> None:
        runtime = _FakeRuntime()

        update_exit_attempt_state(runtime, left_status="SENT", right_status="FILLED")

        self.assertEqual(runtime.reconcile_calls, 1)
        self.assertEqual(runtime.finalized, [])
        self.assertEqual(runtime.active_exit_cycle.cycle_id, 16)

    def test_stale_live_exit_order_triggers_immediate_reconcile(self) -> None:
        runtime = _FakeRuntime()
        runtime._has_live_orders = True
        runtime._has_stale_live_orders = True
        runtime.active_exit_cycle.started_at = int(__import__("time").time() * 1000)

        update_exit_attempt_state(runtime, left_status="SENT", right_status="FILLED")

        self.assertEqual(runtime.reconcile_calls, 1)

    def test_filled_both_legs_commits_without_reconcile(self) -> None:
        runtime = _FakeRuntime()
        runtime._cycle_committed_success = True

        update_exit_attempt_state(runtime, left_status="FILLED", right_status="FILLED")

        self.assertEqual(runtime.commit_calls, 1)
        self.assertEqual(runtime.reconcile_calls, 0)
        self.assertEqual(runtime.deferred_exit_chain_calls, 1)

    def test_filled_status_without_cycle_match_reconciles_instead_of_commit(self) -> None:
        runtime = _FakeRuntime()
        runtime._cycle_committed_success = False

        update_exit_attempt_state(runtime, left_status="FILLED", right_status="FILLED")

        self.assertEqual(runtime.commit_calls, 0)
        self.assertEqual(runtime.reconcile_calls, 1)

    def test_partial_commit_when_cycle_targets_match(self) -> None:
        runtime = _FakeRuntime()
        runtime._cycle_committed_success = True

        def _reconcile_keep_cycle(*, reason: str) -> None:
            runtime.reconcile_calls += 1
            runtime.last_reconcile_reason = reason
            runtime.state.metrics["left_order_status"] = "FILLED"
            runtime.state.metrics["right_order_status"] = "FILLED"

        runtime._reconcile_exit_remainder_from_exchange = _reconcile_keep_cycle

        update_exit_attempt_state(runtime, left_status="SENT", right_status="FILLED")

        self.assertEqual(runtime.reconcile_calls, 0)
        self.assertEqual(runtime.commit_calls, 1)
        self.assertEqual(runtime.deferred_exit_chain_calls, 1)

    def test_partial_no_recovery_one_sided_tail_forces_flatten(self) -> None:
        runtime = _FakeRuntime()
        runtime.left_leg_state.actual_position_qty = Decimal("0.2")
        runtime.right_leg_state.actual_position_qty = Decimal("0")

        def _reconcile_keep_cycle(*, reason: str) -> None:
            runtime.reconcile_calls += 1
            runtime.last_reconcile_reason = reason
            runtime.state.metrics["left_order_status"] = "FILLED"
            runtime.state.metrics["right_order_status"] = "FILLED"

        runtime._reconcile_exit_remainder_from_exchange = _reconcile_keep_cycle

        update_exit_attempt_state(runtime, left_status="SENT", right_status="FILLED")

        self.assertEqual(runtime.reconcile_calls, 1)
        self.assertEqual(runtime.force_flat_calls, 1)


if __name__ == "__main__":
    unittest.main()
