from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyState
from app.core.workers.runtime_exit_orchestrator import exit_trigger_converged_or_flipped

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def exit_signal_active(runtime: WorkerRuntime) -> bool:
    if not runtime._is_spread_entry_runtime or runtime.position is None:
        return False
    if runtime._is_simulated_signal_mode():
        return runtime._simulated_exit_window_open
    return exit_trigger_converged_or_flipped(runtime)


def maybe_restore_in_position_state(runtime: WorkerRuntime) -> bool:
    if not runtime._is_spread_entry_runtime:
        return False
    if runtime._runtime_reconcile_active():
        return False
    if runtime.position is None and (runtime.left_leg_state.filled_qty <= Decimal("0") or runtime.right_leg_state.filled_qty <= Decimal("0")):
        return False
    if runtime.active_entry_cycle is not None or runtime.prefetch_entry_cycle is not None or runtime.active_exit_cycle is not None:
        return False
    if runtime._cycle_recovery_active() or runtime._has_live_leg_orders() or runtime._hedge_protection_active():
        return False
    if not runtime._position_is_hedged():
        return False
    runtime._rebind_restored_position_to_current_task(reason="HEDGED_POSITION_RESTORED")
    runtime._sync_position_from_legs()
    runtime._settle_dual_execution_state(reason="HEDGED_POSITION_RESTORED")
    if runtime.strategy_state is not StrategyState.IN_POSITION:
        runtime.logger.info(
            "strategy state normalized | from=%s | to=%s | reason=%s",
            runtime.strategy_state.value,
            StrategyState.IN_POSITION.value,
            "HEDGED_POSITION_RESTORED",
        )
        runtime._set_strategy_state(StrategyState.IN_POSITION)
    return True


def reset_position_state(runtime: WorkerRuntime) -> None:
    runtime.position = None
    runtime._entry_cycle_success_streak = 0
    runtime._exit_cycle_success_streak = 0
    runtime.state.metrics["entry_cycle_growth_streak"] = 0
    runtime.state.metrics["exit_cycle_growth_streak"] = 0
    runtime.state.metrics["position_direction"] = None
    runtime.state.metrics["position_state"] = None
    runtime.state.metrics["position_entry_edge"] = None
    runtime.state.metrics["position_active_edge"] = None


def mark_leg_flat_confirmed(runtime: WorkerRuntime, leg_name: str) -> None:
    leg_state = runtime._leg_state(leg_name)
    leg_state.filled_qty = Decimal("0")
    leg_state.remaining_qty = Decimal("0")
    leg_state.actual_position_qty = Decimal("0")
    leg_state.remaining_close_qty = Decimal("0")
    leg_state.is_flat = True
    leg_state.flat_confirmed_by_exchange = True
    leg_state.last_position_resync_ts = int(time.time() * 1000)
    leg_state.avg_price = None
    leg_state.order_status = "FILLED"
    runtime.state.metrics[f"{leg_name}_filled_qty"] = "0"
    runtime.state.metrics[f"{leg_name}_order_status"] = "FILLED"
    runtime._sync_active_entry_cycle_from_legs()
    runtime._sync_active_exit_cycle_from_legs()
    runtime._publish_state()


def entry_pipeline_busy_reason(runtime: WorkerRuntime) -> str | None:
    runtime._entry_pipeline_maybe_thaw()
    if runtime._runtime_reconcile_active():
        return "RUNTIME_RECONCILING"
    if runtime._entry_pipeline_frozen:
        return "ENTRY_PIPELINE_FROZEN"
    if runtime.active_exit_cycle is not None:
        return "ENTRY_CYCLE_ACTIVE"
    if runtime.active_entry_cycle is not None:
        if not runtime._entry_pipeline_overlap_enabled():
            return "ENTRY_CYCLE_ACTIVE"
        if runtime.prefetch_entry_cycle is not None:
            return "ENTRY_PIPELINE_PREFETCH_ACTIVE"
        if str(runtime.active_entry_cycle.state.value).upper() == "PLANNED":
            return None
        if not runtime._entry_cycle_ack_ready(runtime.active_entry_cycle):
            return "ENTRY_PIPELINE_WAIT_ACK"
        return None
    if runtime.prefetch_entry_cycle is not None:
        return "ENTRY_PIPELINE_PREFETCH_STAGED"
    if runtime._position_has_qty_mismatch():
        guard_enabled = runtime._global_hedge_guard_enabled() if hasattr(runtime, "_global_hedge_guard_enabled") else True
        if guard_enabled:
            return "HEDGE_MISMATCH_ACTIVE"
    if runtime._entry_recovery_active():
        return "ENTRY_RECOVERY_ACTIVE"
    if runtime._exit_recovery_active():
        return "EXIT_RECOVERY_ACTIVE"
    if runtime._hedge_protection_active():
        return "HEDGE_PROTECTION_ACTIVE"
    if runtime._has_live_leg_orders():
        return "ENTRY_WAIT_ORDER_SETTLE"
    return None
