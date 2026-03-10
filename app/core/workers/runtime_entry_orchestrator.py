from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.models.workers import StrategyCycleState, StrategyState
from app.core.workers.runtime_entry_pipeline_strict import submit_strict_entry

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def evaluate_spread_entry(runtime: WorkerRuntime) -> None:
    if not runtime._is_spread_entry_runtime:
        return
    if not runtime._policy_allow_entry_evaluation():
        return
    allowed_states = {StrategyState.IDLE, StrategyState.COOLDOWN, StrategyState.IN_POSITION}
    if runtime.strategy_state not in allowed_states:
        return
    # Never block quote/event loop waiting on entry lock.
    # If another submit path owns the lock, skip this tick and re-evaluate on next event.
    if not runtime._entry_lock.acquire(blocking=False):
        return
    try:
        with runtime._state_lock:
            allowed_states_locked = {StrategyState.IDLE, StrategyState.COOLDOWN, StrategyState.IN_POSITION}
            if runtime.strategy_state not in allowed_states_locked:
                return
            if runtime.active_entry_cycle is None and runtime.prefetch_entry_cycle is not None:
                runtime._promote_prefetch_entry_cycle()
            decision = runtime._build_entry_decision()
            if decision is None or not decision.is_executable:
                return
            runtime.state.metrics["entry_block_reason"] = None
            runtime.state.metrics["last_result"] = None
        try:
            submit_strict_entry(runtime, decision)
        except Exception as exc:
            handle_entry_submit_failure(runtime, exc)
    finally:
        runtime._entry_lock.release()


def handle_entry_submit_failure(runtime: WorkerRuntime, exc: Exception) -> None:
    error_text = str(exc or "")
    if "Dual execution is already in progress" in error_text:
        # Benign overlap race: keep pipeline/cycles intact and retry on next tick.
        runtime.logger.info("entry submit deferred | reason=%s", error_text)
        return
    with runtime._state_lock:
        if runtime._is_margin_limit_error(error_text):
            runtime._mark_entry_growth_limit_pending(reason="MARGIN_LIMIT_REACHED")
        if runtime._is_margin_limit_error(error_text) and runtime.active_entry_cycle is not None and runtime._has_live_leg_orders():
            runtime.state.last_error = error_text
            runtime.state.metrics["last_result"] = "PARTIAL"
            runtime._finalize_entry_cycle(state=StrategyCycleState.ABORT, error="ENTRY_PARTIAL_NO_RECOVERY")
            runtime._set_strategy_state(StrategyState.FAILED)
            runtime._publish_state()
            return
        if runtime._is_margin_limit_error(error_text) and runtime._preserve_hedged_position_after_entry_limit(
            reason="MARGIN_LIMIT_REACHED",
            last_result="ENTRY_SIZE_LIMITED",
        ):
            return
        runtime.state.metrics["last_result"] = "FAILED"
        runtime._finalize_entry_cycle(state=StrategyCycleState.ABORT, error=error_text)
        runtime._set_strategy_state(StrategyState.FAILED)
        runtime.state.last_error = error_text
        runtime.emit_event("entry_failed", {"reason": error_text, "phase": "submit"})
        runtime._publish_state()
