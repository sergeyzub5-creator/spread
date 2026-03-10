from __future__ import annotations

import time
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyCycleState, StrategyState
from app.core.workers.runtime_transition_helpers import chain_allowed, current_leg_statuses, request_deferred_chain_if_allowed, should_wait_settle_timeout

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def _apply_entry_opened_result(
    runtime: WorkerRuntime,
    *,
    left_status: str,
    right_status: str,
    source: str | None = None,
) -> bool:
    runtime.state.metrics["last_result"] = "OPENED"
    runtime.entry_recovery_plan = None
    runtime._clear_recovery_status(context="ENTRY_CYCLE")
    runtime._sync_position_from_legs()
    runtime._commit_entry_cycle()
    runtime._set_strategy_state(StrategyState.IN_POSITION)
    if source is None:
        if runtime._should_log_entry_attempt_result(
            result="OPENED",
            left_status=left_status,
            right_status=right_status,
        ):
            runtime.logger.info(
                "final entry result | result=%s | left_status=%s | right_status=%s | left_target_qty=%s | left_filled_qty=%s | right_target_qty=%s | right_filled_qty=%s | qty_mismatch=%s",
                "OPENED",
                left_status,
                right_status,
                runtime._format_order_size(runtime.left_leg_state.target_qty),
                runtime._format_order_size(runtime.left_leg_state.filled_qty),
                runtime._format_order_size(runtime.right_leg_state.target_qty),
                runtime._format_order_size(runtime.right_leg_state.filled_qty),
                runtime._format_order_size(runtime._position_qty_mismatch()),
            )
    else:
        runtime.logger.info(
            "final entry result | result=%s | left_status=%s | right_status=%s | source=%s",
            "OPENED",
            left_status,
            right_status,
            source,
        )
    return True


def _apply_entry_partial_no_recovery(
    runtime: WorkerRuntime,
    *,
    left_status: str,
    right_status: str,
    freeze_reason: str,
    log_partial_result: bool,
) -> None:
    runtime.state.metrics["last_result"] = "PARTIAL"
    runtime._entry_cycle_success_streak = 0
    runtime.state.metrics["entry_cycle_growth_streak"] = 0
    if runtime._position_is_hedged():
        # Hedged partial should not permanently stall overlap pipeline:
        # we finalize the cycle but allow chain to continue after state settles.
        runtime._sync_position_from_legs()
        if runtime.active_entry_cycle is not None:
            runtime._finalize_entry_cycle(
                state=StrategyCycleState.ABORT,
                error="ENTRY_PARTIAL_NO_RECOVERY",
                freeze_pipeline=False,
            )
        runtime._set_strategy_state(StrategyState.IN_POSITION)
    else:
        runtime._entry_pipeline_freeze(reason=freeze_reason)
        runtime._reset_position_state()
        if runtime.active_entry_cycle is not None:
            runtime._finalize_entry_cycle(state=StrategyCycleState.ABORT, error="ENTRY_PARTIAL_NO_RECOVERY")
        runtime._set_strategy_state(StrategyState.FAILED)
    if not log_partial_result:
        return
    if runtime._should_log_entry_attempt_result(
        result="PARTIAL",
        left_status=left_status,
        right_status=right_status,
    ):
        runtime.logger.info(
            "final entry result | result=%s | left_status=%s | right_status=%s | left_target_qty=%s | left_filled_qty=%s | right_target_qty=%s | right_filled_qty=%s | qty_mismatch=%s",
            "PARTIAL",
            left_status,
            right_status,
            runtime._format_order_size(runtime.left_leg_state.target_qty),
            runtime._format_order_size(runtime.left_leg_state.filled_qty),
            runtime._format_order_size(runtime.right_leg_state.target_qty),
            runtime._format_order_size(runtime.right_leg_state.filled_qty),
            runtime._format_order_size(runtime._position_qty_mismatch()),
        )


def _can_commit_partial_entry_cycle(runtime: WorkerRuntime) -> bool:
    cycle = runtime.active_entry_cycle
    if cycle is None:
        return False
    left_filled = runtime._decimal_or_zero(cycle.left_filled_qty)
    right_filled = runtime._decimal_or_zero(cycle.right_filled_qty)
    if left_filled <= 0 or right_filled <= 0:
        return False
    # Local cycle guard: only symmetric per-cycle progress can continue the chain.
    return left_filled == right_filled


def update_entry_attempt_state(runtime: WorkerRuntime, left_status: str, right_status: str) -> None:
    should_chain_next_entry = False
    should_resync_entry_after_settle_timeout = False
    should_resync_entry_after_partial = False
    with runtime._state_lock:
        if not runtime._is_spread_entry_runtime:
            return
        runtime._sync_active_entry_cycle_from_legs()
        if runtime._is_entry_full_success(left_status, right_status):
            should_chain_next_entry = _apply_entry_opened_result(
                runtime,
                left_status=left_status,
                right_status=right_status,
            )
        elif runtime._is_entry_full_fail(left_status, right_status):
            runtime._entry_pipeline_freeze(reason="ENTRY_FULL_FAIL")
            if runtime._current_entry_attempt_hit_margin_limit() and runtime._preserve_hedged_position_after_entry_limit(
                reason="MARGIN_LIMIT_REACHED",
                last_result="ENTRY_SIZE_LIMITED",
            ):
                return
            runtime.state.metrics["last_result"] = "FAILED"
            runtime.entry_recovery_plan = None
            runtime._clear_recovery_status(context="ENTRY_CYCLE")
            runtime._reset_position_state()
            runtime._finalize_entry_cycle(state=StrategyCycleState.ABORT, error=runtime.state.last_error)
            runtime._set_strategy_state(StrategyState.FAILED)
            if runtime._should_log_entry_attempt_result(
                result="FAILED",
                left_status=left_status,
                right_status=right_status,
            ):
                runtime.logger.info("final entry result | result=%s | left_status=%s | right_status=%s", "FAILED", left_status, right_status)
            return
        elif runtime._is_entry_partial(left_status, right_status):
            if runtime.active_entry_cycle is not None:
                if runtime._entry_recovery_blocked_by_grace(left_status=left_status, right_status=right_status):
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
                if runtime._entry_recovery_blocked_by_live_order(left_status=left_status, right_status=right_status):
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
                cycle_started_ms = int(runtime.active_entry_cycle.started_at or 0)
                elapsed_ms = max(0, int(time.time() * 1000) - cycle_started_ms) if cycle_started_ms > 0 else 0
                if should_wait_settle_timeout(
                    left_status=left_status,
                    right_status=right_status,
                    elapsed_ms=elapsed_ms,
                    timeout_ms=runtime.ENTRY_CYCLE_SETTLE_TIMEOUT_MS,
                ):
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
                if left_status not in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"} or right_status not in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
                    current_cycle_id = int(runtime.active_entry_cycle.cycle_id)
                    already_handled_cycle_id = int(getattr(runtime, "_entry_settle_timeout_handled_cycle_id", 0) or 0)
                    if already_handled_cycle_id == current_cycle_id:
                        should_resync_entry_after_settle_timeout = False
                    else:
                        runtime._entry_settle_timeout_handled_cycle_id = current_cycle_id
                        runtime._entry_pipeline_freeze(reason="ENTRY_CYCLE_SETTLE_TIMEOUT")
                        for leg_name in ("left", "right"):
                            if runtime._entry_leg_has_stale_live_order(leg_name):
                                runtime._cancel_live_leg_order(leg_name=leg_name, reason="ENTRY_CYCLE_SETTLE_TIMEOUT")
                        runtime.logger.warning(
                            "entry cycle settle timeout | cycle_id=%s | elapsed_ms=%s | left_status=%s | right_status=%s",
                            runtime.active_entry_cycle.cycle_id,
                            elapsed_ms,
                            left_status,
                            right_status,
                        )
                        should_resync_entry_after_settle_timeout = True
                if not should_resync_entry_after_settle_timeout and elapsed_ms < runtime.ENTRY_CYCLE_SETTLE_TIMEOUT_MS and runtime._has_live_leg_orders():
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
                if should_resync_entry_after_settle_timeout:
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
                terminal_statuses = {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
                if (
                    left_status in terminal_statuses
                    and right_status in terminal_statuses
                    and not runtime._has_live_leg_orders()
                    and _can_commit_partial_entry_cycle(runtime)
                ):
                    should_chain_next_entry = _apply_entry_opened_result(
                        runtime,
                        left_status=left_status,
                        right_status=right_status,
                        source="ENTRY_PARTIAL_SYMMETRIC_COMMIT",
                    )
                    return
                if (
                    left_status in terminal_statuses
                    and right_status in terminal_statuses
                    and not runtime._has_live_leg_orders()
                ):
                    # Give exchange state one final reconcile pass before declaring
                    # irrecoverable partial and freezing pipeline.
                    should_resync_entry_after_partial = True
                    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
                    return
            _apply_entry_partial_no_recovery(
                runtime,
                left_status=left_status,
                right_status=right_status,
                freeze_reason="ENTRY_PARTIAL",
                log_partial_result=True,
            )
            return
        elif runtime._is_entry_attempt_active(left_status, right_status):
            runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
            return
        else:
            return
    if should_resync_entry_after_settle_timeout:
        resynced = runtime._reconcile_entry_remainder_from_exchange(reason="ENTRY_SETTLE_TIMEOUT_RECONCILE")
        with runtime._state_lock:
            refreshed_left_status, refreshed_right_status = current_leg_statuses(
                runtime,
                fallback_left=left_status,
                fallback_right=right_status,
            )
            if resynced and runtime._is_entry_full_success(refreshed_left_status, refreshed_right_status):
                _apply_entry_opened_result(
                    runtime,
                    left_status=refreshed_left_status,
                    right_status=refreshed_right_status,
                    source="ENTRY_SETTLE_TIMEOUT_RECONCILE",
                )
                if chain_allowed(runtime, side="entry"):
                    runtime._request_deferred_entry_chain()
                return
            _apply_entry_partial_no_recovery(
                runtime,
                left_status=refreshed_left_status,
                right_status=refreshed_right_status,
                freeze_reason="ENTRY_SETTLE_TIMEOUT_PARTIAL",
                log_partial_result=False,
            )
        return
    if should_resync_entry_after_partial:
        resynced = runtime._reconcile_entry_remainder_from_exchange(reason="ENTRY_PARTIAL_RECONCILE")
        with runtime._state_lock:
            refreshed_left_status, refreshed_right_status = current_leg_statuses(
                runtime,
                fallback_left=left_status,
                fallback_right=right_status,
            )
            if resynced and runtime._is_entry_full_success(refreshed_left_status, refreshed_right_status):
                _apply_entry_opened_result(
                    runtime,
                    left_status=refreshed_left_status,
                    right_status=refreshed_right_status,
                    source="ENTRY_PARTIAL_RECONCILE",
                )
                if chain_allowed(runtime, side="entry"):
                    runtime._request_deferred_entry_chain()
                return
            if (
                runtime.active_entry_cycle is not None
                and not runtime._has_live_leg_orders()
                and _can_commit_partial_entry_cycle(runtime)
            ):
                _apply_entry_opened_result(
                    runtime,
                    left_status=refreshed_left_status,
                    right_status=refreshed_right_status,
                    source="ENTRY_PARTIAL_RECONCILE_COMMIT",
                )
                if chain_allowed(runtime, side="entry"):
                    runtime._request_deferred_entry_chain()
                return
            _apply_entry_partial_no_recovery(
                runtime,
                left_status=refreshed_left_status,
                right_status=refreshed_right_status,
                freeze_reason="ENTRY_PARTIAL",
                log_partial_result=False,
            )
        return
    if should_chain_next_entry:
        request_deferred_chain_if_allowed(runtime, side="entry")
