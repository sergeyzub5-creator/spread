from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def entry_recovery_blocked_by_grace(runtime: WorkerRuntime, *, left_status: str, right_status: str) -> bool:
    cycle = runtime.active_entry_cycle
    if cycle is None:
        return False
    remaining_ms = runtime._rebalance_grace_remaining_ms()
    if remaining_ms <= 0:
        if runtime._last_entry_grace_log_cycle_id != cycle.cycle_id or runtime._last_entry_grace_log_state != "EXPIRED":
            runtime._last_entry_grace_log_cycle_id = cycle.cycle_id
            runtime._last_entry_grace_log_state = "EXPIRED"
        return False
    if runtime._last_entry_grace_log_cycle_id != cycle.cycle_id or runtime._last_entry_grace_log_state != "BLOCKED":
        runtime.logger.info(
            "ENTRY_RECOVERY_BLOCKED_BY_GRACE | cycle_id=%s | remaining_ms=%s | left_status=%s | right_status=%s",
            cycle.cycle_id,
            remaining_ms,
            left_status,
            right_status,
        )
        runtime._last_entry_grace_log_cycle_id = cycle.cycle_id
        runtime._last_entry_grace_log_state = "BLOCKED"
    return True


def entry_recovery_blocked_by_live_order(runtime: WorkerRuntime, *, left_status: str, right_status: str) -> bool:
    cycle = runtime.active_entry_cycle
    if cycle is None:
        return False
    if not runtime._has_live_leg_orders() or runtime._entry_has_stale_live_orders():
        return False
    now_ms = int(time.time() * 1000)
    status_signature = (left_status, right_status)
    should_log = (
        runtime._last_entry_live_order_log_cycle_id != cycle.cycle_id
        or runtime._last_entry_live_order_log_status_signature != status_signature
        or (now_ms - runtime._last_entry_live_order_log_at_ms) >= 1000
    )
    if should_log:
        runtime.logger.info(
            "ENTRY_RECOVERY_BLOCKED_BY_LIVE_ORDER | cycle_id=%s | left_status=%s | right_status=%s",
            cycle.cycle_id,
            left_status,
            right_status,
        )
        runtime._last_entry_live_order_log_cycle_id = cycle.cycle_id
        runtime._last_entry_live_order_log_status_signature = status_signature
        runtime._last_entry_live_order_log_at_ms = now_ms
    return True


def entry_attempt_result_signature(runtime: WorkerRuntime, *, result: str, left_status: str, right_status: str) -> tuple[Any, ...]:
    return (
        runtime.active_entry_cycle.cycle_id if runtime.active_entry_cycle is not None else None,
        runtime.last_entry_cycle.cycle_id if runtime.last_entry_cycle is not None else None,
        result,
        left_status,
        right_status,
        runtime._format_order_size(runtime.left_leg_state.target_qty),
        runtime._format_order_size(runtime.left_leg_state.filled_qty),
        runtime._format_order_size(runtime.right_leg_state.target_qty),
        runtime._format_order_size(runtime.right_leg_state.filled_qty),
        runtime._format_order_size(runtime._position_qty_mismatch()),
    )


def should_log_entry_attempt_result(runtime: WorkerRuntime, *, result: str, left_status: str, right_status: str) -> bool:
    signature = entry_attempt_result_signature(
        runtime,
        result=result,
        left_status=left_status,
        right_status=right_status,
    )
    if signature == runtime._last_entry_attempt_result_signature:
        return False
    runtime._last_entry_attempt_result_signature = signature
    return True


def exit_recovery_blocked_by_grace(runtime: WorkerRuntime, *, left_status: str, right_status: str) -> bool:
    cycle = runtime.active_exit_cycle
    if cycle is None:
        return False
    deadline_ts = int(cycle.exit_grace_deadline_ts or 0)
    if deadline_ts <= 0:
        return False
    now_ms = int(time.time() * 1000)
    if now_ms >= deadline_ts:
        if runtime._last_exit_grace_log_cycle_id != cycle.cycle_id or runtime._last_exit_grace_log_state != "EXPIRED":
            runtime.logger.info("EXIT_GRACE_WINDOW_EXPIRED | cycle_id=%s", cycle.cycle_id)
            runtime._last_exit_grace_log_cycle_id = cycle.cycle_id
            runtime._last_exit_grace_log_state = "EXPIRED"
        return False
    if runtime._last_exit_grace_log_cycle_id != cycle.cycle_id or runtime._last_exit_grace_log_state != "BLOCKED":
        runtime.logger.info(
            "EXIT_RECOVERY_BLOCKED_BY_GRACE | cycle_id=%s | remaining_ms=%s | left_status=%s | right_status=%s",
            cycle.cycle_id,
            max(0, deadline_ts - now_ms),
            left_status,
            right_status,
        )
        runtime._last_exit_grace_log_cycle_id = cycle.cycle_id
        runtime._last_exit_grace_log_state = "BLOCKED"
    return True
