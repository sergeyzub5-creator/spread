from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyCycle

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def entry_pipeline_overlap_enabled(runtime: WorkerRuntime) -> bool:
    return False


def entry_cycle_ack_ready(runtime: WorkerRuntime, cycle: StrategyCycle | None) -> bool:
    return cycle is not None and bool(cycle.left_acked) and bool(cycle.right_acked)


def entry_cycle_order_key(runtime: WorkerRuntime, *, cycle: StrategyCycle, leg_name: str) -> str | None:
    order_id = cycle.left_order_id if leg_name == "left" else cycle.right_order_id
    client_order_id = cycle.left_client_order_id if leg_name == "left" else cycle.right_client_order_id
    return runtime._order_fill_key(order_id=order_id, client_order_id=client_order_id)


def entry_cycle_leg_filled_qty(runtime: WorkerRuntime, *, cycle: StrategyCycle, leg_name: str) -> Decimal:
    order_key = entry_cycle_order_key(runtime, cycle=cycle, leg_name=leg_name)
    if order_key:
        resolver = getattr(runtime, "_tracker_cumulative_for_order_key", None)
        if callable(resolver):
            return max(Decimal("0"), resolver(leg_name=leg_name, order_key=order_key))
        return max(Decimal("0"), runtime._leg_order_fill_tracker.get(leg_name, {}).get(order_key, Decimal("0")))
    current_leg_filled = runtime.left_leg_state.filled_qty if leg_name == "left" else runtime.right_leg_state.filled_qty
    start_qty = cycle.left_start_qty if leg_name == "left" else cycle.right_start_qty
    return max(Decimal("0"), current_leg_filled - start_qty)


def resolve_entry_cycle_for_submit(runtime: WorkerRuntime, cycle_id: int | None) -> StrategyCycle | None:
    if cycle_id is None:
        return runtime.active_entry_cycle
    if runtime.active_entry_cycle is not None and runtime.active_entry_cycle.cycle_id == cycle_id:
        return runtime.active_entry_cycle
    if runtime.prefetch_entry_cycle is not None and runtime.prefetch_entry_cycle.cycle_id == cycle_id:
        return runtime.prefetch_entry_cycle
    return None


def exit_cycle_order_key(runtime: WorkerRuntime, *, cycle: StrategyCycle, leg_name: str) -> str | None:
    order_id = cycle.left_order_id if leg_name == "left" else cycle.right_order_id
    client_order_id = cycle.left_client_order_id if leg_name == "left" else cycle.right_client_order_id
    return runtime._order_fill_key(order_id=order_id, client_order_id=client_order_id)


def resolve_exit_cycle_for_submit(runtime: WorkerRuntime, cycle_id: int | None) -> StrategyCycle | None:
    if cycle_id is None:
        return runtime.active_exit_cycle
    if runtime.active_exit_cycle is not None and runtime.active_exit_cycle.cycle_id == cycle_id:
        return runtime.active_exit_cycle
    if getattr(runtime, "prefetch_exit_cycle", None) is not None and runtime.prefetch_exit_cycle.cycle_id == cycle_id:
        return runtime.prefetch_exit_cycle
    return None


def entry_pipeline_freeze(runtime: WorkerRuntime, *, reason: str) -> None:
    if runtime._entry_pipeline_mode_requested != "overlap_1":
        return
    now_ms = int(time.time() * 1000)
    runtime.logger.warning("entry pipeline frozen | reason=%s", reason)
    runtime._entry_pipeline_frozen = True
    runtime._entry_pipeline_freeze_reason = reason
    runtime._entry_pipeline_freeze_ts = now_ms
    runtime.state.metrics["entry_pipeline_mode"] = runtime._entry_pipeline_mode
    runtime.state.metrics["entry_pipeline_mode_fallback_reason"] = None
    runtime.state.metrics["entry_pipeline_frozen"] = True
    runtime.state.metrics["entry_pipeline_freeze_reason"] = reason
    runtime.state.metrics["entry_pipeline_freeze_ts"] = now_ms


def entry_pipeline_maybe_thaw(runtime: WorkerRuntime) -> bool:
    if not runtime._entry_pipeline_frozen:
        return False
    if runtime._entry_pipeline_mode_requested != "overlap_1":
        return False
    if runtime._runtime_reconcile_active():
        return False
    if runtime.active_entry_cycle is not None or runtime.prefetch_entry_cycle is not None or runtime.active_exit_cycle is not None:
        return False
    if runtime._cycle_recovery_active():
        return False
    if runtime._has_live_leg_orders():
        return False
    if runtime._entry_recovery_active() or runtime._exit_recovery_active() or runtime._hedge_protection_active():
        return False
    if runtime._position_has_qty_mismatch():
        return False
    if (
        runtime.left_leg_state.actual_position_qty > Decimal("0")
        and runtime.right_leg_state.actual_position_qty > Decimal("0")
        and not runtime._position_is_hedged()
    ):
        return False
    previous_reason = str(runtime._entry_pipeline_freeze_reason or "").strip() or "UNKNOWN"
    runtime._entry_pipeline_frozen = False
    runtime._entry_pipeline_freeze_reason = None
    runtime._entry_pipeline_freeze_ts = None
    runtime.state.metrics["entry_pipeline_frozen"] = False
    runtime.state.metrics["entry_pipeline_freeze_reason"] = None
    runtime.state.metrics["entry_pipeline_freeze_ts"] = None
    runtime.logger.info(
        "entry pipeline thawed | reason=%s",
        previous_reason,
    )
    return True


def drop_entry_cycle_order_keys(runtime: WorkerRuntime, *, cycle_id: int) -> None:
    for leg_name in ("left", "right"):
        bucket = runtime._entry_cycle_order_keys.get(leg_name, {})
        stale_keys = [key for key, value in bucket.items() if int(value) == int(cycle_id)]
        for key in stale_keys:
            bucket.pop(key, None)
        if stale_keys and hasattr(runtime, "_remember_order_key_tombstones"):
            runtime._remember_order_key_tombstones(leg_name=leg_name, order_keys=stale_keys)


def drop_exit_cycle_order_keys(runtime: WorkerRuntime, *, cycle_id: int) -> None:
    for leg_name in ("left", "right"):
        bucket = runtime._exit_cycle_order_keys.get(leg_name, {})
        stale_keys = [key for key, value in bucket.items() if int(value) == int(cycle_id)]
        for key in stale_keys:
            bucket.pop(key, None)
        if stale_keys and hasattr(runtime, "_remember_order_key_tombstones"):
            runtime._remember_order_key_tombstones(leg_name=leg_name, order_keys=stale_keys)


def entry_pipeline_inflight_cycle_ids(runtime: WorkerRuntime) -> list[int]:
    cycle_ids: set[int] = set()
    if runtime.active_entry_cycle is not None:
        cycle_ids.add(int(runtime.active_entry_cycle.cycle_id))
    if runtime.prefetch_entry_cycle is not None:
        cycle_ids.add(int(runtime.prefetch_entry_cycle.cycle_id))
    for leg_name in ("left", "right"):
        for cycle_id in runtime._entry_cycle_order_keys.get(leg_name, {}).values():
            try:
                cycle_ids.add(int(cycle_id))
            except Exception:
                continue
    return sorted(cycle_ids)


def enforce_entry_pipeline_inflight_invariant(runtime: WorkerRuntime) -> None:
    if runtime._entry_pipeline_mode_requested != "overlap_1":
        return
    inflight_ids = entry_pipeline_inflight_cycle_ids(runtime)
    runtime.state.metrics["entry_inflight_cycles"] = len(inflight_ids)
    runtime.state.metrics["entry_inflight_cycle_ids"] = ",".join(str(cycle_id) for cycle_id in inflight_ids) or None
    if len(inflight_ids) <= 2:
        return
    reason = "ENTRY_PIPELINE_INFLIGHT_OVERFLOW"
    runtime.logger.warning(
        "entry pipeline invariant violated | reason=%s | inflight_cycles=%s | cycle_ids=%s | active_cycle_id=%s | prefetch_cycle_id=%s",
        reason,
        len(inflight_ids),
        inflight_ids,
        runtime.active_entry_cycle.cycle_id if runtime.active_entry_cycle is not None else None,
        runtime.prefetch_entry_cycle.cycle_id if runtime.prefetch_entry_cycle is not None else None,
    )
    runtime._entry_pipeline_freeze(reason=reason)
