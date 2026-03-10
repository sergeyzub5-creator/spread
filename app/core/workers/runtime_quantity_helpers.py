from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def leg_fill_matches_target(runtime: WorkerRuntime, leg_name: str) -> bool:
    target_qty = entry_leg_target_total_qty(runtime, leg_name)
    leg_state = runtime._leg_state(leg_name)
    return qty_matches_target(target_qty=target_qty, filled_qty=leg_state.filled_qty, tolerance_qty=Decimal("0"))


def entry_cycle_pair_matches_target(runtime: WorkerRuntime) -> bool:
    cycle = runtime.active_entry_cycle
    if cycle is None:
        return False
    left_ok = qty_matches_target(target_qty=cycle.left_target_qty, filled_qty=cycle.left_filled_qty, tolerance_qty=Decimal("0"))
    right_ok = qty_matches_target(target_qty=cycle.right_target_qty, filled_qty=cycle.right_filled_qty, tolerance_qty=Decimal("0"))
    pair_gap_ok = cycle.left_filled_qty == cycle.right_filled_qty
    return left_ok and right_ok and pair_gap_ok


def qty_matches_target(*, target_qty: Decimal, filled_qty: Decimal, tolerance_qty: Decimal) -> bool:
    if target_qty <= Decimal("0") or filled_qty <= Decimal("0"):
        return False
    return abs(filled_qty - target_qty) <= max(Decimal("0"), tolerance_qty)


def is_exit_cycle_committed_success(runtime: WorkerRuntime) -> bool:
    return runtime._exit_cycle_leg_matches_target("left") and runtime._exit_cycle_leg_matches_target("right")


def is_no_position_to_close_error(error_text: str | None) -> bool:
    normalized = str(error_text or "").strip().lower()
    return "no position to close" in normalized


def entry_leg_target_total_qty(runtime: WorkerRuntime, leg_name: str) -> Decimal:
    if runtime.active_entry_cycle is None:
        return runtime._leg_state(leg_name).target_qty
    start_qty = runtime.active_entry_cycle.left_start_qty if leg_name == "left" else runtime.active_entry_cycle.right_start_qty
    cycle_target_qty = runtime.active_entry_cycle.left_target_qty if leg_name == "left" else runtime.active_entry_cycle.right_target_qty
    return start_qty + cycle_target_qty


def entry_has_imbalance(runtime: WorkerRuntime) -> bool:
    return entry_leg_imbalance_notional_usdt(runtime) > max_leg_imbalance_notional_usdt(runtime)


def max_leg_imbalance_notional_usdt(runtime: WorkerRuntime) -> Decimal:
    return runtime._max_leg_imbalance_notional_usdt_value


def resolve_max_leg_imbalance_notional_usdt(runtime: WorkerRuntime) -> Decimal:
    configured = runtime._decimal_or_zero(runtime.task.runtime_params.get("max_leg_imbalance_notional_usdt"))
    if configured > Decimal("0"):
        return configured
    return runtime.DEFAULT_MAX_LEG_IMBALANCE_NOTIONAL_USDT


def entry_leg_imbalance_notional_usdt(runtime: WorkerRuntime) -> Decimal:
    return abs(runtime._filled_leg_notional_usdt("left") - runtime._filled_leg_notional_usdt("right"))
