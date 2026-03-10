from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def is_exit_full_success(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    return runtime.left_leg_state.is_flat and runtime.right_leg_state.is_flat


def is_exit_full_fail(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    terminal_fail_statuses = {"FAILED", "REJECTED", "CANCELED", "CANCELLED"}
    return (
        left_status in terminal_fail_statuses
        and right_status in terminal_fail_statuses
        and not exit_has_any_close_fill(runtime)
    )


def is_exit_partial(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    if is_exit_full_success(runtime, left_status, right_status):
        return False
    if exit_has_any_close_fill(runtime):
        return True
    if any(status in {"PARTIALLY_FILLED", "PARTIALLYFILLED"} for status in (left_status, right_status)):
        return True
    terminal_fail_statuses = {"FAILED", "REJECTED", "CANCELED", "CANCELLED"}
    return (
        (left_status in terminal_fail_statuses and right_status not in terminal_fail_statuses)
        or (right_status in terminal_fail_statuses and left_status not in terminal_fail_statuses)
    )


def exit_has_any_close_fill(runtime: WorkerRuntime) -> bool:
    if runtime.active_exit_cycle is None:
        return False
    return runtime.active_exit_cycle.left_filled_qty > Decimal("0") or runtime.active_exit_cycle.right_filled_qty > Decimal("0")


def exit_cycle_leg_matches_target(runtime: WorkerRuntime, leg_name: str) -> bool:
    cycle = runtime.active_exit_cycle
    if cycle is None:
        return False
    target_qty = cycle.left_target_qty if leg_name == "left" else cycle.right_target_qty
    filled_qty = cycle.left_filled_qty if leg_name == "left" else cycle.right_filled_qty
    return runtime._qty_matches_target(target_qty=target_qty, filled_qty=filled_qty, tolerance_qty=Decimal("0"))


def is_entry_attempt_active(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    active_statuses = {"SENDING", "SENT", "ACK", "ACCEPTED", "NEW"}
    return any(status in active_statuses for status in (left_status, right_status))


def is_entry_full_success(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    if runtime.active_entry_cycle is not None:
        return runtime._entry_cycle_pair_matches_target()
    return runtime._leg_fill_matches_target("left") and runtime._leg_fill_matches_target("right")


def is_entry_full_fail(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    terminal_fail_statuses = {"FAILED", "REJECTED", "CANCELED", "CANCELLED"}
    return (
        left_status in terminal_fail_statuses
        and right_status in terminal_fail_statuses
        and not has_any_entry_fill(runtime)
    )


def is_entry_partial(runtime: WorkerRuntime, left_status: str, right_status: str) -> bool:
    if left_status == "FILLED" and right_status == "FILLED":
        return not (runtime._leg_fill_matches_target("left") and runtime._leg_fill_matches_target("right"))
    if has_any_entry_fill(runtime):
        return True
    if any(status in {"PARTIALLY_FILLED", "PARTIALLYFILLED"} for status in (left_status, right_status)):
        return True
    terminal_fail_statuses = {"FAILED", "REJECTED", "CANCELED", "CANCELLED"}
    return (
        (left_status in terminal_fail_statuses and right_status not in terminal_fail_statuses)
        or (right_status in terminal_fail_statuses and left_status not in terminal_fail_statuses)
    )


def has_any_entry_fill(runtime: WorkerRuntime) -> bool:
    if runtime.active_entry_cycle is not None:
        return runtime.active_entry_cycle.left_filled_qty > Decimal("0") or runtime.active_entry_cycle.right_filled_qty > Decimal("0")
    return runtime.left_leg_state.filled_qty > Decimal("0") or runtime.right_leg_state.filled_qty > Decimal("0")
