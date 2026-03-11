from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def _entry_cycle_in_flight(runtime: WorkerRuntime) -> bool:
    """True if an entry cycle is active or staged — window closing must not block completing it."""
    return runtime.active_entry_cycle is not None or runtime.prefetch_entry_cycle is not None


def _exit_cycle_in_flight(runtime: WorkerRuntime) -> bool:
    """True if an exit cycle is active or staged — window closing must not block completing it."""
    return runtime.active_exit_cycle is not None or getattr(runtime, "prefetch_exit_cycle", None) is not None


def chain_allowed(runtime: WorkerRuntime, *, side: str) -> bool:
    """
    Simulated window is a trigger to *start* a cycle only.
    If a cycle already started (active/prefetch slot), chain/recovery must continue even after window closes.
    """
    normalized = str(side or "").strip().lower()
    if not runtime._is_simulated_signal_mode():
        return True
    if normalized == "entry":
        if _entry_cycle_in_flight(runtime):
            return True
        return bool(runtime._simulated_entry_window_open)
    if normalized == "exit":
        if _exit_cycle_in_flight(runtime):
            return True
        return bool(runtime._simulated_exit_window_open)
    return True


def request_deferred_chain_if_allowed(runtime: WorkerRuntime, *, side: str) -> bool:
    normalized = str(side or "").strip().lower()
    with runtime._state_lock:
        if not chain_allowed(runtime, side=normalized):
            return False
        if normalized == "entry":
            runtime._request_deferred_entry_chain()
            return True
        if normalized == "exit":
            runtime._request_deferred_exit_chain()
            return True
    return False


def current_leg_statuses(runtime: WorkerRuntime, *, fallback_left: str = "IDLE", fallback_right: str = "IDLE") -> tuple[str, str]:
    cycle_type: str | None = None
    cycle_id: int | None = None
    if getattr(runtime, "active_exit_cycle", None) is not None:
        cycle_type = "EXIT"
        cycle_id = int(getattr(runtime.active_exit_cycle, "cycle_id", 0) or 0) or None
    elif getattr(runtime, "active_entry_cycle", None) is not None:
        cycle_type = "ENTRY"
        cycle_id = int(getattr(runtime.active_entry_cycle, "cycle_id", 0) or 0) or None

    def _attempt_status_for_leg(leg_name: str) -> str | None:
        if cycle_type is None or cycle_id is None:
            return None
        iter_attempts = getattr(runtime, "_iter_leg_attempts", None)
        if not callable(iter_attempts):
            return None
        current_epoch = int(getattr(runtime, "_runtime_owner_epoch", 0) or 0)
        fallback_status: str | None = None
        for attempt in iter_attempts(leg_name=leg_name):
            if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                continue
            if int(getattr(attempt, "cycle_id", 0) or 0) != cycle_id:
                continue
            if str(getattr(attempt, "cycle_type", "") or "").strip().upper() != cycle_type:
                continue
            status = str(getattr(attempt, "status", "") or "").strip().upper()
            if not status:
                continue
            if not bool(getattr(attempt, "terminal", False)):
                return status
            if fallback_status is None:
                fallback_status = status
        return fallback_status

    left = (
        _attempt_status_for_leg("left")
        or str(getattr(runtime.left_leg_state, "order_status", None) or fallback_left).strip().upper()
        or "IDLE"
    )
    right = (
        _attempt_status_for_leg("right")
        or str(getattr(runtime.right_leg_state, "order_status", None) or fallback_right).strip().upper()
        or "IDLE"
    )
    return left, right


def classify_dual_exec_status(*, left_status: str, right_status: str) -> str:
    left = str(left_status or "").strip().upper() or "IDLE"
    right = str(right_status or "").strip().upper() or "IDLE"
    failed_count = sum(status == "FAILED" for status in (left, right))
    if failed_count == 2:
        return "FAILED"
    if left == "FILLED" and right == "FILLED":
        return "DONE"
    if failed_count == 1:
        return "PARTIAL"
    if any(status in {"ACK", "ACCEPTED", "FILLED", "NEW", "PARTIALLY_FILLED", "PARTIALLYFILLED"} for status in {left, right}):
        return "PARTIAL"
    if any(status in {"SENDING", "SENT"} for status in {left, right}):
        return "SENDING"
    return "IDLE"


def terminal_order_statuses() -> set[str]:
    return {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}


def should_wait_settle_timeout(*, left_status: str, right_status: str, elapsed_ms: int, timeout_ms: int) -> bool:
    terminals = terminal_order_statuses()
    left = str(left_status or "").strip().upper()
    right = str(right_status or "").strip().upper()
    return (left not in terminals or right not in terminals) and elapsed_ms < int(timeout_ms)


@dataclass(frozen=True)
class DualExecSnapshot:
    owner_epoch: int
    active_entry_cycle_id: int | None
    prefetch_entry_cycle_id: int | None
    active_exit_cycle_id: int | None
    left_status: str
    right_status: str
    left_filled: str
    right_filled: str


def build_dual_exec_snapshot(
    *,
    owner_epoch: int,
    active_entry_cycle_id: int | None,
    prefetch_entry_cycle_id: int | None,
    active_exit_cycle_id: int | None,
    left_status: str,
    right_status: str,
    left_filled: str,
    right_filled: str,
) -> DualExecSnapshot:
    return DualExecSnapshot(
        owner_epoch=int(owner_epoch),
        active_entry_cycle_id=int(active_entry_cycle_id) if active_entry_cycle_id is not None else None,
        prefetch_entry_cycle_id=int(prefetch_entry_cycle_id) if prefetch_entry_cycle_id is not None else None,
        active_exit_cycle_id=int(active_exit_cycle_id) if active_exit_cycle_id is not None else None,
        left_status=str(left_status or "").strip().upper() or "IDLE",
        right_status=str(right_status or "").strip().upper() or "IDLE",
        left_filled=str(left_filled),
        right_filled=str(right_filled),
    )


def select_dual_exec_context(*, active_entry_cycle_id: int | None, active_exit_cycle_id: int | None) -> str:
    if active_exit_cycle_id is not None:
        return "exit"
    if active_entry_cycle_id is not None:
        return "entry"
    return "none"


def build_dual_exec_done_payload(snapshot: DualExecSnapshot) -> dict[str, str]:
    return {"left_status": snapshot.left_status, "right_status": snapshot.right_status}


def build_entry_done_payload(snapshot: DualExecSnapshot, *, current_direction: str | None) -> dict[str, str | None]:
    direction = str(current_direction or "").strip().upper() or None
    active_edge = (
        "edge_1"
        if direction == "LEFT_SELL_RIGHT_BUY"
        else "edge_2"
        if direction == "LEFT_BUY_RIGHT_SELL"
        else None
    )
    return {
        "left_status": snapshot.left_status,
        "right_status": snapshot.right_status,
        "active_edge": active_edge,
        "entry_direction": direction,
    }
