from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.core.models.workers import StrategyCycleState, StrategyState

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def _has_open_exposure(runtime: WorkerRuntime) -> bool:
    return (
        runtime.left_leg_state.filled_qty > Decimal("0")
        or runtime.right_leg_state.filled_qty > Decimal("0")
        or runtime.left_leg_state.actual_position_qty > Decimal("0")
        or runtime.right_leg_state.actual_position_qty > Decimal("0")
    )


def evaluate_spread_exit(runtime: WorkerRuntime) -> None:
    if not runtime._is_spread_entry_runtime:
        return
    if not runtime._policy_allow_exit_evaluation():
        return
    allowed_states = {StrategyState.IN_POSITION}
    if runtime._entry_pipeline_overlap_enabled():
        allowed_states.add(StrategyState.EXIT_SUBMITTING)
    if runtime.strategy_state not in allowed_states:
        return
    with runtime._exit_lock:
        with runtime._state_lock:
            if not runtime._is_spread_entry_runtime or runtime.strategy_state not in allowed_states:
                return
            if runtime._runtime_reconcile_active():
                return
            if runtime.active_exit_cycle is None and runtime.prefetch_exit_cycle is not None:
                runtime._promote_prefetch_exit_cycle()
            if (
                not _has_open_exposure(runtime)
                or runtime.active_entry_cycle is not None
                or runtime.prefetch_entry_cycle is not None
                or (runtime.active_exit_cycle is not None and not runtime._entry_pipeline_overlap_enabled())
                or runtime._entry_recovery_active()
                or runtime._exit_recovery_active()
                or runtime._hedge_protection_active()
            ):
                return
            if runtime.active_exit_cycle is None and runtime._has_live_leg_orders():
                return
            if runtime.active_exit_cycle is not None:
                if runtime.prefetch_exit_cycle is not None:
                    return
                if not runtime._entry_cycle_ack_ready(runtime.active_exit_cycle):
                    return
            decision = runtime._build_exit_decision()
            if decision is None:
                return
            prefetch = runtime.active_exit_cycle is not None
            submit_cycle = runtime._start_exit_cycle(
                direction=decision["direction"],
                edge_name=decision["edge_name"],
                edge_value=decision["edge_value"],
                left_side=decision["left_side"],
                right_side=decision["right_side"],
                left_qty=decision["left_qty"],
                right_qty=decision["right_qty"],
                prefetch=prefetch,
            )
            if prefetch:
                submit_cycle.state = StrategyCycleState.SUBMITTING
                runtime._sync_active_exit_cycle_metrics()
            else:
                runtime._set_strategy_state(StrategyState.EXIT_ARMED)
                runtime._set_exit_cycle_state(StrategyCycleState.SUBMITTING)
                runtime._set_strategy_state(StrategyState.EXIT_SUBMITTING)
            submit_kwargs = {
                "left_side": decision["left_side"],
                "right_side": decision["right_side"],
                "left_qty": runtime._format_order_size(decision["left_qty"]),
                "right_qty": runtime._format_order_size(decision["right_qty"]),
                "left_price_mode": "top_of_book",
                "right_price_mode": "top_of_book",
                "submitted_at_ms": int(time.time() * 1000),
                "exit_cycle_id": submit_cycle.cycle_id,
            }
        runtime.submit_dual_test_orders(**submit_kwargs)


def build_exit_decision(runtime: WorkerRuntime) -> dict[str, Any] | None:
    if not _has_open_exposure(runtime):
        return None
    if runtime._has_live_leg_orders():
        return None
    exit_threshold = runtime._decimal_or_zero(runtime.task.exit_threshold or runtime.task.runtime_params.get("exit_threshold"))
    if exit_threshold <= Decimal("0"):
        return None
    simulated_window_open = runtime._is_simulated_signal_mode() and runtime._simulated_exit_window_open
    if runtime._is_simulated_signal_mode() and not simulated_window_open:
        return None
    exit_edge = current_exit_edge(runtime)
    if exit_edge is None:
        return None
    exit_opportunity = abs(exit_edge)
    # In market mode exit threshold acts as an execution trigger (opportunity floor),
    # not as a directional convergence constraint.
    if not simulated_window_open and exit_opportunity < exit_threshold:
        return None
    left_side, right_side = exit_sides_for_position(runtime)
    if left_side is None or right_side is None:
        return None
    left_qty, right_qty = planned_exit_cycle_sizes(runtime, left_side=left_side, right_side=right_side)
    if left_qty <= Decimal("0") or right_qty <= Decimal("0"):
        return None
    edge_name = None
    if left_side == "SELL" and right_side == "BUY":
        edge_name = "edge_1"
    elif left_side == "BUY" and right_side == "SELL":
        edge_name = "edge_2"
    return {
        "direction": f"LEFT_{left_side}_RIGHT_{right_side}",
        "edge_name": edge_name,
        "edge_value": exit_edge,
        "left_side": left_side,
        "right_side": right_side,
        "left_qty": left_qty,
        "right_qty": right_qty,
    }


def current_exit_edge(runtime: WorkerRuntime) -> Decimal | None:
    left_quote = runtime._latest_quotes.get(runtime._left_instrument)
    right_quote = runtime._latest_quotes.get(runtime._right_instrument)
    if left_quote is None or right_quote is None:
        return None
    left_side, right_side = exit_sides_for_position(runtime)
    if left_side == "BUY" and right_side == "SELL":
        # Exit by buying left/selling right maps to current edge_2 opportunity.
        exit_edge = runtime._safe_edge(right_quote.bid, left_quote.ask)
    elif left_side == "SELL" and right_side == "BUY":
        # Exit by selling left/buying right maps to current edge_1 opportunity.
        exit_edge = runtime._safe_edge(left_quote.bid, right_quote.ask)
    else:
        exit_edge = None
    return exit_edge


def exit_sides_for_position(runtime: WorkerRuntime) -> tuple[str | None, str | None]:
    left_position_side = runtime.position.left_side if runtime.position is not None else None
    right_position_side = runtime.position.right_side if runtime.position is not None else None
    left_open_side = str(runtime.left_leg_state.side or left_position_side or "").strip().upper()
    right_open_side = str(runtime.right_leg_state.side or right_position_side or "").strip().upper()
    if left_open_side in {"BUY", "SELL"} and right_open_side in {"BUY", "SELL"}:
        left_close_side = "SELL" if left_open_side == "BUY" else "BUY"
        right_close_side = "SELL" if right_open_side == "BUY" else "BUY"
        return left_close_side, right_close_side
    return None, None


def planned_exit_cycle_sizes(runtime: WorkerRuntime, *, left_side: str, right_side: str) -> tuple[Decimal, Decimal]:
    left_quote = runtime._latest_quotes.get(runtime._left_instrument)
    right_quote = runtime._latest_quotes.get(runtime._right_instrument)
    if left_quote is None or right_quote is None:
        return Decimal("0"), Decimal("0")
    desired_qty = runtime._compute_shared_dual_leg_quantity(
        left_instrument=runtime._left_instrument,
        right_instrument=runtime._right_instrument,
        left_quote=left_quote,
        right_quote=right_quote,
        left_action=left_side,
        right_action=right_side,
        target_notional=runtime._exit_cycle_notional_usdt(),
    )
    max_close_qty = min(runtime.left_leg_state.filled_qty, runtime.right_leg_state.filled_qty)
    if max_close_qty <= Decimal("0"):
        return Decimal("0"), Decimal("0")
    if desired_qty <= Decimal("0") or desired_qty > max_close_qty:
        desired_qty = max_close_qty
    min_exchange_qty = max(
        runtime._left_instrument.spec.min_qty,
        runtime._right_instrument.spec.min_qty,
    )
    remainder = max_close_qty - desired_qty
    if Decimal("0") < remainder < min_exchange_qty:
        desired_qty = max_close_qty
        runtime.logger.info(
            "exit tail absorbed | remainder_qty=%s < min_exchange_qty=%s | closing full max_close_qty=%s",
            runtime._format_order_size(remainder),
            runtime._format_order_size(min_exchange_qty),
            runtime._format_order_size(max_close_qty),
        )
    return desired_qty, desired_qty
