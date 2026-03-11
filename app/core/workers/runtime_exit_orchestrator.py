from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from app.core.models.workers import StrategyCycleState, StrategyState
from app.core.workers.runtime_spread_utils import calculate_spread_edges

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


# Порог выхода 0 = «схождение к равенству»: abs(edge) должен быть ~0 (доли цены), иначе сравнение <= 0 никогда не выполнится.
EXIT_ZERO_CONVERGENCE_EPSILON = Decimal("0.0000001")


def build_exit_decision(runtime: WorkerRuntime) -> dict[str, Any] | None:
    if not _has_open_exposure(runtime):
        return None
    if runtime._has_live_leg_orders():
        return None
    exit_threshold = runtime._decimal_or_zero(runtime.task.exit_threshold or runtime.task.runtime_params.get("exit_threshold"))
    simulated_window_open = runtime._is_simulated_signal_mode() and runtime._simulated_exit_window_open
    # Window is trigger to *start* exit only; if exit cycle already active/prefetch, continue without window.
    exit_cycle_in_flight = runtime.active_exit_cycle is not None or getattr(runtime, "prefetch_exit_cycle", None) is not None
    if runtime._is_simulated_signal_mode() and not simulated_window_open and not exit_cycle_in_flight:
        return None
    exit_edge = current_exit_edge(runtime)
    if exit_edge is None:
        return None

    allow_exit = False
    entry_signed = getattr(runtime.position, "entry_edge", None) if runtime.position is not None else None
    left_quote = runtime._latest_quotes.get(runtime._left_instrument)
    right_quote = runtime._latest_quotes.get(runtime._right_instrument)

    # Ось по знаку: при входе сохранён знаковый спред по ноге (edge_1/edge_2).
    # Порог выхода — граница на той же оси (в долях; в % можно задать со знаком или по модулю).
    # Пример: вход при -1, порог -0.2 → выход при current > -0.2 (-0.1, 0, +0.1, +2 — всё правее).
    # Симметрично для входа при +1 и пороге +0.2 → выход при current < +0.2.
    if (
        entry_signed is not None
        and left_quote is not None
        and right_quote is not None
        and getattr(runtime.position, "active_edge", None)
    ):
        edge_result = calculate_spread_edges(left_quote, right_quote)
        leg = _normalize_edge_name(getattr(runtime.position, "active_edge", None))
        current_signed = edge_result.edge_1 if leg == "EDGE_1" else edge_result.edge_2 if leg == "EDGE_2" else None
        if current_signed is not None:
            boundary = _exit_boundary_signed(entry_signed, exit_threshold)
            if entry_signed < Decimal("0"):
                # Ушли вправо от границы (к нулю и дальше) — триггер
                if boundary is not None and current_signed > boundary:
                    allow_exit = True
            elif entry_signed > Decimal("0"):
                if boundary is not None and current_signed < boundary:
                    allow_exit = True
            else:
                if abs(current_signed) <= (exit_threshold if exit_threshold > Decimal("0") else EXIT_ZERO_CONVERGENCE_EPSILON):
                    allow_exit = True

    if not allow_exit and not exit_cycle_in_flight:
        # Fallback: схождение по пути закрытия, если нет знакового входа
        effective_exit_ceiling = (
            exit_threshold if exit_threshold > Decimal("0") else EXIT_ZERO_CONVERGENCE_EPSILON
        )
        if abs(exit_edge) <= effective_exit_ceiling:
            allow_exit = True
        # Fallback: доминирует другая нога (старое поведение)
        if not allow_exit and runtime.position is not None and getattr(runtime.position, "active_edge", None):
            if left_quote is not None and right_quote is not None:
                edge_result = calculate_spread_edges(left_quote, right_quote)
                if edge_result.direction:
                    leg = _normalize_edge_name(getattr(runtime.position, "active_edge", None))
                    if leg and str(edge_result.direction).strip().upper() != leg:
                        allow_exit = True

    # Цикл уже в полёте — не переоцениваем порог каждый тик (дожимаем закрытие).
    if not simulated_window_open and not exit_cycle_in_flight and not allow_exit:
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


def _exit_boundary_signed(entry_signed: Decimal, exit_threshold: Decimal) -> Decimal | None:
    """
    Граница на оси: если порог задан со знаком — как есть (в долях).
    Если порог только по модулю (положительный) — ставим по стороне входа:
    вход < 0 → граница -|T|, вход > 0 → граница +|T|.
    """
    if exit_threshold < Decimal("0"):
        return exit_threshold
    if exit_threshold == Decimal("0"):
        return Decimal("0")
    if entry_signed < Decimal("0"):
        return -exit_threshold
    if entry_signed > Decimal("0"):
        return exit_threshold
    return None


def _normalize_edge_name(active_edge: object) -> str | None:
    s = str(active_edge or "").strip().lower()
    if s == "edge_2" or s.endswith("_2"):
        return "EDGE_2"
    if s == "edge_1" or s.endswith("_1"):
        return "EDGE_1"
    return None


def exit_trigger_converged_or_flipped(runtime: WorkerRuntime) -> bool:
    """Выход по оси со знаком (entry_edge + порог) или fallback схождение/переворот."""
    exit_threshold = runtime._decimal_or_zero(runtime.task.exit_threshold or runtime.task.runtime_params.get("exit_threshold"))
    exit_edge = runtime._current_exit_edge()
    if exit_edge is None:
        return False
    entry_signed = getattr(runtime.position, "entry_edge", None) if runtime.position is not None else None
    left_quote = right_quote = None
    if hasattr(runtime, "_latest_quotes"):
        left_quote = runtime._latest_quotes.get(runtime._left_instrument)  # type: ignore[attr-defined]
        right_quote = runtime._latest_quotes.get(runtime._right_instrument)  # type: ignore[attr-defined]
    if (
        entry_signed is not None
        and left_quote is not None
        and right_quote is not None
        and getattr(runtime.position, "active_edge", None)
    ):
        edge_result = calculate_spread_edges(left_quote, right_quote)
        leg = _normalize_edge_name(getattr(runtime.position, "active_edge", None))
        current_signed = edge_result.edge_1 if leg == "EDGE_1" else edge_result.edge_2 if leg == "EDGE_2" else None
        if current_signed is not None:
            boundary = _exit_boundary_signed(entry_signed, exit_threshold)
            if entry_signed < Decimal("0") and boundary is not None and current_signed > boundary:
                return True
            if entry_signed > Decimal("0") and boundary is not None and current_signed < boundary:
                return True
            if entry_signed == Decimal("0"):
                eps = exit_threshold if exit_threshold > Decimal("0") else EXIT_ZERO_CONVERGENCE_EPSILON
                if abs(current_signed) <= eps:
                    return True
    effective_exit_ceiling = (
        exit_threshold if exit_threshold > Decimal("0") else EXIT_ZERO_CONVERGENCE_EPSILON
    )
    if abs(exit_edge) <= effective_exit_ceiling:
        return True
    if runtime.position is None or not getattr(runtime.position, "active_edge", None):
        return False
    if left_quote is None or right_quote is None:
        return False
    edge_result = calculate_spread_edges(left_quote, right_quote)
    if not edge_result.direction:
        return False
    leg = _normalize_edge_name(getattr(runtime.position, "active_edge", None))
    return bool(leg and str(edge_result.direction).strip().upper() != leg)


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
