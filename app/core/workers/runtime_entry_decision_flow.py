from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyState
from app.core.workers.entry_validator import EntryValidationResult
from app.core.workers.runtime_spread_utils import SpreadEdgeResult, calculate_spread_edges

if TYPE_CHECKING:
    from app.core.models.workers import EntryDecision
    from app.core.workers.runtime_core import WorkerRuntime


def build_entry_validation_result(runtime: WorkerRuntime, edge_result: SpreadEdgeResult) -> EntryValidationResult:
    left_quote = runtime._latest_quotes.get(runtime._left_instrument)
    right_quote = runtime._latest_quotes.get(runtime._right_instrument)
    if left_quote is None or right_quote is None:
        return EntryValidationResult(False, "WAITING_QUOTES", False, False, False, False, False, False)
    left_test_size, right_test_size = runtime._current_test_sizes(edge_result)
    enforce_liquidity = should_enforce_entry_liquidity_check(runtime)
    return runtime._entry_validator.validate_entry(
        left_quote=left_quote,
        right_quote=right_quote,
        left_action=str(edge_result.left_action or ""),
        right_action=str(edge_result.right_action or ""),
        left_test_size=left_test_size,
        right_test_size=right_test_size,
        left_quote_age_ms=runtime._int_or_zero(runtime.state.metrics.get("left_quote_age_ms")),
        right_quote_age_ms=runtime._int_or_zero(runtime.state.metrics.get("right_quote_age_ms")),
        max_quote_skew_ms=runtime._int_or_zero(runtime.task.runtime_params.get("max_quote_skew_ms")),
        enforce_liquidity=enforce_liquidity,
    )


def should_enforce_entry_liquidity_check(runtime: WorkerRuntime) -> bool:
    return (
        runtime.position is None
        and runtime.left_leg_state.filled_qty <= Decimal("0")
        and runtime.right_leg_state.filled_qty <= Decimal("0")
    )


def resolve_entry_min_step_percent(runtime: WorkerRuntime) -> Decimal:
    # Backward compatible fallback to legacy key if it still exists in saved UI state.
    raw_value = runtime.task.runtime_params.get("entry_min_step_pct")
    if raw_value is None:
        raw_value = runtime.task.runtime_params.get("entry_min_step_qty")
    try:
        normalized = Decimal(str(raw_value if raw_value is not None else "20"))
    except Exception:
        normalized = Decimal("20")
    if normalized < Decimal("10"):
        normalized = Decimal("10")
    if normalized > Decimal("100"):
        normalized = Decimal("100")
    return normalized


def build_entry_decision(runtime: WorkerRuntime) -> EntryDecision | None:
    allowed_states = {StrategyState.IDLE, StrategyState.COOLDOWN, StrategyState.IN_POSITION}
    if runtime._entry_pipeline_overlap_enabled():
        allowed_states.add(StrategyState.ENTRY_SUBMITTING)
    if runtime.strategy_state not in allowed_states:
        return None
    forced_signal = runtime._take_forced_entry_signal()
    now_ms = int(time.time() * 1000)
    threshold = runtime._decimal_or_zero(runtime.task.runtime_params.get("entry_threshold") or runtime.task.entry_threshold)
    raw_edge_result = calculate_spread_edges(runtime._latest_quotes.get(runtime._left_instrument), runtime._latest_quotes.get(runtime._right_instrument))
    simulated_window_open = runtime._simulated_entry_window_open if runtime._is_simulated_signal_mode() else False
    simulated_locked_edge_result = (
        runtime._simulated_cycle_entry_edge_result(direction=runtime._simulated_entry_direction, threshold=threshold)
        if runtime._is_simulated_signal_mode() and runtime._simulated_entry_direction
        else None
    )
    if runtime._is_simulated_signal_mode() and simulated_window_open and not forced_signal:
        runtime._ensure_simulated_entry_direction_locked(reason="ENTRY_DECISION")
    if (
        runtime.position is None
        and runtime.active_entry_cycle is None
        and runtime.prefetch_entry_cycle is None
        and runtime.active_exit_cycle is None
        and not runtime._has_live_leg_orders()
        and runtime.last_entry_ts is not None
        and max(0, now_ms - runtime.last_entry_ts) < runtime.cooldown_ms
    ):
        runtime._set_strategy_state(StrategyState.COOLDOWN)
        runtime.state.metrics["entry_block_reason"] = "ENTRY_COOLDOWN"
        decision = runtime._make_entry_decision(
            edge_result=simulated_locked_edge_result or raw_edge_result,
            threshold=threshold,
            validation_result=None,
            block_reason="ENTRY_COOLDOWN",
            forced_signal=forced_signal,
            is_executable=False,
        )
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked("ENTRY_COOLDOWN", decision_logged=logged)
        runtime._publish_state()
        return decision
    if runtime.strategy_state is StrategyState.COOLDOWN:
        runtime._set_strategy_state(StrategyState.IDLE)
    pipeline_block_reason = runtime._entry_pipeline_busy_reason()
    if pipeline_block_reason is not None:
        decision = runtime._make_entry_decision(
            edge_result=simulated_locked_edge_result or raw_edge_result,
            threshold=threshold,
            validation_result=None,
            block_reason=pipeline_block_reason,
            forced_signal=forced_signal,
            is_executable=False,
        )
        runtime.state.metrics["entry_block_reason"] = pipeline_block_reason
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked(pipeline_block_reason, decision_logged=logged)
        runtime._publish_state()
        return decision
    # Window closed blocks *new* entry only; do not block while entry cycle already in flight.
    if (
        runtime._is_simulated_signal_mode()
        and not forced_signal
        and not simulated_window_open
        and runtime.active_entry_cycle is None
        and runtime.prefetch_entry_cycle is None
    ):
        decision = runtime._make_entry_decision(
            edge_result=simulated_locked_edge_result or raw_edge_result,
            threshold=threshold,
            validation_result=None,
            block_reason="SIMULATED_ENTRY_WINDOW_CLOSED",
            forced_signal=False,
            is_executable=False,
        )
        runtime.state.metrics["entry_block_reason"] = "SIMULATED_ENTRY_WINDOW_CLOSED"
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked("SIMULATED_ENTRY_WINDOW_CLOSED", decision_logged=logged)
        runtime._publish_state()
        return decision
    simulated_edge_result = (
        runtime._simulated_cycle_entry_edge_result(direction=runtime._simulated_entry_direction, threshold=threshold)
        if runtime._is_simulated_signal_mode() and not forced_signal and simulated_window_open
        else None
    )
    effective_edge_result = simulated_edge_result or runtime._effective_entry_edge_result(
        edge_result=raw_edge_result,
        threshold=threshold,
        forced_signal=forced_signal,
        simulated_window_open=simulated_window_open,
    )
    edge_opportunity = abs(effective_edge_result.best_edge) if effective_edge_result.best_edge is not None else None
    if edge_opportunity is None or threshold <= Decimal("0") or (not forced_signal and edge_opportunity < threshold):
        if effective_edge_result.best_edge is None:
            block_reason = "WAITING_QUOTES"
        elif threshold <= Decimal("0"):
            block_reason = "INVALID_ENTRY_THRESHOLD"
        else:
            block_reason = "BELOW_ENTRY_THRESHOLD"
        decision = runtime._make_entry_decision(
            edge_result=effective_edge_result,
            threshold=threshold,
            block_reason=block_reason,
            validation_result=None,
            forced_signal=forced_signal,
            is_executable=False,
        )
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked(block_reason, decision_logged=logged)
        return decision
    capacity_block_reason = runtime._entry_capacity_block_reason(effective_edge_result)
    if capacity_block_reason is not None:
        if capacity_block_reason in {"POSITION_CAP_REACHED", "POSITION_SIZE_LIMITED_BY_MARGIN"}:
            runtime._reset_cycle_growth(reason="FULL_ENTRY")
        decision = runtime._make_entry_decision(
            edge_result=effective_edge_result,
            threshold=threshold,
            block_reason=capacity_block_reason,
            validation_result=None,
            forced_signal=forced_signal,
            is_executable=False,
        )
        runtime.state.metrics["entry_block_reason"] = capacity_block_reason
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked(capacity_block_reason, decision_logged=logged)
        runtime._publish_state()
        return decision
    planned_size = runtime._planned_entry_size(effective_edge_result)
    entry_notional_usdt = runtime._entry_notional_usdt()
    cycle_notional_usdt = runtime._decimal_or_zero(planned_size.get("cycle_notional_usdt"))
    planned_left_qty = runtime._decimal_or_zero(planned_size.get("left_qty"))
    planned_right_qty = runtime._decimal_or_zero(planned_size.get("right_qty"))
    planned_common_qty = min(planned_left_qty, planned_right_qty)
    min_step_pct = resolve_entry_min_step_percent(runtime)
    min_step_notional_usdt = (entry_notional_usdt * min_step_pct / Decimal("100")) if entry_notional_usdt > Decimal("0") else Decimal("0")
    runtime.state.metrics["entry_min_step_pct"] = runtime._format_order_size(min_step_pct)
    runtime.state.metrics["entry_min_step_notional_usdt"] = runtime._format_order_size(min_step_notional_usdt)
    min_step_qty = Decimal("0")
    if min_step_notional_usdt > Decimal("0"):
        left_quote = runtime._latest_quotes.get(runtime._left_instrument)
        right_quote = runtime._latest_quotes.get(runtime._right_instrument)
        if left_quote is not None and right_quote is not None:
            min_step_qty = runtime._compute_shared_dual_leg_quantity(
                left_instrument=runtime._left_instrument,
                right_instrument=runtime._right_instrument,
                left_quote=left_quote,
                right_quote=right_quote,
                left_action=str(effective_edge_result.left_action or ""),
                right_action=str(effective_edge_result.right_action or ""),
                target_notional=min_step_notional_usdt,
            )
    runtime.state.metrics["entry_min_step_qty"] = runtime._format_order_size(min_step_qty) if min_step_qty > Decimal("0") else None
    if (
        min_step_notional_usdt > Decimal("0")
        and cycle_notional_usdt > Decimal("0")
        and min_step_qty > Decimal("0")
        and planned_common_qty > Decimal("0")
        and planned_common_qty < min_step_qty
    ):
        block_reason = "ENTRY_STEP_BELOW_MIN_PERCENT"
        decision = runtime._make_entry_decision(
            edge_result=effective_edge_result,
            threshold=threshold,
            validation_result=None,
            block_reason=block_reason,
            forced_signal=forced_signal,
            is_executable=False,
            planned_size=planned_size,
        )
        runtime.state.metrics["entry_block_reason"] = block_reason
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked(block_reason, decision_logged=logged)
        runtime._publish_state()
        return decision
    validation_result = build_entry_validation_result(runtime, effective_edge_result)
    decision = runtime._make_entry_decision(
        edge_result=effective_edge_result,
        threshold=threshold,
        validation_result=validation_result,
        block_reason=validation_result.block_reason if not validation_result.is_valid else None,
        forced_signal=forced_signal,
        is_executable=validation_result.is_valid,
        planned_size=planned_size,
    )
    if not validation_result.is_valid:
        runtime.state.metrics["entry_block_reason"] = validation_result.block_reason
        logged = runtime._log_entry_decision(decision=decision)
        runtime._log_entry_blocked(str(validation_result.block_reason), decision_logged=logged)
        runtime.emit_event("entry_blocked", {"reason": validation_result.block_reason, "validation": validation_result.to_dict()})
        runtime._publish_state()
        return decision
    runtime.state.metrics["active_edge"] = decision.edge_name
    runtime.state.metrics["entry_direction"] = decision.direction
    runtime.state.metrics["entry_block_reason"] = None
    runtime._log_entry_decision(decision=decision)
    if forced_signal or simulated_window_open:
        runtime.logger.info(
            "simulated entry window accepted | simulated_best_edge=%s | threshold=%s | direction=%s | forced_signal=%s | simulated_window_open=%s",
            runtime._format_edge(decision.edge),
            runtime._format_edge(threshold),
            decision.direction,
            forced_signal,
            simulated_window_open,
        )
    runtime.logger.info(
        "ENTRY CANDIDATE DETECTED | edge=%s | value=%s | direction=%s | cycle_notional_usdt=%s | position_cap_notional_usdt=%s | left_qty=%s | right_qty=%s",
        decision.edge_name,
        runtime._format_edge(decision.edge),
        decision.direction,
        planned_size.get("cycle_notional_usdt"),
        planned_size.get("entry_notional_usdt"),
        planned_size.get("left_qty"),
        planned_size.get("right_qty"),
    )
    runtime.emit_event(
        "entry_signal_detected",
        {
            "active_edge": decision.edge_name,
            "edge_value": runtime._format_edge(decision.edge),
            "entry_direction": decision.direction,
            "cycle_notional_usdt": runtime._format_order_size(planned_size.get("cycle_notional_usdt", Decimal("0"))),
            "entry_notional_usdt": runtime._format_order_size(planned_size.get("entry_notional_usdt", Decimal("0"))),
            "left_qty": runtime._format_order_size(planned_size.get("left_qty", Decimal("0"))),
            "right_qty": runtime._format_order_size(planned_size.get("right_qty", Decimal("0"))),
        },
    )
    runtime._publish_state()
    return decision
