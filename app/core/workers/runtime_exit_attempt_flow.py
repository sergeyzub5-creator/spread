from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyCycleState, StrategyState
from app.core.workers.runtime_transition_helpers import chain_allowed, current_leg_statuses, request_deferred_chain_if_allowed, should_wait_settle_timeout

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def _apply_exit_cycle_commit(
    runtime: WorkerRuntime,
    *,
    settle_reason: str,
    last_result: str,
    allow_idle_when_flat: bool,
    reset_growth_on_idle: bool = False,
) -> bool:
    runtime.exit_recovery_plan = None
    runtime._clear_recovery_status(context="EXIT_CYCLE")
    runtime._commit_exit_cycle()
    runtime._settle_dual_execution_state(reason=settle_reason)
    left_flat = runtime.left_leg_state.actual_position_qty <= Decimal("0")
    right_flat = runtime.right_leg_state.actual_position_qty <= Decimal("0")
    if allow_idle_when_flat and left_flat and right_flat:
        runtime._reset_position_state()
        if reset_growth_on_idle:
            runtime.left_leg_state.filled_qty = Decimal("0")
            runtime.right_leg_state.filled_qty = Decimal("0")
            runtime._reset_cycle_growth(reason="FULL_EXIT")
        runtime._set_strategy_state(StrategyState.IDLE)
        should_chain_next_exit = False
    else:
        runtime._sync_position_from_legs()
        runtime._set_strategy_state(StrategyState.IN_POSITION)
        should_chain_next_exit = _maybe_close_residual_tails(runtime)
    runtime.state.metrics["last_result"] = last_result
    runtime._publish_state()
    return should_chain_next_exit


def _maybe_close_residual_tails(runtime: WorkerRuntime) -> bool:
    """Close leftover single-leg residuals below exchange minimum after symmetric exit.

    Returns True if the system should attempt another exit chain, False if
    tails were handled (or no tails found) and exit can stop.
    """
    left_qty = runtime.left_leg_state.filled_qty
    right_qty = runtime.right_leg_state.filled_qty
    symmetric_qty = min(left_qty, right_qty)
    min_exchange_qty = max(
        runtime._left_instrument.spec.min_qty,
        runtime._right_instrument.spec.min_qty,
    )
    if symmetric_qty >= min_exchange_qty:
        return True

    tail_legs: list[tuple[str, Decimal]] = []
    if left_qty > Decimal("0") and left_qty < min_exchange_qty:
        tail_legs.append(("left", left_qty))
    if right_qty > Decimal("0") and right_qty < min_exchange_qty:
        tail_legs.append(("right", right_qty))
    if not tail_legs:
        return symmetric_qty > Decimal("0")

    for leg_name, qty in tail_legs:
        open_side = str(runtime._leg_state(leg_name).side or "").strip().upper()
        if open_side not in {"BUY", "SELL"}:
            continue
        close_side = "SELL" if open_side == "BUY" else "BUY"
        runtime.logger.info(
            "exit residual tail close | leg=%s | qty=%s | close_side=%s | reason=BELOW_EXCHANGE_MIN",
            leg_name,
            runtime._format_order_size(qty),
            close_side,
        )
        try:
            runtime._submit_leg_order(
                leg_name=leg_name,
                side=close_side,
                quantity=qty,
                reason="EXIT_RESIDUAL_TAIL_CLOSE",
                reduce_only=True,
            )
        except Exception as exc:
            runtime.logger.error(
                "exit residual tail close failed | leg=%s | error=%s",
                leg_name,
                str(exc),
            )
    return False


def _can_commit_partial_exit_cycle(runtime: WorkerRuntime) -> bool:
    cycle = runtime.active_exit_cycle
    if cycle is None:
        return False
    left_closed = max(Decimal("0"), cycle.left_filled_qty)
    right_closed = max(Decimal("0"), cycle.right_filled_qty)
    if left_closed <= Decimal("0") or right_closed <= Decimal("0"):
        return False
    # Local cycle guard: accept only symmetric per-cycle fills.
    return left_closed == right_closed


def update_exit_attempt_state(runtime: WorkerRuntime, left_status: str, right_status: str) -> None:
    should_reconcile_exit_remainder = False
    should_chain_next_exit = False
    reconcile_cycle_id: int | None = None
    with runtime._state_lock:
        if not runtime._is_spread_entry_runtime or runtime.active_exit_cycle is None:
            return
        # Keep cycle-level closed qty in sync with latest leg states before classifying.
        runtime._sync_active_exit_cycle_from_legs()
        if runtime._is_exit_full_success(left_status, right_status):
            should_chain_next_exit = _apply_exit_cycle_commit(
                runtime,
                settle_reason="EXIT_DONE",
                last_result="EXIT_DONE",
                allow_idle_when_flat=True,
                reset_growth_on_idle=True,
            )
        elif runtime._is_exit_cycle_committed_success():
            should_chain_next_exit = _apply_exit_cycle_commit(
                runtime,
                settle_reason="EXIT_CYCLE_COMMITTED",
                last_result="EXIT_CYCLE_COMMITTED",
                allow_idle_when_flat=False,
            )
        elif runtime._is_exit_full_fail(left_status, right_status):
            runtime.exit_recovery_plan = None
            runtime._clear_recovery_status(context="EXIT_CYCLE")
            runtime._finalize_exit_cycle(state=StrategyCycleState.ABORT, error="EXIT_FULL_FAIL")
            runtime._set_strategy_state(StrategyState.IN_POSITION)
            return
        elif runtime._is_exit_partial(left_status, right_status):
            runtime._set_strategy_state(StrategyState.EXIT_PARTIAL)
            if runtime.active_exit_cycle is not None and runtime.active_exit_cycle.state is not StrategyCycleState.ACTIVE:
                return
            if runtime._exit_tail_resync_in_progress():
                return
            cycle_started_ms = int(runtime.active_exit_cycle.started_at or 0) if runtime.active_exit_cycle is not None else 0
            elapsed_ms = max(0, int(time.time() * 1000) - cycle_started_ms) if cycle_started_ms > 0 else 0
            has_live_orders = runtime._has_live_leg_orders()
            has_stale_live_orders = has_live_orders and runtime._exit_has_stale_live_orders()
            if (
                left_status == "FILLED"
                and right_status == "FILLED"
                and not has_live_orders
                and runtime._is_exit_cycle_committed_success()
            ):
                should_chain_next_exit = _apply_exit_cycle_commit(
                    runtime,
                    settle_reason="EXIT_CYCLE_COMMITTED",
                    last_result="EXIT_CYCLE_COMMITTED",
                    allow_idle_when_flat=True,
                )
                if should_chain_next_exit:
                    if chain_allowed(runtime, side="exit"):
                        runtime._request_deferred_exit_chain()
                return
            if runtime._exit_recovery_blocked_by_grace(left_status=left_status, right_status=right_status):
                return
            settle_grace_ms = runtime._rebalance_grace_remaining_ms()
            if settle_grace_ms > 0:
                return
            if has_live_orders and not has_stale_live_orders:
                runtime._set_strategy_state(StrategyState.EXIT_SUBMITTING)
                return
            if not has_live_orders:
                runtime._sync_active_exit_cycle_from_legs()
                if runtime._is_exit_cycle_committed_success():
                    should_chain_next_exit = _apply_exit_cycle_commit(
                        runtime,
                        settle_reason="EXIT_CYCLE_COMMITTED",
                        last_result="EXIT_CYCLE_COMMITTED",
                        allow_idle_when_flat=False,
                    )
                    if should_chain_next_exit:
                        if chain_allowed(runtime, side="exit"):
                            runtime._request_deferred_exit_chain()
                    return
                # Fast-path: both legs have no live orders but cycle is still not committed.
                # Reconcile immediately instead of waiting grace/settle timers.
                should_reconcile_exit_remainder = True
                reconcile_cycle_id = int(runtime.active_exit_cycle.cycle_id)
                runtime.logger.info(
                    "exit tail reconcile fast-path | cycle_id=%s | reason=NO_LIVE_ORDERS_UNCOMMITTED",
                    reconcile_cycle_id,
                )
                # Continue to shared reconcile path outside lock.
            if should_reconcile_exit_remainder:
                pass
            elif has_live_orders and has_stale_live_orders:
                runtime.logger.warning(
                    "exit live order stale | left_status=%s | right_status=%s | stale_threshold_ms=%s | elapsed_ms=%s",
                    left_status,
                    right_status,
                    runtime.EXIT_LIVE_ORDER_STALE_MS,
                    elapsed_ms,
                )
            elif should_wait_settle_timeout(
                left_status=left_status,
                right_status=right_status,
                elapsed_ms=elapsed_ms,
                timeout_ms=runtime.EXIT_CYCLE_SETTLE_TIMEOUT_MS,
            ):
                runtime._set_strategy_state(StrategyState.EXIT_SUBMITTING)
                return
            should_reconcile_exit_remainder = True
            reconcile_cycle_id = int(runtime.active_exit_cycle.cycle_id)
        elif runtime._is_entry_attempt_active(left_status, right_status):
            runtime._set_strategy_state(StrategyState.EXIT_SUBMITTING)
            return
        else:
            return

    if should_chain_next_exit:
        request_deferred_chain_if_allowed(runtime, side="exit")
        return

    if should_reconcile_exit_remainder:
        runtime._reconcile_exit_remainder_from_exchange(reason="EXIT_PRE_RECOVERY_RECONCILE")
        with runtime._state_lock:
            if runtime.active_exit_cycle is None or int(runtime.active_exit_cycle.cycle_id) != int(reconcile_cycle_id or -1):
                # Reconcile result is stale: active cycle advanced while resync was in-flight.
                return
            runtime._sync_active_exit_cycle_from_legs()
            left_status, right_status = current_leg_statuses(
                runtime,
                fallback_left=left_status,
                fallback_right=right_status,
            )
            if runtime._is_exit_full_success(left_status, right_status):
                pass
            elif runtime._is_exit_cycle_committed_success():
                should_chain_next_exit = _apply_exit_cycle_commit(
                    runtime,
                    settle_reason="EXIT_CYCLE_COMMITTED",
                    last_result="EXIT_CYCLE_COMMITTED",
                    allow_idle_when_flat=False,
                )
                if should_chain_next_exit and chain_allowed(runtime, side="exit"):
                    runtime._request_deferred_exit_chain()
                return
            else:
                if _can_commit_partial_exit_cycle(runtime):
                    should_chain_next_exit = _apply_exit_cycle_commit(
                        runtime,
                        settle_reason="EXIT_CYCLE_PARTIAL_COMMITTED",
                        last_result="EXIT_CYCLE_PARTIAL_COMMITTED",
                        allow_idle_when_flat=False,
                    )
                    # Partial commit should not increase aggressive cycle growth.
                    runtime._exit_cycle_success_streak = 0
                    runtime.state.metrics["exit_cycle_growth_streak"] = 0
                    if should_chain_next_exit and chain_allowed(runtime, side="exit"):
                        runtime._request_deferred_exit_chain()
                    return
                runtime.state.metrics["last_result"] = "EXIT_PARTIAL_NO_RECOVERY"
                runtime._finalize_exit_cycle(state=StrategyCycleState.ABORT, error="EXIT_PARTIAL_NO_RECOVERY")
                if runtime.left_leg_state.actual_position_qty <= Decimal("0") and runtime.right_leg_state.actual_position_qty <= Decimal("0"):
                    runtime._reset_position_state()
                    runtime._set_strategy_state(StrategyState.IDLE)
                else:
                    if runtime.left_leg_state.actual_position_qty > Decimal("0") and runtime.right_leg_state.actual_position_qty > Decimal("0"):
                        runtime._sync_position_from_legs()
                    runtime._set_strategy_state(StrategyState.IN_POSITION)
                    # Hedge mismatch is handled by the global hedge contour.
                    # Keep local exit flow non-hedging to avoid duplicated recovery logic.
                runtime._publish_state()
        if runtime._is_exit_full_success(left_status, right_status):
            runtime._update_strategy_state_from_exit_attempt(left_status, right_status)
            return
