from __future__ import annotations

import threading
import time
from decimal import Decimal

from app.core.models.workers import EntryDecision, StrategyCycle, StrategyCycleState, StrategyCycleType, StrategyState


class WorkerRuntimeCycleMixin:
    def _mark_cycle_activity(self) -> None:
        self._last_cycle_activity_ts = int(time.time() * 1000)

    def _global_hedge_guard_enabled(self) -> bool:
        if (
            self.active_entry_cycle is not None
            or self.prefetch_entry_cycle is not None
            or self.active_exit_cycle is not None
            or getattr(self, "prefetch_exit_cycle", None) is not None
        ):
            return False
        now_ms = int(time.time() * 1000)
        # One-shot early window after cycle close: after post_cycle delay, allow guard once without waiting full idle.
        eligible_at = int(getattr(self, "_post_cycle_hedge_eligible_at_ms", 0) or 0)
        if eligible_at > 0 and now_ms >= eligible_at:
            self._post_cycle_hedge_eligible_at_ms = 0
            return True
        last_ts = int(getattr(self, "_last_cycle_activity_ts", 0) or 0)
        if last_ts <= 0:
            return True
        idle_ms = int(getattr(self, "_global_hedge_guard_idle_ms", 2000) or 2000)
        return (now_ms - last_ts) >= max(0, idle_ms)

    def _schedule_post_cycle_hedge_check(self) -> None:
        """After a cycle closes: arm one-shot hedge check after short delay; 2s idle path remains for later retries."""
        delay_ms = int(getattr(self, "_post_cycle_hedge_guard_ms", 500) or 500)
        delay_ms = max(0, min(delay_ms, 10_000))
        now_ms = int(time.time() * 1000)
        self._post_cycle_hedge_eligible_at_ms = now_ms + delay_ms

        def _fire() -> None:
            try:
                if self.state.status != "running":
                    return
                self._request_hedge_protection_check(reason="POST_CYCLE_DELAY")
            except Exception:
                pass

        if delay_ms <= 0:
            _fire()
            return
        timer = threading.Timer(delay_ms / 1000.0, _fire)
        timer.daemon = True
        timer.start()

    def _reset_cycle_growth(self, *, reason: str) -> None:
        self._entry_cycle_success_streak = 0
        self._exit_cycle_success_streak = 0
        self.state.metrics["entry_cycle_growth_streak"] = 0
        self.state.metrics["exit_cycle_growth_streak"] = 0
        self.state.metrics["cycle_growth_reset_reason"] = reason

    def _maybe_reset_cycle_growth_on_idle(self, *, now_ms: int) -> None:
        if self.strategy_state is not StrategyState.IN_POSITION:
            return
        if self.active_entry_cycle is not None or self.active_exit_cycle is not None:
            return
        if self._cycle_recovery_active():
            return
        if (self._entry_cycle_success_streak <= 0 and self._exit_cycle_success_streak <= 0):
            return
        last_ts = int(getattr(self, "_last_cycle_activity_ts", 0) or 0)
        if last_ts <= 0:
            return
        if (now_ms - last_ts) < int(getattr(self, "_cycle_growth_idle_reset_ms", 15000) or 15000):
            return
        self._reset_cycle_growth(reason="IN_POSITION_IDLE_TIMEOUT")

    def _set_recovery_status(self, *, context: str, state: str, reason: str | None) -> None:
        self.state.metrics["recovery_context"] = context
        self.state.metrics["recovery_state"] = state
        self.state.metrics["recovery_reason"] = reason

    def _clear_recovery_status(self, *, context: str) -> None:
        if str(self.state.metrics.get("recovery_context") or "") != context:
            return
        self.state.metrics["recovery_context"] = None
        self.state.metrics["recovery_state"] = None
        self.state.metrics["recovery_reason"] = None

    def _entry_recovery_active(self) -> bool:
        return self._entry_recovery_thread is not None and self._entry_recovery_thread.is_alive()

    def _exit_recovery_active(self) -> bool:
        return self._exit_recovery_thread is not None and self._exit_recovery_thread.is_alive()

    def _hedge_protection_active(self) -> bool:
        return (
            self._hedge_protection_thread is not None
            and self._hedge_protection_thread.is_alive()
            and self._position_has_qty_mismatch()
        )

    def _cycle_recovery_active(self) -> bool:
        return self._entry_recovery_active() or self._exit_recovery_active()

    def _set_entry_growth_limited(self, *, reason: str) -> None:
        self._entry_growth_limited = True
        self._entry_growth_limit_reason = reason
        self._entry_growth_limit_notional_usdt = self._current_position_notional_snapshot_usdt()
        self._entry_growth_limit_qty = self._current_hedged_position_qty()
        self._entry_growth_limit_pending = False
        self._entry_growth_limit_pending_reason = None
        self.state.metrics["entry_growth_limited"] = True
        self.state.metrics["entry_growth_limit_reason"] = reason
        self.state.metrics["entry_growth_limit_notional_usdt"] = self._format_order_size(self._entry_growth_limit_notional_usdt)
        self.state.metrics["entry_growth_limit_qty"] = self._format_order_size(self._entry_growth_limit_qty)

    def _clear_entry_growth_limited(self) -> None:
        self._entry_growth_limited = False
        self._entry_growth_limit_reason = None
        self._entry_growth_limit_notional_usdt = None
        self._entry_growth_limit_qty = None
        self._entry_growth_limit_pending = False
        self._entry_growth_limit_pending_reason = None
        self.state.metrics["entry_growth_limited"] = False
        self.state.metrics["entry_growth_limit_reason"] = None
        self.state.metrics["entry_growth_limit_notional_usdt"] = None
        self.state.metrics["entry_growth_limit_qty"] = None

    def _rebind_restored_position_to_current_task(self, *, reason: str) -> None:
        if self.position is not None:
            return
        had_growth_limit = bool(self._entry_growth_limited or self._entry_growth_limit_pending)
        if had_growth_limit:
            self.logger.info(
                "restored position rebound to current task | reason=%s | previous_growth_limit_reason=%s",
                reason,
                self._entry_growth_limit_reason or self._entry_growth_limit_pending_reason,
            )
        self._clear_entry_growth_limited()

    def _mark_entry_growth_limit_pending(self, *, reason: str) -> None:
        self._entry_growth_limit_pending = True
        self._entry_growth_limit_pending_reason = reason

    def _current_position_notional_snapshot_usdt(self) -> Decimal:
        left_notional = self._filled_leg_notional_usdt("left")
        right_notional = self._filled_leg_notional_usdt("right")
        return min(left_notional, right_notional) if left_notional > Decimal("0") and right_notional > Decimal("0") else max(left_notional, right_notional)

    def _current_hedged_position_qty(self) -> Decimal:
        left_qty = self.left_leg_state.filled_qty
        right_qty = self.right_leg_state.filled_qty
        if left_qty <= Decimal("0") and right_qty <= Decimal("0"):
            return Decimal("0")
        if left_qty <= Decimal("0") or right_qty <= Decimal("0"):
            return Decimal("0")
        return min(left_qty, right_qty)

    def _position_is_hedged(self) -> bool:
        return self.left_leg_state.filled_qty > Decimal("0") and self.left_leg_state.filled_qty == self.right_leg_state.filled_qty

    def _current_entry_attempt_hit_margin_limit(self) -> bool:
        if self._entry_growth_limit_pending:
            return True
        if self.active_entry_cycle is None:
            return False
        errors = [
            self.state.last_error,
            self.left_leg_state.last_error,
            self.right_leg_state.last_error,
            self.active_entry_cycle.last_error,
            self._entry_growth_limit_pending_reason,
        ]
        return any(self._is_margin_limit_error(error_text) for error_text in errors)

    def _preserve_hedged_position_after_entry_limit(self, *, reason: str, last_result: str) -> bool:
        if self._has_live_leg_orders():
            return False
        if not self._position_is_hedged():
            return False
        self.state.metrics["last_result"] = last_result
        self.entry_recovery_plan = None
        self._clear_recovery_status(context="ENTRY_CYCLE")
        self._set_entry_growth_limited(reason=reason)
        self._sync_position_from_legs()
        self._finalize_entry_cycle(
            state=StrategyCycleState.ABORT,
            error=reason,
            freeze_pipeline=False,
        )
        self._settle_dual_execution_state(reason="ENTRY_GROWTH_LIMITED")
        self._set_strategy_state(StrategyState.IN_POSITION)
        self.logger.warning(
            "entry growth limited by margin | current_position_notional_usdt=%s | reason=%s",
            self._format_order_size(self._entry_growth_limit_notional_usdt),
            self._entry_growth_limit_reason,
        )
        self._publish_state()
        return True

    def _start_entry_cycle(self, decision: EntryDecision, *, prefetch: bool = False) -> StrategyCycle:
        if prefetch and self.prefetch_entry_cycle is not None:
            raise RuntimeError("Entry prefetch cycle already exists")
        planned_size = decision.planned_size
        self._cycle_seq += 1
        self._mark_cycle_activity()
        cycle = StrategyCycle(
            cycle_id=self._cycle_seq,
            cycle_type=StrategyCycleType.ENTRY,
            state=StrategyCycleState.PLANNED,
            direction=decision.direction,
            edge_name=decision.edge_name,
            edge_value=decision.edge,
            target_notional_usdt=planned_size.get("cycle_notional_usdt", Decimal("0")),
            left_start_qty=self.left_leg_state.filled_qty,
            right_start_qty=self.right_leg_state.filled_qty,
            left_target_qty=planned_size.get("left_qty", Decimal("0")),
            right_target_qty=planned_size.get("right_qty", Decimal("0")),
            started_at=int(time.time() * 1000),
            left_side=decision.left_action,
            right_side=decision.right_action,
        )
        if prefetch:
            self.prefetch_entry_cycle = cycle
        else:
            self._bump_runtime_owner_epoch(reason=f"ENTRY_CYCLE_START:{cycle.cycle_id}")
            self.active_entry_cycle = cycle
            self._entry_settle_timeout_handled_cycle_id = None
        self._sync_active_entry_cycle_metrics()
        self.logger.info(
            "entry cycle created | cycle_id=%s | slot=%s | state=%s | cycle_notional_usdt=%s | left_target_qty=%s | right_target_qty=%s",
            cycle.cycle_id,
            "prefetch" if prefetch else "active",
            cycle.state.value,
            self._format_order_size(cycle.target_notional_usdt),
            self._format_order_size(cycle.left_target_qty),
            self._format_order_size(cycle.right_target_qty),
        )
        return cycle

    def _promote_prefetch_entry_cycle(self) -> bool:
        if self.active_entry_cycle is not None or self.prefetch_entry_cycle is None:
            return False
        # Do not rotate owner epoch on prefetch promotion:
        # prefetch orders may already be in-flight under current epoch.
        self.active_entry_cycle = self.prefetch_entry_cycle
        self.prefetch_entry_cycle = None
        self._sync_active_entry_cycle_metrics()
        self.logger.info(
            "entry prefetch promoted | cycle_id=%s",
            self.active_entry_cycle.cycle_id,
        )
        return True

    def _set_entry_cycle_state(self, new_state: StrategyCycleState) -> None:
        if self.active_entry_cycle is None or self.active_entry_cycle.state is new_state:
            return
        previous = self.active_entry_cycle.state
        self.active_entry_cycle.state = new_state
        self._sync_active_entry_cycle_metrics()
        self.logger.info(
            "entry cycle state transition | cycle_id=%s | from=%s | to=%s",
            self.active_entry_cycle.cycle_id,
            previous.value,
            new_state.value,
        )

    def _finalize_entry_cycle(
        self,
        *,
        state: StrategyCycleState,
        error: str | None = None,
        freeze_pipeline: bool = True,
    ) -> None:
        if self.active_entry_cycle is None:
            return
        self._mark_cycle_activity()
        self.active_entry_cycle.state = state
        self.active_entry_cycle.completed_at = int(time.time() * 1000)
        finalized_at_ms = int(self.active_entry_cycle.completed_at or 0)
        self.active_entry_cycle.last_error = error
        finalized_cycle_id = self.active_entry_cycle.cycle_id
        dispatch_ts_ms = self._entry_cycle_dispatch_ts_by_id.pop(int(finalized_cycle_id), None)
        self.last_entry_cycle = self.active_entry_cycle
        self.state.metrics["last_entry_cycle_result"] = state.value
        self.logger.info(
            "entry cycle finalized | cycle_id=%s | state=%s | left_filled_qty=%s | right_filled_qty=%s | error=%s",
            self.active_entry_cycle.cycle_id,
            state.value,
            self._format_order_size(self.active_entry_cycle.left_filled_qty),
            self._format_order_size(self.active_entry_cycle.right_filled_qty),
            error,
        )
        self.logger.info(
            "cycle finalize timing | phase=%s | cycle_id=%s | finalized_ts_ms=%s | dispatch_to_finalize_ms=%s",
            "entry",
            finalized_cycle_id,
            finalized_at_ms or None,
            (finalized_at_ms - int(dispatch_ts_ms)) if dispatch_ts_ms is not None and finalized_at_ms > 0 else None,
        )
        if state is StrategyCycleState.ABORT and freeze_pipeline:
            self._entry_pipeline_freeze(reason=error or "ENTRY_CYCLE_ABORT")
        if state is not StrategyCycleState.COMMITTED:
            self._entry_cycle_success_streak = 0
            self.state.metrics["entry_cycle_growth_streak"] = 0
        self.active_entry_cycle = None
        self._entry_settle_timeout_handled_cycle_id = None
        self._drop_entry_cycle_order_keys(cycle_id=finalized_cycle_id)
        if self._entry_pipeline_overlap_enabled():
            self._promote_prefetch_entry_cycle()
        self._sync_active_entry_cycle_metrics()
        self._schedule_post_cycle_hedge_check()

    def _commit_entry_cycle(self) -> None:
        if self.active_entry_cycle is None:
            return
        self.active_entry_cycle.state = StrategyCycleState.COMMITTED
        self.active_entry_cycle.completed_at = int(time.time() * 1000)
        committed_at_ms = int(self.active_entry_cycle.completed_at or 0)
        committed_cycle_id = self.active_entry_cycle.cycle_id
        dispatch_ts_ms = self._entry_cycle_dispatch_ts_by_id.pop(int(committed_cycle_id), None)
        prev_commit_ts_ms = self._last_entry_cycle_commit_ts_ms
        self.last_entry_cycle = self.active_entry_cycle
        self.state.metrics["last_entry_cycle_result"] = StrategyCycleState.COMMITTED.value
        self.logger.info(
            "entry cycle committed | cycle_id=%s | left_filled_qty=%s | right_filled_qty=%s",
            self.active_entry_cycle.cycle_id,
            self._format_order_size(self.active_entry_cycle.left_filled_qty),
            self._format_order_size(self.active_entry_cycle.right_filled_qty),
        )
        self.logger.info(
            "cycle commit timing | phase=%s | cycle_id=%s | committed_ts_ms=%s | dispatch_to_commit_ms=%s | since_prev_commit_ms=%s",
            "entry",
            committed_cycle_id,
            committed_at_ms or None,
            (committed_at_ms - int(dispatch_ts_ms)) if dispatch_ts_ms is not None and committed_at_ms > 0 else None,
            (committed_at_ms - int(prev_commit_ts_ms)) if prev_commit_ts_ms is not None and committed_at_ms > 0 else None,
        )
        self._last_entry_cycle_commit_ts_ms = committed_at_ms if committed_at_ms > 0 else self._last_entry_cycle_commit_ts_ms
        self._mark_cycle_activity()
        self._entry_cycle_success_streak += 1
        self.state.metrics["entry_cycle_growth_streak"] = int(self._entry_cycle_success_streak)
        self.active_entry_cycle = None
        self._entry_settle_timeout_handled_cycle_id = None
        self._drop_entry_cycle_order_keys(cycle_id=committed_cycle_id)
        if self._entry_pipeline_overlap_enabled():
            self._promote_prefetch_entry_cycle()
        self._entry_recovery_started_ms = None
        self._entry_recovery_thread = None
        self.entry_recovery_plan = None
        self._clear_recovery_status(context="ENTRY_CYCLE")
        self._sync_active_entry_cycle_metrics()
        self._schedule_post_cycle_hedge_check()

    def _sync_active_entry_cycle_from_legs(self) -> None:
        if self.active_entry_cycle is None and self.prefetch_entry_cycle is None:
            return
        for cycle in (self.active_entry_cycle, self.prefetch_entry_cycle):
            if cycle is None:
                continue
            cycle.left_filled_qty = self._entry_cycle_leg_filled_qty(cycle=cycle, leg_name="left")
            cycle.right_filled_qty = self._entry_cycle_leg_filled_qty(cycle=cycle, leg_name="right")
            if cycle is self.active_entry_cycle and cycle.state is StrategyCycleState.SUBMITTING and (
                cycle.left_filled_qty > Decimal("0") or cycle.right_filled_qty > Decimal("0")
            ):
                self._set_entry_cycle_state(StrategyCycleState.ACTIVE)
                return
        self._sync_active_entry_cycle_metrics()
        self._sync_active_exit_cycle_from_legs()

    def _sync_active_entry_cycle_metrics(self) -> None:
        cycle = self.active_entry_cycle
        self.state.metrics["active_entry_cycle_id"] = cycle.cycle_id if cycle is not None else None
        self.state.metrics["active_entry_cycle_state"] = cycle.state.value if cycle is not None else None
        self.state.metrics["active_entry_cycle_notional_usdt"] = self._format_order_size(cycle.target_notional_usdt) if cycle is not None else None
        self.state.metrics["active_entry_cycle_left_target_qty"] = self._format_order_size(cycle.left_target_qty) if cycle is not None else None
        self.state.metrics["active_entry_cycle_right_target_qty"] = self._format_order_size(cycle.right_target_qty) if cycle is not None else None
        self.state.metrics["active_entry_cycle_left_filled_qty"] = self._format_order_size(cycle.left_filled_qty) if cycle is not None else None
        self.state.metrics["active_entry_cycle_right_filled_qty"] = self._format_order_size(cycle.right_filled_qty) if cycle is not None else None
        self.state.metrics["prefetch_entry_cycle_id"] = self.prefetch_entry_cycle.cycle_id if self.prefetch_entry_cycle is not None else None
        self.state.metrics["prefetch_entry_cycle_state"] = self.prefetch_entry_cycle.state.value if self.prefetch_entry_cycle is not None else None
        self._enforce_entry_pipeline_inflight_invariant()

    def _start_exit_cycle(self, *, direction: str, edge_name: str | None, edge_value: Decimal | None, left_side: str, right_side: str, left_qty: Decimal, right_qty: Decimal, prefetch: bool = False) -> StrategyCycle:
        started_at_ms = int(time.time() * 1000)
        self._cycle_seq += 1
        self._mark_cycle_activity()
        cycle = StrategyCycle(
            cycle_id=self._cycle_seq,
            cycle_type=StrategyCycleType.EXIT,
            state=StrategyCycleState.PLANNED,
            direction=direction,
            edge_name=edge_name,
            edge_value=edge_value,
            target_notional_usdt=self._exit_cycle_notional_usdt(),
            left_start_qty=self.left_leg_state.filled_qty,
            right_start_qty=self.right_leg_state.filled_qty,
            left_target_qty=left_qty,
            right_target_qty=right_qty,
            started_at=started_at_ms,
            left_side=left_side,
            right_side=right_side,
        )
        if prefetch:
            self.prefetch_exit_cycle = cycle
        else:
            cycle.exit_grace_deadline_ts = started_at_ms + self.EXIT_GRACE_WINDOW_MS
            self._bump_runtime_owner_epoch(reason=f"EXIT_CYCLE_START:{cycle.cycle_id}")
            self.active_exit_cycle = cycle
        self._sync_active_exit_cycle_metrics()
        self.logger.info(
            "exit cycle created | cycle_id=%s | slot=%s | state=%s | left_target_qty=%s | right_target_qty=%s",
            cycle.cycle_id,
            "prefetch" if prefetch else "active",
            cycle.state.value,
            self._format_order_size(cycle.left_target_qty),
            self._format_order_size(cycle.right_target_qty),
        )
        if not prefetch and self.active_exit_cycle is not None:
            self.logger.info(
                "EXIT_GRACE_WINDOW_STARTED | cycle_id=%s | grace_window_ms=%s | deadline_ts=%s",
                self.active_exit_cycle.cycle_id,
                self.EXIT_GRACE_WINDOW_MS,
                self.active_exit_cycle.exit_grace_deadline_ts,
            )
            self._schedule_exit_grace_reevaluation(
                cycle_id=self.active_exit_cycle.cycle_id,
                deadline_ts=int(self.active_exit_cycle.exit_grace_deadline_ts or 0),
            )
        return cycle

    def _promote_prefetch_exit_cycle(self) -> bool:
        if self.active_exit_cycle is not None or self.prefetch_exit_cycle is None:
            return False
        # Do not rotate owner epoch on prefetch promotion:
        # prefetch orders may already be in-flight under current epoch.
        self.active_exit_cycle = self.prefetch_exit_cycle
        self.prefetch_exit_cycle = None
        self.active_exit_cycle.exit_grace_deadline_ts = int(time.time() * 1000) + self.EXIT_GRACE_WINDOW_MS
        self._sync_active_exit_cycle_metrics()
        self.logger.info(
            "exit prefetch promoted | cycle_id=%s",
            self.active_exit_cycle.cycle_id,
        )
        self.logger.info(
            "EXIT_GRACE_WINDOW_STARTED | cycle_id=%s | grace_window_ms=%s | deadline_ts=%s",
            self.active_exit_cycle.cycle_id,
            self.EXIT_GRACE_WINDOW_MS,
            self.active_exit_cycle.exit_grace_deadline_ts,
        )
        self._schedule_exit_grace_reevaluation(
            cycle_id=self.active_exit_cycle.cycle_id,
            deadline_ts=int(self.active_exit_cycle.exit_grace_deadline_ts or 0),
        )
        return True

    def _schedule_exit_grace_reevaluation(self, *, cycle_id: int, deadline_ts: int) -> None:
        if deadline_ts <= 0:
            return

        def _run() -> None:
            remaining_ms = max(0, deadline_ts - int(time.time() * 1000))
            if remaining_ms > 0:
                time.sleep(remaining_ms / 1000.0)
            if self.state.status != "running":
                return
            if self.active_exit_cycle is None or self.active_exit_cycle.cycle_id != cycle_id:
                return
            self._reevaluate_active_spread_execution()

        threading.Thread(
            target=_run,
            name=f"{self.task.worker_id}-exit-grace-{cycle_id}",
            daemon=True,
        ).start()

    def _set_exit_cycle_state(self, new_state: StrategyCycleState) -> None:
        if self.active_exit_cycle is None or self.active_exit_cycle.state is new_state:
            return
        previous = self.active_exit_cycle.state
        self.active_exit_cycle.state = new_state
        self._sync_active_exit_cycle_metrics()
        self.logger.info(
            "exit cycle state transition | cycle_id=%s | from=%s | to=%s",
            self.active_exit_cycle.cycle_id,
            previous.value,
            new_state.value,
        )

    def _finalize_exit_cycle(self, *, state: StrategyCycleState, error: str | None = None) -> None:
        if self.active_exit_cycle is None:
            return
        self._mark_cycle_activity()
        self.active_exit_cycle.state = state
        self.active_exit_cycle.completed_at = int(time.time() * 1000)
        finalized_at_ms = int(self.active_exit_cycle.completed_at or 0)
        finalized_cycle_id = self.active_exit_cycle.cycle_id
        dispatch_ts_ms = self._exit_cycle_dispatch_ts_by_id.pop(int(finalized_cycle_id), None)
        self.active_exit_cycle.last_error = error
        self.last_exit_cycle = self.active_exit_cycle
        self.state.metrics["last_exit_cycle_result"] = state.value
        self.logger.info(
            "exit cycle finalized | cycle_id=%s | state=%s | left_closed_qty=%s | right_closed_qty=%s | error=%s",
            self.active_exit_cycle.cycle_id,
            state.value,
            self._format_order_size(self.active_exit_cycle.left_filled_qty),
            self._format_order_size(self.active_exit_cycle.right_filled_qty),
            error,
        )
        self.logger.info(
            "cycle finalize timing | phase=%s | cycle_id=%s | finalized_ts_ms=%s | dispatch_to_finalize_ms=%s",
            "exit",
            finalized_cycle_id,
            finalized_at_ms or None,
            (finalized_at_ms - int(dispatch_ts_ms)) if dispatch_ts_ms is not None and finalized_at_ms > 0 else None,
        )
        if state is not StrategyCycleState.COMMITTED:
            self._exit_cycle_success_streak = 0
            self.state.metrics["exit_cycle_growth_streak"] = 0
        self.active_exit_cycle = None
        self._drop_exit_cycle_order_keys(cycle_id=finalized_cycle_id)
        if self._entry_pipeline_overlap_enabled():
            self._promote_prefetch_exit_cycle()
        self._sync_active_exit_cycle_metrics()
        self._schedule_post_cycle_hedge_check()

    def _commit_exit_cycle(self) -> None:
        if self.active_exit_cycle is None:
            return
        self._mark_cycle_activity()
        self.active_exit_cycle.state = StrategyCycleState.COMMITTED
        self.active_exit_cycle.completed_at = int(time.time() * 1000)
        committed_at_ms = int(self.active_exit_cycle.completed_at or 0)
        committed_cycle_id = self.active_exit_cycle.cycle_id
        dispatch_ts_ms = self._exit_cycle_dispatch_ts_by_id.pop(int(committed_cycle_id), None)
        prev_commit_ts_ms = self._last_exit_cycle_commit_ts_ms
        self.last_exit_cycle = self.active_exit_cycle
        self.state.metrics["last_exit_cycle_result"] = StrategyCycleState.COMMITTED.value
        self.logger.info(
            "exit cycle committed | cycle_id=%s | left_closed_qty=%s | right_closed_qty=%s",
            self.active_exit_cycle.cycle_id,
            self._format_order_size(self.active_exit_cycle.left_filled_qty),
            self._format_order_size(self.active_exit_cycle.right_filled_qty),
        )
        self.logger.info(
            "cycle commit timing | phase=%s | cycle_id=%s | committed_ts_ms=%s | dispatch_to_commit_ms=%s | since_prev_commit_ms=%s",
            "exit",
            committed_cycle_id,
            committed_at_ms or None,
            (committed_at_ms - int(dispatch_ts_ms)) if dispatch_ts_ms is not None and committed_at_ms > 0 else None,
            (committed_at_ms - int(prev_commit_ts_ms)) if prev_commit_ts_ms is not None and committed_at_ms > 0 else None,
        )
        self._last_exit_cycle_commit_ts_ms = committed_at_ms if committed_at_ms > 0 else self._last_exit_cycle_commit_ts_ms
        self._exit_cycle_success_streak += 1
        self.state.metrics["exit_cycle_growth_streak"] = int(self._exit_cycle_success_streak)
        self.active_exit_cycle = None
        self._drop_exit_cycle_order_keys(cycle_id=committed_cycle_id)
        if self._entry_pipeline_overlap_enabled():
            self._promote_prefetch_exit_cycle()
        self._sync_active_exit_cycle_metrics()
        self._schedule_post_cycle_hedge_check()

    def _sync_active_exit_cycle_from_legs(self) -> None:
        if self.active_exit_cycle is None:
            return
        self.active_exit_cycle.left_filled_qty = max(Decimal("0"), self.active_exit_cycle.left_start_qty - self.left_leg_state.filled_qty)
        self.active_exit_cycle.right_filled_qty = max(Decimal("0"), self.active_exit_cycle.right_start_qty - self.right_leg_state.filled_qty)
        if self.active_exit_cycle.state is StrategyCycleState.SUBMITTING and (self.active_exit_cycle.left_filled_qty > Decimal("0") or self.active_exit_cycle.right_filled_qty > Decimal("0")):
            self._set_exit_cycle_state(StrategyCycleState.ACTIVE)
            return
        self._sync_active_exit_cycle_metrics()

    def _sync_active_exit_cycle_metrics(self) -> None:
        cycle = self.active_exit_cycle
        self.state.metrics["active_exit_cycle_id"] = cycle.cycle_id if cycle is not None else None
        self.state.metrics["active_exit_cycle_state"] = cycle.state.value if cycle is not None else None
        self.state.metrics["prefetch_exit_cycle_id"] = self.prefetch_exit_cycle.cycle_id if self.prefetch_exit_cycle is not None else None
        self.state.metrics["prefetch_exit_cycle_state"] = self.prefetch_exit_cycle.state.value if self.prefetch_exit_cycle is not None else None
        self.state.metrics["active_exit_cycle_notional_usdt"] = self._format_order_size(cycle.target_notional_usdt) if cycle is not None else None
        self.state.metrics["active_exit_cycle_left_target_qty"] = self._format_order_size(cycle.left_target_qty) if cycle is not None else None
        self.state.metrics["active_exit_cycle_right_target_qty"] = self._format_order_size(cycle.right_target_qty) if cycle is not None else None
        self.state.metrics["active_exit_cycle_left_filled_qty"] = self._format_order_size(cycle.left_filled_qty) if cycle is not None else None
        self.state.metrics["active_exit_cycle_right_filled_qty"] = self._format_order_size(cycle.right_filled_qty) if cycle is not None else None
        self.state.metrics["exit_grace_deadline_ts"] = cycle.exit_grace_deadline_ts if cycle is not None else None
        self.state.metrics["exit_tail_resync_in_progress"] = bool(cycle.tail_resync_in_progress) if cycle is not None else False
        self.state.metrics["exit_tail_resync_attempts"] = int(cycle.tail_resync_attempts) if cycle is not None else 0
        self.state.metrics["exit_tail_reduce_only_seen"] = bool(cycle.tail_reduce_only_seen) if cycle is not None else False
