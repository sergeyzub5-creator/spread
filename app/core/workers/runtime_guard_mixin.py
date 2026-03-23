from __future__ import annotations

import threading
import time
from decimal import Decimal

from app.core.models.workers import RecoveryPlan, StrategyPosition, StrategyState


class WorkerRuntimeGuardMixin:
    def _bump_runtime_owner_epoch(self, *, reason: str) -> int:
        next_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0) + 1
        self._runtime_owner_epoch = next_epoch
        self.state.metrics["runtime_owner_epoch"] = next_epoch
        self.logger.info("runtime owner epoch advanced | epoch=%s | reason=%s", next_epoch, reason)
        return next_epoch

    def _hedge_must_yield_to_cycle_owner(self) -> bool:
        return self.active_entry_cycle is not None or self.prefetch_entry_cycle is not None or self.active_exit_cycle is not None or self._cycle_recovery_active()

    def _runtime_reconcile_active(self) -> bool:
        return self._runtime_reconcile_thread is not None and self._runtime_reconcile_thread.is_alive()

    def _start_runtime_watchdog(self) -> None:
        if not self._is_spread_entry_runtime:
            return
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop_event.clear()
        self._watchdog_thread = threading.Thread(
            target=self._run_runtime_watchdog,
            name=f"{self.task.worker_id}-runtime-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _run_runtime_watchdog(self) -> None:
        wait_seconds = max(0.1, self.WATCHDOG_INTERVAL_MS / 1000.0)
        while not self._watchdog_stop_event.wait(wait_seconds):
            try:
                self._watchdog_tick()
            except Exception as exc:
                self.logger.warning("runtime watchdog tick failed | error=%s", exc)

    def _watchdog_tick(self) -> None:
        if not self._is_spread_entry_runtime or self.state.status != "running":
            return
        should_reconcile_reason: str | None = None
        now_ms = int(time.time() * 1000)
        should_publish_state = False
        with self._state_lock:
            # ÐžÐ±Ð½Ð¾Ð²Ð»ÑÐµÐ¼ Ð¿Ñ€Ð¾Ð¸Ð·Ð²Ð¾Ð´Ð½Ñ‹Ðµ Ð¼ÐµÑ‚Ñ€Ð¸ÐºÐ¸ (Ð²ÐºÐ»ÑŽÑ‡Ð°Ñ Ð²Ð¾Ð·Ñ€Ð°ÑÑ‚ ÐºÐ¾Ñ‚Ð¸Ñ€Ð¾Ð²Ð¾Ðº), Ð´Ð°Ð¶Ðµ ÐµÑÐ»Ð¸ Ð´Ð°Ð²Ð½Ð¾ Ð½Ðµ Ð±Ñ‹Ð»Ð¾ ÑÐ¾Ð±Ñ‹Ñ‚Ð¸Ð¹,
            # Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð´ÐµÑ‚ÐµÐºÑ‚Ð¾Ñ€ Ð´ÐµÐ³Ñ€Ð°Ð´Ð°Ñ†Ð¸Ð¸ ÑÐ²ÑÐ·Ð¸ Ð¾Ð¿Ð¸Ñ€Ð°Ð»ÑÑ Ð½Ð° Ñ€ÐµÐ°Ð»ÑŒÐ½Ð¾Ðµ Ð²Ñ€ÐµÐ¼Ñ, Ð° Ð½Ðµ Ð½Ð° Â«Ð·Ð°Ð¼Ñ‘Ñ€Ð·ÑˆÐ¸ÐµÂ» Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ.
            self._refresh_derived_metrics()
            self._maybe_reset_cycle_growth_on_idle(now_ms=now_ms)
            connectivity_degraded = self._connectivity_degraded()
            owner_context = self._current_owner_context()
            owner_stale_reason = self._stale_owner_reason(owner_context=owner_context)
            previous_health = self._runtime_health_mode
            previous_owner = self.state.metrics.get("runtime_owner")
            if connectivity_degraded:
                self._runtime_health_mode = "DEGRADED_CONNECTIVITY"
            elif self._runtime_reconcile_active():
                self._runtime_health_mode = "RECONCILING"
            else:
                self._runtime_health_mode = "HEALTHY"
            self.state.metrics["runtime_health"] = self._runtime_health_mode
            self.state.metrics["runtime_owner"] = owner_context
            should_publish_state = (
                previous_health != self._runtime_health_mode
                or previous_owner != owner_context
            )
            if previous_health == "DEGRADED_CONNECTIVITY" and self._runtime_health_mode == "HEALTHY":
                # Skip no-op reconcile when runtime is already clean/flat.
                no_live_orders = not self._has_live_leg_orders()
                flat_idle_without_owner = (
                    no_live_orders
                    and owner_context is None
                    and self.strategy_state is StrategyState.IDLE
                    and self.left_leg_state.actual_position_qty <= Decimal("0")
                    and self.right_leg_state.actual_position_qty <= Decimal("0")
                )
                if not flat_idle_without_owner:
                    should_reconcile_reason = "CONNECTIVITY_RESTORED"
            elif owner_stale_reason is not None:
                should_reconcile_reason = owner_stale_reason
            else:
                # Ð›Ñ‘Ð³ÐºÐ¸Ðµ Ð¸Ð½Ð²Ð°Ñ€Ð¸Ð°Ð½Ñ‚Ñ‹: ÐµÑÐ»Ð¸ ÑÑ‚Ñ€Ð°Ñ‚ÐµÐ³Ð¸Ñ Ñ„Ð¾Ñ€Ð¼Ð°Ð»ÑŒÐ½Ð¾ Â«ÑÐ²Ð¾Ð±Ð¾Ð´Ð½Ð°Â», Ð½Ð¾ Ð¾ÑÑ‚Ð°Ð»Ð¸ÑÑŒ Ð°ÐºÑ‚Ð¸Ð²Ð½Ñ‹Ðµ Ñ‡Ð°Ð½ÐºÐ¸/Ð¿Ð¾Ñ‚Ð¾ÐºÐ¸,
                # Ð·Ð°Ð¿ÑƒÑÐºÐ°ÐµÐ¼ Ð¿Ð¾Ð»Ð½Ñ‹Ð¹ reconcile, Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð²Ñ‹Ñ‡Ð¸ÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð¾Ð¼Ð±Ð¸â€‘ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ñ.
                no_live_orders = not self._has_live_leg_orders()
                both_legs_flat = (
                    self.left_leg_state.actual_position_qty <= Decimal("0")
                    and self.right_leg_state.actual_position_qty <= Decimal("0")
                )
                if self.strategy_state is StrategyState.FAILED and no_live_orders and both_legs_flat:
                    should_reconcile_reason = "INVARIANT_FAILED_BUT_FLAT"
                elif self.strategy_state in {StrategyState.IDLE, StrategyState.IN_POSITION} and no_live_orders:
                    if (
                        self.active_entry_cycle is not None
                        or self.active_exit_cycle is not None
                        or self._cycle_recovery_active()
                    ):
                        owner_started_at_ms = 0
                        if self.active_entry_cycle is not None:
                            owner_started_at_ms = int(self.active_entry_cycle.started_at or 0)
                        elif self.active_exit_cycle is not None:
                            owner_started_at_ms = int(self.active_exit_cycle.started_at or 0)
                        owner_stale = owner_started_at_ms > 0 and (now_ms - owner_started_at_ms) >= self.OWNER_STALE_TIMEOUT_MS
                        if owner_stale:
                            should_reconcile_reason = "INVARIANT_IDLE_WITH_ACTIVE_CYCLES"
                elif (
                    no_live_orders
                    and owner_context is None
                    and (now_ms - int(self._last_runtime_reconcile_started_ms or 0)) >= self.FULL_STATE_RECONCILE_INTERVAL_MS
                ):
                    should_reconcile_reason = "PERIODIC_FULL_STATE"
        if should_publish_state:
            self._publish_state(force=True)
        if should_reconcile_reason is not None:
            self._maybe_start_runtime_reconcile(reason=should_reconcile_reason)

    def _connectivity_degraded(self) -> bool:
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        if left_quote is None or right_quote is None:
            return False
        now_ms = int(time.time() * 1000)
        left_age = max(0, now_ms - int(left_quote.ts_local))
        right_age = max(0, now_ms - int(right_quote.ts_local))
        return left_age >= self.CONNECTIVITY_DEGRADED_QUOTE_AGE_MS or right_age >= self.CONNECTIVITY_DEGRADED_QUOTE_AGE_MS

    def _current_owner_context(self) -> str | None:
        if self.active_exit_cycle is not None:
            return f"EXIT_CYCLE#{self.active_exit_cycle.cycle_id}"
        if self.active_entry_cycle is not None:
            return f"ENTRY_CYCLE#{self.active_entry_cycle.cycle_id}"
        if self.prefetch_entry_cycle is not None:
            return f"ENTRY_PREFETCH_CYCLE#{self.prefetch_entry_cycle.cycle_id}"
        if self._exit_recovery_active():
            return "EXIT_RECOVERY"
        if self._entry_recovery_active():
            return "ENTRY_RECOVERY"
        if self._hedge_protection_active():
            return "HEDGE_PROTECTION"
        if self._live_orders_started_at_ms() is not None:
            return "LIVE_ORDERS"
        return None

    def _stale_owner_reason(self, *, owner_context: str | None) -> str | None:
        if owner_context is None:
            return None
        now_ms = int(time.time() * 1000)
        started_at_ms: int | None = None
        if owner_context == "EXIT_RECOVERY":
            started_at_ms = self._exit_recovery_started_ms
        elif owner_context == "ENTRY_RECOVERY":
            started_at_ms = self._entry_recovery_started_ms
        elif owner_context == "HEDGE_PROTECTION":
            started_at_ms = self._hedge_protection_started_ms
        elif owner_context.startswith("ENTRY_CYCLE#") or owner_context.startswith("ENTRY_PREFETCH#"):
            cycle = self.active_entry_cycle if owner_context.startswith("ENTRY_CYCLE#") else self.prefetch_entry_cycle
            started_at_ms = int(cycle.started_at or 0) if cycle is not None else None
        elif owner_context.startswith("EXIT_CYCLE#"):
            cycle = self.active_exit_cycle
            started_at_ms = int(cycle.started_at or 0) if cycle is not None else None
        elif owner_context == "LIVE_ORDERS":
            started_at_ms = self._live_orders_started_at_ms()
        if started_at_ms is None or started_at_ms <= 0:
            return None
        if (now_ms - started_at_ms) < self.OWNER_STALE_TIMEOUT_MS:
            return None
        if owner_context == "LIVE_ORDERS":
            if self._all_live_leg_orders_stale():
                return "OWNER_STALE:LIVE_ORDERS"
            return None
        if owner_context.startswith("ENTRY_CYCLE#") or owner_context.startswith("ENTRY_PREFETCH#") or owner_context.startswith("EXIT_CYCLE#"):
            # Do not hand over active entry/exit cycles to watchdog takeover.
            # Ð˜Ñ… lifecycle Ð´Ð¾Ð»Ð¶ÐµÐ½ Ð·Ð°Ð²ÐµÑ€ÑˆÐ°Ñ‚ÑŒÑÑ Ñ‚Ð¾Ð»ÑŒÐºÐ¾ ÑˆÑ‚Ð°Ñ‚Ð½Ñ‹Ð¼ cycle/recovery-Ð¿Ð¾Ñ‚Ð¾ÐºÐ¾Ð¼.
            return None
        if self._has_live_leg_orders():
            return None
        return f"OWNER_STALE:{owner_context}"

    def _live_orders_started_at_ms(self) -> int | None:
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        terminal_statuses = {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
        iter_attempts = getattr(self, "_iter_leg_attempts", None)
        attempt_started_times: list[int] = []
        if callable(iter_attempts):
            for leg_name in ("left", "right"):
                for attempt in iter_attempts(leg_name=leg_name):
                    if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                        continue
                    if bool(getattr(attempt, "terminal", False)):
                        continue
                    status = str(getattr(attempt, "status", "") or "").strip().upper()
                    if status in terminal_statuses:
                        continue
                    base_ts = int(getattr(attempt, "request_sent_at_ms", 0) or 0) or int(getattr(attempt, "submitted_at_ms", 0) or 0)
                    if base_ts > 0:
                        attempt_started_times.append(base_ts)
        if attempt_started_times:
            return min(attempt_started_times)
        submitted_times = [
            int(clock.get("submitted_at_ms") or 0)
            for clock in self._dual_order_clocks.values()
            if int(clock.get("submitted_at_ms") or 0) > 0
        ]
        return min(submitted_times) if submitted_times else None

    def _maybe_start_runtime_reconcile(self, *, reason: str) -> None:
        if not self._is_spread_entry_runtime or self.state.status != "running":
            return
        now_ms = int(time.time() * 1000)
        with self._state_lock:
            if not self._runtime_reconcile_can_preempt_active_owner(reason=reason):
                return
            if self._runtime_reconcile_active():
                return
            if (now_ms - self._last_runtime_reconcile_started_ms) < self.RUNTIME_RECONCILE_DEBOUNCE_MS:
                return
            self._last_runtime_reconcile_started_ms = now_ms
            self._last_runtime_reconcile_reason = reason
            self._runtime_health_mode = "RECONCILING"
            self.state.metrics["runtime_health"] = self._runtime_health_mode
            self.state.metrics["last_reconcile_reason"] = reason
            self.state.metrics["last_reconcile_ts"] = now_ms
            self.logger.warning("runtime reconcile scheduled | reason=%s | owner=%s", reason, self._current_owner_context())
            self._publish_state(force=True)
            self._runtime_reconcile_thread = threading.Thread(
                target=self._run_runtime_reconcile,
                args=(reason,),
                name=f"{self.task.worker_id}-runtime-reconcile",
                daemon=True,
            )
            self._runtime_reconcile_thread.start()

    def _runtime_reconcile_can_preempt_active_owner(self, *, reason: str) -> bool:
        owner_context = self._current_owner_context()
        if owner_context is None:
            return True
        normalized_reason = str(reason or "").strip().upper()
        if normalized_reason == "INVARIANT_IDLE_WITH_ACTIVE_CYCLES":
            # Break watchdog deadlock: idle/in-position + active cycle owner must be preempted.
            return True
        if normalized_reason.startswith("OWNER_STALE:"):
            return True
        critical_prefixes = (
            "FULL_STATE:ENTRY_RECOVERY_ABORT",
            "FULL_STATE:EXIT_RESTORE",
            "FULL_STATE:HEDGE_PROTECTION",
        )
        if any(normalized_reason.startswith(prefix) for prefix in critical_prefixes):
            return True
        self.logger.info(
            "runtime reconcile deferred | reason=%s | owner=%s",
            reason,
            owner_context,
        )
        return False

    def _request_full_state_reconcile(self, *, reason: str) -> None:
        """Ð˜Ð´Ñ‘Ð¼ Ð² Ð¿Ð¾Ð»Ð½Ñ‹Ð¹ reconcile ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ñ (Ð¿Ð¾ Ñ„Ð°ÐºÑ‚Ñƒ Ð¿Ñ€Ð¾ÑÑ‚Ð¾ Ð¿Ð»Ð°Ð½Ð¸Ñ€ÑƒÐµÐ¼ runtime_reconcile Ñ Ð¿Ð¾Ð¼ÐµÑ‚ÐºÐ¾Ð¹ Ð¿Ñ€Ð¸Ñ‡Ð¸Ð½Ñ‹)."""
        self._maybe_start_runtime_reconcile(reason=f"FULL_STATE:{reason}")

    def _run_runtime_reconcile(self, reason: str) -> None:
        try:
            left_qty = self._resync_exchange_position_qty("left")
            right_qty = self._resync_exchange_position_qty("right")
        except Exception as exc:
            with self._state_lock:
                self._runtime_health_mode = "DEGRADED_CONNECTIVITY"
                self.state.metrics["runtime_health"] = self._runtime_health_mode
                self.state.last_error = str(exc)
                self.logger.warning("runtime reconcile failed | reason=%s | error=%s", reason, exc)
                self._publish_state(force=True)
            return
        should_start_hedge = False
        should_resume_exit = False
        should_resume_entry = False
        with self._state_lock:
            self.logger.warning(
                "runtime reconcile result | reason=%s | left_actual_position_qty=%s | right_actual_position_qty=%s",
                reason,
                self._format_order_size(left_qty),
                self._format_order_size(right_qty),
            )
            self._apply_exchange_position_resync("left", left_qty)
            self._apply_exchange_position_resync("right", right_qty)
            self._take_runtime_owner_takeover(reason=reason)
            left_actual = self.left_leg_state.actual_position_qty
            right_actual = self.right_leg_state.actual_position_qty
            if left_actual <= Decimal("0") and right_actual <= Decimal("0"):
                self._reset_position_state()
                self.left_leg_state.filled_qty = Decimal("0")
                self.right_leg_state.filled_qty = Decimal("0")
                self._refresh_leg_position_derived_fields("left", confirmed_by_exchange=False)
                self._refresh_leg_position_derived_fields("right", confirmed_by_exchange=False)
                self._settle_dual_execution_state(reason=f"RECONCILE_IDLE:{reason}", force=True)
                self._set_strategy_state(StrategyState.IDLE)
                self.state.metrics["last_result"] = "RECONCILED_IDLE"
                should_resume_entry = True
            elif left_actual == right_actual:
                self._rebind_restored_position_to_current_task(reason=f"RECONCILE_HEDGED:{reason}")
                self._sync_position_from_legs()
                self._settle_dual_execution_state(reason=f"RECONCILE_HEDGED:{reason}", force=True)
                self._set_strategy_state(StrategyState.IN_POSITION)
                self.state.metrics["last_result"] = "RECONCILED_IN_POSITION"
                should_resume_exit = True
                should_resume_entry = True
            else:
                self._reset_position_state()
                self._settle_dual_execution_state(reason=f"RECONCILE_MISMATCH:{reason}", force=True)
                self._set_recovery_status(context="RECONCILE", state="RECONCILE_MISMATCH", reason=reason)
                self._set_strategy_state(StrategyState.RECOVERY)
                self.state.metrics["last_result"] = "RECONCILE_MISMATCH"
                should_start_hedge = True
            self._runtime_health_mode = "HEALTHY"
            self.state.metrics["runtime_health"] = self._runtime_health_mode
            self.state.metrics["runtime_owner"] = self._current_owner_context()
            self._publish_state(force=True)
        if should_start_hedge:
            self._maybe_start_hedge_protection()
            return
        if should_resume_exit:
            self._evaluate_spread_exit()
        if should_resume_entry:
            self._evaluate_spread_entry()

    def _take_runtime_owner_takeover(self, *, reason: str) -> None:
        self.logger.warning("runtime owner takeover | reason=%s | owner=%s", reason, self._current_owner_context())
        self._bump_runtime_owner_epoch(reason=f"TAKEOVER:{reason}")
        self.active_entry_cycle = None
        self.prefetch_entry_cycle = None
        self.active_exit_cycle = None
        self.entry_recovery_plan = None
        self.exit_recovery_plan = None
        self.hedge_recovery_plan = None
        self._entry_recovery_thread = None
        self._exit_recovery_thread = None
        self._hedge_protection_thread = None
        self._entry_recovery_started_ms = None
        self._exit_recovery_started_ms = None
        self._hedge_protection_started_ms = None
        self._deferred_entry_chain_requested = False
        self._deferred_exit_chain_requested = False
        self._hedge_check_requested = False
        self._last_hedge_check_request_reason = None
        self._dual_order_clocks = {"left": {}, "right": {}}
        self._dual_poll_threads = {}
        self._dual_poll_attempt_ids = {"left": None, "right": None}
        self._leg_order_fill_tracker = {"left": {}, "right": {}}
        self._leg_order_position_effects = {"left": {}, "right": {}}
        self._entry_cycle_order_keys = {"left": {}, "right": {}}
        self._exit_cycle_order_keys = {"left": {}, "right": {}}
        self._order_key_aliases = {"left": {}, "right": {}}
        self._order_key_tombstones = {"left": {}, "right": {}}
        self._order_attempts = {"left": {}, "right": {}}
        self.state.metrics["left_attempt_id"] = None
        self.state.metrics["right_attempt_id"] = None
        self.state.metrics["left_attempt_owner_epoch"] = None
        self.state.metrics["right_attempt_owner_epoch"] = None
        self.state.metrics["left_active_attempts"] = 0
        self.state.metrics["right_active_attempts"] = 0
        self.state.metrics["active_attempts_total"] = 0
        self.state.metrics["left_active_attempt_ids"] = None
        self.state.metrics["right_active_attempt_ids"] = None
        self.state.metrics["left_last_terminal_attempt_id"] = None
        self.state.metrics["right_last_terminal_attempt_id"] = None
        self.state.metrics["left_rest_poll_active"] = False
        self.state.metrics["right_rest_poll_active"] = False
        self.state.metrics["active_rest_polls_total"] = 0
        self.state.metrics["left_rest_poll_last_stop_reason"] = None
        self.state.metrics["right_rest_poll_last_stop_reason"] = None
        self.state.metrics["left_rest_poll_last_stop_ts"] = None
        self.state.metrics["right_rest_poll_last_stop_ts"] = None
        self.state.metrics["left_rest_poll_last_attempt_id"] = None
        self.state.metrics["right_rest_poll_last_attempt_id"] = None
        self._sync_active_entry_cycle_metrics()
        self._sync_active_exit_cycle_metrics()
        self._clear_recovery_status(context="ENTRY_CYCLE")
        self._clear_recovery_status(context="EXIT_CYCLE")
        self._clear_recovery_status(context="HEDGE_PROTECTION")
        self._clear_recovery_status(context="RECONCILE")
        self._refresh_dual_exec_status()
        self._deferred_exit_chain_requested = False
        self.state.metrics["runtime_owner"] = None

    def _activate_recovery_intervention(self, *, context: str, state: str, reason: str | None) -> None:
        self._set_recovery_status(context=context, state=state, reason=reason)
        self._set_strategy_state(StrategyState.RECOVERY)

    def _start_entry_cycle_recovery(self, left_status: str, right_status: str) -> None:
        # Legacy cycle recovery is disabled.
        return

    def _run_entry_cycle_recovery(self, cycle_id: int) -> None:
        # Legacy cycle recovery is disabled.
        return

    def _leg_has_live_order(self, leg_name: str) -> bool:
        if not self._has_active_execution_owner_context():
            return False
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        attempts = []
        iter_attempts = getattr(self, "_iter_leg_attempts", None)
        if callable(iter_attempts):
            attempts = list(iter_attempts(leg_name=leg_name))
        for attempt in attempts:
            attempt_status = str(getattr(attempt, "status", "") or "").strip().upper()
            if (
                int(getattr(attempt, "owner_epoch", 0) or 0) == current_epoch
                and not bool(getattr(attempt, "terminal", False))
                and attempt_status not in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
            ):
                return True
        status = str(self._leg_state(leg_name).order_status or "").upper()
        # "PARTIALLY_FILLED" Ð¾Ñ‚Ñ€Ð°Ð¶Ð°ÐµÑ‚ Ñ„Ð°ÐºÑ‚ Ñ‡Ð°ÑÑ‚Ð¸Ñ‡Ð½Ð¾Ð³Ð¾ Ð¸ÑÐ¿Ð¾Ð»Ð½ÐµÐ½Ð¸Ñ, Ð½Ð¾ Ð½Ðµ Ð³Ð°Ñ€Ð°Ð½Ñ‚Ð¸Ñ€ÑƒÐµÑ‚,
        # Ñ‡Ñ‚Ð¾ Ð½Ð° Ð±Ð¸Ñ€Ð¶Ðµ Ð²ÑÑ‘ ÐµÑ‰Ñ‘ Ð²Ð¸ÑÐ¸Ñ‚ Ð¶Ð¸Ð²Ð¾Ð¹ outstanding-order. Ð˜Ð½Ð°Ñ‡Ðµ Ð¿Ð¾ÑÐ»Ðµ Ð»ÑŽÐ±Ð¾Ð³Ð¾
        # position resync/runtime reconcile Ð¼Ñ‹ Ð½Ð°Ð²ÑÐµÐ³Ð´Ð° Ð¾ÑÑ‚Ð°Ñ‘Ð¼ÑÑ Ð² Ñ€ÐµÐ¶Ð¸Ð¼Ðµ
        # "ÐµÑÑ‚ÑŒ Ð¶Ð¸Ð²Ñ‹Ðµ Ð¾Ñ€Ð´ÐµÑ€Ð°" Ð¸ Ð±Ð»Ð¾ÐºÐ¸Ñ€ÑƒÐµÐ¼ pipeline.
        if status not in {"SENDING", "SENT", "ACK", "ACCEPTED", "NEW"}:
            return False
        clock = self._dual_order_clocks.get(leg_name) or {}
        clock_epoch = int(clock.get("owner_epoch") or 0)
        if clock_epoch > 0 and clock_epoch != current_epoch:
            return False
        submitted_at_ms = int(clock.get("submitted_at_ms") or 0)
        order_id = str(clock.get("order_id") or "").strip()
        client_order_id = str(clock.get("client_order_id") or "").strip()
        return submitted_at_ms > 0 or bool(order_id or client_order_id)

    def _leg_live_order_age_ms(self, leg_name: str) -> int | None:
        if not self._leg_has_live_order(leg_name):
            return None
        now_ms = int(time.time() * 1000)
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        iter_attempts = getattr(self, "_iter_leg_attempts", None)
        max_age_ms = 0
        seen_attempt = False
        if callable(iter_attempts):
            for attempt in iter_attempts(leg_name=leg_name):
                attempt_status = str(getattr(attempt, "status", "") or "").strip().upper()
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                if bool(getattr(attempt, "terminal", False)):
                    continue
                if attempt_status in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
                    continue
                base_ts = int(getattr(attempt, "request_sent_at_ms", 0) or 0) or int(getattr(attempt, "submitted_at_ms", 0) or 0)
                if base_ts <= 0:
                    continue
                seen_attempt = True
                max_age_ms = max(max_age_ms, max(0, now_ms - base_ts))
        if seen_attempt:
            return max_age_ms
        clock = self._dual_order_clocks.get(leg_name) or {}
        clock_epoch = int(clock.get("owner_epoch") or 0)
        if clock_epoch > 0 and clock_epoch != current_epoch:
            return None
        submitted_at_ms = int(clock.get("submitted_at_ms") or 0)
        if submitted_at_ms <= 0:
            return None
        return max(0, now_ms - submitted_at_ms)

    def _exit_leg_has_stale_live_order(self, leg_name: str) -> bool:
        if self.active_exit_cycle is None:
            return False
        age_ms = self._leg_live_order_age_ms(leg_name)
        return age_ms is not None and age_ms >= self.EXIT_LIVE_ORDER_STALE_MS

    def _entry_leg_has_stale_live_order(self, leg_name: str) -> bool:
        if self.active_entry_cycle is None:
            return False
        age_ms = self._leg_live_order_age_ms(leg_name)
        return age_ms is not None and age_ms >= self.ENTRY_LIVE_ORDER_STALE_MS

    def _entry_has_stale_live_orders(self) -> bool:
        if self.active_entry_cycle is None:
            return False
        return self._entry_leg_has_stale_live_order("left") or self._entry_leg_has_stale_live_order("right")

    def _exit_has_stale_live_orders(self) -> bool:
        if self.active_exit_cycle is None:
            return False
        return self._exit_leg_has_stale_live_order("left") or self._exit_leg_has_stale_live_order("right")

    def _all_live_leg_orders_stale(self) -> bool:
        live_legs = [leg_name for leg_name in ("left", "right") if self._leg_has_live_order(leg_name)]
        if not live_legs:
            return False
        return all((self._leg_live_order_age_ms(leg_name) or 0) >= self.OWNER_STALE_TIMEOUT_MS for leg_name in live_legs)

    def _reevaluate_active_spread_execution(self) -> None:
        if not self._is_spread_entry_runtime:
            return
        if self.active_entry_cycle is None and self.prefetch_entry_cycle is not None:
            self._promote_prefetch_entry_cycle()
        if self.active_entry_cycle is None and self.active_exit_cycle is None:
            return
        self._sync_active_entry_cycle_from_legs()
        self._sync_active_exit_cycle_from_legs()
        self._refresh_dual_exec_status()
        left_status = str(self.left_leg_state.order_status or "IDLE").upper()
        right_status = str(self.right_leg_state.order_status or "IDLE").upper()
        if self.active_exit_cycle is not None:
            self._update_strategy_state_from_exit_attempt(left_status, right_status)
        elif self.active_entry_cycle is not None:
            self._update_strategy_state_from_entry_attempt(left_status, right_status)

    def _has_live_leg_orders(self) -> bool:
        return self._leg_has_live_order("left") or self._leg_has_live_order("right")

    def _has_active_execution_owner_context(self) -> bool:
        return (
            self.active_entry_cycle is not None
            or self.prefetch_entry_cycle is not None
            or self.active_exit_cycle is not None
            or getattr(self, "prefetch_exit_cycle", None) is not None
            or self._has_active_order_attempts()
            or self._entry_recovery_active()
            or self._exit_recovery_active()
            or self._hedge_protection_active()
        )

    def _settle_dual_execution_state(self, *, reason: str, force: bool = False) -> None:
        if not force and self._has_active_execution_owner_context():
            return
        left_status = str(self.left_leg_state.order_status or "IDLE").upper()
        right_status = str(self.right_leg_state.order_status or "IDLE").upper()
        dual_exec_idle = self._classify_dual_exec_status(left_status=left_status, right_status=right_status) == "IDLE"
        already_settled = (
            dual_exec_idle
            and not any(self._dual_order_clocks.values())
            and not any(self._leg_order_fill_tracker.values())
            and not any(self._leg_order_position_effects.values())
            and all(
                self._leg_state(leg_name).order_status == "IDLE"
                and self._leg_state(leg_name).target_qty == Decimal("0")
                and self._leg_state(leg_name).requested_qty == Decimal("0")
                and self._leg_state(leg_name).remaining_qty == Decimal("0")
                and self._leg_state(leg_name).last_error is None
                for leg_name in ("left", "right")
            )
        )
        if already_settled:
            self._last_dual_settle_reason = reason
            return
        self._dual_order_clocks = {"left": {}, "right": {}}
        self._dual_poll_threads = {}
        self._dual_poll_attempt_ids = {"left": None, "right": None}
        self._leg_order_fill_tracker = {"left": {}, "right": {}}
        self._leg_order_position_effects = {"left": {}, "right": {}}
        self._order_key_aliases = {"left": {}, "right": {}}
        self._order_key_tombstones = {"left": {}, "right": {}}
        self._order_attempts = {"left": {}, "right": {}}
        self.state.metrics["left_attempt_id"] = None
        self.state.metrics["right_attempt_id"] = None
        self.state.metrics["left_attempt_owner_epoch"] = None
        self.state.metrics["right_attempt_owner_epoch"] = None
        self.state.metrics["left_active_attempts"] = 0
        self.state.metrics["right_active_attempts"] = 0
        self.state.metrics["active_attempts_total"] = 0
        self.state.metrics["left_active_attempt_ids"] = None
        self.state.metrics["right_active_attempt_ids"] = None
        self.state.metrics["left_last_terminal_attempt_id"] = None
        self.state.metrics["right_last_terminal_attempt_id"] = None
        self.state.metrics["left_rest_poll_active"] = False
        self.state.metrics["right_rest_poll_active"] = False
        self.state.metrics["active_rest_polls_total"] = 0
        self.state.metrics["left_rest_poll_last_stop_reason"] = None
        self.state.metrics["right_rest_poll_last_stop_reason"] = None
        self.state.metrics["left_rest_poll_last_stop_ts"] = None
        self.state.metrics["right_rest_poll_last_stop_ts"] = None
        self.state.metrics["left_rest_poll_last_attempt_id"] = None
        self.state.metrics["right_rest_poll_last_attempt_id"] = None
        self._entry_cycle_order_keys = {"left": {}, "right": {}}
        self._exit_cycle_order_keys = {"left": {}, "right": {}}
        self.prefetch_entry_cycle = None
        self.prefetch_exit_cycle = None
        self.state.metrics["dual_exec_status"] = "IDLE"
        for leg_name in ("left", "right"):
            leg_state = self._leg_state(leg_name)
            leg_state.order_status = "IDLE"
            leg_state.target_qty = Decimal("0")
            leg_state.requested_qty = Decimal("0")
            leg_state.remaining_qty = Decimal("0")
            leg_state.last_order_reduce_only = False
            leg_state.latency_ack_ms = None
            leg_state.latency_fill_ms = None
            leg_state.last_error = None
            self._refresh_leg_position_derived_fields(leg_name, confirmed_by_exchange=False)
            self.state.metrics[f"{leg_name}_order_status"] = "IDLE"
        self._last_dual_settle_reason = reason
        self.logger.info("dual execution state settled | reason=%s", reason)

    def _mark_rebalance_grace(self, *, leg_name: str, reason: str) -> None:
        now_ms = int(time.time() * 1000)
        until_ms = now_ms + self.REBALANCE_GRACE_PERIOD_MS
        if until_ms <= self._rebalance_grace_until_ms:
            return
        self._rebalance_grace_until_ms = until_ms
        self.state.metrics["rebalance_grace_until_ms"] = until_ms
        self.state.metrics["rebalance_grace_remaining_ms"] = self.REBALANCE_GRACE_PERIOD_MS
        self.logger.info(
            "rebalance grace activated | leg=%s | reason=%s | duration_ms=%s | until_ms=%s",
            leg_name,
            reason,
            self.REBALANCE_GRACE_PERIOD_MS,
            until_ms,
        )
        threading.Thread(
            target=self._wait_and_recheck_hedge_protection,
            args=(until_ms,),
            name=f"{self.task.worker_id}-hedge-grace-recheck",
            daemon=True,
        ).start()

    def _rebalance_grace_remaining_ms(self) -> int:
        return max(0, self._rebalance_grace_until_ms - int(time.time() * 1000))

    def _wait_and_recheck_hedge_protection(self, until_ms: int) -> None:
        remaining_ms = max(0, until_ms - int(time.time() * 1000))
        if remaining_ms > 0:
            time.sleep(remaining_ms / 1000.0)
        if self.state.status != "running":
            return
        if until_ms != self._rebalance_grace_until_ms:
            return
        self._maybe_start_hedge_protection()

    def _strategy_allows_hedge_protection(self) -> bool:
        if self._runtime_reconcile_active():
            return False
        # Allow hedge protection when:
        # - we ÑƒÐ¶Ðµ ÑÑ‚Ð¾Ð¸Ð¼ Ð² Ð¿Ð¾Ð·Ð¸Ñ†Ð¸Ð¸ (IN_POSITION),
        # - ÐµÑÑ‚ÑŒ Ñ‡Ð°ÑÑ‚Ð¸Ñ‡Ð½Ñ‹Ð¹ Ð²Ñ…Ð¾Ð´ (ENTRY_PARTIAL),
        # - ÑÑ‚Ñ€Ð°Ñ‚ÐµÐ³Ð¸Ñ Ð² FAILED, Ð½Ð¾ Ð¼Ð¾Ð¶Ð½Ð¾ Ð°ÐºÐºÑƒÑ€Ð°Ñ‚Ð½Ð¾ Ð²Ñ‹Ñ€Ð¾Ð²Ð½ÑÑ‚ÑŒ Ð½Ð¾Ð³Ð¸,
        # - ÑÑ‚Ñ€Ð°Ñ‚ÐµÐ³Ð¸Ñ Ð² RECOVERY (Ð½Ð°Ð¿Ñ€Ð¸Ð¼ÐµÑ€, Ð¿Ð¾ÑÐ»Ðµ RECONCILE_MISMATCH), Ð¿Ñ€Ð¸ ÑÑ‚Ð¾Ð¼ Ð´Ñ€ÑƒÐ³Ð¸Ðµ recovery-Ð¿Ð¾Ñ‚Ð¾ÐºÐ¸ Ð½Ðµ Ð°ÐºÑ‚Ð¸Ð²Ð½Ñ‹.
        # - strategy is IDLE but position mismatch exists from previous session/restart.
        #   In this case hedge protection must be allowed to clear mismatch, otherwise
        #   entry can stay permanently blocked by HEDGE_MISMATCH_ACTIVE.
        return self.strategy_state in {
            StrategyState.IDLE,
            StrategyState.IN_POSITION,
            StrategyState.ENTRY_PARTIAL,
            StrategyState.FAILED,
            StrategyState.RECOVERY,
        }

    def _hedge_position_resync_due(self) -> bool:
        return (int(time.time() * 1000) - int(self._last_hedge_position_resync_ms or 0)) >= self.HEDGE_POSITION_RESYNC_INTERVAL_MS

    def _reconcile_positions_for_hedge_protection(self, *, reason: str) -> bool:
        if self.state.status != "running":
            return False
        with self._state_lock:
            if self._hedge_must_yield_to_cycle_owner():
                self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                return False
        try:
            left_qty = self._resync_exchange_position_qty("left")
            right_qty = self._resync_exchange_position_qty("right")
        except Exception as exc:
            self.state.metrics["hedge_status"] = "WAIT_POSITION_RESYNC"
            self.logger.warning("hedge protection resync failed | reason=%s | error=%s", reason, exc)
            self.emit_event(
                "hedge_protection_resync_failed",
                {
                    "reason": reason,
                    "error": str(exc),
                    "hedge_status": self.state.metrics.get("hedge_status"),
                },
            )
            return False
        with self._state_lock:
            if self._hedge_must_yield_to_cycle_owner():
                self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                return False
            self._apply_exchange_position_resync("left", left_qty)
            self._apply_exchange_position_resync("right", right_qty)
            self._sync_active_entry_cycle_from_legs()
            self._sync_active_exit_cycle_from_legs()
            self._last_hedge_position_resync_ms = int(time.time() * 1000)
            self._maybe_log_hedge_resync(
                reason=reason,
                left_qty=self.left_leg_state.actual_position_qty,
                right_qty=self.right_leg_state.actual_position_qty,
                mismatch=self._position_qty_mismatch(),
            )
            self._publish_state()
        return True

    def _maybe_start_hedge_protection(self) -> None:
        if not self._strategy_allows_hedge_protection():
            return
        if self.active_entry_cycle is not None or self.prefetch_entry_cycle is not None or self.active_exit_cycle is not None:
            return
        if not self._global_hedge_guard_enabled():
            self.state.metrics["hedge_status"] = "WAIT_CYCLE_ACTIVITY_COOLDOWN"
            return
        if self._cycle_recovery_active():
            return
        if self._has_live_leg_orders():
            self.state.metrics["hedge_status"] = "WAIT_ORDER_SETTLE"
            return
        remaining_grace_ms = self._rebalance_grace_remaining_ms()
        if remaining_grace_ms > 0:
            self.state.metrics["hedge_status"] = "WAIT_REBALANCE_GRACE"
            now_ms = int(time.time() * 1000)
            if (now_ms - self._last_rebalance_grace_log_ms) >= 500:
                self.logger.info(
                    "hedge protection delayed | reason=recent_order_submit | remaining_ms=%s",
                    remaining_grace_ms,
                )
                self._last_rebalance_grace_log_ms = now_ms
            return
        if (self._entry_growth_limit_pending or self._entry_growth_limited) and self._position_is_hedged():
            self.state.metrics["hedge_status"] = "OK"
            return
        if not self._position_has_qty_mismatch() and not self._hedge_position_resync_due():
            self.state.metrics["hedge_status"] = "OK"
            self._clear_recovery_status(context="HEDGE_PROTECTION")
            return
        with self._hedge_protection_lock:
            if self._hedge_protection_thread is not None and self._hedge_protection_thread.is_alive():
                return
            self._hedge_protection_thread = threading.Thread(
                target=self._run_hedge_protection,
                name=f"{self.task.worker_id}-hedge-protection",
                daemon=True,
            )
            self._hedge_protection_started_ms = int(time.time() * 1000)
            self._hedge_protection_thread.start()

    def _run_hedge_protection(self) -> None:
        with self._state_lock:
            if self._hedge_must_yield_to_cycle_owner():
                self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                self._clear_recovery_status(context="HEDGE_PROTECTION")
                return
        if self.state.status != "running":
            return
        if not self._reconcile_positions_for_hedge_protection(reason="HEDGE_PROTECTION_PRECHECK"):
            return
        with self._state_lock:
            if self._hedge_must_yield_to_cycle_owner():
                self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                self._clear_recovery_status(context="HEDGE_PROTECTION")
                return
        larger_leg = self._larger_position_leg()
        if larger_leg is None:
            self.state.metrics["hedge_status"] = "OK"
            self._clear_recovery_status(context="HEDGE_PROTECTION")
            return
        trim_qty = self._position_qty_mismatch()
        if trim_qty <= Decimal("0"):
            self.state.metrics["hedge_status"] = "OK"
            self._clear_recovery_status(context="HEDGE_PROTECTION")
            return
        if not self._can_exactly_trim_hedge_mismatch(larger_leg=larger_leg, trim_qty=trim_qty):
            self._set_recovery_status(
                context="HEDGE_PROTECTION",
                state="HEDGE_PROTECTION_FORCE_FLAT",
                reason="UNALIGNABLE_MISMATCH_FORCE_FLAT",
            )
            self.state.metrics["hedge_status"] = "FORCE_FLAT"
            self._set_strategy_state(StrategyState.RECOVERY)
            self.logger.warning(
                "hedge protection exact trim impossible | larger_leg=%s | trim_qty=%s | action=FORCE_FLAT_BOTH",
                larger_leg,
                self._format_order_size(trim_qty),
            )
            self._force_flatten_all_positions_from_hedge()
            return
        self.hedge_recovery_plan = RecoveryPlan(
            deficit_leg="left" if larger_leg == "right" else "right",
            qty_to_rebalance=trim_qty,
            attempts_used=1,
            action_type="trim_larger_leg",
        )
        self._set_recovery_status(context="HEDGE_PROTECTION", state="HEDGE_PROTECTION_ACTIVE", reason="POSITION_QTY_MISMATCH")
        self.state.metrics["hedge_status"] = "MISMATCH"
        self._set_strategy_state(StrategyState.RECOVERY)
        try:
            self._submit_leg_order(
                leg_name=larger_leg,
                side=str(self._opposite_side(str(self._leg_state(larger_leg).side or "")) or ""),
                quantity=trim_qty,
                reduce_only=True,
                reason="hedge_protection_trim",
            )
        except Exception as exc:
            self.state.last_error = str(exc)
            self._set_recovery_status(context="HEDGE_PROTECTION", state="HEDGE_PROTECTION_FAILED", reason=str(exc))
            self._set_strategy_state(StrategyState.FAILED)
            self._request_full_state_reconcile(reason="HEDGE_PROTECTION_FAILED")
            return
        deadline_ms = int(time.time() * 1000) + 5000
        while int(time.time() * 1000) < deadline_ms and self.state.status == "running":
            with self._state_lock:
                if self._hedge_must_yield_to_cycle_owner():
                    self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                    self._clear_recovery_status(context="HEDGE_PROTECTION")
                    return
            self._reconcile_positions_for_hedge_protection(reason="HEDGE_PROTECTION_MONITOR")
            if not self._position_has_qty_mismatch():
                self.hedge_recovery_plan = None
                self._clear_recovery_status(context="HEDGE_PROTECTION")
                self.state.metrics["hedge_status"] = "OK"
                self._settle_dual_execution_state(reason="HEDGE_PROTECTION_COMPLETED")
                if self.left_leg_state.actual_position_qty <= Decimal("0") and self.right_leg_state.actual_position_qty <= Decimal("0"):
                    self._reset_position_state()
                    self._set_strategy_state(StrategyState.IDLE)
                else:
                    self._sync_position_from_legs()
                    self._set_strategy_state(StrategyState.IN_POSITION)
                self._publish_state()
                return
            time.sleep(0.2)
        with self._state_lock:
            if self._hedge_must_yield_to_cycle_owner():
                self.state.metrics["hedge_status"] = "WAIT_OWNER_ACTIVE"
                self._clear_recovery_status(context="HEDGE_PROTECTION")
                return
            self._set_recovery_status(context="HEDGE_PROTECTION", state="HEDGE_PROTECTION_FAILED", reason="HEDGE_PROTECTION_TIMEOUT")
            self.state.metrics["hedge_status"] = "MISMATCH"
            self._set_strategy_state(StrategyState.FAILED)
        self._request_full_state_reconcile(reason="HEDGE_PROTECTION_TIMEOUT")

    def _position_has_qty_mismatch(self) -> bool:
        return self._position_qty_mismatch() > Decimal("0")

    def _position_qty_mismatch(self) -> Decimal:
        return abs(self.left_leg_state.actual_position_qty - self.right_leg_state.actual_position_qty)

    def _larger_position_leg(self) -> str | None:
        if self.left_leg_state.actual_position_qty > self.right_leg_state.actual_position_qty:
            return "left"
        if self.right_leg_state.actual_position_qty > self.left_leg_state.actual_position_qty:
            return "right"
        return None

    def _can_exactly_trim_hedge_mismatch(self, *, larger_leg: str, trim_qty: Decimal) -> bool:
        if trim_qty <= Decimal("0"):
            return True
        instrument = self._left_instrument if larger_leg == "left" else self._right_instrument
        step_size = max(Decimal("0"), instrument.spec.qty_precision)
        min_qty = max(Decimal("0"), instrument.spec.min_qty)
        if trim_qty < min_qty:
            return False
        if step_size <= Decimal("0"):
            return True
        units = trim_qty / step_size
        return units == units.to_integral_value()

    def _force_flatten_all_positions_from_hedge(self) -> None:
        orders: list[tuple[str, str, Decimal]] = []
        for leg_name in ("left", "right"):
            qty = max(Decimal("0"), self._leg_state(leg_name).actual_position_qty)
            if qty <= Decimal("0"):
                continue
            opposite_side = self._opposite_side(str(self._leg_state(leg_name).side or ""))
            if opposite_side is None:
                continue
            orders.append((leg_name, opposite_side, qty))
        if not orders:
            self.hedge_recovery_plan = None
            self._clear_recovery_status(context="HEDGE_PROTECTION")
            self.state.metrics["hedge_status"] = "OK"
            self._reset_position_state()
            self._settle_dual_execution_state(reason="HEDGE_PROTECTION_FORCE_FLAT_EMPTY")
            self._set_strategy_state(StrategyState.IDLE)
            self._publish_state()
            return
        errors: list[str] = []
        lock = threading.Lock()

        def _send_flatten(leg_name: str, side: str, quantity: Decimal) -> None:
            try:
                self._submit_leg_order(
                    leg_name=leg_name,
                    side=side,
                    quantity=quantity,
                    reduce_only=True,
                    reason="hedge_protection_force_flat",
                )
            except Exception as exc:
                with lock:
                    errors.append(f"{leg_name}:{exc}")

        threads = [
            threading.Thread(
                target=_send_flatten,
                args=(leg_name, side, quantity),
                name=f"{self.task.worker_id}-{leg_name}-hedge-force-flat",
                daemon=True,
            )
            for leg_name, side, quantity in orders
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3.0)
        deadline_ms = int(time.time() * 1000) + 5000
        while int(time.time() * 1000) < deadline_ms and self.state.status == "running":
            self._reconcile_positions_for_hedge_protection(reason="HEDGE_PROTECTION_FORCE_FLAT_MONITOR")
            if self.left_leg_state.actual_position_qty <= Decimal("0") and self.right_leg_state.actual_position_qty <= Decimal("0"):
                self.hedge_recovery_plan = None
                self._clear_recovery_status(context="HEDGE_PROTECTION")
                self.state.metrics["hedge_status"] = "OK"
                self._reset_position_state()
                self._settle_dual_execution_state(reason="HEDGE_PROTECTION_FORCE_FLAT_COMPLETED")
                self._set_strategy_state(StrategyState.IDLE)
                self._publish_state()
                return
            time.sleep(0.2)
        if errors:
            self.logger.error("hedge protection force flat errors | errors=%s", errors)
        self._set_recovery_status(context="HEDGE_PROTECTION", state="HEDGE_PROTECTION_FAILED", reason="HEDGE_PROTECTION_FORCE_FLAT_TIMEOUT")
        self.state.metrics["hedge_status"] = "MISMATCH"
        self._set_strategy_state(StrategyState.FAILED)
        self._request_full_state_reconcile(reason="HEDGE_PROTECTION_FORCE_FLAT_TIMEOUT")

    def _start_exit_cycle_recovery(self, left_status: str, right_status: str) -> None:
        # Legacy cycle recovery is disabled.
        return

    def _exit_cycle_deficit_leg(self) -> str | None:
        if self.active_exit_cycle is None:
            return None
        if self._exit_remainder_uses_actual_positions():
            return self._larger_position_leg()
        if self.left_leg_state.filled_qty <= Decimal("0") and self.right_leg_state.filled_qty > Decimal("0"):
            return "right"
        if self.right_leg_state.filled_qty <= Decimal("0") and self.left_leg_state.filled_qty > Decimal("0"):
            return "left"
        left_remaining = self._exit_cycle_remaining_qty("left")
        right_remaining = self._exit_cycle_remaining_qty("right")
        if left_remaining <= Decimal("0") and right_remaining <= Decimal("0"):
            return None
        if left_remaining > Decimal("0") and right_remaining <= Decimal("0"):
            return "left"
        if right_remaining > Decimal("0") and left_remaining <= Decimal("0"):
            return "right"
        return "left" if self.active_exit_cycle.left_filled_qty <= self.active_exit_cycle.right_filled_qty else "right"

    def _exit_cycle_remaining_qty(self, leg_name: str) -> Decimal:
        if self._exit_remainder_uses_actual_positions():
            if leg_name is None:
                return Decimal("0")
            return max(Decimal("0"), self._leg_state(leg_name).remaining_close_qty)
        if self.active_exit_cycle is None:
            return Decimal("0")
        target_qty = self.active_exit_cycle.left_target_qty if leg_name == "left" else self.active_exit_cycle.right_target_qty
        closed_qty = self.active_exit_cycle.left_filled_qty if leg_name == "left" else self.active_exit_cycle.right_filled_qty
        return max(Decimal("0"), target_qty - closed_qty)

    def _exit_remainder_uses_actual_positions(self) -> bool:
        cycle = self.active_exit_cycle
        if cycle is None:
            return False
        if cycle.tail_reduce_only_seen or cycle.tail_resync_attempts > 0:
            return True
        return self.left_leg_state.flat_confirmed_by_exchange or self.right_leg_state.flat_confirmed_by_exchange

    def _exit_one_sided_remaining_leg(self) -> str | None:
        left_remaining = max(Decimal("0"), self.left_leg_state.remaining_close_qty)
        right_remaining = max(Decimal("0"), self.right_leg_state.remaining_close_qty)
        if left_remaining > Decimal("0") and right_remaining <= Decimal("0"):
            return "left"
        if right_remaining > Decimal("0") and left_remaining <= Decimal("0"):
            return "right"
        return None

    def _sync_position_from_legs(self) -> None:
        left_qty = self.left_leg_state.filled_qty
        right_qty = self.right_leg_state.filled_qty
        left_side = str(self.left_leg_state.side or "").strip().upper()
        right_side = str(self.right_leg_state.side or "").strip().upper()
        direction = ""
        if left_side in {"BUY", "SELL"} and right_side in {"BUY", "SELL"}:
            direction = f"LEFT_{left_side}_RIGHT_{right_side}"
        if not direction:
            fallback_left = str((self.position.left_side if self.position is not None else None) or "").strip().upper()
            fallback_right = str((self.position.right_side if self.position is not None else None) or "").strip().upper()
            if fallback_left in {"BUY", "SELL"} and fallback_right in {"BUY", "SELL"}:
                direction = f"LEFT_{fallback_left}_RIGHT_{fallback_right}"
        entry_edge = self.position.entry_edge if self.position is not None else None
        # Знаковый спред в момент входа — ось для порога выхода (например вход при -1, порог -0.2 → выход при current > -0.2).
        if entry_edge is None and self.active_entry_cycle is not None and self.active_entry_cycle.edge_value is not None:
            entry_edge = self.active_entry_cycle.edge_value
        active_edge = self.position.active_edge if self.position is not None else None
        if active_edge is None and entry_edge is not None:
            if direction == "LEFT_SELL_RIGHT_BUY":
                active_edge = "edge_1"
            elif direction == "LEFT_BUY_RIGHT_SELL":
                active_edge = "edge_2"
        entry_time = self.position.entry_time if self.position is not None else self.last_entry_ts
        self.position = StrategyPosition(
            direction=direction,
            entry_edge=entry_edge,
            active_edge=active_edge,
            left_side=left_side or None,
            right_side=right_side or None,
            left_target_qty=left_qty,
            right_target_qty=right_qty,
            left_filled_qty=left_qty,
            right_filled_qty=right_qty,
            left_avg_fill_price=self.left_leg_state.avg_price,
            right_avg_fill_price=self.right_leg_state.avg_price,
            entry_time=entry_time,
            state=StrategyState.IN_POSITION,
        )
        self.state.metrics["position_direction"] = direction or None
        self.state.metrics["position_state"] = self.position.state.value
        self.state.metrics["position_entry_edge"] = self._format_edge(self.position.entry_edge)
        self.state.metrics["position_active_edge"] = self.position.active_edge
