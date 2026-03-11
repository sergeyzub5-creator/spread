from __future__ import annotations

import threading
import time
from decimal import Decimal
from typing import Any

from app.core.events.bus import EventBus
from app.core.execution.adapter import ExecutionAdapter
from app.core.logging.logger_factory import get_logger
from app.core.market_data.service import MarketDataService
from app.core.models.execution import ExecutionOrderRequest, ExecutionStreamEvent
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1
from app.core.models.workers import EntryDecision, LegState, OrderAttempt, RecoveryPlan, StrategyCycle, StrategyCycleState, StrategyCycleType, StrategyPosition, StrategyState, WorkerState, WorkerTask
from app.core.workers.entry_validator import EntryValidationResult, SpreadEntryValidator
from app.core.workers.runtime_cycle_mixin import WorkerRuntimeCycleMixin
from app.core.workers.runtime_attempt_classifiers import exit_cycle_leg_matches_target, exit_has_any_close_fill, has_any_entry_fill, is_entry_attempt_active, is_entry_full_fail, is_entry_full_success, is_entry_partial, is_exit_full_fail, is_exit_full_success, is_exit_partial
from app.core.workers.runtime_attempt_helpers import entry_attempt_result_signature, entry_recovery_blocked_by_grace, entry_recovery_blocked_by_live_order, exit_recovery_blocked_by_grace, should_log_entry_attempt_result
from app.core.workers.runtime_entry_decision_flow import build_entry_validation_result, should_enforce_entry_liquidity_check
from app.core.workers.runtime_entry_pipeline_helpers import drop_entry_cycle_order_keys, drop_exit_cycle_order_keys, enforce_entry_pipeline_inflight_invariant, entry_cycle_ack_ready, entry_cycle_leg_filled_qty, entry_cycle_order_key, entry_pipeline_freeze, entry_pipeline_inflight_cycle_ids, entry_pipeline_maybe_thaw, entry_pipeline_overlap_enabled, exit_cycle_order_key, resolve_entry_cycle_for_submit, resolve_exit_cycle_for_submit
from app.core.workers.runtime_entry_orchestrator import evaluate_spread_entry, handle_entry_submit_failure
from app.core.workers.runtime_execution_mixin import WorkerRuntimeExecutionMixin
from app.core.workers.runtime_entry_attempt_flow import update_entry_attempt_state
from app.core.workers.runtime_exit_attempt_flow import update_exit_attempt_state
from app.core.workers.runtime_exit_recovery_helpers import exit_recovery_allowed, exit_tail_resync_in_progress
from app.core.workers.runtime_exit_orchestrator import current_exit_edge, evaluate_spread_exit, exit_sides_for_position, planned_exit_cycle_sizes
from app.core.workers.runtime_parts import WorkerRuntimePartsMixin
from app.core.workers.runtime_policy import RuntimePolicy, create_runtime_policy
from app.core.workers.runtime_quantity_helpers import entry_cycle_pair_matches_target, entry_has_imbalance, entry_leg_imbalance_notional_usdt, entry_leg_target_total_qty, is_exit_cycle_committed_success, is_no_position_to_close_error, leg_fill_matches_target, max_leg_imbalance_notional_usdt, qty_matches_target, resolve_max_leg_imbalance_notional_usdt
from app.core.workers.runtime_guard_mixin import WorkerRuntimeGuardMixin
from app.core.workers.runtime_sizing_mixin import WorkerRuntimeSizingMixin
from app.core.workers.runtime_spread_utils import SpreadEdgeResult, calculate_spread_edges, format_edge, safe_edge
from app.core.workers.runtime_state_guards import entry_pipeline_busy_reason, exit_signal_active, mark_leg_flat_confirmed, maybe_restore_in_position_state, reset_position_state


class WorkerRuntime(WorkerRuntimeExecutionMixin, WorkerRuntimeSizingMixin, WorkerRuntimeCycleMixin, WorkerRuntimeGuardMixin, WorkerRuntimePartsMixin):
    """Reusable runtime that can handle a single-instrument test slice or a pair task."""

    DEFAULT_MAX_LEG_IMBALANCE_NOTIONAL_USDT = Decimal("1")
    DEFAULT_ENTRY_FRESHNESS_THRESHOLD_MS = 2000
    REBALANCE_GRACE_PERIOD_MS = 3000
    HEDGE_POSITION_RESYNC_INTERVAL_MS = 1000
    EXIT_LIVE_ORDER_STALE_MS = 2000
    ENTRY_LIVE_ORDER_STALE_MS = 2000
    EXIT_GRACE_WINDOW_MS = 1200
    EXIT_RECOVERY_DEBOUNCE_MS = 1000
    EXIT_TAIL_RESYNC_MAX_ATTEMPTS = 2
    GLOBAL_HEDGE_GUARD_IDLE_MS = 2000
    # After a cycle closes, allow hedge protection to run once after this delay
    # without waiting the full GLOBAL_HEDGE_GUARD_IDLE_MS (2s backstop still applies later).
    POST_CYCLE_HEDGE_GUARD_IDLE_MS = 500
    EXIT_FLAT_EPSILON = Decimal("0.00000001")
    ENTRY_CYCLE_SETTLE_TIMEOUT_MS = 5000
    EXIT_CYCLE_SETTLE_TIMEOUT_MS = 5000
    REST_ORDER_POLL_INTERVAL_MS = 250
    REST_ORDER_POLL_ACTIVE_INTERVAL_MS = 100
    REST_ORDER_POLL_FAST_INTERVAL_MS = 60
    ORDER_KEY_TOMBSTONE_TTL_MS = 15000
    ORDER_ATTEMPT_TERMINAL_TTL_MS = 30000
    WATCHDOG_INTERVAL_MS = 500
    CONNECTIVITY_DEGRADED_QUOTE_AGE_MS = 4000
    OWNER_STALE_TIMEOUT_MS = 8000
    RUNTIME_RECONCILE_DEBOUNCE_MS = 1500
    FULL_STATE_RECONCILE_INTERVAL_MS = 60000
    VALIDATION_LOG_INTERVAL_MS = 15000
    VALIDATION_STALE_LOG_INTERVAL_MS = 30000
    ENTRY_CYCLE_CLAMP_LOG_INTERVAL_MS = 20000

    def __init__(self, task: WorkerTask, market_data_service: MarketDataService, event_bus: EventBus) -> None:
        self.task = task
        self.market_data_service = market_data_service
        self.event_bus = event_bus
        self.logger = get_logger("worker.runtime", worker_id=task.worker_id)
        self._latest_quotes: dict[InstrumentId, QuoteL1] = {}
        self._subscribed_instruments = tuple(self._unique_instruments())
        self._execution_adapter: ExecutionAdapter | None = None
        self._left_execution_adapter: ExecutionAdapter | None = None
        self._right_execution_adapter: ExecutionAdapter | None = None
        self._active_instrument = task.left_instrument
        self._pending_order_clock: dict[str, Any] | None = None
        self._left_instrument = task.left_instrument
        self._right_instrument = task.right_instrument
        self._run_mode = str(task.run_mode or "").strip().lower()
        self._is_dual_quotes_runtime = self._run_mode == "dual_exchange_quotes"
        self._is_dual_execution_runtime = self._run_mode == "dual_exchange_test_execution"
        self._is_spread_entry_runtime = self._run_mode == "spread_entry_execution"
        self._is_dual_runtime = self._is_dual_quotes_runtime or self._is_dual_execution_runtime or self._is_spread_entry_runtime
        self._dual_order_clocks: dict[str, dict[str, Any]] = {"left": {}, "right": {}}
        self._dual_poll_threads: dict[str, threading.Thread] = {}
        self._dual_poll_attempt_ids: dict[str, str | None] = {"left": None, "right": None}
        self._leg_order_fill_tracker: dict[str, dict[str, Decimal]] = {"left": {}, "right": {}}
        self._leg_order_position_effects: dict[str, dict[str, Decimal]] = {"left": {}, "right": {}}
        self._order_key_aliases: dict[str, dict[str, str]] = {"left": {}, "right": {}}
        self._order_key_tombstones: dict[str, dict[str, int]] = {"left": {}, "right": {}}
        self._order_attempts: dict[str, dict[str, OrderAttempt]] = {"left": {}, "right": {}}
        self._order_attempt_seq = 0
        self._order_pair_seq = 0
        self._entry_cycle_order_keys: dict[str, dict[str, int]] = {"left": {}, "right": {}}
        self._exit_cycle_order_keys: dict[str, dict[str, int]] = {"left": {}, "right": {}}
        # Serialize cross-thread mutations of runtime state between market-data,
        # execution callbacks and recovery threads.
        self._state_lock = threading.RLock()
        self._entry_lock = threading.Lock()
        self._exit_lock = threading.Lock()
        self._entry_recovery_lock = threading.Lock()
        self._hedge_protection_lock = threading.Lock()
        self._forced_entry_signal_requested = False
        self._simulated_entry_window_open = False
        self._simulated_exit_window_open = False
        self._simulated_entry_direction: str | None = None
        self._strategy_signal_mode = self._normalize_strategy_signal_mode(task.runtime_params.get("strategy_signal_mode"))
        policy_name = str(task.runtime_params.get("runtime_policy") or "new").strip().lower()
        self._runtime_policy: RuntimePolicy = create_runtime_policy(policy_name)
        self._runtime_policy_name = str(getattr(self._runtime_policy, "name", policy_name or "new"))
        self._new_policy_bootstrap_logged = False
        self._entry_pipeline_mode_requested = "strict"
        self._entry_pipeline_mode = "strict"
        self._entry_pipeline_mode_fallback_reason: str | None = None
        self._entry_pipeline_frozen = False
        self._entry_pipeline_freeze_reason: str | None = None
        self._entry_pipeline_freeze_ts: int | None = None
        self._rebalance_grace_until_ms = 0
        self._last_rebalance_grace_log_ms = 0
        self._last_hedge_position_resync_ms = 0
        configured_freshness_ms = self._int_or_zero(task.runtime_params.get("max_quote_age_ms"))
        self._entry_freshness_threshold_ms = configured_freshness_ms if configured_freshness_ms > 0 else self.DEFAULT_ENTRY_FRESHNESS_THRESHOLD_MS
        configured_depth_freshness_ms = self._int_or_zero(task.runtime_params.get("max_depth_age_ms"))
        self._depth_freshness_threshold_ms = configured_depth_freshness_ms if configured_depth_freshness_ms > 0 else self._entry_freshness_threshold_ms
        self._max_leg_imbalance_notional_usdt_value = self._resolve_max_leg_imbalance_notional_usdt()
        self._entry_validator = SpreadEntryValidator(freshness_threshold_ms=self._entry_freshness_threshold_ms)
        self._entry_growth_limited = False
        self._entry_growth_limit_reason: str | None = None
        self._entry_growth_limit_notional_usdt: Decimal | None = None
        self._entry_growth_limit_qty: Decimal | None = None
        self._entry_growth_limit_pending = False
        self._entry_growth_limit_pending_reason: str | None = None
        self._last_dual_settle_reason: str | None = None
        self._last_dual_exec_eval_signature: tuple[Any, ...] | None = None
        self._last_entry_block_log_reason: str | None = None
        self._last_entry_block_log_state: str | None = None
        self._last_entry_block_log_at_ms = 0
        self._last_exit_grace_log_cycle_id: int | None = None
        self._last_exit_grace_log_state: str | None = None
        self._last_spread_log_signature: tuple[Any, ...] | None = None
        self._last_validation_log_signature: tuple[Any, ...] | None = None
        self._last_validation_log_at_ms = 0
        self._last_hedge_resync_log_signature: tuple[Any, ...] | None = None
        self._last_hedge_resync_log_at_ms = 0
        self._last_entry_recovery_wait_log_signature: tuple[Any, ...] | None = None
        self._last_entry_recovery_wait_log_at_ms = 0
        self._last_entry_grace_log_cycle_id: int | None = None
        self._last_entry_grace_log_state: str | None = None
        self._last_entry_live_order_log_cycle_id: int | None = None
        self._last_entry_live_order_log_status_signature: tuple[str, str] | None = None
        self._last_entry_live_order_log_at_ms = 0
        self._entry_settle_timeout_handled_cycle_id: int | None = None
        self._last_entry_cycle_clamp_signature: tuple[Any, ...] | None = None
        self._last_entry_cycle_clamp_log_at_ms = 0
        self._last_entry_cycle_clamp_reason_signature: tuple[str, bool] | None = None
        self._last_entry_sizing_log_signature: tuple[Any, ...] | None = None
        self._last_entry_sizing_log_at_ms = 0
        self._last_ignored_foreign_event_log_signature: tuple[Any, ...] | None = None
        self._last_ignored_foreign_event_log_at_ms = 0
        self._entry_cycle_success_streak = 0
        self._exit_cycle_success_streak = 0
        self._last_cycle_activity_ts = int(time.time() * 1000)
        idle_reset_ms = self._int_or_zero(task.runtime_params.get("cycle_growth_idle_reset_ms"))
        self._cycle_growth_idle_reset_ms = idle_reset_ms if idle_reset_ms > 0 else 5000
        global_guard_idle_ms = self._int_or_zero(task.runtime_params.get("global_hedge_guard_idle_ms"))
        self._global_hedge_guard_idle_ms = global_guard_idle_ms if global_guard_idle_ms > 0 else self.GLOBAL_HEDGE_GUARD_IDLE_MS
        post_cycle_ms = self._int_or_zero(task.runtime_params.get("post_cycle_hedge_guard_idle_ms"))
        self._post_cycle_hedge_guard_ms = post_cycle_ms if post_cycle_ms > 0 else self.POST_CYCLE_HEDGE_GUARD_IDLE_MS
        # After cycle commit/finalize: first hedge check allowed at this timestamp (one-shot bypass of long idle).
        self._post_cycle_hedge_eligible_at_ms: int = 0
        configured_state_publish_interval_ms = self._int_or_zero(task.runtime_params.get("state_publish_interval_ms"))
        self._state_publish_interval_ms = configured_state_publish_interval_ms if configured_state_publish_interval_ms > 0 else 500
        self._last_state_publish_ms = 0
        self._state_publish_timer_scheduled = False
        self._state_publish_deferred_timer: threading.Timer | None = None
        self._last_entry_attempt_result_signature: tuple[Any, ...] | None = None
        self._last_exit_attempt_result_signature: tuple[Any, ...] | None = None
        self._entry_cycle_dispatch_ts_by_id: dict[int, int] = {}
        self._exit_cycle_dispatch_ts_by_id: dict[int, int] = {}
        self._last_entry_cycle_dispatch_ts_ms: int | None = None
        self._last_exit_cycle_dispatch_ts_ms: int | None = None
        self._last_entry_cycle_commit_ts_ms: int | None = None
        self._last_exit_cycle_commit_ts_ms: int | None = None
        self._deferred_entry_chain_requested = False
        self._deferred_exit_chain_requested = False
        self._deferred_runtime_actions_scheduled = False
        self._last_execution_stream_health_status: str | None = None
        self._last_execution_stream_warning_signature: tuple[str, str, str] | None = None
        self._pending_execution_stream_health_event: tuple[str, dict[str, Any]] | None = None
        self._execution_stream_prev_reconnect: dict[str, tuple[int, int]] = {}
        self._execution_stream_disconnected_since_ms: dict[str, int] = {}
        self._watchdog_stop_event = threading.Event()
        self._watchdog_thread: threading.Thread | None = None
        self._runtime_reconcile_thread: threading.Thread | None = None
        self._entry_recovery_started_ms: int | None = None
        self._exit_recovery_started_ms: int | None = None
        self._hedge_protection_started_ms: int | None = None
        self._runtime_health_mode = "HEALTHY"
        self._runtime_owner_epoch = 1
        self._last_runtime_reconcile_started_ms = 0
        self._last_runtime_reconcile_reason: str | None = None
        self._hedge_check_requested = False
        self._last_hedge_check_request_reason: str | None = None
        self.strategy_state = StrategyState.IDLE
        self.entry_state = self.strategy_state
        self.position: StrategyPosition | None = None
        self._cycle_seq = 0
        self.active_entry_cycle: StrategyCycle | None = None
        self.prefetch_entry_cycle: StrategyCycle | None = None
        self.active_exit_cycle: StrategyCycle | None = None
        self.prefetch_exit_cycle: StrategyCycle | None = None
        self.last_entry_cycle: StrategyCycle | None = None
        self.last_exit_cycle: StrategyCycle | None = None
        self.entry_recovery_plan: RecoveryPlan | None = None
        self.exit_recovery_plan: RecoveryPlan | None = None
        self.hedge_recovery_plan: RecoveryPlan | None = None
        self._entry_recovery_thread: threading.Thread | None = None
        self._exit_recovery_thread: threading.Thread | None = None
        self._hedge_protection_thread: threading.Thread | None = None
        self.left_leg_state = LegState(exchange=self._left_instrument.exchange, symbol=self._left_instrument.symbol)
        self.right_leg_state = LegState(exchange=self._right_instrument.exchange, symbol=self._right_instrument.symbol)
        self.last_entry_ts: int | None = None
        self.cooldown_ms = 3000
        self.state = WorkerState(
            worker_id=task.worker_id,
            status="created",
            current_pair=(task.left_instrument, task.right_instrument),
            last_error=None,
            started_at=None,
            stopped_at=None,
            metrics={
                "quote_count": 0,
                "bid": None,
                "ask": None,
                "last_quote_ts_local": None,
                "last_order_ack_status": None,
                "last_order_id": None,
                "last_execution_type": None,
                "last_order_status": None,
                "last_fill_qty": None,
                "last_fill_price": None,
                "last_realized_pnl": None,
                "last_ack_latency_ms": None,
                "last_first_event_latency_ms": None,
                "last_fill_latency_ms": None,
                "last_click_to_send_latency_ms": None,
                "last_send_to_ack_latency_ms": None,
                "last_send_to_first_event_latency_ms": None,
                "last_send_to_fill_latency_ms": None,
                "last_transport_connection_mode": None,
                "left_bid": None,
                "left_ask": None,
                "right_bid": None,
                "right_ask": None,
                "left_quote_ts": None,
                "right_quote_ts": None,
                "left_quote_age_ms": None,
                "right_quote_age_ms": None,
                "left_depth20_levels": 0,
                "right_depth20_levels": 0,
                "left_last_depth_ts_ms": None,
                "right_last_depth_ts_ms": None,
                "left_depth_age_ms": None,
                "right_depth_age_ms": None,
                "left_depth_updates_count": 0,
                "right_depth_updates_count": 0,
                "left_depth_reject_count": 0,
                "right_depth_reject_count": 0,
                "left_depth_last_reason": "NO_DATA",
                "right_depth_last_reason": "NO_DATA",
                "edge_1": None,
                "edge_2": None,
                "spread_state": "WAITING_QUOTES" if self._is_dual_runtime else None,
                "left_order_status": "IDLE",
                "right_order_status": "IDLE",
                "left_ack_latency_ms": None,
                "right_ack_latency_ms": None,
                "left_first_event_latency_ms": None,
                "right_first_event_latency_ms": None,
                "left_fill_latency_ms": None,
                "right_fill_latency_ms": None,
                "left_filled_qty": None,
                "right_filled_qty": None,
                "left_actual_position_qty": None,
                "right_actual_position_qty": None,
                "left_real_remaining_qty": None,
                "right_real_remaining_qty": None,
                "dual_exec_status": "IDLE",
                "entry_threshold": str(task.runtime_params.get("entry_threshold") or task.entry_threshold or "0"),
                "entry_enabled": self._is_spread_entry_runtime,
                "active_edge": None,
                "entry_direction": None,
                "entry_block_reason": None,
                "entry_requested_qty": None,
                "entry_executable_qty": None,
                "entry_min_step_pct": None,
                "entry_min_step_notional_usdt": None,
                "entry_min_step_qty": None,
                "last_entry_policy_ts": None,
                "entry_growth_limited": False,
                "entry_growth_limit_reason": None,
                "entry_growth_limit_notional_usdt": None,
                "entry_growth_limit_qty": None,
                "entry_cycle_growth_streak": 0,
                "exit_cycle_growth_streak": 0,
                "cycle_growth_reset_reason": None,
                "activity_status": "STOPPED",
                "strategy_signal_mode": self._strategy_signal_mode,
                "entry_pipeline_mode_requested": self._entry_pipeline_mode_requested,
                "entry_pipeline_mode": self._entry_pipeline_mode,
                "entry_pipeline_mode_fallback_reason": self._entry_pipeline_mode_fallback_reason,
                "entry_pipeline_frozen": False,
                "entry_pipeline_freeze_reason": None,
                "entry_pipeline_freeze_ts": None,
                "simulated_entry_window_open": False,
                "simulated_exit_window_open": False,
                "simulated_entry_direction": None,
                "entry_count": 0,
                "last_entry_ts": None,
                "strategy_state": self.strategy_state.value,
                "entry_state": self.strategy_state.value,
                "last_result": None,
                "position_direction": None,
                "position_state": None,
                "position_entry_edge": None,
                "position_active_edge": None,
                "active_entry_cycle_id": None,
                "active_entry_cycle_state": None,
                "active_entry_cycle_notional_usdt": None,
                "active_entry_cycle_left_target_qty": None,
                "active_entry_cycle_right_target_qty": None,
                "active_entry_cycle_left_filled_qty": None,
                "active_entry_cycle_right_filled_qty": None,
                "prefetch_entry_cycle_id": None,
                "prefetch_entry_cycle_state": None,
                "entry_inflight_cycles": 0,
                "entry_inflight_cycle_ids": None,
                "active_exit_cycle_id": None,
                "active_exit_cycle_state": None,
                "prefetch_exit_cycle_id": None,
                "prefetch_exit_cycle_state": None,
                "active_exit_cycle_notional_usdt": None,
                "active_exit_cycle_left_target_qty": None,
                "active_exit_cycle_right_target_qty": None,
                "active_exit_cycle_left_filled_qty": None,
                "active_exit_cycle_right_filled_qty": None,
                "last_exit_cycle_result": None,
                "exit_grace_deadline_ts": None,
                "exit_tail_resync_in_progress": False,
                "exit_tail_resync_attempts": 0,
                "exit_tail_reduce_only_seen": False,
                "last_entry_cycle_result": None,
                "recovery_state": None,
                "recovery_reason": None,
                "recovery_context": None,
                "hedge_status": None,
                "rebalance_grace_until_ms": None,
                "rebalance_grace_remaining_ms": 0,
                "max_leg_imbalance_notional_usdt": self._format_order_size(self._max_leg_imbalance_notional_usdt_value),
                "best_edge": None,
                "left_action": None,
                "right_action": None,
                "cooldown_ms": self.cooldown_ms,
                "runtime_health": self._runtime_health_mode,
                "runtime_owner": None,
                "runtime_owner_epoch": self._runtime_owner_epoch,
                "left_active_attempts": 0,
                "right_active_attempts": 0,
                "active_attempts_total": 0,
                "left_active_attempt_ids": None,
                "right_active_attempt_ids": None,
                "left_last_terminal_attempt_id": None,
                "right_last_terminal_attempt_id": None,
                "left_rest_poll_active": False,
                "right_rest_poll_active": False,
                "active_rest_polls_total": 0,
                "left_rest_poll_last_stop_reason": None,
                "right_rest_poll_last_stop_reason": None,
                "left_rest_poll_last_stop_ts": None,
                "right_rest_poll_last_stop_ts": None,
                "left_rest_poll_last_attempt_id": None,
                "right_rest_poll_last_attempt_id": None,
                "last_reconcile_reason": None,
                "last_reconcile_ts": None,
                "execution_stream_health_status": None,
                "execution_stream_health": None,
            },
        )

    def start(self) -> None:
        with self._state_lock:
            self.state.status = "running"
            self.state.started_at = int(time.time() * 1000)
            self.state.last_error = None
            self.logger.info("worker start | run_mode=%s | execution_mode=%s | instruments=%s", self.task.run_mode, self.task.execution_mode, [instrument.symbol for instrument in self._subscribed_instruments])
            if self._is_spread_entry_runtime:
                self.logger.info(
                    "spread entry validator configured | freshness_threshold_ms=%s | max_quote_skew_ms=%s | max_leg_imbalance_notional_usdt=%s | entry_success_rule=filled_equals_target_qty",
                    self._entry_freshness_threshold_ms,
                    self._int_or_zero(self.task.runtime_params.get("max_quote_skew_ms")),
                    self._format_order_size(self._max_leg_imbalance_notional_usdt_value),
                )
                self.logger.info(
                    "entry pipeline mode | requested=%s | effective=%s | fallback_reason=%s",
                    self._entry_pipeline_mode_requested,
                    self._entry_pipeline_mode,
                    self._entry_pipeline_mode_fallback_reason,
                )
        for instrument in self._subscribed_instruments:
            self.market_data_service.subscribe_l1(instrument, self.on_quote)
            self.logger.info("worker subscribed to L1 | exchange=%s | symbol=%s", instrument.exchange, instrument.symbol)
        with self._state_lock:
            if self._is_dual_execution_runtime or self._is_spread_entry_runtime:
                self._ensure_dual_execution_adapters()
            elif not self._is_dual_quotes_runtime:
                self._ensure_execution_adapter()
            self._update_activity_status()
            self._publish_state(force=True)
            self.emit_event("runtime_started" if self._is_dual_runtime else "worker_started", {"instruments": [instrument.symbol for instrument in self._subscribed_instruments], "left_instrument": self._left_instrument.symbol, "right_instrument": self._right_instrument.symbol})
        self._start_runtime_watchdog()

    def stop(self) -> None:
        self.logger.info("worker stop requested")
        deferred = self._state_publish_deferred_timer
        if deferred is not None:
            deferred.cancel()
            self._state_publish_deferred_timer = None
        self._watchdog_stop_event.set()
        watchdog_thread = self._watchdog_thread
        entry_recovery_thread = self._entry_recovery_thread
        exit_recovery_thread = self._exit_recovery_thread
        hedge_thread = self._hedge_protection_thread
        runtime_reconcile_thread = self._runtime_reconcile_thread
        for instrument in self._subscribed_instruments:
            self.market_data_service.unsubscribe_l1(instrument, self.on_quote)
        with self._state_lock:
            single_adapter = self._execution_adapter
            left_adapter = self._left_execution_adapter
            right_adapter = self._right_execution_adapter
            self._execution_adapter = None
            self._left_execution_adapter = None
            self._right_execution_adapter = None
        if single_adapter is not None:
            single_adapter.close()
        if left_adapter is not None:
            left_adapter.close()
        if right_adapter is not None and right_adapter is not left_adapter:
            right_adapter.close()
        with self._state_lock:
            self.state.status = "stopped"
            self.state.stopped_at = int(time.time() * 1000)
            self._simulated_entry_window_open = False
            self._simulated_exit_window_open = False
            self._simulated_entry_direction = None
            self._rebalance_grace_until_ms = 0
            self.state.metrics["simulated_entry_window_open"] = False
            self.state.metrics["simulated_exit_window_open"] = False
            self.state.metrics["simulated_entry_direction"] = None
            self.state.metrics["rebalance_grace_until_ms"] = None
            self.state.metrics["rebalance_grace_remaining_ms"] = 0
            self._entry_growth_limited = False
            self._entry_growth_limit_reason = None
            self._entry_growth_limit_notional_usdt = None
            self._entry_growth_limit_qty = None
            self.state.metrics["entry_growth_limited"] = False
            self.state.metrics["entry_growth_limit_reason"] = None
            self.state.metrics["entry_growth_limit_notional_usdt"] = None
            self.state.metrics["entry_growth_limit_qty"] = None
            self.entry_recovery_plan = None
            self.exit_recovery_plan = None
            self.hedge_recovery_plan = None
            self._entry_recovery_thread = None
            self._exit_recovery_thread = None
            self._hedge_protection_thread = None
            self._runtime_reconcile_thread = None
            self._entry_recovery_started_ms = None
            self._exit_recovery_started_ms = None
            self._hedge_protection_started_ms = None
            self._clear_recovery_status(context="ENTRY_CYCLE")
            self._clear_recovery_status(context="EXIT_CYCLE")
            self._clear_recovery_status(context="HEDGE_PROTECTION")
            self._clear_recovery_status(context="RECONCILE")
            self._runtime_health_mode = "STOPPED"
            self.state.metrics["runtime_health"] = self._runtime_health_mode
            self.state.metrics["runtime_owner"] = None
            self._update_activity_status()
            self._publish_state(force=True)
            self.emit_event("runtime_stopped" if self._is_dual_runtime else "worker_stopped", {})
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=1.0)
        for thread in (entry_recovery_thread, exit_recovery_thread, hedge_thread, runtime_reconcile_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=1.0)
        self._watchdog_thread = None
        self.logger.info("worker stop completed")

    def _request_deferred_entry_chain(self) -> None:
        self._deferred_entry_chain_requested = True
        self._schedule_deferred_runtime_actions()

    def _request_deferred_exit_chain(self) -> None:
        self._deferred_exit_chain_requested = True
        self._schedule_deferred_runtime_actions()

    def _request_hedge_protection_check(self, *, reason: str) -> None:
        with self._state_lock:
            if self.active_entry_cycle is not None or self.prefetch_entry_cycle is not None or self.active_exit_cycle is not None:
                return
            if self._cycle_recovery_active():
                return
            self._hedge_check_requested = True
            self._last_hedge_check_request_reason = str(reason or "").strip() or "UNSPECIFIED"
        self._schedule_deferred_runtime_actions()

    def _schedule_deferred_runtime_actions(self) -> None:
        with self._state_lock:
            if self._deferred_runtime_actions_scheduled:
                return
            self._deferred_runtime_actions_scheduled = True

        def _run() -> None:
            try:
                # Break call-chain recursion and run deferred actions out-of-band.
                time.sleep(0.01)
                if self.state.status != "running":
                    return
                self._run_deferred_runtime_actions()
            finally:
                reschedule = False
                with self._state_lock:
                    self._deferred_runtime_actions_scheduled = False
                    reschedule = bool(
                        self._deferred_entry_chain_requested
                        or self._deferred_exit_chain_requested
                        or self._hedge_check_requested
                    )
                if reschedule and self.state.status == "running":
                    self._schedule_deferred_runtime_actions()

        threading.Thread(
            target=_run,
            name=f"{self.task.worker_id}-deferred-actions",
            daemon=True,
        ).start()

    def _run_deferred_runtime_actions(self) -> None:
        run_entry = False
        run_exit = False
        run_hedge = False
        with self._state_lock:
            if self._deferred_entry_chain_requested:
                self._deferred_entry_chain_requested = False
                run_entry = True
            if self._deferred_exit_chain_requested:
                self._deferred_exit_chain_requested = False
                run_exit = True
            if self._hedge_check_requested:
                self._hedge_check_requested = False
                run_hedge = True
        if run_exit:
            self._evaluate_spread_exit()
        if run_hedge:
            self._maybe_start_hedge_protection()
        if run_entry:
            self._evaluate_spread_entry()

    def trigger_entry_signal(self) -> None:
        with self._state_lock:
            if not self._is_spread_entry_runtime:
                raise RuntimeError("Manual entry trigger is available only for spread entry runtime")
            self._forced_entry_signal_requested = True
            self.logger.info("manual entry trigger requested | mode=forced_signal")
        self._evaluate_spread_entry()

    @staticmethod
    def _normalize_strategy_signal_mode(mode: object) -> str:
        normalized = str(mode or "").strip().lower()
        return "simulated" if normalized == "simulated" else "market"

    @staticmethod
    def _normalize_entry_pipeline_mode(mode: object) -> str:
        return "strict"

    def _is_simulated_signal_mode(self) -> bool:
        return self._strategy_signal_mode == "simulated"

    def set_strategy_signal_mode(self, mode: str) -> None:
        should_evaluate_entry = False
        should_evaluate_exit = False
        with self._state_lock:
            if not self._is_spread_entry_runtime:
                raise RuntimeError("Strategy signal mode is available only for spread entry runtime")
            normalized_mode = self._normalize_strategy_signal_mode(mode)
            if normalized_mode == self._strategy_signal_mode:
                return
            self._strategy_signal_mode = normalized_mode
            self.state.metrics["strategy_signal_mode"] = normalized_mode
            if normalized_mode != "simulated":
                self._simulated_entry_window_open = False
                self._simulated_exit_window_open = False
                self.state.metrics["simulated_entry_window_open"] = False
                self.state.metrics["simulated_exit_window_open"] = False
            else:
                # Lock simulated direction as soon as simulated mode is selected.
                self._ensure_simulated_entry_direction_locked(reason="MODE_SWITCH")
            self.logger.info("strategy signal mode changed | mode=%s", normalized_mode)
            self._update_activity_status()
            self._publish_state()
            if normalized_mode == "simulated":
                should_evaluate_entry = self._simulated_entry_window_open
                should_evaluate_exit = self._simulated_exit_window_open
            else:
                should_evaluate_entry = True
                should_evaluate_exit = True
        if should_evaluate_entry:
            self._evaluate_spread_entry()
        if should_evaluate_exit:
            self._evaluate_spread_exit()

    def set_simulated_entry_window(self, enabled: bool) -> None:
        should_evaluate_entry = False
        with self._state_lock:
            if not self._is_spread_entry_runtime:
                raise RuntimeError("Simulated entry window is available only for spread entry runtime")
            self._simulated_entry_window_open = bool(enabled)
            if self._simulated_entry_window_open:
                self._ensure_simulated_entry_direction_locked(reason="WINDOW_OPEN")
            self.state.metrics["simulated_entry_window_open"] = self._simulated_entry_window_open
            self.state.metrics["simulated_entry_direction"] = self._simulated_entry_direction
            self.logger.info("simulated entry window toggled | enabled=%s", self._simulated_entry_window_open)
            if self._simulated_entry_window_open:
                self.logger.info("simulated entry direction locked | direction=%s", self._simulated_entry_direction)
            self._update_activity_status()
            self._publish_state()
            should_evaluate_entry = self._simulated_entry_window_open
        if should_evaluate_entry:
            self._evaluate_spread_entry()

    def _ensure_simulated_entry_direction_locked(self, *, reason: str) -> None:
        if not self._is_simulated_signal_mode():
            return
        if self._simulated_entry_direction is not None:
            return
        raw_edge_result = calculate_spread_edges(self._latest_quotes.get(self._left_instrument), self._latest_quotes.get(self._right_instrument))
        if not raw_edge_result.left_action or not raw_edge_result.right_action:
            return
        self._simulated_entry_direction = f"LEFT_{raw_edge_result.left_action}_RIGHT_{raw_edge_result.right_action}"
        self.state.metrics["simulated_entry_direction"] = self._simulated_entry_direction
        self.logger.info(
            "simulated entry direction locked | direction=%s | reason=%s",
            self._simulated_entry_direction,
            reason,
        )

    def set_simulated_exit_window(self, enabled: bool) -> None:
        should_evaluate_exit = False
        with self._state_lock:
            if not self._is_spread_entry_runtime:
                raise RuntimeError("Simulated exit window is available only for spread entry runtime")
            self._simulated_exit_window_open = bool(enabled)
            self.state.metrics["simulated_exit_window_open"] = self._simulated_exit_window_open
            self.logger.info("simulated exit window toggled | enabled=%s", self._simulated_exit_window_open)
            self._update_activity_status()
            self._publish_state()
            should_evaluate_exit = self._simulated_exit_window_open
        if should_evaluate_exit:
            self._evaluate_spread_exit()

    def on_execution_event(self, event: ExecutionStreamEvent) -> None:
        if event.symbol and event.symbol != self._active_instrument.symbol:
            return
        now_ms = int(time.time() * 1000)
        if self._pending_order_clock is not None and self._matches_pending_order(event):
            submitted_at_ms = int(self._pending_order_clock.get("submitted_at_ms") or now_ms)
            request_sent_at_ms = int(self._pending_order_clock.get("request_sent_at_ms") or submitted_at_ms)
            if not bool(self._pending_order_clock.get("first_event_seen")):
                self.state.metrics["last_first_event_latency_ms"] = max(0, now_ms - submitted_at_ms)
                self.state.metrics["last_send_to_first_event_latency_ms"] = max(0, now_ms - request_sent_at_ms)
                self._pending_order_clock["first_event_seen"] = True
            if event.order_status == "FILLED" and not bool(self._pending_order_clock.get("filled_seen")):
                self.state.metrics["last_fill_latency_ms"] = max(0, now_ms - submitted_at_ms)
                self.state.metrics["last_send_to_fill_latency_ms"] = max(0, now_ms - request_sent_at_ms)
                self._pending_order_clock["filled_seen"] = True
        self.state.metrics["last_execution_type"] = event.execution_type
        self.state.metrics["last_order_status"] = event.order_status
        self.state.metrics["last_fill_qty"] = event.last_fill_qty
        self.state.metrics["last_fill_price"] = event.last_fill_price or event.average_price
        self.state.metrics["last_realized_pnl"] = event.realized_pnl
        self._publish_state()
        self.emit_event("execution_event_received", event.to_dict())

    def _refresh_derived_metrics(self) -> None:
        self._refresh_execution_stream_health_metrics()
        if not self._is_dual_runtime:
            return
        now_ms = int(time.time() * 1000)
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        self.state.metrics["left_quote_age_ms"] = max(0, now_ms - int(left_quote.ts_local)) if left_quote is not None else None
        self.state.metrics["right_quote_age_ms"] = max(0, now_ms - int(right_quote.ts_local)) if right_quote is not None else None
        left_depth20 = self.market_data_service.get_depth20_snapshot(self._left_instrument)
        right_depth20 = self.market_data_service.get_depth20_snapshot(self._right_instrument)
        self.state.metrics["left_depth20_levels"] = len(left_depth20.bids) if left_depth20 is not None else 0
        self.state.metrics["right_depth20_levels"] = len(right_depth20.asks) if right_depth20 is not None else 0
        left_depth_diag = self.market_data_service.get_depth20_diagnostics(self._left_instrument)
        right_depth_diag = self.market_data_service.get_depth20_diagnostics(self._right_instrument)
        left_last_depth_ts = self._int_or_zero(left_depth_diag.get("last_depth_ts_ms")) or None
        right_last_depth_ts = self._int_or_zero(right_depth_diag.get("last_depth_ts_ms")) or None
        self.state.metrics["left_last_depth_ts_ms"] = left_last_depth_ts
        self.state.metrics["right_last_depth_ts_ms"] = right_last_depth_ts
        self.state.metrics["left_depth_age_ms"] = max(0, now_ms - left_last_depth_ts) if left_last_depth_ts is not None else None
        self.state.metrics["right_depth_age_ms"] = max(0, now_ms - right_last_depth_ts) if right_last_depth_ts is not None else None
        self.state.metrics["left_depth_updates_count"] = self._int_or_zero(left_depth_diag.get("depth_updates_count"))
        self.state.metrics["right_depth_updates_count"] = self._int_or_zero(right_depth_diag.get("depth_updates_count"))
        self.state.metrics["left_depth_reject_count"] = self._int_or_zero(left_depth_diag.get("depth_reject_count"))
        self.state.metrics["right_depth_reject_count"] = self._int_or_zero(right_depth_diag.get("depth_reject_count"))
        self.state.metrics["left_depth_last_reason"] = str(left_depth_diag.get("depth_last_reason") or "NO_DATA")
        self.state.metrics["right_depth_last_reason"] = str(right_depth_diag.get("depth_last_reason") or "NO_DATA")
        self.state.metrics["spread_state"] = "LIVE" if self._has_live_spread() else "WAITING_QUOTES"
        hedged_qty = min(self.left_leg_state.filled_qty, self.right_leg_state.filled_qty)
        if hedged_qty > Decimal("0") and left_quote is not None and right_quote is not None:
            expensive_price = max(left_quote.ask, right_quote.ask)
            self.state.metrics["current_position_notional_usdt"] = str((hedged_qty * expensive_price).quantize(Decimal("0.01")))
        else:
            self.state.metrics["current_position_notional_usdt"] = "0"
        remaining_grace_ms = max(0, self._rebalance_grace_until_ms - now_ms)
        self.state.metrics["rebalance_grace_until_ms"] = self._rebalance_grace_until_ms or None
        self.state.metrics["rebalance_grace_remaining_ms"] = remaining_grace_ms

    def _refresh_execution_stream_health_metrics(self) -> None:
        snapshot = self._build_execution_stream_health_snapshot()
        self.state.metrics["execution_stream_health"] = snapshot
        self.state.metrics["execution_stream_health_status"] = snapshot.get("status")

    def _build_execution_stream_health_snapshot(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        streams: dict[str, dict[str, Any]] = {}
        adapters = self._execution_health_adapters()
        for leg, adapter in adapters.items():
            streams[leg] = self._normalize_execution_adapter_health(leg=leg, adapter=adapter, now_ms=now_ms)
        status = self._derive_execution_stream_status(streams)
        warning = self._derive_execution_stream_warning(streams, now_ms=now_ms)
        return {
            "status": status,
            "updated_at_ms": now_ms,
            "streams": streams,
            "warning": warning,
        }

    def _execution_health_adapters(self) -> dict[str, ExecutionAdapter]:
        adapters: dict[str, ExecutionAdapter] = {}
        if self._execution_adapter is not None:
            adapters["primary"] = self._execution_adapter
        if self._left_execution_adapter is not None:
            adapters["left"] = self._left_execution_adapter
        if self._right_execution_adapter is not None:
            adapters["right"] = self._right_execution_adapter
        return adapters

    def _normalize_execution_adapter_health(
        self,
        *,
        leg: str,
        adapter: ExecutionAdapter,
        now_ms: int,
    ) -> dict[str, Any]:
        try:
            raw = adapter.diagnostics()
        except Exception as exc:
            return {
                "route": adapter.route_name(),
                "connected": False,
                "authenticated": None,
                "reconnect_attempts_total": 0,
                "last_error": str(exc),
                "reconnect_attempts_delta": 0,
                "disconnected_for_ms": 0,
            }
        details = dict(raw if isinstance(raw, dict) else {})
        connected_values: list[bool] = []
        authenticated_values: list[bool] = []
        reconnect_attempts_total = 0
        last_error: str | None = None
        last_ping_ts_ms = 0
        last_pong_ts_ms = 0
        ping_fail_count = 0
        for component in details.values():
            if not isinstance(component, dict):
                continue
            if "connected" in component:
                connected_values.append(bool(component.get("connected")))
            if "authenticated" in component:
                authenticated_values.append(bool(component.get("authenticated")))
            reconnect_attempts_total = max(
                reconnect_attempts_total,
                int(component.get("reconnect_attempts_total") or 0),
            )
            error_text = str(component.get("last_error_text") or "").strip()
            if error_text and not last_error:
                last_error = error_text
            last_ping_ts_ms = max(last_ping_ts_ms, int(component.get("last_ping_ts_ms") or 0))
            last_pong_ts_ms = max(last_pong_ts_ms, int(component.get("last_pong_ts_ms") or 0))
            ping_fail_count = max(ping_fail_count, int(component.get("ping_fail_count") or 0))
        if "connected" in details:
            connected_values.append(bool(details.get("connected")))
        if "authenticated" in details:
            authenticated_values.append(bool(details.get("authenticated")))
        reconnect_attempts_total = max(reconnect_attempts_total, int(details.get("reconnect_attempts_total") or 0))
        if not last_error:
            top_level_error = str(details.get("last_error_text") or "").strip()
            if top_level_error:
                last_error = top_level_error
        last_ping_ts_ms = max(last_ping_ts_ms, int(details.get("last_ping_ts_ms") or 0))
        last_pong_ts_ms = max(last_pong_ts_ms, int(details.get("last_pong_ts_ms") or 0))
        ping_fail_count = max(ping_fail_count, int(details.get("ping_fail_count") or 0))
        previous_reconnect = self._execution_stream_prev_reconnect.get(leg)
        self._execution_stream_prev_reconnect[leg] = (reconnect_attempts_total, now_ms)
        connected = True if not connected_values else all(connected_values)
        if not connected:
            self._execution_stream_disconnected_since_ms.setdefault(leg, now_ms)
        else:
            self._execution_stream_disconnected_since_ms.pop(leg, None)
        return {
            "route": str(details.get("route") or adapter.route_name()),
            "connected": connected,
            "authenticated": None if not authenticated_values else all(authenticated_values),
            "reconnect_attempts_total": reconnect_attempts_total,
            "last_error": last_error,
            "last_ping_ts_ms": last_ping_ts_ms or None,
            "last_pong_ts_ms": last_pong_ts_ms or None,
            "ping_fail_count": ping_fail_count,
            "reconnect_attempts_delta": self._reconnect_attempts_delta(previous_reconnect, reconnect_attempts_total),
            "disconnected_for_ms": (
                max(0, now_ms - self._execution_stream_disconnected_since_ms.get(leg, now_ms))
                if not connected
                else 0
            ),
        }

    @staticmethod
    def _reconnect_attempts_delta(previous: tuple[int, int] | None, current_count: int) -> int:
        if previous is None:
            return 0
        previous_count, _previous_at_ms = previous
        return max(0, current_count - previous_count)

    @staticmethod
    def _derive_execution_stream_status(streams: dict[str, dict[str, Any]]) -> str:
        if not streams:
            return "UNKNOWN"
        connected_values = [bool(item.get("connected")) for item in streams.values()]
        if all(connected_values):
            return "HEALTHY"
        if any(connected_values):
            return "DEGRADED"
        return "DISCONNECTED"

    def _derive_execution_stream_warning(self, streams: dict[str, dict[str, Any]], *, now_ms: int) -> dict[str, Any] | None:
        for leg, item in streams.items():
            reconnect_delta = int(item.get("reconnect_attempts_delta") or 0)
            if reconnect_delta >= 3:
                return {
                    "level": "warning",
                    "code": "RECONNECT_RATE_HIGH",
                    "leg": leg,
                    "message": f"{leg} reconnect attempts are growing quickly",
                    "timestamp": now_ms,
                }
            disconnected_for_ms = int(item.get("disconnected_for_ms") or 0)
            if disconnected_for_ms >= 15_000:
                return {
                    "level": "warning",
                    "code": "STREAM_DISCONNECTED_LONG",
                    "leg": leg,
                    "message": f"{leg} stream is disconnected for too long",
                    "timestamp": now_ms,
                    "disconnected_for_ms": disconnected_for_ms,
                }
        return None

    def _apply_dual_quote_metrics(self, quote: QuoteL1) -> None:
        if quote.instrument_id == self._left_instrument:
            self.state.metrics["left_bid"] = str(quote.bid)
            self.state.metrics["left_ask"] = str(quote.ask)
            self.state.metrics["left_quote_ts"] = int(quote.ts_local)
        elif quote.instrument_id == self._right_instrument:
            self.state.metrics["right_bid"] = str(quote.bid)
            self.state.metrics["right_ask"] = str(quote.ask)
            self.state.metrics["right_quote_ts"] = int(quote.ts_local)
        self._recompute_spread_metrics()

    def _recompute_spread_metrics(self) -> None:
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        edge_result = calculate_spread_edges(left_quote, right_quote)
        if edge_result.best_edge is None:
            self.state.metrics["edge_1"] = None
            self.state.metrics["edge_2"] = None
            self.state.metrics["best_edge"] = None
            self.state.metrics["left_action"] = None
            self.state.metrics["right_action"] = None
            self.state.metrics["spread_state"] = "WAITING_QUOTES"
            return
        self.state.metrics["edge_1"] = self._format_edge(edge_result.edge_1)
        self.state.metrics["edge_2"] = self._format_edge(edge_result.edge_2)
        self.state.metrics["best_edge"] = self._format_edge(edge_result.best_edge)
        self.state.metrics["left_action"] = edge_result.left_action
        self.state.metrics["right_action"] = edge_result.right_action
        self.state.metrics["spread_state"] = "LIVE"
        if self._is_spread_entry_runtime and self._is_simulated_signal_mode():
            self._ensure_simulated_entry_direction_locked(reason="SESSION_START")
        if self._is_spread_entry_runtime:
            validation_result = self._build_entry_validation_result(edge_result)
            self._maybe_log_spread_recalculated(edge_result)
            self._maybe_log_validation_result(validation_result)

    def _maybe_log_spread_recalculated(self, edge_result: SpreadEdgeResult) -> None:
        signature = (
            self._format_edge(edge_result.edge_1),
            self._format_edge(edge_result.edge_2),
            self._format_edge(edge_result.best_edge),
            edge_result.direction,
            edge_result.left_action,
            edge_result.right_action,
        )
        if signature == self._last_spread_log_signature:
            return
        self._last_spread_log_signature = signature
        self.logger.debug(
            "spread recalculated | edge_1=%s | edge_2=%s | best_edge=%s | direction=%s | left_action=%s | right_action=%s | left_quote_age_ms=%s | right_quote_age_ms=%s",
            signature[0],
            signature[1],
            signature[2],
            signature[3],
            signature[4],
            signature[5],
            self.state.metrics.get("left_quote_age_ms"),
            self.state.metrics.get("right_quote_age_ms"),
        )

    def _maybe_log_validation_result(self, validation_result: EntryValidationResult) -> None:
        signature = (
            validation_result.is_valid,
            validation_result.block_reason,
            validation_result.left_valid,
            validation_result.right_valid,
            validation_result.liquidity_ok,
            validation_result.fresh_ok,
            validation_result.left_liquidity_ok,
            validation_result.right_liquidity_ok,
        )
        now_ms = int(time.time() * 1000)
        normalized_reason = str(validation_result.block_reason or "").strip().upper()
        min_interval_ms = self.VALIDATION_LOG_INTERVAL_MS
        if normalized_reason in {"WAITING_QUOTES", "LEFT_STALE_QUOTE", "RIGHT_STALE_QUOTE"}:
            min_interval_ms = self.VALIDATION_STALE_LOG_INTERVAL_MS
        elif not bool(validation_result.is_valid):
            min_interval_ms = self.VALIDATION_LOG_INTERVAL_MS
        if signature == self._last_validation_log_signature and (now_ms - self._last_validation_log_at_ms) < min_interval_ms:
            return
        self._last_validation_log_signature = signature
        self._last_validation_log_at_ms = now_ms
        self.logger.info(
            "validation result | freshness_threshold_ms=%s | is_valid=%s | reason=%s | left_valid=%s | right_valid=%s | liquidity_ok=%s | fresh_ok=%s",
            self._entry_freshness_threshold_ms,
            validation_result.is_valid,
            validation_result.block_reason,
            validation_result.left_valid,
            validation_result.right_valid,
            validation_result.liquidity_ok,
            validation_result.fresh_ok,
        )

    def _maybe_log_entry_recovery_waiting(self, *, settle_grace_ms: int, left_status: str, right_status: str) -> None:
        signature = (left_status, right_status, int(settle_grace_ms // 250))
        now_ms = int(time.time() * 1000)
        if signature == self._last_entry_recovery_wait_log_signature and (now_ms - self._last_entry_recovery_wait_log_at_ms) < 1000:
            return
        self._last_entry_recovery_wait_log_signature = signature
        self._last_entry_recovery_wait_log_at_ms = now_ms
        self.logger.info(
            "entry recovery waiting for settle grace | remaining_ms=%s | left_status=%s | right_status=%s",
            settle_grace_ms,
            left_status,
            right_status,
        )

    def _maybe_log_hedge_resync(self, *, reason: str, left_qty: Decimal, right_qty: Decimal, mismatch: Decimal) -> None:
        mismatch_fmt = self._format_order_size(mismatch)
        signature = (
            reason,
            self._format_order_size(left_qty),
            self._format_order_size(right_qty),
            mismatch_fmt,
        )
        now_ms = int(time.time() * 1000)
        if mismatch <= Decimal("0"):
            if signature == self._last_hedge_resync_log_signature and (now_ms - self._last_hedge_resync_log_at_ms) < 5000:
                return
        elif signature == self._last_hedge_resync_log_signature and (now_ms - self._last_hedge_resync_log_at_ms) < 1000:
            return
        self._last_hedge_resync_log_signature = signature
        self._last_hedge_resync_log_at_ms = now_ms
        self.logger.info(
            "hedge protection resynced | reason=%s | left_actual_position_qty=%s | right_actual_position_qty=%s | qty_mismatch=%s",
            reason,
            signature[1],
            signature[2],
            mismatch_fmt,
        )

    def _maybe_log_entry_cycle_clamp(
        self,
        *,
        reason: str,
        base_cycle_notional: Decimal,
        effective_cycle_notional: Decimal,
        position_cap_notional: Decimal,
        current_position_notional: Decimal,
        remaining_notional: Decimal,
        growth_limit_qty: Decimal | None = None,
        current_qty: Decimal | None = None,
        remaining_qty: Decimal | None = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        reason_signature = (reason, bool(effective_cycle_notional <= Decimal("0")))
        if reason_signature == self._last_entry_cycle_clamp_reason_signature:
            if (now_ms - self._last_entry_cycle_clamp_log_at_ms) < self.ENTRY_CYCLE_CLAMP_LOG_INTERVAL_MS:
                return
        signature = (
            reason,
            self._format_order_size(base_cycle_notional),
            self._format_order_size(effective_cycle_notional),
            self._format_order_size(position_cap_notional),
            self._format_order_size(current_position_notional),
            self._format_order_size(remaining_notional),
            self._format_order_size(growth_limit_qty or Decimal("0")),
            self._format_order_size(current_qty or Decimal("0")),
            self._format_order_size(remaining_qty or Decimal("0")),
        )
        if signature == self._last_entry_cycle_clamp_signature:
            return
        self._last_entry_cycle_clamp_reason_signature = reason_signature
        self._last_entry_cycle_clamp_log_at_ms = now_ms
        self._last_entry_cycle_clamp_signature = signature
        self.logger.info(
            "entry cycle clamped | reason=%s | base_cycle_notional_usdt=%s | effective_cycle_notional_usdt=%s | position_cap_notional_usdt=%s | current_position_notional_usdt=%s | remaining_notional_usdt=%s | growth_limit_qty=%s | current_qty=%s | remaining_qty=%s",
            signature[0],
            signature[1],
            signature[2],
            signature[3],
            signature[4],
            signature[5],
            signature[6],
            signature[7],
            signature[8],
        )

    def _entry_recovery_blocked_by_grace(self, *, left_status: str, right_status: str) -> bool:
        return entry_recovery_blocked_by_grace(self, left_status=left_status, right_status=right_status)

    def _entry_recovery_blocked_by_live_order(self, *, left_status: str, right_status: str) -> bool:
        return entry_recovery_blocked_by_live_order(self, left_status=left_status, right_status=right_status)

    def _evaluate_spread_entry(self) -> None:
        evaluate_spread_entry(self)

    def _handle_entry_submit_failure(self, exc: Exception) -> None:
        handle_entry_submit_failure(self, exc)

    def _build_entry_validation_result(self, edge_result: SpreadEdgeResult) -> EntryValidationResult:
        return build_entry_validation_result(self, edge_result)

    def _should_enforce_entry_liquidity_check(self) -> bool:
        return should_enforce_entry_liquidity_check(self)

    def _build_entry_decision(self) -> EntryDecision | None:
        return self._runtime_policy.build_entry_decision(self)

    def _policy_allow_entry_evaluation(self) -> bool:
        return bool(self._runtime_policy.allow_entry_evaluation(self))

    def _has_live_spread(self) -> bool:
        return self._latest_quotes.get(self._left_instrument) is not None and self._latest_quotes.get(self._right_instrument) is not None

    @staticmethod
    def _safe_edge(numerator_left: Decimal, denominator_right: Decimal) -> Decimal | None:
        return safe_edge(numerator_left, denominator_right)

    @staticmethod
    def _format_edge(value: Decimal | None) -> str | None:
        return format_edge(value)

    def _ensure_dual_execution_adapters(self) -> dict[str, ExecutionAdapter]:
        if self._left_execution_adapter is None:
            self._left_execution_adapter = self._create_execution_adapter(instrument=self._left_instrument, credentials=self.task.left_execution_credentials)
            self._log_selected_execution_route("left", self._left_instrument, self._left_execution_adapter)
            self._left_execution_adapter.connect()
            self._left_execution_adapter.on_execution_event(lambda event, leg_name="left": self._on_dual_execution_event(leg_name, event))
        if self._right_execution_adapter is None:
            self._right_execution_adapter = self._create_execution_adapter(instrument=self._right_instrument, credentials=self.task.right_execution_credentials)
            self._log_selected_execution_route("right", self._right_instrument, self._right_execution_adapter)
            self._right_execution_adapter.connect()
            self._right_execution_adapter.on_execution_event(lambda event, leg_name="right": self._on_dual_execution_event(leg_name, event))
        return {"left": self._left_execution_adapter, "right": self._right_execution_adapter}

    def _ensure_execution_adapter(self) -> ExecutionAdapter:
        if self._execution_adapter is not None:
            return self._execution_adapter
        credentials = self.task.execution_credentials
        if credentials is None:
            raise RuntimeError("Execution credentials are not configured for worker")
        adapter = self._create_execution_adapter(instrument=self._active_instrument, credentials=credentials)
        self._log_selected_execution_route("single", self._active_instrument, adapter)
        adapter.connect()
        adapter.on_execution_event(self.on_execution_event)
        self._execution_adapter = adapter
        return adapter

    def _create_execution_adapter(self, *, instrument: InstrumentId, credentials) -> ExecutionAdapter:
        if credentials is None:
            raise RuntimeError(f"Execution credentials are not configured for {instrument.exchange}")
        from app.core.execution.binance_usdm_adapter import BinanceUsdmExecutionAdapter
        from app.core.execution.bybit_linear_adapter import BybitLinearExecutionAdapter
        from app.core.execution.bitget_linear_adapter import BitgetLinearExecutionAdapter
        from app.core.execution.bitget_linear_rest_adapter import BitgetLinearRestExecutionAdapter
        if instrument.exchange == "binance" and instrument.market_type == "linear_perp":
            return BinanceUsdmExecutionAdapter(credentials)
        if instrument.exchange == "bybit" and instrument.market_type == "linear_perp":
            return BybitLinearExecutionAdapter(credentials)
        if instrument.exchange == "bitget" and instrument.market_type == "linear_perp":
            if self._is_spread_entry_runtime:
                return BitgetLinearRestExecutionAdapter(credentials)
            selected_route = str(credentials.account_profile.get("selected_execution_route") or credentials.account_profile.get("preferred_execution_route") or "").strip().lower()
            if selected_route == "bitget_linear_rest_probe":
                return BitgetLinearRestExecutionAdapter(credentials)
            return BitgetLinearExecutionAdapter(credentials)
        raise RuntimeError(f"No execution adapter for {instrument.exchange}:{instrument.market_type}")

    def _log_selected_execution_route(self, leg_name: str, instrument: InstrumentId, adapter: ExecutionAdapter) -> None:
        self.logger.info(
            "execution route selected | leg=%s | exchange=%s | symbol=%s | run_mode=%s | route=%s",
            leg_name,
            instrument.exchange,
            instrument.symbol,
            self._run_mode,
            adapter.route_name(),
        )

    def _log_entry_decision(
        self,
        *,
        decision: EntryDecision,
    ) -> bool:
        if decision.block_reason is None:
            self._reset_entry_block_log_dedupe()
        elif not self._should_log_entry_block_reason(decision.block_reason):
            return False
        validation_result = decision.validation_result
        self.logger.info(
            "entry decision | best_edge=%s | entry_threshold=%s | direction=%s | entry_state=%s | block_reason=%s | freshness_threshold_ms=%s | left_quote_age_ms=%s | right_quote_age_ms=%s | left_liquidity_ok=%s | right_liquidity_ok=%s",
            self._format_edge(decision.edge),
            self._format_edge(decision.threshold),
            decision.direction,
            self.strategy_state.value,
            decision.block_reason,
            self._entry_freshness_threshold_ms,
            self.state.metrics.get("left_quote_age_ms"),
            self.state.metrics.get("right_quote_age_ms"),
            validation_result.get("left_liquidity_ok"),
            validation_result.get("right_liquidity_ok"),
        )
        return True

    def _should_log_entry_block_reason(self, reason: str) -> bool:
        now_ms = int(time.time() * 1000)
        state_value = self.strategy_state.value
        normalized_reason = str(reason or "").strip().upper()
        min_interval_ms = 2000
        if normalized_reason in {
            "SIMULATED_ENTRY_WINDOW_CLOSED",
            "WAITING_QUOTES",
            "LEFT_STALE_QUOTE",
            "RIGHT_STALE_QUOTE",
        }:
            # Noisy background states: keep first log, then throttle harder.
            min_interval_ms = 5000
        should_log = (
            reason != self._last_entry_block_log_reason
            or state_value != self._last_entry_block_log_state
            or (now_ms - self._last_entry_block_log_at_ms) >= min_interval_ms
        )
        if should_log:
            self._last_entry_block_log_reason = reason
            self._last_entry_block_log_state = state_value
            self._last_entry_block_log_at_ms = now_ms
        return should_log

    def _reset_entry_block_log_dedupe(self) -> None:
        self._last_entry_block_log_reason = None
        self._last_entry_block_log_state = None
        self._last_entry_block_log_at_ms = 0

    def _log_entry_blocked(self, reason: str, *, decision_logged: bool) -> None:
        if decision_logged:
            self.logger.info("entry blocked | reason=%s", reason)

    def _set_strategy_state(self, new_state: StrategyState) -> None:
        with self._state_lock:
            if self.strategy_state is new_state:
                return
            previous = self.strategy_state
            self.strategy_state = new_state
            self.entry_state = new_state
            self.state.metrics["strategy_state"] = new_state.value
            self.state.metrics["entry_state"] = new_state.value
            self._update_activity_status()
            self.logger.info("strategy state transition | from=%s | to=%s", previous.value, new_state.value)

    def _update_activity_status(self) -> None:
        status = "STOPPED"
        if self.state.status == "running":
            if self.strategy_state in {StrategyState.ENTRY_ARMED, StrategyState.ENTRY_SUBMITTING, StrategyState.ENTRY_PARTIAL}:
                status = "ENTERING"
            elif self.strategy_state in {StrategyState.EXIT_ARMED, StrategyState.EXIT_SUBMITTING, StrategyState.EXIT_PARTIAL}:
                status = "EXITING"
            elif self.strategy_state is StrategyState.RECOVERY:
                recovery_state = str(self.state.metrics.get("recovery_state") or "")
                if recovery_state.startswith("HEDGE_PROTECTION"):
                    status = "REBALANCING"
                elif recovery_state.startswith("EXIT_RESTORE"):
                    status = "RESTORE_HEDGE"
                elif recovery_state.endswith("ABORT"):
                    status = "EMERGENCY_CLOSE"
                else:
                    status = "RECOVERY"
            elif self.strategy_state is StrategyState.FAILED:
                status = "FAILED"
            elif self.position is not None:
                status = "WAITING_EXIT"
            else:
                status = "WAITING_ENTRY"
        self.state.metrics["activity_status"] = status

    @staticmethod
    def _is_margin_limit_error(error_text: str | None) -> bool:
        normalized = str(error_text or "").strip().lower()
        if not normalized:
            return False
        return "margin is insufficient" in normalized or "insufficient margin" in normalized or "insufficient balance" in normalized

    def _exit_signal_active(self) -> bool:
        return exit_signal_active(self)

    def _maybe_restore_in_position_state(self) -> bool:
        return maybe_restore_in_position_state(self)

    def _update_strategy_state_from_entry_attempt(self, left_status: str, right_status: str) -> None:
        update_entry_attempt_state(self, left_status, right_status)

    def _update_strategy_state_from_exit_attempt(self, left_status: str, right_status: str) -> None:
        update_exit_attempt_state(self, left_status, right_status)

    def _entry_attempt_result_signature(self, *, result: str, left_status: str, right_status: str) -> tuple[Any, ...]:
        return entry_attempt_result_signature(self, result=result, left_status=left_status, right_status=right_status)

    def _should_log_entry_attempt_result(self, *, result: str, left_status: str, right_status: str) -> bool:
        return should_log_entry_attempt_result(self, result=result, left_status=left_status, right_status=right_status)

    def _evaluate_spread_exit(self) -> None:
        evaluate_spread_exit(self)

    def _build_exit_decision(self) -> dict[str, Any] | None:
        return self._runtime_policy.build_exit_decision(self)

    def _policy_allow_exit_evaluation(self) -> bool:
        return bool(self._runtime_policy.allow_exit_evaluation(self))

    def _current_exit_edge(self) -> Decimal | None:
        return current_exit_edge(self)

    def _exit_sides_for_position(self) -> tuple[str | None, str | None]:
        return exit_sides_for_position(self)

    def _planned_exit_cycle_sizes(self, *, left_side: str, right_side: str) -> tuple[Decimal, Decimal]:
        return planned_exit_cycle_sizes(self, left_side=left_side, right_side=right_side)

    def _reset_position_state(self) -> None:
        reset_position_state(self)

    def _is_exit_full_success(self, left_status: str, right_status: str) -> bool:
        return is_exit_full_success(self, left_status, right_status)

    def _is_exit_full_fail(self, left_status: str, right_status: str) -> bool:
        return is_exit_full_fail(self, left_status, right_status)

    def _is_exit_partial(self, left_status: str, right_status: str) -> bool:
        return is_exit_partial(self, left_status, right_status)

    def _exit_has_any_close_fill(self) -> bool:
        return exit_has_any_close_fill(self)

    def _exit_cycle_leg_matches_target(self, leg_name: str) -> bool:
        return exit_cycle_leg_matches_target(self, leg_name)

    def _is_entry_attempt_active(self, left_status: str, right_status: str) -> bool:
        return is_entry_attempt_active(self, left_status, right_status)

    def _is_entry_full_success(self, left_status: str, right_status: str) -> bool:
        return is_entry_full_success(self, left_status, right_status)

    def _is_entry_full_fail(self, left_status: str, right_status: str) -> bool:
        return is_entry_full_fail(self, left_status, right_status)

    def _is_entry_partial(self, left_status: str, right_status: str) -> bool:
        return is_entry_partial(self, left_status, right_status)

    def _has_any_entry_fill(self) -> bool:
        return has_any_entry_fill(self)

    def _leg_fill_matches_target(self, leg_name: str) -> bool:
        return leg_fill_matches_target(self, leg_name)

    def _entry_cycle_pair_matches_target(self) -> bool:
        return entry_cycle_pair_matches_target(self)

    @staticmethod
    def _qty_matches_target(*, target_qty: Decimal, filled_qty: Decimal, tolerance_qty: Decimal) -> bool:
        return qty_matches_target(target_qty=target_qty, filled_qty=filled_qty, tolerance_qty=tolerance_qty)

    def _is_exit_cycle_committed_success(self) -> bool:
        return is_exit_cycle_committed_success(self)

    @staticmethod
    def _is_no_position_to_close_error(error_text: str | None) -> bool:
        return is_no_position_to_close_error(error_text)

    def _mark_leg_flat_confirmed(self, leg_name: str) -> None:
        mark_leg_flat_confirmed(self, leg_name)

    def _exit_tail_resync_in_progress(self) -> bool:
        return exit_tail_resync_in_progress(self)

    def _exit_recovery_blocked_by_grace(self, *, left_status: str, right_status: str) -> bool:
        return exit_recovery_blocked_by_grace(self, left_status=left_status, right_status=right_status)

    def _exit_recovery_allowed(self) -> bool:
        return exit_recovery_allowed(self)

    def _entry_leg_target_total_qty(self, leg_name: str) -> Decimal:
        return entry_leg_target_total_qty(self, leg_name)

    def _entry_pipeline_overlap_enabled(self) -> bool:
        return entry_pipeline_overlap_enabled(self)

    def _entry_cycle_ack_ready(self, cycle: StrategyCycle | None) -> bool:
        return entry_cycle_ack_ready(self, cycle)

    def _entry_cycle_order_key(self, *, cycle: StrategyCycle, leg_name: str) -> str | None:
        return entry_cycle_order_key(self, cycle=cycle, leg_name=leg_name)

    def _entry_cycle_leg_filled_qty(self, *, cycle: StrategyCycle, leg_name: str) -> Decimal:
        return entry_cycle_leg_filled_qty(self, cycle=cycle, leg_name=leg_name)

    def _resolve_entry_cycle_for_submit(self, cycle_id: int | None) -> StrategyCycle | None:
        return resolve_entry_cycle_for_submit(self, cycle_id)

    def _entry_pipeline_freeze(self, *, reason: str) -> None:
        entry_pipeline_freeze(self, reason=reason)

    def _drop_entry_cycle_order_keys(self, *, cycle_id: int) -> None:
        drop_entry_cycle_order_keys(self, cycle_id=cycle_id)

    def _drop_exit_cycle_order_keys(self, *, cycle_id: int) -> None:
        drop_exit_cycle_order_keys(self, cycle_id=cycle_id)

    def _entry_pipeline_busy_reason(self) -> str | None:
        return entry_pipeline_busy_reason(self)

    def _entry_pipeline_maybe_thaw(self) -> bool:
        return entry_pipeline_maybe_thaw(self)

    def _exit_cycle_order_key(self, *, cycle: StrategyCycle, leg_name: str) -> str | None:
        return exit_cycle_order_key(self, cycle=cycle, leg_name=leg_name)

    def _resolve_exit_cycle_for_submit(self, cycle_id: int | None) -> StrategyCycle | None:
        return resolve_exit_cycle_for_submit(self, cycle_id)

    def _entry_pipeline_inflight_cycle_ids(self) -> list[int]:
        return entry_pipeline_inflight_cycle_ids(self)

    def _enforce_entry_pipeline_inflight_invariant(self) -> None:
        enforce_entry_pipeline_inflight_invariant(self)

    def _entry_has_imbalance(self) -> bool:
        return entry_has_imbalance(self)

    def _max_leg_imbalance_notional_usdt(self) -> Decimal:
        return max_leg_imbalance_notional_usdt(self)

    def _resolve_max_leg_imbalance_notional_usdt(self) -> Decimal:
        return resolve_max_leg_imbalance_notional_usdt(self)

    def _entry_leg_imbalance_notional_usdt(self) -> Decimal:
        return entry_leg_imbalance_notional_usdt(self)

    def _matches_pending_order(self, event: ExecutionStreamEvent) -> bool:
        pending = self._pending_order_clock
        if pending is None:
            return False
        if event.order_id is not None and pending.get("order_id") is not None:
            return event.order_id == pending.get("order_id")
        if event.client_order_id and pending.get("client_order_id"):
            return event.client_order_id == pending.get("client_order_id")
        return event.symbol == pending.get("symbol")

    def _unique_instruments(self) -> list[InstrumentId]:
        instruments: list[InstrumentId] = []
        for instrument in (self.task.left_instrument, self.task.right_instrument):
            if instrument not in instruments:
                instruments.append(instrument)
        return instruments
