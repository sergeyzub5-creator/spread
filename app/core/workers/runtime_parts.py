from __future__ import annotations

import threading
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

from app.core.execution.adapter import ExecutionAdapter
from app.core.execution.binance_usdm_adapter import BinanceUsdmExecutionAdapter
from app.core.execution.bitget_linear_adapter import BitgetLinearExecutionAdapter
from app.core.execution.bitget_linear_rest_adapter import BitgetLinearRestExecutionAdapter
from app.core.execution.bybit_linear_adapter import BybitLinearExecutionAdapter
from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1
from app.core.logging.logger_factory import append_runtime_event
from app.core.models.workers import StrategyState, WorkerEvent


class WorkerRuntimePartsMixin:
    def execution_stream_health_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            snapshot = self.state.metrics.get("execution_stream_health")
            return dict(snapshot) if isinstance(snapshot, dict) else {}

    def on_quote(self, quote: QuoteL1) -> None:
        should_reevaluate_spread_execution = False
        should_restore_in_position = False
        should_check_hedge_protection = False
        should_evaluate_exit = False
        should_evaluate_entry = False
        should_emit_quote_received = False
        startup_order_actions_blocked = False
        execution_order_actions_blocked = False
        with self._state_lock:
            first_quote_for_instrument = quote.instrument_id not in self._latest_quotes
            self._latest_quotes[quote.instrument_id] = quote
            self.state.metrics["quote_count"] = int(self.state.metrics.get("quote_count", 0) or 0) + 1
            self.state.metrics["last_quote_ts_local"] = quote.ts_local
            if quote.instrument_id == self._active_instrument:
                self.state.metrics["bid"] = str(quote.bid)
                self.state.metrics["ask"] = str(quote.ask)
                self.state.metrics["bid_qty"] = str(quote.bid_qty)
                self.state.metrics["ask_qty"] = str(quote.ask_qty)
            if self._is_dual_runtime:
                self._apply_dual_quote_metrics(quote)
            if first_quote_for_instrument:
                self.logger.info("worker first quote received | symbol=%s | bid=%s | ask=%s | ts_exchange=%s | ts_local=%s", quote.instrument_id.symbol, quote.bid, quote.ask, quote.ts_exchange, quote.ts_local)
            self._publish_state()
            if self._is_dual_runtime:
                if quote.instrument_id == self._left_instrument:
                    self.emit_event("left_quote_update", {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange})
                elif quote.instrument_id == self._right_instrument:
                    self.emit_event("right_quote_update", {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange})
                if self._has_live_spread():
                    self.emit_event("spread_update", {"edge_1": self.state.metrics.get("edge_1"), "edge_2": self.state.metrics.get("edge_2"), "spread_state": self.state.metrics.get("spread_state")})
                    if getattr(self, "_mid_alarm_enabled", False):
                        self._mid_alarm_tick()
                if self._is_spread_entry_runtime:
                    startup_order_actions_blocked = (
                        self.position is None
                        and self.active_entry_cycle is None
                        and self.prefetch_entry_cycle is None
                        and self.active_exit_cycle is None
                        and self._startup_entry_gate_block_reason() is not None
                    )
                    execution_order_actions_blocked = (
                        not startup_order_actions_blocked
                        and self._execution_order_actions_block_reason(
                            require_depth20=(
                                self.position is None
                                and self.active_entry_cycle is None
                                and self.prefetch_entry_cycle is None
                                and self.active_exit_cycle is None
                            )
                        ) is not None
                    )
                    if not startup_order_actions_blocked and not execution_order_actions_blocked:
                        should_reevaluate_spread_execution = True
                        should_restore_in_position = True
                        should_check_hedge_protection = True
                        if self.strategy_state is StrategyState.IN_POSITION:
                            should_evaluate_exit = True
                            should_evaluate_entry = self.active_exit_cycle is None
                        else:
                            should_evaluate_entry = True
            else:
                should_emit_quote_received = True
        if should_emit_quote_received:
            self.emit_event("quote_received", {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange})
        if should_reevaluate_spread_execution:
            self._reevaluate_active_spread_execution()
        if should_restore_in_position:
            self._maybe_restore_in_position_state()
        if should_check_hedge_protection:
            self._request_hedge_protection_check(reason="QUOTE_UPDATE")
        if should_evaluate_exit:
            self._evaluate_spread_exit()
        if should_evaluate_entry:
            with self._state_lock:
                can_evaluate_entry = not (self.strategy_state is StrategyState.IN_POSITION and self.active_exit_cycle is not None)
            if can_evaluate_entry:
                self._evaluate_spread_entry()

    def submit_test_order(self, side: str, submitted_at_ms: int | None = None) -> dict[str, Any]:
        try:
            adapter = self._ensure_execution_adapter()
            quote = self._latest_quotes.get(self._active_instrument)
            if quote is None:
                raise RuntimeError("No live quote for active instrument")
            side_upper = str(side or "").strip().upper()
            if side_upper not in {"BUY", "SELL"}:
                raise ValueError(f"Unsupported side: {side}")
            reference_price = quote.ask if side_upper == "BUY" else quote.bid
            if reference_price <= Decimal("0"):
                raise RuntimeError("Invalid reference price")
            quantity = self._compute_order_quantity(target_notional=self.task.target_notional, reference_price=reference_price, step_size=self._active_instrument.spec.qty_precision, min_qty=self._active_instrument.spec.min_qty)
            request = ExecutionOrderRequest(instrument_id=self._active_instrument, side=side_upper, order_type="MARKET", quantity=quantity, response_type="RESULT")
            send_started_ms = int(submitted_at_ms or int(time.time() * 1000))
            self._pending_order_clock = {"submitted_at_ms": send_started_ms, "symbol": self._active_instrument.symbol, "first_event_seen": False, "filled_seen": False, "order_id": None, "client_order_id": None, "request_sent_at_ms": None, "connection_reused": None}
            def _on_request_sent(meta: dict[str, Any]) -> None:
                request_sent_at_ms = int(meta.get("sent_at_ms") or int(time.time() * 1000))
                if self._pending_order_clock is not None:
                    self._pending_order_clock["request_sent_at_ms"] = request_sent_at_ms
                    self._pending_order_clock["connection_reused"] = bool(meta.get("connection_reused"))
                self.state.metrics["last_click_to_send_latency_ms"] = max(0, request_sent_at_ms - send_started_ms)
                self.state.metrics["last_transport_connection_mode"] = "warm" if bool(meta.get("connection_reused")) else "cold"
            ack = adapter.place_order(request, on_request_sent=_on_request_sent)
            self.state.last_error = None
            self.state.metrics["last_order_ack_status"] = ack.status
            self.state.metrics["last_order_id"] = ack.order_id
            ack_meta = ack.raw.get("_transport_meta", {}) if isinstance(ack.raw, dict) else {}
            response_at_ms = int(ack_meta.get("response_at_ms") or int(time.time() * 1000))
            request_sent_at_ms = int(ack_meta.get("sent_at_ms") or send_started_ms)
            self.state.metrics["last_ack_latency_ms"] = max(0, response_at_ms - send_started_ms)
            self.state.metrics["last_send_to_ack_latency_ms"] = max(0, response_at_ms - request_sent_at_ms)
            if self._pending_order_clock is not None:
                self._pending_order_clock["order_id"] = ack.order_id
                self._pending_order_clock["client_order_id"] = ack.client_order_id
                self._pending_order_clock["request_sent_at_ms"] = request_sent_at_ms
            self._publish_state()
            self.emit_event("order_ack_received", {"symbol": ack.symbol, "side": ack.side, "status": ack.status, "order_id": ack.order_id, "client_order_id": ack.client_order_id, "ack_latency_ms": self.state.metrics["last_ack_latency_ms"], "click_to_send_latency_ms": self.state.metrics["last_click_to_send_latency_ms"], "send_to_ack_latency_ms": self.state.metrics["last_send_to_ack_latency_ms"], "connection_mode": self.state.metrics["last_transport_connection_mode"]})
            return ack.to_dict()
        except Exception as exc:
            self.state.last_error = str(exc)
            self._publish_state()
            self.emit_event("order_failed", {"symbol": self._active_instrument.symbol, "side": str(side or "").strip().upper(), "error": str(exc)})
            self.logger.error("worker test order failed | symbol=%s | error=%s", self._active_instrument.symbol, exc)
            raise

    def submit_dual_test_orders(
        self,
        *,
        left_side: str,
        right_side: str,
        left_qty: str,
        right_qty: str,
        left_price_mode: str = "top_of_book",
        right_price_mode: str = "top_of_book",
        submitted_at_ms: int | None = None,
        entry_cycle_id: int | None = None,
        exit_cycle_id: int | None = None,
    ) -> dict[str, Any]:
        if not (self._is_dual_execution_runtime or self._is_spread_entry_runtime):
            raise RuntimeError("Dual execution is not enabled for this runtime")
        active_cycle_id: int | None = None
        is_entry_submit = False
        is_exit_submit = False
        try:
            submit_entry_cycle = self._resolve_entry_cycle_for_submit(entry_cycle_id)
            submit_exit_cycle = self._resolve_exit_cycle_for_submit(exit_cycle_id)
            is_entry_submit = self._is_spread_entry_runtime and submit_entry_cycle is not None
            is_exit_submit = self._is_spread_entry_runtime and submit_exit_cycle is not None
            overlap_prefetch_submit = bool(
                self._is_spread_entry_runtime
                and self._entry_pipeline_overlap_enabled()
                and (
                    (is_entry_submit and submit_entry_cycle is not None and self.prefetch_entry_cycle is submit_entry_cycle and self.active_entry_cycle is not None)
                    or (is_exit_submit and submit_exit_cycle is not None and self.prefetch_exit_cycle is submit_exit_cycle and self.active_exit_cycle is not None)
                )
            )
            if self._has_active_execution_owner_context() and self._dual_execution_in_progress() and not overlap_prefetch_submit:
                raise RuntimeError("Dual execution is already in progress")
            left_quote = self._latest_quotes.get(self._left_instrument)
            right_quote = self._latest_quotes.get(self._right_instrument)
            if left_quote is None or right_quote is None:
                raise RuntimeError("Both live quotes are required before dual execution")
            send_started_ms = int(submitted_at_ms or int(time.time() * 1000))
            if not overlap_prefetch_submit:
                self._reset_dual_execution_metrics()
            else:
                # Keep active-cycle trackers intact while submitting prefetch cycle in overlap mode.
                for key in ("left_ack_latency_ms", "right_ack_latency_ms", "left_first_event_latency_ms", "right_first_event_latency_ms", "left_fill_latency_ms", "right_fill_latency_ms"):
                    self.state.metrics[key] = None
                self.state.metrics["left_order_status"] = "SENDING"
                self.state.metrics["right_order_status"] = "SENDING"
            self.state.last_error = None
            self.state.metrics["dual_exec_status"] = "SENDING"
            self._publish_state()
            normalized_left_side = str(left_side or "").strip().upper()
            normalized_right_side = str(right_side or "").strip().upper()
            submitted_direction = (
                f"LEFT_{normalized_left_side}_RIGHT_{normalized_right_side}"
                if normalized_left_side in {"BUY", "SELL"} and normalized_right_side in {"BUY", "SELL"}
                else None
            )
            submitted_edge_name = (
                "edge_1"
                if normalized_left_side == "SELL" and normalized_right_side == "BUY"
                else "edge_2"
                if normalized_left_side == "BUY" and normalized_right_side == "SELL"
                else None
            )
            left_attempt_id: str | None = None
            right_attempt_id: str | None = None
            self._order_pair_seq = int(getattr(self, "_order_pair_seq", 0) or 0) + 1
            order_pair_id = f"pair-{self._order_pair_seq}"
            if self._is_spread_entry_runtime:
                active_cycle_id = submit_entry_cycle.cycle_id if submit_entry_cycle is not None else submit_exit_cycle.cycle_id if submit_exit_cycle is not None else None
                if active_cycle_id is not None:
                    phase = "entry" if is_entry_submit else "exit" if is_exit_submit else "dual"
                    if phase == "entry":
                        prev_dispatch_ts = self._last_entry_cycle_dispatch_ts_ms
                        prev_commit_ts = self._last_entry_cycle_commit_ts_ms
                        self._entry_cycle_dispatch_ts_by_id[int(active_cycle_id)] = send_started_ms
                        self._last_entry_cycle_dispatch_ts_ms = send_started_ms
                    elif phase == "exit":
                        prev_dispatch_ts = self._last_exit_cycle_dispatch_ts_ms
                        prev_commit_ts = self._last_exit_cycle_commit_ts_ms
                        self._exit_cycle_dispatch_ts_by_id[int(active_cycle_id)] = send_started_ms
                        self._last_exit_cycle_dispatch_ts_ms = send_started_ms
                    else:
                        prev_dispatch_ts = None
                        prev_commit_ts = None
                    since_prev_dispatch_ms = (send_started_ms - int(prev_dispatch_ts)) if prev_dispatch_ts is not None else None
                    since_prev_commit_ms = (send_started_ms - int(prev_commit_ts)) if prev_commit_ts is not None else None
                    self.logger.info(
                        "cycle dispatch timing | phase=%s | cycle_id=%s | dispatch_ts_ms=%s | since_prev_dispatch_ms=%s | since_prev_commit_ms=%s",
                        phase,
                        active_cycle_id,
                        send_started_ms,
                        since_prev_dispatch_ms,
                        since_prev_commit_ms,
                    )
                self.logger.info(
                    "dual submit start | order_pair_id=%s | cycle_id=%s | entry_direction=%s | active_edge=%s | left_symbol=%s | right_symbol=%s | left_side=%s | right_side=%s | left_qty=%s | right_qty=%s",
                    order_pair_id,
                    active_cycle_id,
                    submitted_direction,
                    submitted_edge_name,
                    self._left_instrument.symbol,
                    self._right_instrument.symbol,
                    normalized_left_side,
                    normalized_right_side,
                    str(left_qty or "").strip(),
                    str(right_qty or "").strip(),
                )
            left_request = self._build_dual_order_request(instrument=self._left_instrument, quote=left_quote, side=left_side, qty_text=left_qty, price_mode=left_price_mode)
            right_request = self._build_dual_order_request(instrument=self._right_instrument, quote=right_quote, side=right_side, qty_text=right_qty, price_mode=right_price_mode)
            if is_exit_submit:
                left_request = ExecutionOrderRequest(
                    instrument_id=left_request.instrument_id,
                    side=left_request.side,
                    order_type=left_request.order_type,
                    quantity=left_request.quantity,
                    price=left_request.price,
                    time_in_force=left_request.time_in_force,
                    position_side=left_request.position_side,
                    position_idx=left_request.position_idx,
                    reduce_only=True,
                    close_position=left_request.close_position,
                    new_client_order_id=left_request.new_client_order_id,
                    response_type=left_request.response_type,
                    stop_price=left_request.stop_price,
                    activation_price=left_request.activation_price,
                    callback_rate=left_request.callback_rate,
                    working_type=left_request.working_type,
                    price_protect=left_request.price_protect,
                )
                right_request = ExecutionOrderRequest(
                    instrument_id=right_request.instrument_id,
                    side=right_request.side,
                    order_type=right_request.order_type,
                    quantity=right_request.quantity,
                    price=right_request.price,
                    time_in_force=right_request.time_in_force,
                    position_side=right_request.position_side,
                    position_idx=right_request.position_idx,
                    reduce_only=True,
                    close_position=right_request.close_position,
                    new_client_order_id=right_request.new_client_order_id,
                    response_type=right_request.response_type,
                    stop_price=right_request.stop_price,
                    activation_price=right_request.activation_price,
                    callback_rate=right_request.callback_rate,
                    working_type=right_request.working_type,
                    price_protect=right_request.price_protect,
                )
            if self._is_spread_entry_runtime:
                self._sync_leg_request_state(leg_name="left", request=left_request, entry_cycle=submit_entry_cycle)
                self._sync_leg_request_state(leg_name="right", request=right_request, entry_cycle=submit_entry_cycle)
            left_effect = int(self._resolve_leg_request_position_effect(leg_name="left", request=left_request))
            right_effect = int(self._resolve_leg_request_position_effect(leg_name="right", request=right_request))
            owner_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            self._dual_order_clocks["left"] = {"submitted_at_ms": send_started_ms, "first_event_seen": False, "filled_seen": False, "request_sent_at_ms": None, "order_id": None, "client_order_id": None, "position_effect": left_effect, "request_side": left_request.side, "owner_epoch": owner_epoch, "attempt_id": None, "order_pair_id": order_pair_id}
            self._dual_order_clocks["right"] = {"submitted_at_ms": send_started_ms, "first_event_seen": False, "filled_seen": False, "request_sent_at_ms": None, "order_id": None, "client_order_id": None, "position_effect": right_effect, "request_side": right_request.side, "owner_epoch": owner_epoch, "attempt_id": None, "order_pair_id": order_pair_id}
            cycle_type = "ENTRY" if is_entry_submit else "EXIT" if is_exit_submit else None
            left_attempt_id = self._register_order_attempt(
                leg_name="left",
                side=left_request.side,
                reduce_only=bool(left_request.reduce_only),
                position_effect=left_effect,
                cycle_id=active_cycle_id,
                cycle_type=cycle_type,
                submitted_at_ms=send_started_ms,
            )
            right_attempt_id = self._register_order_attempt(
                leg_name="right",
                side=right_request.side,
                reduce_only=bool(right_request.reduce_only),
                position_effect=right_effect,
                cycle_id=active_cycle_id,
                cycle_type=cycle_type,
                submitted_at_ms=send_started_ms,
            )
            self._dual_order_clocks["left"]["attempt_id"] = left_attempt_id
            self._dual_order_clocks["right"]["attempt_id"] = right_attempt_id
            # Force deterministic client ids (when absent) so overlap prefetch events
            # can always be correlated to the correct attempt/cycle early.
            if not str(left_request.new_client_order_id or "").strip():
                left_client_oid = f"{left_attempt_id}-{int(send_started_ms)}"
                left_request = ExecutionOrderRequest(
                    instrument_id=left_request.instrument_id,
                    side=left_request.side,
                    order_type=left_request.order_type,
                    quantity=left_request.quantity,
                    price=left_request.price,
                    time_in_force=left_request.time_in_force,
                    position_side=left_request.position_side,
                    position_idx=left_request.position_idx,
                    reduce_only=left_request.reduce_only,
                    close_position=left_request.close_position,
                    new_client_order_id=left_client_oid,
                    response_type=left_request.response_type,
                    stop_price=left_request.stop_price,
                    activation_price=left_request.activation_price,
                    callback_rate=left_request.callback_rate,
                    working_type=left_request.working_type,
                    price_protect=left_request.price_protect,
                )
            if not str(right_request.new_client_order_id or "").strip():
                right_client_oid = f"{right_attempt_id}-{int(send_started_ms)}"
                right_request = ExecutionOrderRequest(
                    instrument_id=right_request.instrument_id,
                    side=right_request.side,
                    order_type=right_request.order_type,
                    quantity=right_request.quantity,
                    price=right_request.price,
                    time_in_force=right_request.time_in_force,
                    position_side=right_request.position_side,
                    position_idx=right_request.position_idx,
                    reduce_only=right_request.reduce_only,
                    close_position=right_request.close_position,
                    new_client_order_id=right_client_oid,
                    response_type=right_request.response_type,
                    stop_price=right_request.stop_price,
                    activation_price=right_request.activation_price,
                    callback_rate=right_request.callback_rate,
                    working_type=right_request.working_type,
                    price_protect=right_request.price_protect,
                )
            self.logger.info(
                "dual attempts bound | order_pair_id=%s | cycle_id=%s | phase=%s | left_attempt_id=%s | right_attempt_id=%s",
                order_pair_id,
                active_cycle_id,
                "exit" if is_exit_submit else "entry" if is_entry_submit else "dual",
                left_attempt_id,
                right_attempt_id,
            )
            if is_entry_submit:
                self.state.metrics["entry_enabled"] = False
                self.state.metrics["entry_count"] = int(self.state.metrics.get("entry_count") or 0) + 1
                self.state.metrics["last_entry_ts"] = send_started_ms
                self.last_entry_ts = send_started_ms
                self.emit_event(
                    "entry_started",
                    {
                        "left_side": normalized_left_side,
                        "right_side": normalized_right_side,
                        "left_qty": str(left_qty or "").strip(),
                        "right_qty": str(right_qty or "").strip(),
                        "active_edge": submitted_edge_name,
                        "entry_direction": submitted_direction,
                        "order_pair_id": order_pair_id,
                        "entry_cycle_id": active_cycle_id,
                        "left_attempt_id": left_attempt_id,
                        "right_attempt_id": right_attempt_id,
                    },
                )
            elif is_exit_submit:
                self.emit_event(
                    "exit_started",
                    {
                        "left_side": normalized_left_side,
                        "right_side": normalized_right_side,
                        "left_qty": str(left_qty or "").strip(),
                        "right_qty": str(right_qty or "").strip(),
                        "position_direction": submitted_direction,
                        "active_edge": submitted_edge_name,
                        "order_pair_id": order_pair_id,
                        "exit_cycle_id": active_cycle_id,
                        "left_attempt_id": left_attempt_id,
                        "right_attempt_id": right_attempt_id,
                    },
                )
            self.emit_event(
                "dual_exec_started",
                {
                    "left_symbol": self._left_instrument.symbol,
                    "right_symbol": self._right_instrument.symbol,
                    "left_side": normalized_left_side,
                    "right_side": normalized_right_side,
                    "left_qty": str(left_qty or "").strip(),
                    "right_qty": str(right_qty or "").strip(),
                    "order_pair_id": order_pair_id,
                    "strategy_phase": "exit" if is_exit_submit else "entry" if is_entry_submit else "dual",
                    "cycle_id": active_cycle_id,
                    "entry_cycle_id": active_cycle_id if is_entry_submit else None,
                    "exit_cycle_id": active_cycle_id if is_exit_submit else None,
                    "left_attempt_id": left_attempt_id,
                    "right_attempt_id": right_attempt_id,
                },
            )
            self.emit_event(
                "dual_exec_attempts_bound",
                {
                    "order_pair_id": order_pair_id,
                    "strategy_phase": "exit" if is_exit_submit else "entry" if is_entry_submit else "dual",
                    "cycle_id": active_cycle_id,
                    "entry_cycle_id": active_cycle_id if is_entry_submit else None,
                    "exit_cycle_id": active_cycle_id if is_exit_submit else None,
                    "left_attempt_id": left_attempt_id,
                    "right_attempt_id": right_attempt_id,
                },
            )
            if is_entry_submit:
                self.emit_event(
                    "entry_attempts_bound",
                    {
                        "order_pair_id": order_pair_id,
                        "cycle_id": active_cycle_id,
                        "entry_cycle_id": active_cycle_id,
                        "left_attempt_id": left_attempt_id,
                        "right_attempt_id": right_attempt_id,
                    },
                )
            elif is_exit_submit:
                self.emit_event(
                    "exit_attempts_bound",
                    {
                        "order_pair_id": order_pair_id,
                        "cycle_id": active_cycle_id,
                        "exit_cycle_id": active_cycle_id,
                        "left_attempt_id": left_attempt_id,
                        "right_attempt_id": right_attempt_id,
                    },
                )
            adapters = self._ensure_dual_execution_adapters()
            if self._is_spread_entry_runtime:
                self.logger.info(
                    "request summary | leg=%s | attempt_id=%s | order_pair_id=%s | cycle_id=%s | exchange=%s | symbol=%s | side=%s | qty=%s | price=%s | order_type=%s | tif=%s | route=%s | effective_slippage_pct=%s",
                    "left",
                    left_attempt_id,
                    order_pair_id,
                    active_cycle_id,
                    self._left_instrument.exchange,
                    self._left_instrument.symbol,
                    left_request.side,
                    left_request.quantity,
                    left_request.price,
                    left_request.order_type,
                    left_request.time_in_force,
                    adapters["left"].route_name(),
                    self._format_order_size(self._entry_max_slippage_pct()),
                )
                self.logger.info(
                    "request summary | leg=%s | attempt_id=%s | order_pair_id=%s | cycle_id=%s | exchange=%s | symbol=%s | side=%s | qty=%s | price=%s | order_type=%s | tif=%s | route=%s | effective_slippage_pct=%s",
                    "right",
                    right_attempt_id,
                    order_pair_id,
                    active_cycle_id,
                    self._right_instrument.exchange,
                    self._right_instrument.symbol,
                    right_request.side,
                    right_request.quantity,
                    right_request.price,
                    right_request.order_type,
                    right_request.time_in_force,
                    adapters["right"].route_name(),
                    self._format_order_size(self._entry_max_slippage_pct()),
                )
            results: dict[str, Any] = {}
            errors: list[str] = []
            result_lock = threading.Lock()
            def _send_leg(leg_name: str, adapter: ExecutionAdapter, request: ExecutionOrderRequest) -> None:
                try:
                    leg_attempt_id = str(self._dual_order_clocks.get(leg_name, {}).get("attempt_id") or "").strip() or None
                    ack = adapter.place_order(
                        request,
                        on_request_sent=lambda meta, leg=leg_name, aid=leg_attempt_id: self._on_dual_request_sent(
                            leg,
                            meta,
                            attempt_id=aid,
                        ),
                    )
                    self._on_dual_order_ack(
                        leg_name,
                        ack,
                        entry_cycle_id=active_cycle_id if is_entry_submit else None,
                        exit_cycle_id=active_cycle_id if is_exit_submit else None,
                        attempt_id=leg_attempt_id,
                    )
                    with result_lock:
                        ack_payload = ack.to_dict()
                        ack_payload["attempt_id"] = leg_attempt_id
                        ack_payload["order_pair_id"] = order_pair_id
                        ack_payload["cycle_id"] = active_cycle_id
                        ack_payload["entry_cycle_id"] = active_cycle_id if is_entry_submit else None
                        ack_payload["exit_cycle_id"] = active_cycle_id if is_exit_submit else None
                        ack_payload["strategy_phase"] = "exit" if is_exit_submit else "entry" if is_entry_submit else "dual"
                        results[leg_name] = ack_payload
                except Exception as exc:
                    handled = self._on_dual_order_failed(
                        leg_name,
                        exc,
                        attempt_id=leg_attempt_id,
                    )
                    with result_lock:
                        if not handled:
                            errors.append(f"{leg_name}[attempt_id={leg_attempt_id},order_pair_id={order_pair_id},cycle_id={active_cycle_id}]:{exc}")
            left_thread = threading.Thread(target=_send_leg, args=("left", adapters["left"], left_request), name=f"{self.task.worker_id}-left-order", daemon=True)
            right_thread = threading.Thread(target=_send_leg, args=("right", adapters["right"], right_request), name=f"{self.task.worker_id}-right-order", daemon=True)
            left_thread.start()
            right_thread.start()
            left_thread.join()
            right_thread.join()
            if errors:
                self._refresh_dual_exec_status()
                raise RuntimeError("; ".join(errors))
            return results
        except Exception as exc:
            self.state.last_error = str(exc)
            self._publish_state()
            self.emit_event(
                "runtime_error",
                {
                    "error": str(exc),
                    "order_pair_id": order_pair_id,
                    "left_attempt_id": str(self._dual_order_clocks.get("left", {}).get("attempt_id") or "").strip() or None,
                    "right_attempt_id": str(self._dual_order_clocks.get("right", {}).get("attempt_id") or "").strip() or None,
                    "cycle_id": int(active_cycle_id) if active_cycle_id is not None else None,
                    "entry_cycle_id": int(active_cycle_id) if active_cycle_id is not None and is_entry_submit else None,
                    "exit_cycle_id": int(active_cycle_id) if active_cycle_id is not None and is_exit_submit else None,
                    "strategy_phase": "exit" if is_exit_submit else "entry" if is_entry_submit else "dual",
                    "attempts_bound": bool(
                        str(self._dual_order_clocks.get("left", {}).get("attempt_id") or "").strip()
                        or str(self._dual_order_clocks.get("right", {}).get("attempt_id") or "").strip()
                    ),
                },
            )
            raise

    def emit_event(self, event_type: str, payload: dict[str, Any]) -> None:
        ts_ms = int(time.time() * 1000)
        event = WorkerEvent(worker_id=self.task.worker_id, event_type=event_type, timestamp=ts_ms, payload=dict(payload))
        append_runtime_event(
            worker_id=self.task.worker_id,
            event_type=event_type,
            timestamp_ms=ts_ms,
            payload=event.payload,
        )
        self.event_bus.publish("worker_events", event)

    def _publish_state(self, *, force: bool = False) -> None:
        publish_delay_seconds: float | None = None
        should_publish_now = True
        health_event_payload: dict[str, Any] | None = None
        with self._state_lock:
            self._refresh_derived_metrics()
            health_snapshot = self.state.metrics.get("execution_stream_health")
            health_status = self.state.metrics.get("execution_stream_health_status")
            if isinstance(health_snapshot, dict):
                warning_payload = health_snapshot.get("warning")
                if isinstance(warning_payload, dict):
                    warning_code = str(warning_payload.get("code") or "")
                    warning_leg = str(warning_payload.get("leg") or "")
                    warning_signature = (warning_code, warning_leg, str(health_status or ""))
                    if warning_signature != getattr(self, "_last_execution_stream_warning_signature", None):
                        self._last_execution_stream_warning_signature = warning_signature
                        health_event_payload = {
                            "status": health_status,
                            "warning": warning_payload,
                            "streams": health_snapshot.get("streams"),
                        }
                else:
                    self._last_execution_stream_warning_signature = None
                if health_status != getattr(self, "_last_execution_stream_health_status", None):
                    self._pending_execution_stream_health_event = (
                        "execution_stream_health_updated",
                        {
                            "status": health_status,
                            "streams": health_snapshot.get("streams"),
                            "warning": warning_payload if isinstance(warning_payload, dict) else None,
                        },
                    )
                    self._last_execution_stream_health_status = str(health_status or "")
            now_ms = int(time.time() * 1000)
            if not force and self._state_publish_interval_ms > 0 and self._last_state_publish_ms > 0:
                elapsed_ms = now_ms - self._last_state_publish_ms
                if elapsed_ms < self._state_publish_interval_ms:
                    if not self._state_publish_timer_scheduled:
                        self._state_publish_timer_scheduled = True
                        publish_delay_seconds = max(0.0, (self._state_publish_interval_ms - elapsed_ms) / 1000.0)
                    should_publish_now = False
            if should_publish_now:
                self._last_state_publish_ms = now_ms
                self._state_publish_timer_scheduled = False
                self.event_bus.publish("worker_state", self.state)
            elif publish_delay_seconds is None:
                return
            stream_health_event = getattr(self, "_pending_execution_stream_health_event", None)
            self._pending_execution_stream_health_event = None
        if stream_health_event is not None:
            event_type, payload = stream_health_event
            self.emit_event(event_type, payload)
        if health_event_payload is not None:
            self.emit_event("execution_stream_health_warning", health_event_payload)
        if publish_delay_seconds is None:
            return

        existing = getattr(self, "_state_publish_deferred_timer", None)
        if existing is not None:
            existing.cancel()
        timer = threading.Timer(publish_delay_seconds, self._publish_state, kwargs={"force": True})
        timer.daemon = True
        timer.name = f"{self.task.worker_id}-state-deferred"
        self._state_publish_deferred_timer = timer
        timer.start()
