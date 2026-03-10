from __future__ import annotations

import threading
import time
import re
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any

from app.core.accounts.binance_account_connector_impl import BinanceAccountConnector
from app.core.accounts.bitget_account_connector import BitgetAccountConnector
from app.core.accounts.bybit_account_connector import BybitAccountConnector
from app.core.bitget.http_client import BitgetSignedHttpClient
from app.core.bybit.http_client import BybitV5HttpClient
from app.core.models.execution import ExecutionOrderRequest, ExecutionStreamEvent
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1
from app.core.models.workers import StrategyCycle, StrategyCycleState, StrategyState
from app.core.workers.runtime_transition_helpers import build_dual_exec_done_payload, build_dual_exec_snapshot, build_entry_done_payload, classify_dual_exec_status, select_dual_exec_context


class WorkerRuntimeExecutionMixin:
    def _build_dual_order_request(self, *, instrument: InstrumentId, quote: QuoteL1, side: str, qty_text: str, price_mode: str) -> ExecutionOrderRequest:
        normalized_side = str(side or "").strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side}")
        try:
            quantity = Decimal(str(qty_text).strip())
        except Exception as exc:
            raise ValueError(f"Invalid quantity: {qty_text}") from exc
        if quantity <= Decimal("0"):
            raise ValueError(f"Invalid quantity: {qty_text}")
        if str(price_mode or "").strip().lower() != "top_of_book":
            raise ValueError(f"Unsupported price mode: {price_mode}")
        order_price = self._resolve_entry_order_price(instrument=instrument, quote=quote, side=normalized_side)
        return ExecutionOrderRequest(instrument_id=instrument, side=normalized_side, order_type="LIMIT", quantity=quantity, price=order_price, time_in_force="GTC", response_type="ACK")

    def _submit_leg_order(self, *, leg_name: str, side: str, quantity: Decimal, reduce_only: bool, reason: str) -> dict[str, Any]:
        instrument = self._left_instrument if leg_name == "left" else self._right_instrument
        quote = self._latest_quotes.get(instrument)
        if quote is None:
            raise RuntimeError(f"No live quote for {leg_name} leg")
        adapters = self._ensure_dual_execution_adapters()
        adapter = adapters[leg_name]
        request = self._build_dual_order_request(
            instrument=instrument,
            quote=quote,
            side=side,
            qty_text=self._format_order_size(quantity),
            price_mode="top_of_book",
        )
        request = self._apply_recovery_escalation_to_request(
            leg_name=leg_name,
            request=request,
            quote=quote,
            reason=reason,
        )
        if reduce_only:
            request = ExecutionOrderRequest(
                instrument_id=request.instrument_id,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                price=request.price,
                time_in_force=request.time_in_force,
                position_side=request.position_side,
                position_idx=request.position_idx,
                reduce_only=True,
                close_position=request.close_position,
                new_client_order_id=request.new_client_order_id,
                response_type=request.response_type,
                stop_price=request.stop_price,
                activation_price=request.activation_price,
                callback_rate=request.callback_rate,
                working_type=request.working_type,
                price_protect=request.price_protect,
            )
        submitted_at_ms = int(time.time() * 1000)
        self._dual_order_clocks[leg_name] = {
            "submitted_at_ms": submitted_at_ms,
            "first_event_seen": False,
            "filled_seen": False,
            "request_sent_at_ms": None,
            "order_id": None,
            "client_order_id": None,
            "position_effect": -1 if reduce_only else 1,
            "owner_epoch": int(getattr(self, "_runtime_owner_epoch", 0) or 0),
            "attempt_id": None,
        }
        attempt_id = self._register_order_attempt(
            leg_name=leg_name,
            side=request.side,
            reduce_only=bool(reduce_only),
            position_effect=(-1 if reduce_only else 1),
            cycle_id=None,
            cycle_type=None,
            submitted_at_ms=submitted_at_ms,
        )
        self._dual_order_clocks[leg_name]["attempt_id"] = attempt_id
        self._sync_leg_request_state(leg_name=leg_name, request=request)
        self.logger.info(
            "single leg order submit | leg=%s | reason=%s | side=%s | qty=%s | price=%s | reduce_only=%s | route=%s",
            leg_name,
            reason,
            request.side,
            request.quantity,
            request.price,
            reduce_only,
            adapter.route_name(),
        )
        try:
            ack = adapter.place_order(
                request,
                on_request_sent=lambda meta, leg=leg_name, aid=attempt_id: self._on_dual_request_sent(leg, meta, attempt_id=aid),
            )
        except Exception as exc:
            if self._handle_special_exit_failure(leg_name=leg_name, exc=exc):
                return {"status": "TAIL_RESYNC_HANDLED", "leg": leg_name}
            raise
        self._on_dual_order_ack(leg_name, ack, attempt_id=attempt_id)
        return ack.to_dict()

    def _resolve_entry_order_price(self, *, instrument: InstrumentId, quote: QuoteL1, side: str) -> Decimal:
        reference_price = quote.ask if side == "BUY" else quote.bid
        if not self._is_spread_entry_runtime:
            return reference_price
        slippage_pct = self._entry_max_slippage_pct()
        if slippage_pct <= Decimal("0"):
            return reference_price
        multiplier = Decimal("1") + (slippage_pct / Decimal("100")) if side == "BUY" else Decimal("1") - (slippage_pct / Decimal("100"))
        adjusted_price = reference_price * multiplier
        if adjusted_price <= Decimal("0"):
            raise RuntimeError(f"Invalid aggressive limit price for {instrument.symbol}: {adjusted_price}")
        rounding_mode = ROUND_UP if side == "BUY" else ROUND_DOWN
        return self._round_price_to_tick(price=adjusted_price, tick_size=instrument.spec.price_precision, rounding_mode=rounding_mode)

    def _apply_recovery_escalation_to_request(self, *, leg_name: str, request: ExecutionOrderRequest, quote: QuoteL1, reason: str) -> ExecutionOrderRequest:
        if not self._is_spread_entry_runtime:
            return request
        normalized_reason = str(reason or "").strip().lower()
        attempts_used = 0
        if normalized_reason == "entry_recovery_topup" and self.entry_recovery_plan is not None:
            attempts_used = max(0, int(self.entry_recovery_plan.attempts_used))
        elif normalized_reason == "exit_recovery_close" and self.exit_recovery_plan is not None:
            attempts_used = max(0, int(self.exit_recovery_plan.attempts_used))
        if attempts_used <= 0:
            return request
        multiplier = min(Decimal("3"), Decimal("1") + (Decimal("0.5") * Decimal(attempts_used)))
        base_slippage_pct = self._entry_max_slippage_pct()
        effective_slippage_pct = base_slippage_pct * multiplier
        max_slippage_pct = self._decimal_or_zero(self.task.runtime_params.get("max_slippage_pct"))
        if max_slippage_pct <= Decimal("0"):
            max_slippage_pct = Decimal("0.5")
        if effective_slippage_pct > max_slippage_pct:
            effective_slippage_pct = max_slippage_pct
        if effective_slippage_pct <= Decimal("0"):
            return request
        adjusted_price = self._resolve_recovery_order_price(
            instrument=request.instrument_id,
            quote=quote,
            side=request.side,
            effective_slippage_pct=effective_slippage_pct,
        )
        self.logger.info(
            "cycle escalation applied | leg=%s | reason=%s | attempts_used=%s | base_slippage_pct=%s | effective_slippage_pct=%s | price=%s",
            leg_name,
            reason,
            attempts_used,
            self._format_order_size(base_slippage_pct),
            self._format_order_size(effective_slippage_pct),
            self._format_order_size(adjusted_price),
        )
        return ExecutionOrderRequest(
            instrument_id=request.instrument_id,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            price=adjusted_price,
            time_in_force=request.time_in_force,
            position_side=request.position_side,
            position_idx=request.position_idx,
            reduce_only=request.reduce_only,
            close_position=request.close_position,
            new_client_order_id=request.new_client_order_id,
            response_type=request.response_type,
            stop_price=request.stop_price,
            activation_price=request.activation_price,
            callback_rate=request.callback_rate,
            working_type=request.working_type,
            price_protect=request.price_protect,
        )

    def _resolve_recovery_order_price(self, *, instrument: InstrumentId, quote: QuoteL1, side: str, effective_slippage_pct: Decimal) -> Decimal:
        reference_price = quote.ask if side == "BUY" else quote.bid
        multiplier = Decimal("1") + (effective_slippage_pct / Decimal("100")) if side == "BUY" else Decimal("1") - (effective_slippage_pct / Decimal("100"))
        adjusted_price = reference_price * multiplier
        rounding_mode = ROUND_UP if side == "BUY" else ROUND_DOWN
        return self._round_price_to_tick(price=adjusted_price, tick_size=instrument.spec.price_precision, rounding_mode=rounding_mode)

    def _on_dual_request_sent(self, leg_name: str, meta: dict[str, Any], attempt_id: str | None = None) -> None:
        with self._state_lock:
            clock = self._dual_order_clocks.get(leg_name)
            if not clock and not attempt_id:
                return
            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            attempt_id_norm = str(attempt_id or "").strip() or None
            attempt = self._resolve_attempt_for_update(leg_name=leg_name, attempt_id=attempt_id_norm)
            if attempt_id_norm and attempt is None:
                self.logger.debug(
                    "stale request_sent ignored by attempt id | leg=%s | attempt_id=%s | current_epoch=%s",
                    leg_name,
                    attempt_id_norm,
                    current_epoch,
                )
                return
            if attempt is not None and int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                self.logger.debug(
                    "stale request_sent ignored by attempt epoch | leg=%s | attempt_epoch=%s | current_epoch=%s | attempt_id=%s",
                    leg_name,
                    int(getattr(attempt, "owner_epoch", 0) or 0),
                    current_epoch,
                    attempt_id_norm,
                )
                return
            active_attempt_id = (
                str(getattr(attempt, "attempt_id", "") or "").strip() or None
                if attempt is not None
                else attempt_id_norm or (str(clock.get("attempt_id") or "").strip() if clock else None) or None
            )
            if clock and active_attempt_id:
                clock["attempt_id"] = active_attempt_id
            if clock:
                clock_epoch = int(clock.get("owner_epoch") or 0)
                if clock_epoch > 0 and clock_epoch != current_epoch:
                    self.logger.debug(
                        "stale request_sent ignored by owner epoch | leg=%s | clock_epoch=%s | current_epoch=%s",
                        leg_name,
                        clock_epoch,
                        current_epoch,
                    )
                    return
            request_sent_at_ms = int(meta.get("sent_at_ms") or int(time.time() * 1000))
            if clock and (
                active_attempt_id is None
                or str(clock.get("attempt_id") or "").strip() == active_attempt_id
            ):
                clock["request_sent_at_ms"] = request_sent_at_ms
            self._mark_order_attempt_request_sent(
                leg_name=leg_name,
                request_sent_at_ms=request_sent_at_ms,
                attempt_id=active_attempt_id,
            )
            self._mark_rebalance_grace(leg_name=leg_name, reason="order_request_sent")
            self.state.metrics[f"{leg_name}_order_status"] = "SENT"
            submitted_at_ms = (
                int(getattr(attempt, "submitted_at_ms", 0) or 0)
                if attempt is not None
                else int(clock.get("submitted_at_ms") or 0)
            )
            send_latency_ms = max(0, request_sent_at_ms - submitted_at_ms) if submitted_at_ms > 0 else None
            self.emit_event(
                f"{leg_name}_order_sent",
                {
                    "request_sent_at_ms": request_sent_at_ms,
                    "send_latency_ms": send_latency_ms,
                    "attempt_id": active_attempt_id,
                },
            )
            if self._is_spread_entry_runtime:
                self.emit_event(
                    f"entry_{leg_name}_sent",
                    {
                        "request_sent_at_ms": request_sent_at_ms,
                        "send_latency_ms": send_latency_ms,
                        "attempt_id": active_attempt_id,
                    },
                )
            if self._is_spread_entry_runtime:
                left_request_side = str(self._dual_order_clocks.get("left", {}).get("request_side") or "").strip().upper()
                right_request_side = str(self._dual_order_clocks.get("right", {}).get("request_side") or "").strip().upper()
                mode = (
                    f"LEFT_{left_request_side}_RIGHT_{right_request_side}"
                    if left_request_side in {"BUY", "SELL"} and right_request_side in {"BUY", "SELL"}
                    else self._current_direction_from_leg_sides()
                )
                self.logger.info(
                    "%s ORDER SENT | mode=%s | attempt_id=%s",
                    leg_name.upper(),
                    mode,
                    active_attempt_id,
                )
            self._refresh_dual_exec_status()
            self._publish_state()

    def _on_dual_order_ack(
        self,
        leg_name: str,
        ack,
        entry_cycle_id: int | None = None,
        exit_cycle_id: int | None = None,
        attempt_id: str | None = None,
    ) -> None:
        with self._state_lock:
            now_ms = int(time.time() * 1000)
            clock = self._dual_order_clocks.get(leg_name, {})
            order_pair_id = str(clock.get("order_pair_id") or "").strip() or None
            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            attempt_id_norm = str(attempt_id or "").strip() or None
            attempt = self._resolve_attempt_for_update(leg_name=leg_name, attempt_id=attempt_id_norm)
            if attempt_id_norm and attempt is None:
                self.logger.debug(
                    "stale ack ignored by attempt id | leg=%s | attempt_id=%s | current_epoch=%s | order_id=%s | client_order_id=%s",
                    leg_name,
                    attempt_id_norm,
                    current_epoch,
                    ack.order_id,
                    ack.client_order_id,
                )
                return
            active_attempt_id = (
                str(getattr(attempt, "attempt_id", "") or "").strip() or None
                if attempt is not None
                else attempt_id_norm or str(clock.get("attempt_id") or "").strip() or None
            )
            if clock and active_attempt_id:
                clock["attempt_id"] = active_attempt_id
            if attempt is not None and int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                self.logger.debug(
                    "stale ack ignored by attempt epoch | leg=%s | attempt_epoch=%s | current_epoch=%s | attempt_id=%s | order_id=%s | client_order_id=%s",
                    leg_name,
                    int(getattr(attempt, "owner_epoch", 0) or 0),
                    current_epoch,
                    attempt_id_norm,
                    ack.order_id,
                    ack.client_order_id,
                )
                return
            if clock:
                clock_epoch = int(clock.get("owner_epoch") or 0)
                if clock_epoch > 0 and clock_epoch != current_epoch:
                    self.logger.debug(
                        "stale ack ignored by owner epoch | leg=%s | clock_epoch=%s | current_epoch=%s | order_id=%s | client_order_id=%s",
                        leg_name,
                        clock_epoch,
                        current_epoch,
                        ack.order_id,
                        ack.client_order_id,
                    )
                    return
            submitted_at_ms = (
                int(getattr(attempt, "submitted_at_ms", 0) or 0)
                if attempt is not None
                else int(clock.get("submitted_at_ms") or 0)
            )
            if submitted_at_ms <= 0:
                submitted_at_ms = now_ms
            if clock and (
                attempt_id_norm is None
                or str(clock.get("attempt_id") or "").strip() == attempt_id_norm
            ):
                clock["order_id"] = ack.order_id
                clock["client_order_id"] = ack.client_order_id
            elif clock and active_attempt_id and str(clock.get("attempt_id") or "").strip() == active_attempt_id:
                clock["order_id"] = ack.order_id
                clock["client_order_id"] = ack.client_order_id
            target_entry_cycle = self._resolve_entry_cycle_for_submit(entry_cycle_id) if self._is_spread_entry_runtime else None
            target_exit_cycle = self._resolve_exit_cycle_for_submit(exit_cycle_id) if self._is_spread_entry_runtime else None
            self._mark_order_attempt_acked(
                leg_name=leg_name,
                order_id=str(ack.order_id or "") or None,
                client_order_id=str(ack.client_order_id or "") or None,
                status=str(ack.status or "ACK"),
                attempt_id=active_attempt_id,
                cycle_id=(
                    int(target_entry_cycle.cycle_id)
                    if target_entry_cycle is not None
                    else int(target_exit_cycle.cycle_id)
                    if target_exit_cycle is not None
                    else None
                ),
                cycle_type=(
                    "ENTRY"
                    if target_entry_cycle is not None
                    else "EXIT"
                    if target_exit_cycle is not None
                    else None
                ),
            )
            if target_entry_cycle is not None:
                if leg_name == "left":
                    target_entry_cycle.left_order_id = str(ack.order_id or "") or None
                    target_entry_cycle.left_client_order_id = str(ack.client_order_id or "") or None
                    target_entry_cycle.left_acked = True
                else:
                    target_entry_cycle.right_order_id = str(ack.order_id or "") or None
                    target_entry_cycle.right_client_order_id = str(ack.client_order_id or "") or None
                    target_entry_cycle.right_acked = True
                order_keys = self._all_order_fill_keys(order_id=str(ack.order_id or "") or None, client_order_id=str(ack.client_order_id or "") or None)
                if not order_keys:
                    order_key = self._entry_cycle_order_key(cycle=target_entry_cycle, leg_name=leg_name)
                    if order_key:
                        order_keys = [order_key]
                for order_key in order_keys:
                    self._entry_cycle_order_keys.setdefault(leg_name, {})[order_key] = target_entry_cycle.cycle_id
                if self._entry_pipeline_overlap_enabled() and self.active_entry_cycle is target_entry_cycle and self._entry_cycle_ack_ready(target_entry_cycle):
                    self.logger.info(
                        "entry pipeline ack gate passed | cycle_id=%s | action=allow_prefetch",
                        target_entry_cycle.cycle_id,
                    )
                    self._request_deferred_entry_chain()
            elif target_exit_cycle is not None:
                if leg_name == "left":
                    target_exit_cycle.left_order_id = str(ack.order_id or "") or None
                    target_exit_cycle.left_client_order_id = str(ack.client_order_id or "") or None
                    target_exit_cycle.left_acked = True
                else:
                    target_exit_cycle.right_order_id = str(ack.order_id or "") or None
                    target_exit_cycle.right_client_order_id = str(ack.client_order_id or "") or None
                    target_exit_cycle.right_acked = True
                order_keys = self._all_order_fill_keys(order_id=str(ack.order_id or "") or None, client_order_id=str(ack.client_order_id or "") or None)
                if not order_keys:
                    order_key = self._exit_cycle_order_key(cycle=target_exit_cycle, leg_name=leg_name)
                    if order_key:
                        order_keys = [order_key]
                for order_key in order_keys:
                    getattr(self, "_exit_cycle_order_keys", {}).setdefault(leg_name, {})[order_key] = target_exit_cycle.cycle_id
                if self._entry_pipeline_overlap_enabled() and self.active_exit_cycle is target_exit_cycle and self._entry_cycle_ack_ready(target_exit_cycle):
                    self.logger.info(
                        "exit pipeline ack gate passed | cycle_id=%s | action=allow_prefetch",
                        target_exit_cycle.cycle_id,
                    )
                    self._request_deferred_exit_chain()
            self.state.metrics[f"{leg_name}_order_status"] = ack.status or "ACK"
            self.state.metrics[f"{leg_name}_ack_latency_ms"] = max(0, now_ms - submitted_at_ms)
            ack_effect = (
                int(getattr(attempt, "position_effect", 1) or 1)
                if attempt is not None
                else int(clock.get("position_effect") or 1)
            )
            self._sync_leg_ack_state(leg_name=leg_name, ack=ack, position_effect=ack_effect)
            if self._is_spread_entry_runtime:
                self.logger.info(
                    "ack per leg | leg=%s | symbol=%s | route=%s | status=%s | order_id=%s | client_order_id=%s | ack_latency_ms=%s | attempt_id=%s",
                    leg_name,
                    ack.symbol,
                    ack.route,
                    ack.status,
                    ack.order_id,
                    ack.client_order_id,
                    self.state.metrics.get(f"{leg_name}_ack_latency_ms"),
                    active_attempt_id,
                )
            self._refresh_dual_exec_status()
            self._publish_state()
            ack_entry_cycle_id = int(target_entry_cycle.cycle_id) if target_entry_cycle is not None else None
            ack_exit_cycle_id = int(target_exit_cycle.cycle_id) if target_exit_cycle is not None else None
            ack_strategy_phase = "exit" if ack_exit_cycle_id is not None else "entry" if ack_entry_cycle_id is not None else "dual"
            ack_cycle_id = ack_exit_cycle_id if ack_exit_cycle_id is not None else ack_entry_cycle_id
            self.emit_event(
                f"{leg_name}_order_ack",
                {
                    "symbol": ack.symbol,
                    "order_id": ack.order_id,
                    "client_order_id": ack.client_order_id,
                    "status": ack.status,
                    "ack_latency_ms": self.state.metrics.get(f"{leg_name}_ack_latency_ms"),
                    "attempt_id": active_attempt_id,
                    "order_pair_id": order_pair_id,
                    "strategy_phase": ack_strategy_phase,
                    "cycle_id": ack_cycle_id,
                    "entry_cycle_id": ack_entry_cycle_id,
                    "exit_cycle_id": ack_exit_cycle_id,
                },
            )
            if self._is_spread_entry_runtime:
                self.emit_event(
                    f"entry_{leg_name}_ack",
                    {
                        "symbol": ack.symbol,
                        "order_id": ack.order_id,
                        "status": ack.status,
                        "ack_latency_ms": self.state.metrics.get(f"{leg_name}_ack_latency_ms"),
                        "attempt_id": active_attempt_id,
                        "order_pair_id": order_pair_id,
                        "strategy_phase": ack_strategy_phase,
                        "cycle_id": ack_cycle_id,
                        "entry_cycle_id": ack_entry_cycle_id,
                        "exit_cycle_id": ack_exit_cycle_id,
                    },
                )
            self._start_rest_order_poll(
                leg_name,
                ack,
                attempt_id=active_attempt_id,
                entry_cycle_id=int(target_entry_cycle.cycle_id) if target_entry_cycle is not None else None,
                exit_cycle_id=int(target_exit_cycle.cycle_id) if target_exit_cycle is not None else None,
            )
        self._run_deferred_runtime_actions()

    def _on_dual_execution_event(self, leg_name: str, event: ExecutionStreamEvent) -> None:
        with self._state_lock:
            instrument = self._left_instrument if leg_name == "left" else self._right_instrument
            if event.symbol and event.symbol != instrument.symbol:
                return
            clock = self._dual_order_clocks.get(leg_name, {})
            order_pair_id = str(clock.get("order_pair_id") or "").strip() or None
            clock_epoch = int(clock.get("owner_epoch") or 0)
            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            event_order_id = str(event.order_id or "").strip() or None
            event_client_order_id = str(event.client_order_id or "").strip() or None
            preferred_attempt_id = (
                str(clock.get("attempt_id") or "").strip() or None
                if not (event_order_id or event_client_order_id)
                else None
            )
            event_attempt = self._resolve_attempt_for_event(
                leg_name=leg_name,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                preferred_attempt_id=preferred_attempt_id,
            )
            if clock_epoch > 0 and clock_epoch != current_epoch:
                if event_attempt is None or int(getattr(event_attempt, "owner_epoch", 0) or 0) != current_epoch:
                    self.logger.debug(
                        "stale event ignored by owner epoch | leg=%s | clock_epoch=%s | current_epoch=%s | event_order_id=%s | event_client_order_id=%s",
                        leg_name,
                        clock_epoch,
                        current_epoch,
                        event.order_id,
                        event.client_order_id,
                    )
                    return
            active_attempt_id = (
                str(getattr(event_attempt, "attempt_id", "") or "").strip() or None
                if event_attempt is not None
                else str(clock.get("attempt_id") or "").strip() or None
            )
            if clock and active_attempt_id:
                clock["attempt_id"] = active_attempt_id
            if clock:
                clock_attempt_id = str(clock.get("attempt_id") or "").strip() or None
                if active_attempt_id is None or clock_attempt_id == active_attempt_id:
                    if event_order_id:
                        clock["order_id"] = event_order_id
                    if event_client_order_id:
                        clock["client_order_id"] = event_client_order_id
            expected_order_ids, expected_client_order_ids = self._active_attempt_expected_ids(leg_name=leg_name)
            event_admitted, admission_reason = self._is_event_admissible(
                leg_name=leg_name,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                event_attempt=event_attempt,
            )
            if not event_admitted:
                if admission_reason == "LATE_TOMBSTONED":
                    self.logger.info(
                        "late terminal event observed | leg=%s | event_order_id=%s | event_client_order_id=%s | action=post_finalize_reconcile",
                        leg_name,
                        event.order_id,
                        event.client_order_id,
                    )
                    self.emit_event(
                        "execution_event_anomaly",
                        {
                            "kind": "LATE_AFTER_FINALIZE",
                            "leg": leg_name,
                            "event_order_id": event.order_id,
                            "event_client_order_id": event.client_order_id,
                            "action": "POST_FINALIZE_RECONCILE",
                        },
                    )
                    self._request_full_state_reconcile(reason="LATE_TERMINAL_EVENT")
                    return
                active_cycle_id = (
                    self.active_entry_cycle.cycle_id
                    if self.active_entry_cycle is not None
                    else self.active_exit_cycle.cycle_id
                    if self.active_exit_cycle is not None
                    else None
                )
                clock = self._dual_order_clocks.get(leg_name, {})
                log_fn = self.logger.info
                if not event.order_id and not event.client_order_id:
                    # No-id stream noise is expected on some venues; keep it in debug.
                    log_fn = self.logger.debug
                clock_order_id, clock_client_order_id = self._clock_ids_if_active_attempt(leg_name=leg_name)
                expected_order_id = (
                    (sorted(expected_order_ids)[0] if expected_order_ids else None)
                    or clock_order_id
                    or None
                )
                expected_client_order_id = (
                    (sorted(expected_client_order_ids)[0] if expected_client_order_ids else None)
                    or clock_client_order_id
                    or None
                )
                if self._should_log_ignored_foreign_event(
                    leg_name=leg_name,
                    active_cycle_id=active_cycle_id,
                    event_order_id=event.order_id,
                    event_client_order_id=event.client_order_id,
                    expected_order_id=expected_order_id,
                    expected_client_order_id=expected_client_order_id,
                    is_no_id_event=not bool(event.order_id or event.client_order_id),
                ):
                    log_fn(
                        "execution event rejected as foreign | leg=%s | order_pair_id=%s | active_cycle_id=%s | event_order_id=%s | event_client_order_id=%s | expected_order_id=%s | expected_client_order_id=%s",
                        leg_name,
                        order_pair_id,
                        active_cycle_id,
                        event.order_id,
                        event.client_order_id,
                        expected_order_id,
                        expected_client_order_id,
                    )
                return
            if admission_reason == "ORDER_KEY_KNOWN":
                self.emit_event(
                    "execution_event_anomaly",
                    {
                        "kind": "ORDER_KEY_ADMITTED_WITHOUT_ACTIVE_ATTEMPT",
                        "leg": leg_name,
                        "event_order_id": event.order_id,
                        "event_client_order_id": event.client_order_id,
                    },
                )
            if event_attempt is not None:
                # Policy migration note:
                # event admission/routing is attempt-first and must not depend on active cycle timing.
                pass
            now_ms = int(time.time() * 1000)
            clock = self._dual_order_clocks.get(leg_name, {})
            submitted_at_ms = (
                int(getattr(event_attempt, "submitted_at_ms", 0) or 0)
                if event_attempt is not None
                else int(clock.get("submitted_at_ms") or 0)
            )
            if submitted_at_ms <= 0:
                submitted_at_ms = now_ms
            first_event_seen = bool(getattr(event_attempt, "first_event_seen", False)) if event_attempt is not None else bool(clock.get("first_event_seen"))
            if not first_event_seen:
                self.state.metrics[f"{leg_name}_first_event_latency_ms"] = max(0, now_ms - submitted_at_ms)
                if event_attempt is not None:
                    event_attempt.first_event_seen = True
                else:
                    clock["first_event_seen"] = True
            self.state.metrics[f"{leg_name}_order_status"] = event.order_status or event.execution_type or "EVENT"
            if event.last_fill_qty:
                self.state.metrics[f"{leg_name}_filled_qty"] = event.cumulative_fill_qty or event.last_fill_qty
            self._sync_leg_event_state(leg_name=leg_name, event=event)
            self._mark_order_attempt_event(
                leg_name=leg_name,
                status=str(event.order_status or event.execution_type or "EVENT"),
                order_id=event_order_id,
                client_order_id=event_client_order_id,
                attempt_id=active_attempt_id,
            )
            self._publish_state()
            event_cycle_id = (
                int(getattr(event_attempt, "cycle_id", 0) or 0)
                if event_attempt is not None and getattr(event_attempt, "cycle_id", None) is not None
                else None
            )
            event_cycle_type = str(getattr(event_attempt, "cycle_type", "") or "").strip().upper() if event_attempt is not None else ""
            event_status = str(event.order_status or event.execution_type or "EVENT")
            self._maybe_mark_cycle_leg_acked_from_event(
                leg_name=leg_name,
                event_status=event_status,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                event_cycle_id=event_cycle_id,
                event_cycle_type=event_cycle_type,
            )
            event_entry_cycle_id = event_cycle_id if event_cycle_type == "ENTRY" else None
            event_exit_cycle_id = event_cycle_id if event_cycle_type == "EXIT" else None
            event_strategy_phase = "exit" if event_exit_cycle_id is not None else "entry" if event_entry_cycle_id is not None else "dual"
            if event_entry_cycle_id is None and event_exit_cycle_id is None:
                mapped_entry_cycle_id, mapped_exit_cycle_id, mapped_phase = self._resolve_cycle_context_from_order(
                    leg_name=leg_name,
                    order_id=event_order_id,
                    client_order_id=event_client_order_id,
                )
                if mapped_entry_cycle_id is not None:
                    event_entry_cycle_id = mapped_entry_cycle_id
                    event_cycle_id = mapped_entry_cycle_id
                elif mapped_exit_cycle_id is not None:
                    event_exit_cycle_id = mapped_exit_cycle_id
                    event_cycle_id = mapped_exit_cycle_id
                event_strategy_phase = mapped_phase if mapped_phase in {"entry", "exit"} else event_strategy_phase
            event_payload = event.to_dict()
            event_payload["attempt_id"] = active_attempt_id
            event_payload["order_pair_id"] = order_pair_id
            event_payload["strategy_phase"] = event_strategy_phase
            event_payload["cycle_id"] = event_cycle_id
            event_payload["entry_cycle_id"] = event_entry_cycle_id
            event_payload["exit_cycle_id"] = event_exit_cycle_id
            self.emit_event(f"{leg_name}_order_event", event_payload)
            if self._is_spread_entry_runtime:
                self.emit_event(
                    f"entry_{leg_name}_event",
                    {**event_payload, "first_event_latency_ms": self.state.metrics.get(f"{leg_name}_first_event_latency_ms")},
                )
            if str(event.order_status or "").upper() == "FILLED":
                filled_seen = bool(getattr(event_attempt, "filled_seen", False)) if event_attempt is not None else bool(clock.get("filled_seen"))
                if not filled_seen:
                    self.state.metrics[f"{leg_name}_fill_latency_ms"] = max(0, now_ms - submitted_at_ms)
                    if event_attempt is not None:
                        event_attempt.filled_seen = True
                    else:
                        clock["filled_seen"] = True
                self.state.metrics[f"{leg_name}_filled_qty"] = event.cumulative_fill_qty or event.last_fill_qty
                self.state.metrics[f"{leg_name}_order_status"] = "FILLED"
                self._refresh_dual_exec_status()
                self._publish_state()
                self.emit_event(
                    f"{leg_name}_order_filled",
                    {
                        "symbol": event.symbol,
                        "order_id": event.order_id,
                        "fill_latency_ms": self.state.metrics.get(f"{leg_name}_fill_latency_ms"),
                        "filled_qty": self.state.metrics.get(f"{leg_name}_filled_qty"),
                        "attempt_id": active_attempt_id,
                        "strategy_phase": event_strategy_phase,
                        "cycle_id": event_cycle_id,
                        "entry_cycle_id": event_entry_cycle_id,
                        "exit_cycle_id": event_exit_cycle_id,
                    },
                )
                if self._is_spread_entry_runtime:
                    self.emit_event(
                        f"entry_{leg_name}_fill",
                        {
                            "symbol": event.symbol,
                            "order_id": event.order_id,
                            "fill_latency_ms": self.state.metrics.get(f"{leg_name}_fill_latency_ms"),
                            "filled_qty": self.state.metrics.get(f"{leg_name}_filled_qty"),
                            "attempt_id": active_attempt_id,
                            "strategy_phase": event_strategy_phase,
                            "cycle_id": event_cycle_id,
                            "entry_cycle_id": event_entry_cycle_id,
                            "exit_cycle_id": event_exit_cycle_id,
                        },
                    )
            else:
                self._refresh_dual_exec_status()
            self._request_hedge_protection_check(reason="EXECUTION_EVENT")
        self._run_deferred_runtime_actions()

    def _event_matches_active_leg_order(self, *, leg_name: str, event: ExecutionStreamEvent) -> bool:
        # During active cycle lifecycle we accept only events of the currently tracked leg order.
        # This prevents stale/foreign events from previous orders from corrupting the active cycle state.
        event_order_key = self._order_fill_key(order_id=str(event.order_id or "") or None, client_order_id=str(event.client_order_id or "") or None)
        if event_order_key is not None and self._is_order_key_tombstoned(leg_name=leg_name, order_key=event_order_key):
            return False
        if self.active_entry_cycle is None and self.active_exit_cycle is None:
            # Without active cycle, accept only if there is an active tracked order on this leg.
            if not self._is_spread_entry_runtime:
                return True
            clock = self._dual_order_clocks.get(leg_name, {})
            event_order_id = str(event.order_id or "").strip()
            event_client_order_id = str(event.client_order_id or "").strip()
            if not (event_order_id or event_client_order_id):
                return False
            preferred_attempt_id = (
                str(clock.get("attempt_id") or "").strip() or None
                if not (event_order_id or event_client_order_id)
                else None
            )
            return self._attempt_accepts_pre_ack_event(
                leg_name=leg_name,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                preferred_attempt_id=preferred_attempt_id,
            )
        if (self.active_entry_cycle is not None or self.prefetch_entry_cycle is not None) and self._is_spread_entry_runtime:
            order_key = self._order_fill_key(order_id=str(event.order_id or "") or None, client_order_id=str(event.client_order_id or "") or None)
            if order_key is not None and order_key in self._entry_cycle_order_keys.get(leg_name, {}):
                return True
        if (self.active_exit_cycle is not None or getattr(self, "prefetch_exit_cycle", None) is not None) and self._is_spread_entry_runtime:
            order_key = self._order_fill_key(order_id=str(event.order_id or "") or None, client_order_id=str(event.client_order_id or "") or None)
            if order_key is not None and order_key in getattr(self, "_exit_cycle_order_keys", {}).get(leg_name, {}):
                return True
        expected_order_ids, expected_client_order_ids = self._active_attempt_expected_ids(leg_name=leg_name)
        clock = self._dual_order_clocks.get(leg_name, {})
        clock_order_id, clock_client_order_id = self._clock_ids_if_active_attempt(leg_name=leg_name)
        if clock_order_id:
            expected_order_ids.add(clock_order_id)
        if clock_client_order_id:
            expected_client_order_ids.add(clock_client_order_id)
        event_order_id = str(event.order_id or "").strip()
        event_client_order_id = str(event.client_order_id or "").strip()
        if event_client_order_id.startswith("attempt-"):
            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            mapped_attempt = self._resolve_attempt_for_update(
                leg_name=leg_name,
                attempt_id=event_client_order_id,
            )
            if (
                mapped_attempt is not None
                and int(getattr(mapped_attempt, "owner_epoch", 0) or 0) == current_epoch
                and not bool(getattr(mapped_attempt, "terminal", False))
            ):
                return True
        if not expected_order_ids and not expected_client_order_ids:
            # ACK may arrive after the first identified stream event.
            # Allow identified events while there is an active in-epoch attempt for this leg.
            if not (event_order_id or event_client_order_id):
                return False
            preferred_attempt_id = (
                str(clock.get("attempt_id") or "").strip() or None
                if not (event_order_id or event_client_order_id)
                else None
            )
            if self._attempt_accepts_pre_ack_event(
                leg_name=leg_name,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                preferred_attempt_id=preferred_attempt_id,
            ):
                return True
            current_status = self._normalized_leg_status(self._leg_state(leg_name).order_status)
            return current_status in {"SENDING", "SUBMITTING", "SENT", "ACK", "ACCEPTED", "NEW", "PARTIALLY_FILLED", "PARTIALLYFILLED"}
        if event_order_id and event_order_id in expected_order_ids:
            return True
        if event_client_order_id and event_client_order_id in expected_client_order_ids:
            return True
        # In overlap mode, identified stream events may arrive for a newer
        # unresolved attempt while older attempt ids are still present in expected sets.
        # Allow routing if pre-ACK matcher can safely bind the event to one unresolved attempt.
        if event_order_id or event_client_order_id:
            preferred_attempt_id = str(clock.get("attempt_id") or "").strip() or None
            if self._attempt_accepts_pre_ack_event(
                leg_name=leg_name,
                event_order_id=event_order_id,
                event_client_order_id=event_client_order_id,
                preferred_attempt_id=preferred_attempt_id,
            ):
                return True
        if not event_order_id and not event_client_order_id:
            return False
        return False

    def _clock_ids_if_active_attempt(self, *, leg_name: str) -> tuple[str | None, str | None]:
        clock = self._dual_order_clocks.get(leg_name, {})
        clock_attempt_id = str(clock.get("attempt_id") or "").strip() or None
        if not clock_attempt_id:
            return None, None
        attempt = self._resolve_attempt_for_update(leg_name=leg_name, attempt_id=clock_attempt_id)
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        if (
            attempt is None
            or bool(getattr(attempt, "terminal", False))
            or int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch
        ):
            return None, None
        order_id = str(clock.get("order_id") or "").strip() or None
        client_order_id = str(clock.get("client_order_id") or "").strip() or None
        return order_id, client_order_id

    def _resolve_cycle_context_from_order(
        self,
        *,
        leg_name: str,
        order_id: str | None,
        client_order_id: str | None,
    ) -> tuple[int | None, int | None, str]:
        order_keys = self._all_order_fill_keys(order_id=order_id, client_order_id=client_order_id)
        entry_map = self._entry_cycle_order_keys.get(leg_name, {})
        exit_map = getattr(self, "_exit_cycle_order_keys", {}).get(leg_name, {})
        for order_key in order_keys:
            entry_cycle_id = entry_map.get(order_key)
            if entry_cycle_id is not None:
                try:
                    normalized_entry_cycle_id = int(entry_cycle_id)
                except Exception:
                    normalized_entry_cycle_id = None
                if normalized_entry_cycle_id is not None:
                    return normalized_entry_cycle_id, None, "entry"
            exit_cycle_id = exit_map.get(order_key)
            if exit_cycle_id is not None:
                try:
                    normalized_exit_cycle_id = int(exit_cycle_id)
                except Exception:
                    normalized_exit_cycle_id = None
                if normalized_exit_cycle_id is not None:
                    return None, normalized_exit_cycle_id, "exit"
        return None, None, "dual"

    def _active_attempt_expected_ids(self, *, leg_name: str) -> tuple[set[str], set[str]]:
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        order_ids: set[str] = set()
        client_order_ids: set[str] = set()
        terminal_statuses = {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
        for attempt in self._iter_leg_attempts(leg_name=leg_name):
            if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                continue
            if bool(getattr(attempt, "terminal", False)):
                continue
            status = str(getattr(attempt, "status", "") or "").strip().upper()
            if status in terminal_statuses:
                continue
            order_id = str(getattr(attempt, "order_id", "") or "").strip()
            client_order_id = str(getattr(attempt, "client_order_id", "") or "").strip()
            if order_id:
                order_ids.add(order_id)
            if client_order_id:
                client_order_ids.add(client_order_id)
        return order_ids, client_order_ids

    def _should_log_ignored_foreign_event(
        self,
        *,
        leg_name: str,
        active_cycle_id: int | None,
        event_order_id: str | None,
        event_client_order_id: str | None,
        expected_order_id: str | None,
        expected_client_order_id: str | None,
        is_no_id_event: bool,
    ) -> bool:
        now_ms = int(time.time() * 1000)
        signature = (
            str(leg_name),
            int(active_cycle_id) if active_cycle_id is not None else None,
            str(event_order_id or ""),
            str(event_client_order_id or ""),
            str(expected_order_id or ""),
            str(expected_client_order_id or ""),
        )
        last_signature = getattr(self, "_last_ignored_foreign_event_log_signature", None)
        last_at_ms = int(getattr(self, "_last_ignored_foreign_event_log_at_ms", 0) or 0)
        dedupe_window_ms = 5000 if is_no_id_event else 1500
        if signature == last_signature and (now_ms - last_at_ms) < dedupe_window_ms:
            return False
        self._last_ignored_foreign_event_log_signature = signature
        self._last_ignored_foreign_event_log_at_ms = now_ms
        return True

    def _on_dual_order_failed(self, leg_name: str, exc: Exception, attempt_id: str | None = None) -> bool:
        if self._handle_special_exit_failure(leg_name=leg_name, exc=exc):
            return True
        with self._state_lock:
            clock = self._dual_order_clocks.get(leg_name, {})
            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
            attempt_id_norm = str(attempt_id or "").strip() or None
            attempt = self._resolve_attempt_for_update(
                leg_name=leg_name,
                attempt_id=attempt_id_norm or str(clock.get("attempt_id") or "").strip() or None,
                order_id=str(clock.get("order_id") or "").strip() or None,
                client_order_id=str(clock.get("client_order_id") or "").strip() or None,
            )
            if attempt is not None and int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                self.logger.debug(
                    "stale order_failed ignored by attempt epoch | leg=%s | attempt_epoch=%s | current_epoch=%s | attempt_id=%s | error=%s",
                    leg_name,
                    int(getattr(attempt, "owner_epoch", 0) or 0),
                    current_epoch,
                    attempt_id_norm,
                    exc,
                )
                return True
            if attempt is None:
                clock_epoch = int(clock.get("owner_epoch") or 0)
                if clock_epoch > 0 and clock_epoch != current_epoch:
                    self.logger.debug(
                        "stale order_failed ignored by owner epoch | leg=%s | clock_epoch=%s | current_epoch=%s | error=%s",
                        leg_name,
                        clock_epoch,
                        current_epoch,
                        exc,
                    )
                    return True
            self.state.last_error = str(exc)
            terminal_attempt_id = (
                str(getattr(attempt, "attempt_id", "") or "").strip() or None
                if attempt is not None
                else attempt_id_norm or str(clock.get("attempt_id") or "").strip() or None
            )
            self._mark_order_attempt_terminal(
                leg_name=leg_name,
                status="FAILED",
                attempt_id=terminal_attempt_id,
                order_id=str(getattr(attempt, "order_id", "") or "").strip() or None if attempt is not None else None,
                client_order_id=str(getattr(attempt, "client_order_id", "") or "").strip() or None if attempt is not None else None,
            )
            if self._is_spread_entry_runtime and self.active_entry_cycle is not None and self._is_margin_limit_error(str(exc)):
                self._mark_entry_growth_limit_pending(reason="MARGIN_LIMIT_REACHED")
            if self._is_spread_entry_runtime and self.active_entry_cycle is not None:
                self._entry_pipeline_freeze(reason="ENTRY_ORDER_FAILED")
            self.state.metrics[f"{leg_name}_order_status"] = "FAILED"
            self._sync_leg_failure_state(leg_name=leg_name, error=str(exc))
            self._refresh_dual_exec_status()
            self._maybe_restore_in_position_state()
            self._publish_state()
            self.emit_event(
                "dual_exec_failed",
                {
                    "leg": leg_name,
                    "error": str(exc),
                    "attempt_id": terminal_attempt_id,
                    "strategy_phase": "exit" if self.active_exit_cycle is not None else "entry" if self.active_entry_cycle is not None else "dual",
                    "cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                    "entry_cycle_id": self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                    "exit_cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else None,
                    "left_last_terminal_attempt_id": self.state.metrics.get("left_last_terminal_attempt_id"),
                    "right_last_terminal_attempt_id": self.state.metrics.get("right_last_terminal_attempt_id"),
                },
            )
            if self._is_spread_entry_runtime:
                self.emit_event(
                    "entry_failed",
                    {
                        "leg": leg_name,
                        "error": str(exc),
                        "attempt_id": terminal_attempt_id,
                        "strategy_phase": "exit" if self.active_exit_cycle is not None else "entry" if self.active_entry_cycle is not None else "dual",
                        "cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                        "entry_cycle_id": self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                        "exit_cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else None,
                        "left_last_terminal_attempt_id": self.state.metrics.get("left_last_terminal_attempt_id"),
                        "right_last_terminal_attempt_id": self.state.metrics.get("right_last_terminal_attempt_id"),
                    },
                )
            self.emit_event(
                "runtime_error",
                {
                    "leg": leg_name,
                    "error": str(exc),
                    "attempt_id": terminal_attempt_id,
                    "strategy_phase": "exit" if self.active_exit_cycle is not None else "entry" if self.active_entry_cycle is not None else "dual",
                    "cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                    "entry_cycle_id": self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
                    "exit_cycle_id": self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else None,
                    "left_last_terminal_attempt_id": self.state.metrics.get("left_last_terminal_attempt_id"),
                    "right_last_terminal_attempt_id": self.state.metrics.get("right_last_terminal_attempt_id"),
                },
            )
            return False

    def _handle_special_exit_failure(self, *, leg_name: str, exc: Exception) -> bool:
        if not self._is_reduce_only_rejected_on_exit_tail(leg_name=leg_name, error_text=str(exc)):
            return False
        self._handle_reduce_only_reject_tail(leg_name=leg_name, error_text=str(exc))
        return True

    def _start_rest_order_poll(
        self,
        leg_name: str,
        ack,
        attempt_id: str | None = None,
        entry_cycle_id: int | None = None,
        exit_cycle_id: int | None = None,
    ) -> None:
        adapter = self._left_execution_adapter if leg_name == "left" else self._right_execution_adapter
        if adapter is None or (not ack.order_id and not ack.client_order_id):
            return
        normalized_attempt_id = str(attempt_id or "").strip() or None

        def _poll() -> None:
            deadline_ms = int(time.time() * 1000) + 15000
            symbol = ack.symbol
            poll_attempt_id = normalized_attempt_id
            stop_reason = "DEADLINE_REACHED"
            try:
                while int(time.time() * 1000) < deadline_ms and self.state.status == "running":
                    with self._state_lock:
                        if poll_attempt_id:
                            poll_attempt = self._resolve_attempt_for_update(
                                leg_name=leg_name,
                                attempt_id=poll_attempt_id,
                                order_id=str(ack.order_id or "") or None,
                                client_order_id=str(ack.client_order_id or "") or None,
                            )
                            current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
                            if (
                                poll_attempt is None
                                or int(getattr(poll_attempt, "owner_epoch", 0) or 0) != current_epoch
                                or bool(getattr(poll_attempt, "terminal", False))
                            ):
                                stop_reason = "ATTEMPT_INACTIVE"
                                self.logger.debug(
                                    "rest poll stopped by attempt state | leg=%s | attempt_id=%s | current_epoch=%s | poll_attempt_epoch=%s | terminal=%s",
                                    leg_name,
                                    poll_attempt_id,
                                    current_epoch,
                                    int(getattr(poll_attempt, "owner_epoch", 0) or 0) if poll_attempt is not None else None,
                                    bool(getattr(poll_attempt, "terminal", False)) if poll_attempt is not None else None,
                                )
                                return
                    try:
                        result = adapter.query_order(symbol=symbol, order_id=ack.order_id, client_order_id=ack.client_order_id)
                        event = ExecutionStreamEvent(exchange=result.exchange, event_type="rest_query", event_time=result.update_time, transaction_time=result.update_time, symbol=result.symbol, order_id=result.order_id, client_order_id=result.client_order_id, order_status=result.status, execution_type=result.status, side=result.side, order_type=result.order_type, position_side=result.position_side, last_fill_qty=result.executed_qty, cumulative_fill_qty=result.executed_qty, last_fill_price=result.avg_price or result.price, average_price=result.avg_price or result.price, realized_pnl=None, raw=result.raw)
                        self._on_dual_execution_event(leg_name, event)
                        if str(result.status or "").upper() in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "FAILED"}:
                            stop_reason = "ORDER_TERMINAL"
                            return
                    except Exception as exc:
                        stop_reason = "QUERY_ERROR"
                        self.logger.warning(
                            "dual rest poll failed | leg=%s | attempt_id=%s | error=%s",
                            leg_name,
                            poll_attempt_id,
                            exc,
                        )
                        return
                    time.sleep(self._rest_order_poll_interval_seconds(leg_name=leg_name))
                if self.state.status != "running":
                    stop_reason = "RUNTIME_STOPPED"
            finally:
                active_polls_total = 0
                stop_ts = int(time.time() * 1000)
                with self._state_lock:
                    current_thread = threading.current_thread()
                    if self._dual_poll_threads.get(leg_name) is current_thread:
                        self._dual_poll_threads.pop(leg_name, None)
                        self._dual_poll_attempt_ids[leg_name] = None
                    self.state.metrics[f"{leg_name}_rest_poll_active"] = False
                    active_polls_total = int(bool(self._dual_poll_threads.get("left"))) + int(bool(self._dual_poll_threads.get("right")))
                    self.state.metrics["active_rest_polls_total"] = active_polls_total
                    self.state.metrics[f"{leg_name}_rest_poll_last_stop_reason"] = stop_reason
                    self.state.metrics[f"{leg_name}_rest_poll_last_stop_ts"] = stop_ts
                    self.state.metrics[f"{leg_name}_rest_poll_last_attempt_id"] = poll_attempt_id
                self.emit_event(
                    "rest_poll_stopped",
                    {
                        "leg": leg_name,
                        "attempt_id": poll_attempt_id,
                        "strategy_phase": "exit" if exit_cycle_id is not None else "entry" if entry_cycle_id is not None else "dual",
                        "cycle_id": exit_cycle_id if exit_cycle_id is not None else entry_cycle_id,
                        "entry_cycle_id": entry_cycle_id,
                        "exit_cycle_id": exit_cycle_id,
                        "reason": stop_reason,
                        "active_rest_polls_total": active_polls_total,
                    },
                )

        with self._state_lock:
            existing_thread = self._dual_poll_threads.get(leg_name)
            existing_attempt_id = str(self._dual_poll_attempt_ids.get(leg_name) or "").strip() or None
            if (
                existing_thread is not None
                and existing_thread.is_alive()
                and existing_attempt_id == normalized_attempt_id
            ):
                return
            poll_thread = threading.Thread(target=_poll, name=f"{self.task.worker_id}-{leg_name}-rest-poll", daemon=True)
            self._dual_poll_threads[leg_name] = poll_thread
            self._dual_poll_attempt_ids[leg_name] = normalized_attempt_id
            self.state.metrics[f"{leg_name}_rest_poll_active"] = True
            self.state.metrics["active_rest_polls_total"] = int(bool(self._dual_poll_threads.get("left"))) + int(bool(self._dual_poll_threads.get("right")))
            self.state.metrics[f"{leg_name}_rest_poll_last_attempt_id"] = normalized_attempt_id
            active_polls_total = int(self.state.metrics.get("active_rest_polls_total") or 0)
        self.emit_event(
            "rest_poll_started",
            {
                "leg": leg_name,
                "attempt_id": normalized_attempt_id,
                "strategy_phase": "exit" if exit_cycle_id is not None else "entry" if entry_cycle_id is not None else "dual",
                "cycle_id": exit_cycle_id if exit_cycle_id is not None else entry_cycle_id,
                "entry_cycle_id": entry_cycle_id,
                "exit_cycle_id": exit_cycle_id,
                "active_rest_polls_total": active_polls_total,
            },
        )
        poll_thread.start()

    def _rest_order_poll_interval_seconds(self, *, leg_name: str) -> float:
        base_ms = int(getattr(self, "REST_ORDER_POLL_INTERVAL_MS", 250) or 250)
        active_ms = int(getattr(self, "REST_ORDER_POLL_ACTIVE_INTERVAL_MS", base_ms) or base_ms)
        fast_ms = int(getattr(self, "REST_ORDER_POLL_FAST_INTERVAL_MS", active_ms) or active_ms)
        interval_ms = base_ms
        with self._state_lock:
            active_cycle = self.active_entry_cycle is not None or self.active_exit_cycle is not None
            if self._is_spread_entry_runtime and active_cycle:
                interval_ms = active_ms
                other_leg = "right" if leg_name == "left" else "left"
                other_status = self._normalized_leg_status(self._leg_state(other_leg).order_status)
                current_status = self._normalized_leg_status(self._leg_state(leg_name).order_status)
                if other_status == "FILLED" and current_status not in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
                    interval_ms = min(interval_ms, fast_ms)
        return max(0.03, interval_ms / 1000.0)

    def _cancel_live_leg_order(self, *, leg_name: str, reason: str) -> bool:
        adapter = self._left_execution_adapter if leg_name == "left" else self._right_execution_adapter
        if adapter is None:
            return False
        with self._state_lock:
            clock = self._dual_order_clocks.get(leg_name) or {}
            clock_attempt_id = str(clock.get("attempt_id") or "").strip() or None
            attempt = self._resolve_attempt_for_update(
                leg_name=leg_name,
                attempt_id=clock_attempt_id,
                order_id=str(clock.get("order_id") or "").strip() or None,
                client_order_id=str(clock.get("client_order_id") or "").strip() or None,
            )
            order_id = (
                str(getattr(attempt, "order_id", "") or "").strip() or None
                if attempt is not None
                else str(clock.get("order_id") or "").strip() or None
            )
            client_order_id = (
                str(getattr(attempt, "client_order_id", "") or "").strip() or None
                if attempt is not None
                else str(clock.get("client_order_id") or "").strip() or None
            )
            attempt_id = (
                str(getattr(attempt, "attempt_id", "") or "").strip() or None
                if attempt is not None
                else clock_attempt_id
            )
        if order_id is None and client_order_id is None:
            return False
        self.logger.warning(
            "cancel stale live order | leg=%s | reason=%s | attempt_id=%s | order_id=%s | client_order_id=%s",
            leg_name,
            reason,
            attempt_id,
            order_id,
            client_order_id,
        )
        try:
            result = adapter.cancel_order(
                symbol=(self._left_instrument if leg_name == "left" else self._right_instrument).symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
        except Exception as exc:
            self.logger.warning(
                "cancel stale live order failed | leg=%s | reason=%s | error=%s",
                leg_name,
                reason,
                exc,
            )
            return False
        with self._state_lock:
            status = str(result.status or "CANCELED").upper()
            self.state.metrics[f"{leg_name}_order_status"] = status
            leg_state = self._leg_state(leg_name)
            leg_state.order_status = status
            if status in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
                self._mark_order_attempt_terminal(
                    leg_name=leg_name,
                    status=status,
                    attempt_id=attempt_id,
                    order_id=order_id,
                    client_order_id=client_order_id,
                )
            self._refresh_dual_exec_status()
            self._publish_state()
        return True

    def _refresh_dual_exec_status(self) -> None:
        left_attempts, right_attempts = self._active_attempt_counts_by_leg()
        left_attempt_ids, right_attempt_ids = self._active_attempt_ids_by_leg()
        self.state.metrics["left_active_attempts"] = left_attempts
        self.state.metrics["right_active_attempts"] = right_attempts
        self.state.metrics["active_attempts_total"] = left_attempts + right_attempts
        self.state.metrics["left_active_attempt_ids"] = ",".join(left_attempt_ids) if left_attempt_ids else None
        self.state.metrics["right_active_attempt_ids"] = ",".join(right_attempt_ids) if right_attempt_ids else None
        if not self._has_active_execution_owner_context():
            self.state.metrics["dual_exec_status"] = "IDLE"
            self._last_dual_exec_eval_signature = None
            return
        left_status = self._normalized_leg_status(self.left_leg_state.order_status)
        right_status = self._normalized_leg_status(self.right_leg_state.order_status)
        self.state.metrics["left_order_status"] = left_status
        self.state.metrics["right_order_status"] = right_status
        left_filled = self._format_order_size(self.left_leg_state.filled_qty)
        right_filled = self._format_order_size(self.right_leg_state.filled_qty)
        snapshot = build_dual_exec_snapshot(
            owner_epoch=int(getattr(self, "_runtime_owner_epoch", 0) or 0),
            active_entry_cycle_id=self.active_entry_cycle.cycle_id if self.active_entry_cycle is not None else None,
            prefetch_entry_cycle_id=self.prefetch_entry_cycle.cycle_id if self.prefetch_entry_cycle is not None else None,
            active_exit_cycle_id=self.active_exit_cycle.cycle_id if self.active_exit_cycle is not None else None,
            left_status=left_status,
            right_status=right_status,
            left_filled=left_filled,
            right_filled=right_filled,
        )
        if snapshot == self._last_dual_exec_eval_signature:
            return
        self._last_dual_exec_eval_signature = snapshot
        dual_status = self._classify_dual_exec_status(left_status=snapshot.left_status, right_status=snapshot.right_status)
        self.state.metrics["dual_exec_status"] = dual_status
        state_updater = self._active_dual_exec_state_updater(snapshot=snapshot)
        if dual_status == "DONE":
            strategy_phase = "exit" if snapshot.active_exit_cycle_id is not None else "entry" if snapshot.active_entry_cycle_id is not None else "dual"
            cycle_id = snapshot.active_exit_cycle_id if snapshot.active_exit_cycle_id is not None else snapshot.active_entry_cycle_id
            done_payload = build_dual_exec_done_payload(snapshot)
            done_payload["strategy_phase"] = strategy_phase
            done_payload["cycle_id"] = cycle_id
            done_payload["entry_cycle_id"] = snapshot.active_entry_cycle_id
            done_payload["exit_cycle_id"] = snapshot.active_exit_cycle_id
            done_payload["left_active_attempt_ids"] = self.state.metrics.get("left_active_attempt_ids")
            done_payload["right_active_attempt_ids"] = self.state.metrics.get("right_active_attempt_ids")
            done_payload["active_attempts_total"] = self.state.metrics.get("active_attempts_total")
            done_payload["left_last_terminal_attempt_id"] = self.state.metrics.get("left_last_terminal_attempt_id")
            done_payload["right_last_terminal_attempt_id"] = self.state.metrics.get("right_last_terminal_attempt_id")
            self.emit_event("dual_exec_done", done_payload)
            if self._is_spread_entry_runtime:
                entry_done_payload = build_entry_done_payload(snapshot, current_direction=self._current_direction_from_leg_sides())
                entry_done_payload["strategy_phase"] = strategy_phase
                entry_done_payload["cycle_id"] = cycle_id
                entry_done_payload["entry_cycle_id"] = snapshot.active_entry_cycle_id
                entry_done_payload["exit_cycle_id"] = snapshot.active_exit_cycle_id
                entry_done_payload["left_active_attempt_ids"] = self.state.metrics.get("left_active_attempt_ids")
                entry_done_payload["right_active_attempt_ids"] = self.state.metrics.get("right_active_attempt_ids")
                entry_done_payload["active_attempts_total"] = self.state.metrics.get("active_attempts_total")
                entry_done_payload["left_last_terminal_attempt_id"] = self.state.metrics.get("left_last_terminal_attempt_id")
                entry_done_payload["right_last_terminal_attempt_id"] = self.state.metrics.get("right_last_terminal_attempt_id")
                self.emit_event("entry_done", entry_done_payload)
            if state_updater is not None:
                state_updater(snapshot.left_status, snapshot.right_status)
            return
        if state_updater is not None:
            state_updater(snapshot.left_status, snapshot.right_status)

    def _active_dual_exec_state_updater(self, *, snapshot):
        context = select_dual_exec_context(
            active_entry_cycle_id=snapshot.active_entry_cycle_id,
            active_exit_cycle_id=snapshot.active_exit_cycle_id,
        )
        if context == "exit":
            return self._update_strategy_state_from_exit_attempt
        if context == "entry":
            return self._update_strategy_state_from_entry_attempt
        return None

    def _classify_dual_exec_status(self, *, left_status: str, right_status: str) -> str:
        return classify_dual_exec_status(left_status=left_status, right_status=right_status)

    def _has_active_order_attempts(self) -> bool:
        self._prune_order_attempts()
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        for leg_name in ("left", "right"):
            for attempt in self._iter_leg_attempts(leg_name=leg_name):
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                if bool(getattr(attempt, "terminal", False)):
                    continue
                status = str(getattr(attempt, "status", "") or "").strip().upper()
                if status in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
                    continue
                return True
        return False

    def _active_attempt_counts_by_leg(self) -> tuple[int, int]:
        self._prune_order_attempts()
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        terminal_statuses = {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
        counts: list[int] = []
        for leg_name in ("left", "right"):
            count = 0
            for attempt in self._iter_leg_attempts(leg_name=leg_name):
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                if bool(getattr(attempt, "terminal", False)):
                    continue
                status = str(getattr(attempt, "status", "") or "").strip().upper()
                if status in terminal_statuses:
                    continue
                count += 1
            counts.append(count)
        return counts[0], counts[1]

    def _active_attempt_ids_by_leg(self) -> tuple[list[str], list[str]]:
        self._prune_order_attempts()
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        terminal_statuses = {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}
        ids_by_leg: list[list[str]] = []
        for leg_name in ("left", "right"):
            ids: list[str] = []
            preferred_attempt_id = str(self._dual_order_clocks.get(leg_name, {}).get("attempt_id") or "").strip() or None
            for attempt in self._iter_leg_attempts(leg_name=leg_name):
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                if bool(getattr(attempt, "terminal", False)):
                    continue
                status = str(getattr(attempt, "status", "") or "").strip().upper()
                if status in terminal_statuses:
                    continue
                attempt_id = str(getattr(attempt, "attempt_id", "") or "").strip()
                if attempt_id:
                    ids.append(attempt_id)
            if preferred_attempt_id and preferred_attempt_id in ids:
                ids = [preferred_attempt_id, *[attempt_id for attempt_id in ids if attempt_id != preferred_attempt_id]]
            ids_by_leg.append(ids)
        return ids_by_leg[0], ids_by_leg[1]

    def _dual_execution_in_progress(self) -> bool:
        if self._has_active_order_attempts():
            return True
        left_status = self._normalized_leg_status(self.left_leg_state.order_status)
        right_status = self._normalized_leg_status(self.right_leg_state.order_status)
        dual_status = self._classify_dual_exec_status(left_status=left_status, right_status=right_status)
        return dual_status in {"SENDING", "PARTIAL"}

    def _next_order_attempt_id(self) -> str:
        self._order_attempt_seq = int(getattr(self, "_order_attempt_seq", 0) or 0) + 1
        return f"attempt-{self._order_attempt_seq}"

    def _register_order_attempt(
        self,
        *,
        leg_name: str,
        side: str | None,
        reduce_only: bool,
        position_effect: int,
        cycle_id: int | None,
        cycle_type: str | None,
        submitted_at_ms: int,
    ) -> str:
        from app.core.models.workers import OrderAttempt

        self._prune_order_attempts()
        owner_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        attempt = OrderAttempt(
            attempt_id=self._next_order_attempt_id(),
            leg_name=leg_name,
            owner_epoch=owner_epoch,
            cycle_id=cycle_id,
            cycle_type=cycle_type,
            side=str(side or "").strip().upper() or None,
            reduce_only=bool(reduce_only),
            position_effect=int(position_effect or 1),
            submitted_at_ms=int(submitted_at_ms or 0),
            status="SUBMITTING",
            terminal=False,
        )
        self._order_attempts.setdefault(leg_name, {})[attempt.attempt_id] = attempt
        self._update_attempt_metrics_for_leg(leg_name=leg_name)
        return attempt.attempt_id

    def _mark_order_attempt_request_sent(self, *, leg_name: str, request_sent_at_ms: int, attempt_id: str | None = None) -> None:
        self._prune_order_attempts()
        attempt = self._resolve_attempt_for_update(leg_name=leg_name, attempt_id=attempt_id)
        if attempt is None:
            return
        attempt.request_sent_at_ms = int(request_sent_at_ms or 0)
        attempt.status = "SENT"
        self._update_attempt_metrics_for_leg(leg_name=leg_name)

    def _mark_order_attempt_acked(
        self,
        *,
        leg_name: str,
        order_id: str | None,
        client_order_id: str | None,
        status: str,
        attempt_id: str | None = None,
        cycle_id: int | None = None,
        cycle_type: str | None = None,
    ) -> None:
        self._prune_order_attempts()
        attempt = self._resolve_attempt_for_update(
            leg_name=leg_name,
            order_id=order_id,
            client_order_id=client_order_id,
            attempt_id=attempt_id,
            cycle_id=cycle_id,
            cycle_type=cycle_type,
        )
        if attempt is None:
            return
        attempt.order_id = order_id
        attempt.client_order_id = client_order_id
        attempt.status = str(status or "ACK").strip().upper() or "ACK"
        self._update_attempt_metrics_for_leg(leg_name=leg_name)

    def _mark_order_attempt_event(
        self,
        *,
        leg_name: str,
        status: str,
        order_id: str | None,
        client_order_id: str | None,
        attempt_id: str | None = None,
    ) -> None:
        self._prune_order_attempts()
        attempt = self._resolve_attempt_for_update(
            leg_name=leg_name,
            order_id=order_id,
            client_order_id=client_order_id,
            attempt_id=attempt_id,
        )
        if attempt is None:
            return
        if order_id:
            attempt.order_id = order_id
        if client_order_id:
            attempt.client_order_id = client_order_id
        normalized = str(status or "EVENT").strip().upper() or "EVENT"
        attempt.status = normalized
        if normalized in {"FILLED", "FAILED", "REJECTED", "CANCELED", "CANCELLED", "IDLE"}:
            attempt.terminal = True
            self.state.metrics[f"{leg_name}_last_terminal_attempt_id"] = str(getattr(attempt, "attempt_id", "") or "").strip() or None
        self._update_attempt_metrics_for_leg(leg_name=leg_name)

    def _mark_order_attempt_terminal(
        self,
        *,
        leg_name: str,
        status: str,
        attempt_id: str | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> None:
        self._prune_order_attempts()
        attempt = self._resolve_attempt_for_update(
            leg_name=leg_name,
            order_id=order_id,
            client_order_id=client_order_id,
            attempt_id=attempt_id,
        )
        if attempt is None:
            return
        attempt.status = str(status or "FAILED").strip().upper() or "FAILED"
        attempt.terminal = True
        self.state.metrics[f"{leg_name}_last_terminal_attempt_id"] = str(getattr(attempt, "attempt_id", "") or "").strip() or None
        self._update_attempt_metrics_for_leg(leg_name=leg_name)

    def _reset_dual_execution_metrics(self) -> None:
        for key in ("left_order_status", "right_order_status", "left_ack_latency_ms", "right_ack_latency_ms", "left_first_event_latency_ms", "right_first_event_latency_ms", "left_fill_latency_ms", "right_fill_latency_ms", "left_filled_qty", "right_filled_qty"):
            self.state.metrics[key] = None
        self.state.metrics["left_order_status"] = "SENDING"
        self.state.metrics["right_order_status"] = "SENDING"
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
        self._dual_poll_threads = {}
        self._dual_poll_attempt_ids = {"left": None, "right": None}
        self._order_attempts = {"left": {}, "right": {}}
        self._leg_order_fill_tracker = {"left": {}, "right": {}}
        self._leg_order_position_effects = {"left": {}, "right": {}}
        preserve_position_state = self.position is not None or self.active_exit_cycle is not None or self.strategy_state in {StrategyState.IN_POSITION, StrategyState.EXIT_ARMED, StrategyState.EXIT_SUBMITTING, StrategyState.EXIT_PARTIAL, StrategyState.RECOVERY}
        for leg_state in (self.left_leg_state, self.right_leg_state):
            if not preserve_position_state:
                leg_state.side = None
                leg_state.filled_qty = Decimal("0")
                leg_state.avg_price = None
                leg_state.actual_position_qty = Decimal("0")
                leg_state.remaining_close_qty = Decimal("0")
                leg_state.is_flat = True
                leg_state.flat_confirmed_by_exchange = False
                leg_state.last_position_resync_ts = None
            leg_state.target_qty = Decimal("0")
            leg_state.requested_qty = Decimal("0")
            leg_state.remaining_qty = Decimal("0")
            leg_state.order_status = "SENDING"
            leg_state.last_order_reduce_only = False
            leg_state.latency_ack_ms = None
            leg_state.latency_fill_ms = None
            leg_state.last_error = None
        if not preserve_position_state:
            self._reset_position_state()

    def _filled_leg_notional_usdt(self, leg_name: str) -> Decimal:
        leg_state = self._leg_state(leg_name)
        if leg_state.filled_qty <= Decimal("0"):
            return Decimal("0")
        price = leg_state.avg_price or self._latest_reference_price(leg_name)
        if price <= Decimal("0"):
            return Decimal("0")
        return (leg_state.filled_qty * price).normalize()

    def _latest_reference_price(self, leg_name: str) -> Decimal:
        instrument = self._left_instrument if leg_name == "left" else self._right_instrument
        quote = self._latest_quotes.get(instrument)
        if quote is None:
            return Decimal("0")
        side = self._leg_state(leg_name).side or ""
        normalized_side = str(side).strip().upper()
        if normalized_side == "BUY":
            return quote.ask
        if normalized_side == "SELL":
            return quote.bid
        return ((quote.bid + quote.ask) / Decimal("2")).normalize()

    @staticmethod
    def _opposite_side(side: str) -> str | None:
        normalized_side = str(side or "").strip().upper()
        if normalized_side == "BUY":
            return "SELL"
        if normalized_side == "SELL":
            return "BUY"
        return None

    def _current_direction_from_leg_sides(self) -> str | None:
        left_side = str(self.left_leg_state.side or "").strip().upper()
        right_side = str(self.right_leg_state.side or "").strip().upper()
        if left_side in {"BUY", "SELL"} and right_side in {"BUY", "SELL"}:
            return f"LEFT_{left_side}_RIGHT_{right_side}"
        return None

    def _resolve_leg_request_position_effect(self, *, leg_name: str, request: ExecutionOrderRequest) -> Decimal:
        if bool(request.reduce_only):
            return Decimal("-1")
        leg_state = self._leg_state(leg_name)
        current_side = str(leg_state.side or "").strip().upper()
        request_side = str(request.side or "").strip().upper()
        if request_side not in {"BUY", "SELL"}:
            return Decimal("1")
        if leg_state.filled_qty <= Decimal("0") or current_side not in {"BUY", "SELL"}:
            return Decimal("1")
        if current_side == request_side:
            return Decimal("1")
        # Opposite non-reduce side reduces/open-flips net exposure on this leg.
        return Decimal("-1")

    def _leg_state(self, leg_name: str):
        return self.left_leg_state if leg_name == "left" else self.right_leg_state

    def _sync_leg_request_state(self, *, leg_name: str, request: ExecutionOrderRequest, entry_cycle: StrategyCycle | None = None) -> None:
        leg_state = self._leg_state(leg_name)
        if leg_state.side is None:
            leg_state.side = request.side
        leg_state.last_order_reduce_only = bool(request.reduce_only)
        if entry_cycle is not None and not bool(request.reduce_only):
            if entry_cycle is self.active_entry_cycle:
                leg_state.target_qty = self._entry_leg_target_total_qty(leg_name)
            elif leg_state.target_qty <= Decimal("0"):
                leg_state.target_qty = request.quantity
        elif self.active_entry_cycle is not None and not bool(request.reduce_only):
            leg_state.target_qty = self._entry_leg_target_total_qty(leg_name)
        elif self.active_exit_cycle is None and leg_state.target_qty <= Decimal("0"):
            leg_state.target_qty = request.quantity
        leg_state.requested_qty = request.quantity
        leg_state.remaining_qty = max(Decimal("0"), leg_state.target_qty - leg_state.filled_qty)
        leg_state.order_status = "SUBMITTING"
        leg_state.last_error = None
        leg_state.flat_confirmed_by_exchange = False
        self._sync_active_entry_cycle_from_legs()
        self._sync_active_exit_cycle_from_legs()

    def _sync_leg_ack_state(self, *, leg_name: str, ack, position_effect: int | None = None) -> None:
        leg_state = self._leg_state(leg_name)
        leg_state.order_status = self._merge_leg_order_status(
            current_status=leg_state.order_status,
            incoming_status=str(ack.status or "ACK"),
        )
        leg_state.latency_ack_ms = self.state.metrics.get(f"{leg_name}_ack_latency_ms")
        order_id = str(ack.order_id or "") or None
        client_order_id = str(ack.client_order_id or "") or None
        order_keys = self._all_order_fill_keys(order_id=order_id, client_order_id=client_order_id)
        self._link_order_fill_key_aliases(leg_name=leg_name, order_id=order_id, client_order_id=client_order_id)
        if order_keys:
            effect = Decimal(str(position_effect if position_effect is not None else self._dual_order_clocks.get(leg_name, {}).get("position_effect") or "1"))
            for order_key in order_keys:
                self._leg_order_fill_tracker.setdefault(leg_name, {}).setdefault(order_key, Decimal("0"))
                self._leg_order_position_effects.setdefault(leg_name, {})[order_key] = effect
        self.state.metrics[f"{leg_name}_order_status"] = leg_state.order_status
        self._sync_active_entry_cycle_from_legs()
        self._sync_active_exit_cycle_from_legs()

    def _sync_leg_event_state(self, *, leg_name: str, event: ExecutionStreamEvent) -> None:
        leg_state = self._leg_state(leg_name)
        leg_state.order_status = self._merge_leg_order_status(
            current_status=leg_state.order_status,
            incoming_status=str(event.order_status or event.execution_type or leg_state.order_status),
        )
        cumulative_fill_qty = self._decimal_or_none(event.cumulative_fill_qty)
        last_fill_qty = self._decimal_or_none(event.last_fill_qty)
        average_price = self._decimal_or_none(event.average_price or event.last_fill_price)
        order_key = self._order_fill_key(order_id=event.order_id, client_order_id=event.client_order_id)
        fill_delta = Decimal("0")
        effect = Decimal("1")
        if cumulative_fill_qty is not None and order_key is not None:
            tracker = self._leg_order_fill_tracker.setdefault(leg_name, {})
            effect_tracker = self._leg_order_position_effects.setdefault(leg_name, {})
            alias_keys = self._resolve_order_fill_tracker_keys(leg_name=leg_name, order_key=order_key)
            previous_cumulative = max((tracker.get(key, Decimal("0")) for key in alias_keys), default=Decimal("0"))
            if cumulative_fill_qty > previous_cumulative:
                fill_delta = cumulative_fill_qty - previous_cumulative
                for key in alias_keys:
                    tracker[key] = cumulative_fill_qty
            effect = next((effect_tracker.get(key) for key in alias_keys if key in effect_tracker), Decimal("1"))
        elif last_fill_qty is not None and last_fill_qty > Decimal("0"):
            fill_delta = last_fill_qty
        if fill_delta > Decimal("0"):
            previous_total_filled = leg_state.filled_qty
            leg_state.filled_qty = max(Decimal("0"), leg_state.filled_qty + (effect * fill_delta))
            fill_side = str(event.side or "").strip().upper()
            if effect > Decimal("0") and fill_side in {"BUY", "SELL"} and leg_state.filled_qty > Decimal("0"):
                leg_state.side = fill_side
            if effect > Decimal("0") and average_price is not None and average_price > Decimal("0"):
                leg_state.avg_price = self._merge_average_price(
                    current_avg=leg_state.avg_price,
                    current_qty=previous_total_filled,
                    fill_price=average_price,
                    fill_qty=fill_delta,
                )
            elif effect < Decimal("0") and leg_state.filled_qty <= Decimal("0"):
                leg_state.avg_price = None
                leg_state.side = None
        elif average_price is not None and average_price > Decimal("0") and leg_state.avg_price is None:
            leg_state.avg_price = average_price
        if leg_state.target_qty > Decimal("0"):
            leg_state.remaining_qty = max(Decimal("0"), leg_state.target_qty - leg_state.filled_qty)
        if self.active_entry_cycle is not None and self._entry_cycle_pair_matches_target():
            leg_state.order_status = "FILLED"
        leg_state.latency_fill_ms = self.state.metrics.get(f"{leg_name}_fill_latency_ms")
        self._refresh_leg_position_derived_fields(leg_name, confirmed_by_exchange=False)
        self.state.metrics[f"{leg_name}_order_status"] = leg_state.order_status
        self._sync_active_entry_cycle_from_legs()
        self._sync_active_exit_cycle_from_legs()

    def _sync_leg_failure_state(self, *, leg_name: str, error: str) -> None:
        leg_state = self._leg_state(leg_name)
        leg_state.order_status = "FAILED"
        leg_state.last_error = error
        self._refresh_leg_position_derived_fields(leg_name, confirmed_by_exchange=False)
        self.state.metrics[f"{leg_name}_order_status"] = leg_state.order_status
        self._sync_active_entry_cycle_from_legs()
        self._sync_active_exit_cycle_from_legs()

    @staticmethod
    def _normalized_leg_status(status: str | None) -> str:
        normalized = str(status or "").strip().upper()
        return normalized or "IDLE"

    def _merge_leg_order_status(self, *, current_status: str | None, incoming_status: str | None) -> str:
        current = self._normalized_leg_status(current_status)
        incoming = self._normalized_leg_status(incoming_status)
        if current == incoming:
            return current
        status_rank = {
            "IDLE": 0,
            "SENDING": 1,
            "SUBMITTING": 1,
            "SENT": 2,
            "ACK": 3,
            "ACCEPTED": 4,
            "NEW": 5,
            "EVENT": 5,
            "PARTIALLYFILLED": 6,
            "PARTIALLY_FILLED": 6,
            "FILLED": 7,
            "CANCELED": 8,
            "CANCELLED": 8,
            "REJECTED": 8,
            "FAILED": 8,
        }
        if current == "FILLED" and incoming not in {"FAILED", "REJECTED", "CANCELED", "CANCELLED"}:
            return current
        if current in {"PARTIALLY_FILLED", "PARTIALLYFILLED"} and incoming in {"NEW", "EVENT", "ACK", "ACCEPTED", "SENT", "SENDING", "SUBMITTING"}:
            return current
        return incoming if status_rank.get(incoming, 0) >= status_rank.get(current, 0) else current

    @staticmethod
    def _order_fill_key(*, order_id: str | None, client_order_id: str | None) -> str | None:
        if order_id:
            return f"order:{order_id}"
        if client_order_id:
            return f"client:{client_order_id}"
        return None

    def _all_order_fill_keys(self, *, order_id: str | None, client_order_id: str | None) -> list[str]:
        keys: list[str] = []
        if order_id:
            keys.append(f"order:{order_id}")
        if client_order_id:
            keys.append(f"client:{client_order_id}")
        return keys

    def _remember_order_key_tombstones(self, *, leg_name: str, order_keys: list[str]) -> None:
        if not order_keys:
            return
        now_ms = int(time.time() * 1000)
        expires_at_ms = now_ms + int(getattr(self, "ORDER_KEY_TOMBSTONE_TTL_MS", 15000) or 15000)
        tombstones = getattr(self, "_order_key_tombstones", {}).setdefault(leg_name, {})
        aliases = getattr(self, "_order_key_aliases", {}).get(leg_name, {})
        for key in order_keys:
            tombstones[str(key)] = expires_at_ms
            alias = aliases.get(str(key))
            if alias:
                tombstones[str(alias)] = expires_at_ms
        self._prune_order_key_tombstones(leg_name=leg_name, now_ms=now_ms)

    def _prune_order_key_tombstones(self, *, leg_name: str, now_ms: int | None = None) -> None:
        current_ms = int(now_ms if now_ms is not None else int(time.time() * 1000))
        tombstones = getattr(self, "_order_key_tombstones", {}).get(leg_name, {})
        if not tombstones:
            return
        stale = [key for key, expiry in tombstones.items() if int(expiry or 0) <= current_ms]
        for key in stale:
            tombstones.pop(key, None)

    def _is_order_key_tombstoned(self, *, leg_name: str, order_key: str) -> bool:
        now_ms = int(time.time() * 1000)
        self._prune_order_key_tombstones(leg_name=leg_name, now_ms=now_ms)
        tombstones = getattr(self, "_order_key_tombstones", {}).get(leg_name, {})
        aliases = getattr(self, "_order_key_aliases", {}).get(leg_name, {})
        if int(tombstones.get(order_key, 0) or 0) > now_ms:
            return True
        alias_key = aliases.get(order_key)
        if alias_key and int(tombstones.get(alias_key, 0) or 0) > now_ms:
            return True
        return False

    def _link_order_fill_key_aliases(self, *, leg_name: str, order_id: str | None, client_order_id: str | None) -> None:
        keys = self._all_order_fill_keys(order_id=order_id, client_order_id=client_order_id)
        if len(keys) < 2:
            return
        aliases = getattr(self, "_order_key_aliases", {}).setdefault(leg_name, {})
        canonical = aliases.get(keys[0]) or aliases.get(keys[1]) or keys[0]
        for key in keys:
            aliases[key] = canonical

    def _resolve_order_fill_tracker_keys(self, *, leg_name: str, order_key: str) -> list[str]:
        aliases = getattr(self, "_order_key_aliases", {}).get(leg_name, {})
        canonical = aliases.get(order_key, order_key)
        tracker = self._leg_order_fill_tracker.setdefault(leg_name, {})
        keys = [key for key, value in aliases.items() if value == canonical]
        if order_key not in keys:
            keys.append(order_key)
        if canonical not in keys:
            keys.append(canonical)
        # Keep only keys that are known in trackers/aliases to avoid accidental broad matches.
        return [key for key in dict.fromkeys(keys) if key in tracker or key in aliases or key == order_key]

    def _tracker_cumulative_for_order_key(self, *, leg_name: str, order_key: str) -> Decimal:
        tracker = self._leg_order_fill_tracker.get(leg_name, {})
        keys = self._resolve_order_fill_tracker_keys(leg_name=leg_name, order_key=order_key)
        return max((tracker.get(key, Decimal("0")) for key in keys), default=Decimal("0"))

    def _is_known_order_key(self, *, leg_name: str, order_key: str) -> bool:
        if not order_key:
            return False
        aliases = getattr(self, "_order_key_aliases", {}).get(leg_name, {})
        tracker = self._leg_order_fill_tracker.get(leg_name, {})
        if order_key in aliases or order_key in tracker:
            return True
        canonical = aliases.get(order_key)
        if canonical and (canonical in aliases or canonical in tracker):
            return True
        return False

    def _attempt_accepts_pre_ack_event(
        self,
        *,
        leg_name: str,
        event_order_id: str,
        event_client_order_id: str,
        preferred_attempt_id: str | None = None,
    ) -> bool:
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        preferred_id_norm = str(preferred_attempt_id or "").strip()
        attempts = [
            attempt
            for attempt in self._iter_leg_attempts(leg_name=leg_name)
            if int(getattr(attempt, "owner_epoch", 0) or 0) == current_epoch
            and not bool(getattr(attempt, "terminal", False))
        ]

        def _matches(candidate) -> bool:
            candidate_order_id = str(getattr(candidate, "order_id", "") or "").strip()
            candidate_client_order_id = str(getattr(candidate, "client_order_id", "") or "").strip()
            if candidate_order_id and event_order_id:
                return event_order_id == candidate_order_id
            if candidate_client_order_id and event_client_order_id:
                return event_client_order_id == candidate_client_order_id
            if not (candidate_order_id or candidate_client_order_id):
                return bool(event_order_id or event_client_order_id)
            return False

        if preferred_id_norm:
            for attempt in attempts:
                if str(getattr(attempt, "attempt_id", "") or "").strip() != preferred_id_norm:
                    continue
                if _matches(attempt):
                    return True
                break

        unresolved_count = 0
        for attempt in attempts:
            attempt_order_id = str(getattr(attempt, "order_id", "") or "").strip()
            attempt_client_order_id = str(getattr(attempt, "client_order_id", "") or "").strip()
            if attempt_order_id and event_order_id:
                if event_order_id == attempt_order_id:
                    return True
                continue
            if attempt_client_order_id and event_client_order_id:
                if event_client_order_id == attempt_client_order_id:
                    return True
                continue
            if not attempt_order_id and not attempt_client_order_id:
                unresolved_count += 1
        if not (event_order_id or event_client_order_id):
            return False
        # Ambiguous pre-ACK routing is unsafe when multiple unresolved attempts exist.
        return unresolved_count == 1

    def _resolve_attempt_for_event(
        self,
        *,
        leg_name: str,
        event_order_id: str | None,
        event_client_order_id: str | None,
        preferred_attempt_id: str | None = None,
    ):
        attempt_id_from_client_order = None
        client_order_id_norm = str(event_client_order_id or "").strip()
        if client_order_id_norm.startswith("attempt-"):
            # exchange-facing client IDs may be "<attempt_id>-<nonce>".
            # recover canonical attempt id for routing.
            match = re.match(r"^(attempt-\d+)", client_order_id_norm)
            attempt_id_from_client_order = str(match.group(1)) if match else client_order_id_norm
        return self._resolve_attempt_for_update(
            leg_name=leg_name,
            order_id=event_order_id,
            client_order_id=event_client_order_id,
            attempt_id=attempt_id_from_client_order or preferred_attempt_id,
        )

    def _is_event_admissible(
        self,
        *,
        leg_name: str,
        event_order_id: str | None,
        event_client_order_id: str | None,
        event_attempt,
    ) -> tuple[bool, str]:
        # Exchange events are admitted by order/attempt identity, never by cycle expectation.
        # Cycles aggregate fills and monitor symmetry, but do not own event admission.
        if event_attempt is not None:
            return True, "ATTEMPT_IDENTITY"
        event_order_key = self._order_fill_key(order_id=event_order_id, client_order_id=event_client_order_id)
        if event_order_key and self._is_known_order_key(leg_name=leg_name, order_key=event_order_key):
            return True, "ORDER_KEY_KNOWN"
        client_order_id_norm = str(event_client_order_id or "").strip()
        if client_order_id_norm.startswith("attempt-"):
            match = re.match(r"^(attempt-\d+)", client_order_id_norm)
            attempt_id = str(match.group(1)) if match else client_order_id_norm
            attempt = self._resolve_attempt_for_update(leg_name=leg_name, attempt_id=attempt_id)
            if attempt is not None:
                return True, "ATTEMPT_PREFIX_IDENTITY"
        clock_attempt_id = str(self._dual_order_clocks.get(leg_name, {}).get("attempt_id") or "").strip() or None
        if self._attempt_accepts_pre_ack_event(
            leg_name=leg_name,
            event_order_id=event_order_id,
            event_client_order_id=event_client_order_id,
            preferred_attempt_id=clock_attempt_id,
        ):
            return True, "ATTEMPT_PRE_ACK_MATCH"
        expected_order_ids, expected_client_order_ids = self._active_attempt_expected_ids(leg_name=leg_name)
        clock_order_id, clock_client_order_id = self._clock_ids_if_active_attempt(leg_name=leg_name)
        if clock_order_id:
            expected_order_ids.add(str(clock_order_id))
        if clock_client_order_id:
            expected_client_order_ids.add(str(clock_client_order_id))
        if event_order_id and str(event_order_id) in expected_order_ids:
            return True, "EXPECTED_ORDER_ID"
        if event_client_order_id and str(event_client_order_id) in expected_client_order_ids:
            return True, "EXPECTED_CLIENT_ORDER_ID"
        if event_order_key is not None and self._is_order_key_tombstoned(leg_name=leg_name, order_key=event_order_key):
            return False, "LATE_TOMBSTONED"
        return False, "FOREIGN_UNKNOWN"

    def _maybe_mark_cycle_leg_acked_from_event(
        self,
        *,
        leg_name: str,
        event_status: str,
        event_order_id: str | None,
        event_client_order_id: str | None,
        event_cycle_id: int | None,
        event_cycle_type: str | None,
    ) -> None:
        if not self._is_spread_entry_runtime:
            return
        status_norm = str(event_status or "").strip().upper()
        ack_like_statuses = {"NEW", "ACK", "ACCEPTED", "PARTIALLY_FILLED", "PARTIALLYFILLED", "FILLED"}
        if status_norm not in ack_like_statuses:
            return
        cycle_type_norm = str(event_cycle_type or "").strip().upper()
        target_entry_cycle = self._resolve_entry_cycle_for_submit(event_cycle_id) if cycle_type_norm == "ENTRY" else None
        target_exit_cycle = self._resolve_exit_cycle_for_submit(event_cycle_id) if cycle_type_norm == "EXIT" else None
        if target_entry_cycle is None and target_exit_cycle is None:
            mapped_entry_cycle_id, mapped_exit_cycle_id, _ = self._resolve_cycle_context_from_order(
                leg_name=leg_name,
                order_id=event_order_id,
                client_order_id=event_client_order_id,
            )
            if mapped_entry_cycle_id is not None:
                target_entry_cycle = self._resolve_entry_cycle_for_submit(mapped_entry_cycle_id)
            elif mapped_exit_cycle_id is not None:
                target_exit_cycle = self._resolve_exit_cycle_for_submit(mapped_exit_cycle_id)
        if target_entry_cycle is not None:
            if leg_name == "left":
                target_entry_cycle.left_order_id = str(event_order_id or "") or target_entry_cycle.left_order_id
                target_entry_cycle.left_client_order_id = str(event_client_order_id or "") or target_entry_cycle.left_client_order_id
                target_entry_cycle.left_acked = True
            else:
                target_entry_cycle.right_order_id = str(event_order_id or "") or target_entry_cycle.right_order_id
                target_entry_cycle.right_client_order_id = str(event_client_order_id or "") or target_entry_cycle.right_client_order_id
                target_entry_cycle.right_acked = True
            for order_key in self._all_order_fill_keys(order_id=event_order_id, client_order_id=event_client_order_id):
                self._entry_cycle_order_keys.setdefault(leg_name, {})[order_key] = target_entry_cycle.cycle_id
            if self._entry_pipeline_overlap_enabled() and self.active_entry_cycle is target_entry_cycle and self._entry_cycle_ack_ready(target_entry_cycle):
                self._request_deferred_entry_chain()
            return
        if target_exit_cycle is not None:
            if leg_name == "left":
                target_exit_cycle.left_order_id = str(event_order_id or "") or target_exit_cycle.left_order_id
                target_exit_cycle.left_client_order_id = str(event_client_order_id or "") or target_exit_cycle.left_client_order_id
                target_exit_cycle.left_acked = True
            else:
                target_exit_cycle.right_order_id = str(event_order_id or "") or target_exit_cycle.right_order_id
                target_exit_cycle.right_client_order_id = str(event_client_order_id or "") or target_exit_cycle.right_client_order_id
                target_exit_cycle.right_acked = True
            for order_key in self._all_order_fill_keys(order_id=event_order_id, client_order_id=event_client_order_id):
                getattr(self, "_exit_cycle_order_keys", {}).setdefault(leg_name, {})[order_key] = target_exit_cycle.cycle_id
            if self._entry_pipeline_overlap_enabled() and self.active_exit_cycle is target_exit_cycle and self._entry_cycle_ack_ready(target_exit_cycle):
                self._request_deferred_exit_chain()

    def _iter_leg_attempts(self, *, leg_name: str):
        self._prune_order_attempts(leg_name=leg_name)
        bucket = getattr(self, "_order_attempts", {}).get(leg_name, {})
        attempts = list(bucket.values()) if isinstance(bucket, dict) else []
        attempts.sort(key=lambda item: int(getattr(item, "submitted_at_ms", 0) or 0), reverse=True)
        return attempts

    def _prune_order_attempts(self, *, leg_name: str | None = None) -> None:
        ttl_ms = int(getattr(self, "ORDER_ATTEMPT_TERMINAL_TTL_MS", 30000) or 30000)
        now_ms = int(time.time() * 1000)
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        leg_names = (leg_name,) if leg_name in {"left", "right"} else ("left", "right")
        attempts_by_leg = getattr(self, "_order_attempts", {})
        if not isinstance(attempts_by_leg, dict):
            return
        for current_leg in leg_names:
            bucket = attempts_by_leg.setdefault(current_leg, {})
            if not isinstance(bucket, dict) or not bucket:
                continue
            stale_ids: list[str] = []
            for attempt_id, attempt in bucket.items():
                if attempt is None:
                    stale_ids.append(str(attempt_id))
                    continue
                attempt_epoch = int(getattr(attempt, "owner_epoch", 0) or 0)
                submitted_at_ms = int(getattr(attempt, "submitted_at_ms", 0) or 0)
                if attempt_epoch != current_epoch:
                    if submitted_at_ms <= 0 or (now_ms - submitted_at_ms) >= ttl_ms:
                        stale_ids.append(str(attempt_id))
                    continue
                if not bool(getattr(attempt, "terminal", False)):
                    continue
                terminal_anchor_ms = int(getattr(attempt, "request_sent_at_ms", 0) or 0) or int(getattr(attempt, "submitted_at_ms", 0) or 0)
                if terminal_anchor_ms <= 0 or (now_ms - terminal_anchor_ms) >= ttl_ms:
                    stale_ids.append(str(attempt_id))
            for attempt_id in stale_ids:
                bucket.pop(attempt_id, None)

    def _resolve_attempt_for_update(
        self,
        *,
        leg_name: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
        attempt_id: str | None = None,
        cycle_id: int | None = None,
        cycle_type: str | None = None,
    ):
        order_id_norm = str(order_id or "").strip()
        client_order_id_norm = str(client_order_id or "").strip()
        attempt_id_norm = str(attempt_id or "").strip()
        cycle_type_norm = str(cycle_type or "").strip().upper() or None
        cycle_id_norm = int(cycle_id) if cycle_id is not None else None
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        if attempt_id_norm:
            bucket = getattr(self, "_order_attempts", {}).get(leg_name, {})
            candidate = bucket.get(attempt_id_norm) if isinstance(bucket, dict) else None
            if candidate is None:
                return None
            if bool(getattr(candidate, "terminal", False)):
                return None
            if int(getattr(candidate, "owner_epoch", 0) or 0) != current_epoch:
                return None
            candidate_order_id = str(getattr(candidate, "order_id", "") or "").strip()
            candidate_client_order_id = str(getattr(candidate, "client_order_id", "") or "").strip()
            candidate_cycle_id = getattr(candidate, "cycle_id", None)
            candidate_cycle_type = str(getattr(candidate, "cycle_type", "") or "").strip().upper() or None
            if order_id_norm and candidate_order_id and order_id_norm != candidate_order_id:
                return None
            if client_order_id_norm and candidate_client_order_id and client_order_id_norm != candidate_client_order_id:
                return None
            if cycle_id_norm is not None and candidate_cycle_id is not None and cycle_id_norm != candidate_cycle_id:
                return None
            if cycle_type_norm is not None and candidate_cycle_type is not None and cycle_type_norm != candidate_cycle_type:
                return None
            return candidate
        if cycle_id_norm is not None and cycle_type_norm in {"ENTRY", "EXIT"}:
            for attempt in self._iter_leg_attempts(leg_name=leg_name):
                if bool(getattr(attempt, "terminal", False)):
                    continue
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                attempt_cycle_id = getattr(attempt, "cycle_id", None)
                attempt_cycle_type = str(getattr(attempt, "cycle_type", "") or "").strip().upper() or None
                if attempt_cycle_id == cycle_id_norm and attempt_cycle_type == cycle_type_norm:
                    return attempt
        for attempt in self._iter_leg_attempts(leg_name=leg_name):
            if bool(getattr(attempt, "terminal", False)):
                continue
            if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                continue
            attempt_order_id = str(getattr(attempt, "order_id", "") or "").strip()
            attempt_client_order_id = str(getattr(attempt, "client_order_id", "") or "").strip()
            if order_id_norm and attempt_order_id and order_id_norm == attempt_order_id:
                return attempt
            if client_order_id_norm and attempt_client_order_id and client_order_id_norm == attempt_client_order_id:
                return attempt
        if order_id_norm or client_order_id_norm:
            unresolved = []
            for attempt in self._iter_leg_attempts(leg_name=leg_name):
                if bool(getattr(attempt, "terminal", False)):
                    continue
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                attempt_order_id = str(getattr(attempt, "order_id", "") or "").strip()
                attempt_client_order_id = str(getattr(attempt, "client_order_id", "") or "").strip()
                if attempt_order_id or attempt_client_order_id:
                    continue
                unresolved.append(attempt)
            if len(unresolved) == 1:
                return unresolved[0]
            if len(unresolved) > 1:
                return None
        for attempt in self._iter_leg_attempts(leg_name=leg_name):
            if not bool(getattr(attempt, "terminal", False)):
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                return attempt
        return None

    def _update_attempt_metrics_for_leg(self, *, leg_name: str) -> None:
        current_epoch = int(getattr(self, "_runtime_owner_epoch", 0) or 0)
        preferred_attempt_id = str(self._dual_order_clocks.get(leg_name, {}).get("attempt_id") or "").strip() or None
        if preferred_attempt_id:
            bucket = getattr(self, "_order_attempts", {}).get(leg_name, {})
            candidate = bucket.get(preferred_attempt_id) if isinstance(bucket, dict) else None
            if (
                candidate is not None
                and not bool(getattr(candidate, "terminal", False))
                and int(getattr(candidate, "owner_epoch", 0) or 0) == current_epoch
            ):
                attempt_id = str(getattr(candidate, "attempt_id", "") or "").strip() or None
                owner_epoch = int(getattr(candidate, "owner_epoch", 0) or 0)
                self.state.metrics[f"{leg_name}_attempt_id"] = attempt_id
                self.state.metrics[f"{leg_name}_attempt_owner_epoch"] = owner_epoch
                return
        active_attempt = None
        for attempt in self._iter_leg_attempts(leg_name=leg_name):
            if not bool(getattr(attempt, "terminal", False)):
                if int(getattr(attempt, "owner_epoch", 0) or 0) != current_epoch:
                    continue
                active_attempt = attempt
                break
        if active_attempt is None:
            self.state.metrics[f"{leg_name}_attempt_id"] = None
            self.state.metrics[f"{leg_name}_attempt_owner_epoch"] = None
            return
        attempt_id = str(getattr(active_attempt, "attempt_id", "") or "").strip() or None
        owner_epoch = int(getattr(active_attempt, "owner_epoch", 0) or 0)
        self.state.metrics[f"{leg_name}_attempt_id"] = attempt_id
        self.state.metrics[f"{leg_name}_attempt_owner_epoch"] = owner_epoch

    @staticmethod
    def _merge_average_price(*, current_avg: Decimal | None, current_qty: Decimal, fill_price: Decimal, fill_qty: Decimal) -> Decimal:
        if fill_qty <= Decimal("0"):
            return current_avg or fill_price
        if current_avg is None or current_qty <= Decimal("0"):
            return fill_price
        total_qty = current_qty + fill_qty
        if total_qty <= Decimal("0"):
            return fill_price
        return ((current_avg * current_qty) + (fill_price * fill_qty)) / total_qty

    def _refresh_leg_position_derived_fields(self, leg_name: str, *, confirmed_by_exchange: bool) -> None:
        leg_state = self._leg_state(leg_name)
        position_qty = max(Decimal("0"), leg_state.filled_qty)
        leg_state.actual_position_qty = position_qty
        leg_state.is_flat = self._is_effectively_flat_qty(leg_name=leg_name, qty=position_qty)
        if confirmed_by_exchange:
            leg_state.flat_confirmed_by_exchange = leg_state.is_flat
            leg_state.last_position_resync_ts = int(time.time() * 1000)
        leg_state.remaining_close_qty = Decimal("0") if leg_state.is_flat else position_qty
        self.state.metrics[f"{leg_name}_actual_position_qty"] = self._format_order_size(leg_state.actual_position_qty)
        self.state.metrics[f"{leg_name}_real_remaining_qty"] = self._format_order_size(leg_state.remaining_close_qty)

    def _is_effectively_flat_qty(self, *, leg_name: str, qty: Decimal) -> bool:
        return qty <= Decimal("0")

    def _cycle_fill_tolerance_qty(self, leg_name: str) -> Decimal:
        return Decimal("0")

    def _final_flat_tolerance_qty(self, leg_name: str) -> Decimal:
        return Decimal("0")

    def _position_match_tolerance_qty(self) -> Decimal:
        return Decimal("0")

    def _is_reduce_only_rejected_on_exit_tail(self, *, leg_name: str, error_text: str) -> bool:
        if self.active_exit_cycle is None:
            return False
        if not bool(self._leg_state(leg_name).last_order_reduce_only):
            return False
        normalized = str(error_text or "").strip().lower()
        return "-2022" in normalized or "reduceonly order is rejected" in normalized or "reduce only order is rejected" in normalized

    def _handle_reduce_only_reject_tail(self, *, leg_name: str, error_text: str) -> None:
        cycle_id: int | None = None
        with self._state_lock:
            cycle = self.active_exit_cycle
            if cycle is None:
                return
            now_ms = int(time.time() * 1000)
            if cycle.tail_resync_in_progress:
                return
            if cycle.tail_resync_attempts >= self.EXIT_TAIL_RESYNC_MAX_ATTEMPTS:
                self.logger.warning(
                    "EXIT_TAIL_REDUCE_ONLY_REJECT | cycle_id=%s | leg=%s | error=%s | ignored=TAIL_RESYNC_LIMIT",
                    cycle.cycle_id,
                    leg_name,
                    error_text,
                )
                return
            cycle_id = cycle.cycle_id
            cycle.tail_reduce_only_seen = True
            cycle.tail_resync_in_progress = True
            cycle.tail_resync_attempts += 1
            cycle.exit_grace_deadline_ts = max(int(cycle.exit_grace_deadline_ts or 0), now_ms + self.EXIT_GRACE_WINDOW_MS)
            self._sync_active_exit_cycle_metrics()
            self.logger.warning(
                "EXIT_TAIL_REDUCE_ONLY_REJECT | cycle_id=%s | leg=%s | attempts=%s | error=%s",
                cycle.cycle_id,
                leg_name,
                cycle.tail_resync_attempts,
                error_text,
            )
            self.logger.info(
                "EXIT_TAIL_RESYNC_STARTED | cycle_id=%s | trigger_leg=%s | attempts=%s",
                cycle.cycle_id,
                leg_name,
                cycle.tail_resync_attempts,
            )
        try:
            left_qty = self._resync_exchange_position_qty("left")
            right_qty = self._resync_exchange_position_qty("right")
            finalize_success = False
            with self._state_lock:
                cycle = self.active_exit_cycle
                if cycle is None or cycle.cycle_id != cycle_id:
                    return
                self._apply_exchange_position_resync("left", left_qty)
                self._apply_exchange_position_resync("right", right_qty)
                self.logger.info(
                    "EXIT_TAIL_RESYNC_RESULT | cycle_id=%s | left_position_qty=%s | right_position_qty=%s",
                    cycle.cycle_id,
                    self._format_order_size(left_qty),
                    self._format_order_size(right_qty),
                )
                if self.left_leg_state.is_flat:
                    self.logger.info("EXIT_TAIL_LEG_CONFIRMED_FLAT | cycle_id=%s | leg=left", cycle.cycle_id)
                if self.right_leg_state.is_flat:
                    self.logger.info("EXIT_TAIL_LEG_CONFIRMED_FLAT | cycle_id=%s | leg=right", cycle.cycle_id)
                self._sync_active_exit_cycle_from_legs()
                self._publish_state()
                self.logger.info(
                    "EXIT_TAIL_REMAINDER_RECOMPUTED | cycle_id=%s | left_remaining_close_qty=%s | right_remaining_close_qty=%s | qty_mismatch=%s",
                    cycle.cycle_id,
                    self._format_order_size(self.left_leg_state.remaining_close_qty),
                    self._format_order_size(self.right_leg_state.remaining_close_qty),
                    self._format_order_size(self._position_qty_mismatch()),
                )
                finalize_success = self.left_leg_state.is_flat and self.right_leg_state.is_flat
            if finalize_success:
                self._finalize_exit_after_tail_resync_success()
        finally:
            with self._state_lock:
                if self.active_exit_cycle is not None and self.active_exit_cycle.cycle_id == cycle_id:
                    self.active_exit_cycle.tail_resync_in_progress = False
                    self._sync_active_exit_cycle_metrics()

    def _finalize_exit_after_tail_resync_success(self) -> None:
        cycle = self.active_exit_cycle
        if cycle is None:
            return
        self.logger.info(
            "EXIT_FINALIZED_AFTER_TAIL_RESYNC | cycle_id=%s | left_position_qty=%s | right_position_qty=%s",
            cycle.cycle_id,
            self._format_order_size(self.left_leg_state.actual_position_qty),
            self._format_order_size(self.right_leg_state.actual_position_qty),
        )
        self.exit_recovery_plan = None
        self._clear_recovery_status(context="EXIT_CYCLE")
        self._finalize_exit_cycle(state=StrategyCycleState.SUCCESS)
        self._reset_position_state()
        self.left_leg_state.filled_qty = Decimal("0")
        self.right_leg_state.filled_qty = Decimal("0")
        self._refresh_leg_position_derived_fields("left", confirmed_by_exchange=False)
        self._refresh_leg_position_derived_fields("right", confirmed_by_exchange=False)
        self._settle_dual_execution_state(reason="EXIT_FINALIZED_AFTER_TAIL_RESYNC")
        self.state.metrics["last_result"] = "EXIT_DONE"
        self._set_strategy_state(StrategyState.IDLE)
        self._publish_state()

    def _reconcile_exit_remainder_from_exchange(self, *, reason: str) -> None:
        cycle = self.active_exit_cycle
        if cycle is None or cycle.tail_resync_in_progress:
            return
        try:
            left_qty = self._resync_exchange_position_qty("left")
            right_qty = self._resync_exchange_position_qty("right")
        except Exception as exc:
            self.logger.warning(
                "exit remainder resync failed | cycle_id=%s | reason=%s | error=%s",
                cycle.cycle_id if cycle is not None else None,
                reason,
                exc,
            )
            return
        self._apply_exchange_position_resync("left", left_qty)
        self._apply_exchange_position_resync("right", right_qty)
        self._sync_active_exit_cycle_from_legs()
        self.logger.info(
            "exit remainder resynced | cycle_id=%s | reason=%s | left_real_remaining_qty=%s | right_real_remaining_qty=%s",
            cycle.cycle_id,
            reason,
            self._format_order_size(self.left_leg_state.remaining_close_qty),
            self._format_order_size(self.right_leg_state.remaining_close_qty),
        )
        self._publish_state()

    def _reconcile_entry_remainder_from_exchange(self, *, reason: str) -> bool:
        cycle = self.active_entry_cycle
        if cycle is None:
            return False
        try:
            left_qty = self._resync_exchange_position_qty("left")
            right_qty = self._resync_exchange_position_qty("right")
        except Exception as exc:
            self.logger.warning(
                "entry remainder resync failed | cycle_id=%s | reason=%s | error=%s",
                cycle.cycle_id,
                reason,
                exc,
            )
            return False
        self._apply_exchange_position_resync("left", left_qty)
        self._apply_exchange_position_resync("right", right_qty)
        self._sync_active_entry_cycle_from_legs()
        self.logger.info(
            "entry remainder resynced | cycle_id=%s | reason=%s | left_filled_qty=%s | right_filled_qty=%s",
            cycle.cycle_id,
            reason,
            self._format_order_size(self.active_entry_cycle.left_filled_qty if self.active_entry_cycle is not None else Decimal("0")),
            self._format_order_size(self.active_entry_cycle.right_filled_qty if self.active_entry_cycle is not None else Decimal("0")),
        )
        self._publish_state()
        return True

    def _apply_exchange_position_resync(self, leg_name: str, position_qty: Decimal) -> None:
        leg_state = self._leg_state(leg_name)
        normalized_qty = max(Decimal("0"), position_qty)
        leg_state.filled_qty = normalized_qty
        # Ресинк с биржи отражает только текущую позицию, а не наличие живых ордеров.
        # Здесь намеренно не помечаем ногу как PARTIALLY_FILLED, чтобы не создавать фантомные «живые ордера».
        if normalized_qty <= Decimal("0"):
            leg_state.avg_price = None
        # В обоих случаях считаем ордера завершёнными с точки зрения состояния исполнения.
        leg_state.order_status = "IDLE" if normalized_qty <= Decimal("0") else "FILLED"
        leg_state.requested_qty = Decimal("0")
        leg_state.remaining_qty = Decimal("0")
        leg_state.target_qty = Decimal("0")
        leg_state.last_error = None
        self._refresh_leg_position_derived_fields(leg_name, confirmed_by_exchange=True)
        self.state.metrics[f"{leg_name}_filled_qty"] = self._format_order_size(leg_state.actual_position_qty)
        self.state.metrics[f"{leg_name}_order_status"] = leg_state.order_status

    def _resync_exchange_position_qty(self, leg_name: str) -> Decimal:
        credentials = self.task.left_execution_credentials if leg_name == "left" else self.task.right_execution_credentials
        instrument = self._left_instrument if leg_name == "left" else self._right_instrument
        if credentials is None:
            return self._leg_state(leg_name).filled_qty
        exchange = str(credentials.exchange or instrument.exchange or "").strip().lower()
        if exchange == "binance":
            return self._resync_binance_position_qty(leg_name=leg_name, credentials=credentials, symbol=instrument.symbol)
        if exchange == "bitget":
            return self._resync_bitget_position_qty(leg_name=leg_name, credentials=credentials, symbol=instrument.symbol)
        if exchange == "bybit":
            return self._resync_bybit_position_qty(leg_name=leg_name, credentials=credentials, symbol=instrument.symbol)
        return self._leg_state(leg_name).filled_qty

    def _resync_binance_position_qty(self, *, leg_name: str, credentials, symbol: str) -> Decimal:
        connector = BinanceAccountConnector()
        payload = connector._signed_get(
            base_url=connector.FUTURES_BASE_URL,
            time_path=connector.FUTURES_TIME_PATH,
            path=connector.FUTURES_ACCOUNT_PATH,
            credentials=credentials,
        )
        positions = payload.get("positions", []) if isinstance(payload, dict) else []
        expected_entry_side = str(self._leg_state(leg_name).side or "").upper()
        total = Decimal("0")
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("symbol", "")).strip().upper() != str(symbol or "").strip().upper():
                continue
            position_amt = connector._decimal_value(item.get("positionAmt"))
            if position_amt == Decimal("0"):
                continue
            position_side = str(item.get("positionSide", "")).strip().upper()
            if expected_entry_side == "BUY":
                if position_amt > Decimal("0") or position_side == "LONG":
                    total += abs(position_amt)
            elif expected_entry_side == "SELL":
                if position_amt < Decimal("0") or position_side == "SHORT":
                    total += abs(position_amt)
        return total

    def _resync_bitget_position_qty(self, *, leg_name: str, credentials, symbol: str) -> Decimal:
        client = BitgetSignedHttpClient(credentials)
        positions = client.get(
            BitgetAccountConnector.FUTURES_POSITIONS_PATH,
            params={"productType": BitgetAccountConnector.PRODUCT_TYPE, "marginCoin": BitgetAccountConnector.MARGIN_COIN},
        ).get("data", [])
        expected_hold_side = "long" if str(self._leg_state(leg_name).side or "").upper() == "BUY" else "short"
        total = Decimal("0")
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("symbol", "")).strip().upper() != str(symbol or "").strip().upper():
                continue
            hold_side = str(item.get("holdSide", "") or item.get("posSide", "")).strip().lower()
            if hold_side and hold_side != expected_hold_side:
                continue
            total += BitgetAccountConnector._decimal_value(item.get("total"))
        return total

    def _resync_bybit_position_qty(self, *, leg_name: str, credentials, symbol: str) -> Decimal:
        connector = BybitAccountConnector()
        client = BybitV5HttpClient(credentials, timeout_seconds=connector._client_timeout_seconds)
        positions = connector._load_linear_positions(client)
        expected_side = "BUY" if str(self._leg_state(leg_name).side or "").upper() == "BUY" else "SELL"
        total = Decimal("0")
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("symbol", "")).strip().upper() != str(symbol or "").strip().upper():
                continue
            side = str(item.get("side", "")).strip().upper()
            if side and side != expected_side:
                continue
            total += BybitAccountConnector._decimal_value(item.get("size"))
        return total
