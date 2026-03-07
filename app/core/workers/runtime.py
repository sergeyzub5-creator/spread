from __future__ import annotations

import time
import threading
from decimal import Decimal, ROUND_DOWN
from typing import Any

from app.core.events.bus import EventBus
from app.core.execution.binance_usdm_adapter import BinanceUsdmExecutionAdapter
from app.core.execution.bitget_linear_adapter import BitgetLinearExecutionAdapter
from app.core.execution.bitget_linear_rest_adapter import BitgetLinearRestExecutionAdapter
from app.core.execution.bybit_linear_adapter import BybitLinearExecutionAdapter
from app.core.execution.adapter import ExecutionAdapter
from app.core.logging.logger_factory import get_logger
from app.core.market_data.service import MarketDataService
from app.core.models.execution import ExecutionOrderRequest, ExecutionStreamEvent
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1
from app.core.models.workers import WorkerEvent, WorkerState, WorkerTask


class WorkerRuntime:
    """Reusable runtime that can handle a single-instrument test slice or a pair task."""

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
        self._is_dual_runtime = (
            self._is_dual_quotes_runtime or self._is_dual_execution_runtime or self._is_spread_entry_runtime
        )
        self._dual_order_clocks: dict[str, dict[str, Any]] = {"left": {}, "right": {}}
        self._dual_poll_threads: dict[str, threading.Thread] = {}
        self._entry_lock = threading.Lock()
        self._entry_cycle_completed = False
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
                "dual_exec_status": "IDLE",
                "entry_threshold": str(task.runtime_params.get("entry_threshold") or task.entry_threshold or "0"),
                "entry_enabled": self._is_spread_entry_runtime,
                "active_edge": None,
                "entry_direction": None,
                "entry_block_reason": None,
                "entry_count": 0,
                "last_entry_ts": None,
            },
        )

    def start(self) -> None:
        self.state.status = "running"
        self.state.started_at = int(time.time() * 1000)
        self.state.last_error = None
        self.logger.info(
            "worker start | run_mode=%s | execution_mode=%s | instruments=%s",
            self.task.run_mode,
            self.task.execution_mode,
            [instrument.symbol for instrument in self._subscribed_instruments],
        )
        for instrument in self._subscribed_instruments:
            self.market_data_service.subscribe_l1(instrument, self.on_quote)
            self.logger.info("worker subscribed to L1 | exchange=%s | symbol=%s", instrument.exchange, instrument.symbol)
        if self._is_dual_execution_runtime or self._is_spread_entry_runtime:
            self._ensure_dual_execution_adapters()
        elif not self._is_dual_quotes_runtime:
            self._ensure_execution_adapter()
        self._publish_state()
        self.emit_event(
            "runtime_started" if self._is_dual_runtime else "worker_started",
            {
                "instruments": [instrument.symbol for instrument in self._subscribed_instruments],
                "left_instrument": self._left_instrument.symbol,
                "right_instrument": self._right_instrument.symbol,
            },
        )

    def stop(self) -> None:
        self.logger.info("worker stop requested")
        for instrument in self._subscribed_instruments:
            self.market_data_service.unsubscribe_l1(instrument, self.on_quote)
        if self._execution_adapter is not None:
            self._execution_adapter.close()
            self._execution_adapter = None
        if self._left_execution_adapter is not None:
            self._left_execution_adapter.close()
            self._left_execution_adapter = None
        if self._right_execution_adapter is not None and self._right_execution_adapter is not self._left_execution_adapter:
            self._right_execution_adapter.close()
            self._right_execution_adapter = None
        self.state.status = "stopped"
        self.state.stopped_at = int(time.time() * 1000)
        self._publish_state()
        self.emit_event("runtime_stopped" if self._is_dual_runtime else "worker_stopped", {})

    def on_quote(self, quote: QuoteL1) -> None:
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
            self.logger.info(
                "worker first quote received | symbol=%s | bid=%s | ask=%s | ts_exchange=%s | ts_local=%s",
                quote.instrument_id.symbol,
                quote.bid,
                quote.ask,
                quote.ts_exchange,
                quote.ts_local,
            )
        self._publish_state()
        if self._is_dual_runtime:
            if quote.instrument_id == self._left_instrument:
                self.emit_event(
                    "left_quote_update",
                    {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange},
                )
            elif quote.instrument_id == self._right_instrument:
                self.emit_event(
                    "right_quote_update",
                    {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange},
                )
            if self._has_live_spread():
                self.emit_event(
                    "spread_update",
                    {
                        "edge_1": self.state.metrics.get("edge_1"),
                        "edge_2": self.state.metrics.get("edge_2"),
                        "spread_state": self.state.metrics.get("spread_state"),
                    },
                )
            if self._is_spread_entry_runtime:
                self._evaluate_spread_entry()
        else:
            self.emit_event(
                "quote_received",
                {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange},
            )

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

            quantity = self._compute_order_quantity(
                target_notional=self.task.target_notional,
                reference_price=reference_price,
                step_size=self._active_instrument.spec.qty_precision,
                min_qty=self._active_instrument.spec.min_qty,
            )
            request = ExecutionOrderRequest(
                instrument_id=self._active_instrument,
                side=side_upper,
                order_type="MARKET",
                quantity=quantity,
                response_type="RESULT",
            )
            self.logger.info(
                "worker sending test order | symbol=%s | side=%s | reference_price=%s | quantity=%s | target_notional=%s | submitted_at_ms=%s",
                self._active_instrument.symbol,
                side_upper,
                reference_price,
                quantity,
                self.task.target_notional,
                submitted_at_ms or int(time.time() * 1000),
            )
            send_started_ms = int(submitted_at_ms or int(time.time() * 1000))
            self._pending_order_clock = {
                "submitted_at_ms": send_started_ms,
                "symbol": self._active_instrument.symbol,
                "first_event_seen": False,
                "filled_seen": False,
                "order_id": None,
                "client_order_id": None,
                "request_sent_at_ms": None,
                "connection_reused": None,
            }

            def _on_request_sent(meta: dict[str, Any]) -> None:
                request_sent_at_ms = int(meta.get("sent_at_ms") or int(time.time() * 1000))
                if self._pending_order_clock is not None:
                    self._pending_order_clock["request_sent_at_ms"] = request_sent_at_ms
                    self._pending_order_clock["connection_reused"] = bool(meta.get("connection_reused"))
                self.state.metrics["last_click_to_send_latency_ms"] = max(0, request_sent_at_ms - send_started_ms)
                self.state.metrics["last_transport_connection_mode"] = (
                    "warm" if bool(meta.get("connection_reused")) else "cold"
                )
                self.logger.info(
                    "worker order transport send | symbol=%s | side=%s | click_to_send_ms=%s | connection_mode=%s | time_offset_ms=%s",
                    self._active_instrument.symbol,
                    side_upper,
                    self.state.metrics["last_click_to_send_latency_ms"],
                    self.state.metrics["last_transport_connection_mode"],
                    meta.get("time_offset_ms"),
                )

            ack = adapter.place_order(request, on_request_sent=_on_request_sent)
            self.state.last_error = None
            self.state.metrics["last_order_ack_status"] = ack.status
            self.state.metrics["last_order_id"] = ack.order_id
            ack_meta = ack.raw.get("_transport_meta", {}) if isinstance(ack.raw, dict) else {}
            response_at_ms = int(ack_meta.get("response_at_ms") or int(time.time() * 1000))
            request_sent_at_ms = int(ack_meta.get("sent_at_ms") or send_started_ms)
            self.state.metrics["last_ack_latency_ms"] = max(0, response_at_ms - send_started_ms)
            self.state.metrics["last_send_to_ack_latency_ms"] = max(0, response_at_ms - request_sent_at_ms)
            self.logger.info(
                "worker order ack latency | symbol=%s | order_id=%s | ack_latency_ms=%s | send_to_ack_ms=%s | status=%s",
                ack.symbol,
                ack.order_id,
                self.state.metrics["last_ack_latency_ms"],
                self.state.metrics["last_send_to_ack_latency_ms"],
                ack.status,
            )
            if self._pending_order_clock is not None:
                self._pending_order_clock["order_id"] = ack.order_id
                self._pending_order_clock["client_order_id"] = ack.client_order_id
                self._pending_order_clock["request_sent_at_ms"] = request_sent_at_ms
            self._publish_state()
            self.emit_event(
                "order_ack_received",
                {
                    "symbol": ack.symbol,
                    "side": ack.side,
                    "status": ack.status,
                    "order_id": ack.order_id,
                    "client_order_id": ack.client_order_id,
                    "ack_latency_ms": self.state.metrics["last_ack_latency_ms"],
                    "click_to_send_latency_ms": self.state.metrics["last_click_to_send_latency_ms"],
                    "send_to_ack_latency_ms": self.state.metrics["last_send_to_ack_latency_ms"],
                    "connection_mode": self.state.metrics["last_transport_connection_mode"],
                },
            )
            return ack.to_dict()
        except Exception as exc:
            self.state.last_error = str(exc)
            self._publish_state()
            self.emit_event(
                "order_failed",
                {
                    "symbol": self._active_instrument.symbol,
                    "side": str(side or "").strip().upper(),
                    "error": str(exc),
                },
            )
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
    ) -> dict[str, Any]:
        if not (self._is_dual_execution_runtime or self._is_spread_entry_runtime):
            raise RuntimeError("Dual execution is not enabled for this runtime")
        if self.state.metrics.get("dual_exec_status") in {"SENDING", "PARTIAL"}:
            raise RuntimeError("Dual execution is already in progress")
        try:
            left_quote = self._latest_quotes.get(self._left_instrument)
            right_quote = self._latest_quotes.get(self._right_instrument)
            if left_quote is None or right_quote is None:
                raise RuntimeError("Both live quotes are required before dual execution")

            send_started_ms = int(submitted_at_ms or int(time.time() * 1000))
            self._reset_dual_execution_metrics()
            self.state.last_error = None
            self.state.metrics["dual_exec_status"] = "SENDING"
            self._publish_state()
            self.emit_event(
                "dual_exec_started",
                {
                    "left_symbol": self._left_instrument.symbol,
                    "right_symbol": self._right_instrument.symbol,
                    "left_side": str(left_side or "").strip().upper(),
                    "right_side": str(right_side or "").strip().upper(),
                    "left_qty": str(left_qty or "").strip(),
                    "right_qty": str(right_qty or "").strip(),
                },
            )
            if self._is_spread_entry_runtime:
                self.state.metrics["entry_enabled"] = False
                self.state.metrics["entry_count"] = int(self.state.metrics.get("entry_count") or 0) + 1
                self.state.metrics["last_entry_ts"] = send_started_ms
                self._entry_cycle_completed = True
                self.emit_event(
                    "entry_started",
                    {
                        "left_side": str(left_side or "").strip().upper(),
                        "right_side": str(right_side or "").strip().upper(),
                        "left_qty": str(left_qty or "").strip(),
                        "right_qty": str(right_qty or "").strip(),
                        "active_edge": self.state.metrics.get("active_edge"),
                        "entry_direction": self.state.metrics.get("entry_direction"),
                    },
                )

            left_request = self._build_dual_order_request(
                instrument=self._left_instrument,
                quote=left_quote,
                side=left_side,
                qty_text=left_qty,
                price_mode=left_price_mode,
            )
            right_request = self._build_dual_order_request(
                instrument=self._right_instrument,
                quote=right_quote,
                side=right_side,
                qty_text=right_qty,
                price_mode=right_price_mode,
            )

            self._dual_order_clocks["left"] = {
                "submitted_at_ms": send_started_ms,
                "first_event_seen": False,
                "filled_seen": False,
                "request_sent_at_ms": None,
                "order_id": None,
                "client_order_id": None,
            }
            self._dual_order_clocks["right"] = {
                "submitted_at_ms": send_started_ms,
                "first_event_seen": False,
                "filled_seen": False,
                "request_sent_at_ms": None,
                "order_id": None,
                "client_order_id": None,
            }

            adapters = self._ensure_dual_execution_adapters()
            results: dict[str, Any] = {}
            errors: list[str] = []
            result_lock = threading.Lock()

            def _send_leg(leg_name: str, adapter: ExecutionAdapter, request: ExecutionOrderRequest) -> None:
                try:
                    ack = adapter.place_order(
                        request,
                        on_request_sent=lambda meta, leg=leg_name: self._on_dual_request_sent(leg, meta),
                    )
                    self._on_dual_order_ack(leg_name, ack)
                    with result_lock:
                        results[leg_name] = ack.to_dict()
                except Exception as exc:
                    self._on_dual_order_failed(leg_name, exc)
                    with result_lock:
                        errors.append(f"{leg_name}:{exc}")

            left_thread = threading.Thread(
                target=_send_leg,
                args=("left", adapters["left"], left_request),
                name=f"{self.task.worker_id}-left-order",
                daemon=True,
            )
            right_thread = threading.Thread(
                target=_send_leg,
                args=("right", adapters["right"], right_request),
                name=f"{self.task.worker_id}-right-order",
                daemon=True,
            )
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
            self.emit_event("runtime_error", {"error": str(exc)})
            raise

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
                self.logger.info(
                    "worker first execution event latency | symbol=%s | order_id=%s | event_type=%s | order_status=%s | latency_ms=%s | send_to_event_ms=%s",
                    event.symbol,
                    event.order_id,
                    event.execution_type,
                    event.order_status,
                    self.state.metrics["last_first_event_latency_ms"],
                    self.state.metrics["last_send_to_first_event_latency_ms"],
                )
            if event.order_status == "FILLED" and not bool(self._pending_order_clock.get("filled_seen")):
                self.state.metrics["last_fill_latency_ms"] = max(0, now_ms - submitted_at_ms)
                self.state.metrics["last_send_to_fill_latency_ms"] = max(0, now_ms - request_sent_at_ms)
                self._pending_order_clock["filled_seen"] = True
                self.logger.info(
                    "worker fill latency | symbol=%s | order_id=%s | fill_latency_ms=%s | send_to_fill_ms=%s | last_fill_qty=%s | last_fill_price=%s",
                    event.symbol,
                    event.order_id,
                    self.state.metrics["last_fill_latency_ms"],
                    self.state.metrics["last_send_to_fill_latency_ms"],
                    event.last_fill_qty,
                    event.last_fill_price or event.average_price,
                )
        self.state.metrics["last_execution_type"] = event.execution_type
        self.state.metrics["last_order_status"] = event.order_status
        self.state.metrics["last_fill_qty"] = event.last_fill_qty
        self.state.metrics["last_fill_price"] = event.last_fill_price or event.average_price
        self.state.metrics["last_realized_pnl"] = event.realized_pnl
        self.logger.info(
            "worker execution event applied | symbol=%s | order_id=%s | execution_type=%s | order_status=%s | event_time=%s | transaction_time=%s",
            event.symbol,
            event.order_id,
            event.execution_type,
            event.order_status,
            event.event_time,
            event.transaction_time,
        )
        self._publish_state()
        self.emit_event("execution_event_received", event.to_dict())

    def emit_event(self, event_type: str, payload: dict[str, Any]) -> None:
        event = WorkerEvent(
            worker_id=self.task.worker_id,
            event_type=event_type,
            timestamp=int(time.time() * 1000),
            payload=dict(payload),
        )
        self.event_bus.publish("worker_events", event)

    def _publish_state(self) -> None:
        self._refresh_derived_metrics()
        self.event_bus.publish("worker_state", self.state)

    def _refresh_derived_metrics(self) -> None:
        if not self._is_dual_runtime:
            return
        now_ms = int(time.time() * 1000)
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        self.state.metrics["left_quote_age_ms"] = (
            max(0, now_ms - int(left_quote.ts_local)) if left_quote is not None else None
        )
        self.state.metrics["right_quote_age_ms"] = (
            max(0, now_ms - int(right_quote.ts_local)) if right_quote is not None else None
        )
        self.state.metrics["spread_state"] = "LIVE" if self._has_live_spread() else "WAITING_QUOTES"

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
        if left_quote is None or right_quote is None:
            self.state.metrics["edge_1"] = None
            self.state.metrics["edge_2"] = None
            self.state.metrics["spread_state"] = "WAITING_QUOTES"
            return
        edge_1 = self._safe_edge(left_quote.bid, right_quote.ask)
        edge_2 = self._safe_edge(right_quote.bid, left_quote.ask)
        self.state.metrics["edge_1"] = self._format_edge(edge_1)
        self.state.metrics["edge_2"] = self._format_edge(edge_2)
        self.state.metrics["spread_state"] = "LIVE"

    def _evaluate_spread_entry(self) -> None:
        if not self._is_spread_entry_runtime:
            return
        if self._entry_cycle_completed:
            return
        if self.state.metrics.get("dual_exec_status") in {"SENDING", "PARTIAL", "DONE"}:
            return
        with self._entry_lock:
            if self._entry_cycle_completed:
                return
            signal = self._select_entry_signal()
            if signal is None:
                return
            edge_name, edge_value, left_side, right_side = signal
            self.state.metrics["active_edge"] = edge_name
            self.state.metrics["entry_direction"] = f"LEFT_{left_side}_RIGHT_{right_side}"
            self.emit_event(
                "entry_signal_detected",
                {
                    "active_edge": edge_name,
                    "edge_value": self._format_edge(edge_value),
                    "entry_direction": self.state.metrics.get("entry_direction"),
                },
            )
            block_reason = self._validate_entry_conditions(left_side=left_side, right_side=right_side)
            if block_reason is not None:
                self.state.metrics["entry_block_reason"] = block_reason
                self.emit_event(
                    "entry_blocked",
                    {
                        "reason": block_reason,
                        "active_edge": edge_name,
                        "entry_direction": self.state.metrics.get("entry_direction"),
                    },
                )
                self._publish_state()
                return
            self.state.metrics["entry_block_reason"] = None
            try:
                self.submit_dual_test_orders(
                    left_side=left_side,
                    right_side=right_side,
                    left_qty=str(self.task.runtime_params.get("left_qty") or "0"),
                    right_qty=str(self.task.runtime_params.get("right_qty") or "0"),
                    left_price_mode=str(self.task.runtime_params.get("left_price_mode") or "top_of_book"),
                    right_price_mode=str(self.task.runtime_params.get("right_price_mode") or "top_of_book"),
                    submitted_at_ms=int(time.time() * 1000),
                )
            except Exception:
                # Failure path is already published inside dual execution callbacks.
                pass

    def _select_entry_signal(self) -> tuple[str, Decimal, str, str] | None:
        threshold = self._decimal_or_zero(self.task.runtime_params.get("entry_threshold") or self.task.entry_threshold)
        if threshold <= Decimal("0"):
            return None
        edge_1 = self._decimal_or_none(self.state.metrics.get("edge_1"))
        edge_2 = self._decimal_or_none(self.state.metrics.get("edge_2"))
        if edge_1 is not None and edge_1 >= threshold:
            return ("edge_1", edge_1, "SELL", "BUY")
        if edge_2 is not None and edge_2 >= threshold:
            return ("edge_2", edge_2, "BUY", "SELL")
        return None

    def _validate_entry_conditions(self, *, left_side: str, right_side: str) -> str | None:
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        if left_quote is None or right_quote is None:
            return "WAITING_QUOTES"
        quote_reason = self._validate_quote(left_quote, "LEFT")
        if quote_reason is not None:
            return quote_reason
        quote_reason = self._validate_quote(right_quote, "RIGHT")
        if quote_reason is not None:
            return quote_reason
        max_age_ms = self._int_or_zero(self.task.runtime_params.get("max_quote_age_ms"))
        if max_age_ms > 0:
            left_age_ms = int(self.state.metrics.get("left_quote_age_ms") or 0)
            right_age_ms = int(self.state.metrics.get("right_quote_age_ms") or 0)
            if left_age_ms > max_age_ms:
                return "LEFT_STALE_QUOTE"
            if right_age_ms > max_age_ms:
                return "RIGHT_STALE_QUOTE"
        max_skew_ms = self._int_or_zero(self.task.runtime_params.get("max_quote_skew_ms"))
        if max_skew_ms > 0:
            skew_ms = abs(int(left_quote.ts_local) - int(right_quote.ts_local))
            if skew_ms > max_skew_ms:
                return "QUOTE_SKEW_TOO_LARGE"
        left_qty = self._decimal_or_zero(self.task.runtime_params.get("left_qty"))
        right_qty = self._decimal_or_zero(self.task.runtime_params.get("right_qty"))
        if left_qty <= Decimal("0") or right_qty <= Decimal("0"):
            return "INVALID_TEST_QTY"
        if not self._has_top_qty(quote=left_quote, side=left_side, required_qty=left_qty):
            return "INSUFFICIENT_TOP_QTY"
        if not self._has_top_qty(quote=right_quote, side=right_side, required_qty=right_qty):
            return "INSUFFICIENT_TOP_QTY"
        return None

    @staticmethod
    def _validate_quote(quote: QuoteL1, prefix: str) -> str | None:
        if quote.bid <= Decimal("0") or quote.ask <= Decimal("0"):
            return f"{prefix}_INVALID_PRICE"
        if quote.bid >= quote.ask:
            return f"{prefix}_CROSSED_BOOK"
        if quote.bid_qty <= Decimal("0") or quote.ask_qty <= Decimal("0"):
            return f"{prefix}_INVALID_QTY"
        return None

    @staticmethod
    def _has_top_qty(*, quote: QuoteL1, side: str, required_qty: Decimal) -> bool:
        available_qty = quote.ask_qty if str(side or "").strip().upper() == "BUY" else quote.bid_qty
        return available_qty >= required_qty

    def _has_live_spread(self) -> bool:
        return self._latest_quotes.get(self._left_instrument) is not None and self._latest_quotes.get(self._right_instrument) is not None

    @staticmethod
    def _safe_edge(numerator_left: Decimal, denominator_right: Decimal) -> Decimal | None:
        if denominator_right <= Decimal("0"):
            return None
        return (numerator_left - denominator_right) / denominator_right

    @staticmethod
    def _format_edge(value: Decimal | None) -> str | None:
        if value is None:
            return None
        return f"{value:.6f}"

    def _ensure_dual_execution_adapters(self) -> dict[str, ExecutionAdapter]:
        if self._left_execution_adapter is None:
            self._left_execution_adapter = self._create_execution_adapter(
                instrument=self._left_instrument,
                credentials=self.task.left_execution_credentials,
            )
            self._left_execution_adapter.connect()
            self._left_execution_adapter.on_execution_event(
                lambda event, leg_name="left": self._on_dual_execution_event(leg_name, event)
            )
        if self._right_execution_adapter is None:
            self._right_execution_adapter = self._create_execution_adapter(
                instrument=self._right_instrument,
                credentials=self.task.right_execution_credentials,
            )
            self._right_execution_adapter.connect()
            self._right_execution_adapter.on_execution_event(
                lambda event, leg_name="right": self._on_dual_execution_event(leg_name, event)
            )
        return {"left": self._left_execution_adapter, "right": self._right_execution_adapter}

    def _ensure_execution_adapter(self) -> ExecutionAdapter:
        if self._execution_adapter is not None:
            return self._execution_adapter
        credentials = self.task.execution_credentials
        if credentials is None:
            raise RuntimeError("Execution credentials are not configured for worker")
        adapter = self._create_execution_adapter(instrument=self._active_instrument, credentials=credentials)
        adapter.connect()
        adapter.on_execution_event(self.on_execution_event)
        self._execution_adapter = adapter
        return adapter

    def _create_execution_adapter(
        self,
        *,
        instrument: InstrumentId,
        credentials,
    ) -> ExecutionAdapter:
        if credentials is None:
            raise RuntimeError(f"Execution credentials are not configured for {instrument.exchange}")
        if instrument.exchange == "binance" and instrument.market_type == "linear_perp":
            return BinanceUsdmExecutionAdapter(credentials)
        if instrument.exchange == "bybit" and instrument.market_type == "linear_perp":
            return BybitLinearExecutionAdapter(credentials)
        if instrument.exchange == "bitget" and instrument.market_type == "linear_perp":
            selected_route = str(
                credentials.account_profile.get("selected_execution_route")
                or credentials.account_profile.get("preferred_execution_route")
                or ""
            ).strip().lower()
            if selected_route == "bitget_linear_rest_probe":
                return BitgetLinearRestExecutionAdapter(credentials)
            return BitgetLinearExecutionAdapter(credentials)
        raise RuntimeError(f"No execution adapter for {instrument.exchange}:{instrument.market_type}")

    def _build_dual_order_request(
        self,
        *,
        instrument: InstrumentId,
        quote: QuoteL1,
        side: str,
        qty_text: str,
        price_mode: str,
    ) -> ExecutionOrderRequest:
        normalized_side = str(side or "").strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported side: {side}")
        try:
            quantity = Decimal(str(qty_text).strip())
        except Exception as exc:
            raise ValueError(f"Invalid quantity: {qty_text}") from exc
        if quantity <= Decimal("0"):
            raise ValueError(f"Invalid quantity: {qty_text}")
        order_price = quote.ask if normalized_side == "BUY" else quote.bid
        if str(price_mode or "").strip().lower() != "top_of_book":
            raise ValueError(f"Unsupported price mode: {price_mode}")
        return ExecutionOrderRequest(
            instrument_id=instrument,
            side=normalized_side,
            order_type="LIMIT",
            quantity=quantity,
            price=order_price,
            time_in_force="GTC",
            response_type="ACK",
        )

    def _on_dual_request_sent(self, leg_name: str, meta: dict[str, Any]) -> None:
        clock = self._dual_order_clocks.get(leg_name)
        if not clock:
            return
        clock["request_sent_at_ms"] = int(meta.get("sent_at_ms") or int(time.time() * 1000))
        self.state.metrics[f"{leg_name}_order_status"] = "SENT"
        self._refresh_dual_exec_status()
        self._publish_state()

    def _on_dual_order_ack(self, leg_name: str, ack: ExecutionOrderResult) -> None:
        now_ms = int(time.time() * 1000)
        clock = self._dual_order_clocks.get(leg_name, {})
        submitted_at_ms = int(clock.get("submitted_at_ms") or now_ms)
        clock["order_id"] = ack.order_id
        clock["client_order_id"] = ack.client_order_id
        self.state.metrics[f"{leg_name}_order_status"] = ack.status or "ACK"
        self.state.metrics[f"{leg_name}_ack_latency_ms"] = max(0, now_ms - submitted_at_ms)
        self._refresh_dual_exec_status()
        self._publish_state()
        self.emit_event(
            f"{leg_name}_order_ack",
            {
                "symbol": ack.symbol,
                "order_id": ack.order_id,
                "client_order_id": ack.client_order_id,
                "status": ack.status,
                "ack_latency_ms": self.state.metrics.get(f"{leg_name}_ack_latency_ms"),
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
                },
            )
        if ack.route == BitgetLinearRestExecutionAdapter.ROUTE_NAME:
            self._start_rest_order_poll(leg_name, ack)

    def _on_dual_execution_event(self, leg_name: str, event: ExecutionStreamEvent) -> None:
        instrument = self._left_instrument if leg_name == "left" else self._right_instrument
        if event.symbol and event.symbol != instrument.symbol:
            return
        now_ms = int(time.time() * 1000)
        clock = self._dual_order_clocks.get(leg_name, {})
        submitted_at_ms = int(clock.get("submitted_at_ms") or now_ms)
        if not bool(clock.get("first_event_seen")):
            self.state.metrics[f"{leg_name}_first_event_latency_ms"] = max(0, now_ms - submitted_at_ms)
            clock["first_event_seen"] = True
        self.state.metrics[f"{leg_name}_order_status"] = event.order_status or event.execution_type or "EVENT"
        if event.last_fill_qty:
            self.state.metrics[f"{leg_name}_filled_qty"] = event.cumulative_fill_qty or event.last_fill_qty
        self._publish_state()
        self.emit_event(f"{leg_name}_order_event", event.to_dict())
        if self._is_spread_entry_runtime:
            self.emit_event(
                f"entry_{leg_name}_event",
                {
                    **event.to_dict(),
                    "first_event_latency_ms": self.state.metrics.get(f"{leg_name}_first_event_latency_ms"),
                },
            )
        if str(event.order_status or "").upper() == "FILLED":
            if not bool(clock.get("filled_seen")):
                self.state.metrics[f"{leg_name}_fill_latency_ms"] = max(0, now_ms - submitted_at_ms)
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
                    },
                )
        else:
            self._refresh_dual_exec_status()

    def _on_dual_order_failed(self, leg_name: str, exc: Exception) -> None:
        self.state.last_error = str(exc)
        self.state.metrics[f"{leg_name}_order_status"] = "FAILED"
        self._refresh_dual_exec_status()
        self._publish_state()
        self.emit_event(
            "dual_exec_failed",
            {"leg": leg_name, "error": str(exc)},
        )
        if self._is_spread_entry_runtime:
            self.emit_event(
                "entry_failed",
                {"leg": leg_name, "error": str(exc)},
            )
        self.emit_event(
            "runtime_error",
            {"leg": leg_name, "error": str(exc)},
        )

    def _start_rest_order_poll(self, leg_name: str, ack: ExecutionOrderResult) -> None:
        adapter = self._left_execution_adapter if leg_name == "left" else self._right_execution_adapter
        if adapter is None:
            return
        if not ack.order_id and not ack.client_order_id:
            return

        def _poll() -> None:
            deadline_ms = int(time.time() * 1000) + 15000
            symbol = ack.symbol
            while int(time.time() * 1000) < deadline_ms and self.state.status == "running":
                try:
                    result = adapter.query_order(symbol=symbol, order_id=ack.order_id, client_order_id=ack.client_order_id)
                    event = ExecutionStreamEvent(
                        exchange=result.exchange,
                        event_type="rest_query",
                        event_time=result.update_time,
                        transaction_time=result.update_time,
                        symbol=result.symbol,
                        order_id=result.order_id,
                        client_order_id=result.client_order_id,
                        order_status=result.status,
                        execution_type=result.status,
                        side=result.side,
                        order_type=result.order_type,
                        position_side=result.position_side,
                        last_fill_qty=result.executed_qty,
                        cumulative_fill_qty=result.executed_qty,
                        last_fill_price=result.avg_price or result.price,
                        average_price=result.avg_price or result.price,
                        realized_pnl=None,
                        raw=result.raw,
                    )
                    self._on_dual_execution_event(leg_name, event)
                    if str(result.status or "").upper() in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "FAILED"}:
                        return
                except Exception as exc:
                    self.logger.warning("dual rest poll failed | leg=%s | error=%s", leg_name, exc)
                    return
                time.sleep(0.25)

        poll_thread = threading.Thread(
            target=_poll,
            name=f"{self.task.worker_id}-{leg_name}-rest-poll",
            daemon=True,
        )
        self._dual_poll_threads[leg_name] = poll_thread
        poll_thread.start()

    def _refresh_dual_exec_status(self) -> None:
        left_status = str(self.state.metrics.get("left_order_status") or "IDLE").upper()
        right_status = str(self.state.metrics.get("right_order_status") or "IDLE").upper()
        if "FAILED" in {left_status, right_status}:
            self.state.metrics["dual_exec_status"] = "FAILED"
            return
        if left_status == "FILLED" and right_status == "FILLED":
            self.state.metrics["dual_exec_status"] = "DONE"
            self.emit_event(
                "dual_exec_done",
                {
                    "left_status": left_status,
                    "right_status": right_status,
                },
            )
            if self._is_spread_entry_runtime:
                self.emit_event(
                    "entry_done",
                    {
                        "left_status": left_status,
                        "right_status": right_status,
                        "active_edge": self.state.metrics.get("active_edge"),
                        "entry_direction": self.state.metrics.get("entry_direction"),
                    },
                )
            return
        if any(status in {"ACK", "ACCEPTED", "FILLED", "NEW", "PARTIALLY_FILLED", "PARTIALLYFILLED"} for status in {left_status, right_status}):
            self.state.metrics["dual_exec_status"] = "PARTIAL"
            return
        if any(status in {"SENDING", "SENT"} for status in {left_status, right_status}):
            self.state.metrics["dual_exec_status"] = "SENDING"
            return
        self.state.metrics["dual_exec_status"] = "IDLE"

    def _reset_dual_execution_metrics(self) -> None:
        for key in (
            "left_order_status",
            "right_order_status",
            "left_ack_latency_ms",
            "right_ack_latency_ms",
            "left_first_event_latency_ms",
            "right_first_event_latency_ms",
            "left_fill_latency_ms",
            "right_fill_latency_ms",
            "left_filled_qty",
            "right_filled_qty",
        ):
            self.state.metrics[key] = None
        self.state.metrics["left_order_status"] = "SENDING"
        self.state.metrics["right_order_status"] = "SENDING"

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

    @staticmethod
    def _compute_order_quantity(
        *,
        target_notional: Decimal,
        reference_price: Decimal,
        step_size: Decimal,
        min_qty: Decimal,
    ) -> Decimal:
        if reference_price <= Decimal("0"):
            raise RuntimeError("Reference price must be positive")
        raw_quantity = target_notional / reference_price
        if step_size > Decimal("0"):
            steps = (raw_quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
            quantity = steps * step_size
        else:
            quantity = raw_quantity
        if min_qty > Decimal("0") and quantity < min_qty:
            quantity = min_qty
        if quantity <= Decimal("0"):
            raise RuntimeError("Computed order quantity is zero")
        return quantity.normalize()

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value in (None, "", "-"):
            return None
        try:
            return Decimal(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _decimal_or_zero(value: Any) -> Decimal:
        parsed = WorkerRuntime._decimal_or_none(value)
        return parsed if parsed is not None else Decimal("0")

    @staticmethod
    def _int_or_zero(value: Any) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return 0
