from __future__ import annotations

import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

from app.core.events.bus import EventBus
from app.core.execution.binance_usdm_adapter import BinanceUsdmExecutionAdapter
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
        self._active_instrument = task.left_instrument
        self._pending_order_clock: dict[str, Any] | None = None
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
        self._ensure_execution_adapter()
        self._publish_state()
        self.emit_event(
            "worker_started",
            {"instruments": [instrument.symbol for instrument in self._subscribed_instruments]},
        )

    def stop(self) -> None:
        self.logger.info("worker stop requested")
        for instrument in self._subscribed_instruments:
            self.market_data_service.unsubscribe_l1(instrument, self.on_quote)
        if self._execution_adapter is not None:
            self._execution_adapter.close()
            self._execution_adapter = None
        self.state.status = "stopped"
        self.state.stopped_at = int(time.time() * 1000)
        self._publish_state()
        self.emit_event("worker_stopped", {})

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
        self.event_bus.publish("worker_state", self.state)

    def _ensure_execution_adapter(self) -> ExecutionAdapter:
        if self._execution_adapter is not None:
            return self._execution_adapter
        credentials = self.task.execution_credentials
        if credentials is None:
            raise RuntimeError("Execution credentials are not configured for worker")
        if self._active_instrument.exchange == "binance" and self._active_instrument.market_type == "linear_perp":
            adapter = BinanceUsdmExecutionAdapter(credentials)
            adapter.connect()
            adapter.on_execution_event(self.on_execution_event)
            self._execution_adapter = adapter
            return adapter
        if self._active_instrument.exchange == "bybit" and self._active_instrument.market_type == "linear_perp":
            adapter = BybitLinearExecutionAdapter(credentials)
            adapter.connect()
            adapter.on_execution_event(self.on_execution_event)
            self._execution_adapter = adapter
            return adapter
        raise RuntimeError(
            f"No execution adapter for {self._active_instrument.exchange}:{self._active_instrument.market_type}"
        )

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
