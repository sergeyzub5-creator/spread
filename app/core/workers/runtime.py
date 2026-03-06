from __future__ import annotations

import time
from decimal import Decimal

from app.core.events.bus import EventBus
from app.core.logging.logger_factory import get_logger
from app.core.market_data.service import MarketDataService
from app.core.models.market_data import QuoteL1
from app.core.models.workers import WorkerEvent, WorkerState, WorkerTask


class WorkerRuntime:
    """Single reusable runtime implementation for any worker_id."""

    def __init__(self, task: WorkerTask, market_data_service: MarketDataService, event_bus: EventBus) -> None:
        self.task = task
        self.market_data_service = market_data_service
        self.event_bus = event_bus
        self.logger = get_logger("worker.runtime", worker_id=task.worker_id)
        self._latest_quotes: dict[InstrumentId, QuoteL1] = {}
        self.state = WorkerState(
            worker_id=task.worker_id,
            status="created",
            current_pair=(task.left_instrument, task.right_instrument),
            last_error=None,
            started_at=None,
            stopped_at=None,
            metrics={"quote_count": 0, "last_bid_ask_gap": str(Decimal("0"))},
        )

    def start(self) -> None:
        self.state.status = "running"
        self.state.started_at = int(time.time() * 1000)
        self.market_data_service.subscribe_l1(self.task.left_instrument, self.on_quote)
        self.market_data_service.subscribe_l1(self.task.right_instrument, self.on_quote)
        self.emit_event("worker_started", {"pair": [self.task.left_instrument.symbol, self.task.right_instrument.symbol]})

    def stop(self) -> None:
        self.market_data_service.unsubscribe_l1(self.task.left_instrument, self.on_quote)
        self.market_data_service.unsubscribe_l1(self.task.right_instrument, self.on_quote)
        self.state.status = "stopped"
        self.state.stopped_at = int(time.time() * 1000)
        self.emit_event("worker_stopped", {})

    def on_quote(self, quote: QuoteL1) -> None:
        self._latest_quotes[quote.instrument_id] = quote
        self.state.metrics["quote_count"] = int(self.state.metrics.get("quote_count", 0)) + 1
        self.state.metrics["last_quote_ts_local"] = quote.ts_local
        self.emit_event(
            "quote_received",
            {"instrument": quote.instrument_id.symbol, "exchange": quote.instrument_id.exchange},
        )

    def emit_event(self, event_type: str, payload: dict) -> None:
        event = WorkerEvent(
            worker_id=self.task.worker_id,
            event_type=event_type,
            timestamp=int(time.time() * 1000),
            payload=dict(payload),
        )
        self.event_bus.publish("worker_events", event)
