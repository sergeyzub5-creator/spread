from __future__ import annotations

import threading
import unittest
from decimal import Decimal

from app.core.events.bus import EventBus
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec
from app.core.models.workers import WorkerEvent, WorkerState, WorkerTask
from app.core.workers.manager import WorkerManager


def _make_instrument(symbol: str) -> InstrumentId:
    return InstrumentId(
        key=InstrumentKey(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
        ),
        spec=InstrumentSpec(
            base_asset=symbol.replace("USDT", ""),
            quote_asset="USDT",
            contract_type="spot",
            settle_asset="USDT",
            price_precision=Decimal("0.01"),
            qty_precision=Decimal("0.0001"),
            min_qty=Decimal("0.0001"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_channel="bookTicker",
            ws_symbol=symbol.lower(),
            order_route="spot_rest",
        ),
    )


class _FakeMarketDataService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.subscribed: list[tuple[InstrumentId, object]] = []
        self.unsubscribed: list[tuple[InstrumentId, object]] = []

    def subscribe_l1(self, instrument: InstrumentId, callback) -> None:
        with self._lock:
            self.subscribed.append((instrument, callback))

    def unsubscribe_l1(self, instrument: InstrumentId, callback) -> None:
        with self._lock:
            self.unsubscribed.append((instrument, callback))


class WorkerRuntimeLifecycleTests(unittest.TestCase):
    def test_start_stop_publishes_state_and_events(self) -> None:
        event_bus = EventBus()
        market_data_service = _FakeMarketDataService()
        manager = WorkerManager(market_data_service=market_data_service, event_bus=event_bus)

        state_statuses: list[str] = []
        state_snapshots: list[dict] = []
        events: list[WorkerEvent] = []
        event_bus.subscribe("worker_state", lambda state: state_statuses.append(str(getattr(state, "status", ""))))
        event_bus.subscribe("worker_state", lambda state: state_snapshots.append(state.to_dict()))
        event_bus.subscribe("worker_events", lambda event: events.append(event))

        task = WorkerTask(
            worker_id="lifecycle-worker",
            left_instrument=_make_instrument("BTCUSDT"),
            right_instrument=_make_instrument("ETHUSDT"),
            entry_threshold=Decimal("0"),
            exit_threshold=Decimal("0"),
            target_notional=Decimal("0"),
            step_notional=Decimal("0"),
            execution_mode="quotes_only",
            run_mode="dual_exchange_quotes",
        )

        manager.start_worker(task)
        manager.stop_worker(task.worker_id)

        self.assertEqual(len(market_data_service.subscribed), 2)
        self.assertEqual(len(market_data_service.unsubscribed), 2)
        self.assertGreaterEqual(len(state_statuses), 2)
        self.assertIn("running", state_statuses)
        self.assertEqual(state_statuses[-1], "stopped")
        event_types = [item.event_type for item in events]
        self.assertIn("runtime_started", event_types)
        self.assertIn("runtime_stopped", event_types)
        self.assertTrue(state_snapshots)
        latest_metrics = state_snapshots[-1].get("metrics", {})
        self.assertIn("execution_stream_health", latest_metrics)
        self.assertIn("execution_stream_health_status", latest_metrics)


if __name__ == "__main__":
    unittest.main()
