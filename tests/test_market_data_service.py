from __future__ import annotations

import threading
import unittest
from decimal import Decimal

from app.core.market_data.service import MarketDataService
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec
from app.core.models.market_data import QuoteL1


def _make_instrument() -> InstrumentId:
    return InstrumentId(
        key=InstrumentKey(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
        ),
        spec=InstrumentSpec(
            base_asset="BTC",
            quote_asset="USDT",
            contract_type="spot",
            settle_asset="USDT",
            price_precision=Decimal("0.01"),
            qty_precision=Decimal("0.0001"),
            min_qty=Decimal("0.0001"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_symbol="btcusdt",
            ws_channel="bookTicker",
            order_route="spot_rest",
        ),
    )


class _FakeConnector:
    def __init__(self) -> None:
        self.connected = 0
        self.subscribed = 0
        self.unsubscribed = 0
        self.closed = 0
        self._callback = None

    def connect(self) -> None:
        self.connected += 1

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        self.subscribed += 1

    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        self.unsubscribed += 1

    def close(self) -> None:
        self.closed += 1

    def on_quote(self, callback) -> None:
        self._callback = callback


class _FakeNormalizer:
    def normalize_l1(self, *, instrument: InstrumentId, payload: dict, ts_local: int) -> QuoteL1:
        return QuoteL1(
            instrument_id=instrument,
            bid=payload["bid"],
            ask=payload["ask"],
            bid_qty=payload["bid_qty"],
            ask_qty=payload["ask_qty"],
            ts_exchange=int(payload.get("ts_exchange") or 0),
            ts_local=ts_local,
            source="public_ws",
        )


class MarketDataServiceTests(unittest.TestCase):
    def test_subscribe_unsubscribe_lifecycle_is_stable(self) -> None:
        service = MarketDataService()
        connector = _FakeConnector()
        service.register_exchange_transport("binance:spot", connector, _FakeNormalizer())
        instrument = _make_instrument()

        def callback(_quote: QuoteL1) -> None:
            return

        service.subscribe_l1(instrument, callback)
        service.subscribe_l1(instrument, callback)
        service.unsubscribe_l1(instrument, callback)
        service.unsubscribe_l1(instrument, callback)

        self.assertEqual(connector.connected, 1)
        self.assertEqual(connector.subscribed, 1)
        self.assertEqual(connector.unsubscribed, 1)

    def test_concurrent_publish_subscribe_unsubscribe_does_not_crash(self) -> None:
        service = MarketDataService()
        connector = _FakeConnector()
        service.register_exchange_transport("binance:spot", connector, _FakeNormalizer())
        instrument = _make_instrument()
        stop_event = threading.Event()

        def callback(_quote: QuoteL1) -> None:
            return

        def churn_subscriptions() -> None:
            while not stop_event.is_set():
                service.subscribe_l1(instrument, callback)
                service.unsubscribe_l1(instrument, callback)

        worker = threading.Thread(target=churn_subscriptions, daemon=True)
        worker.start()
        try:
            for _ in range(200):
                service.publish_quote(
                    QuoteL1(
                        instrument_id=instrument,
                        bid=Decimal("1"),
                        ask=Decimal("2"),
                        bid_qty=Decimal("3"),
                        ask_qty=Decimal("4"),
                        ts_exchange=1,
                        ts_local=1,
                        source="public_ws",
                    )
                )
        finally:
            stop_event.set()
            worker.join(timeout=1.0)
            service.shutdown()

        self.assertGreaterEqual(connector.connected, 1)
        self.assertGreaterEqual(connector.closed, 1)

    def test_publish_quote_continues_after_subscriber_exception(self) -> None:
        service = MarketDataService()
        connector = _FakeConnector()
        service.register_exchange_transport("binance:spot", connector, _FakeNormalizer())
        instrument = _make_instrument()
        received: list[QuoteL1] = []

        def bad_callback(_quote: QuoteL1) -> None:
            raise RuntimeError("boom")

        def good_callback(quote: QuoteL1) -> None:
            received.append(quote)

        service.subscribe_l1(instrument, bad_callback)
        service.subscribe_l1(instrument, good_callback)
        service.publish_quote(
            QuoteL1(
                instrument_id=instrument,
                bid=Decimal("1"),
                ask=Decimal("2"),
                bid_qty=Decimal("3"),
                ask_qty=Decimal("4"),
                ts_exchange=1,
                ts_local=1,
                source="public_ws",
            )
        )

        self.assertEqual(len(received), 1)

    def test_shutdown_is_idempotent(self) -> None:
        service = MarketDataService()
        connector = _FakeConnector()
        service.register_exchange_transport("binance:spot", connector, _FakeNormalizer())

        service.shutdown()
        service.shutdown()

        self.assertEqual(connector.closed, 1)


if __name__ == "__main__":
    unittest.main()
