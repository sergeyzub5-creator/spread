from __future__ import annotations

import threading
import unittest
from decimal import Decimal

from app.core.instruments.registry import InstrumentRegistry
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec
from app.core.models.market_data import QuoteL1
from app.ui.coordinator_service_parts import UiCoordinatorPartsMixin


def _instrument() -> InstrumentId:
    return InstrumentId(
        key=InstrumentKey(exchange="binance", market_type="linear_perp", symbol="XRPUSDT"),
        spec=InstrumentSpec(
            base_asset="XRP",
            quote_asset="USDT",
            contract_type="linear_perpetual",
            settle_asset="USDT",
            price_precision=Decimal("0.0001"),
            qty_precision=Decimal("1"),
            min_qty=Decimal("1"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_channel="bookTicker",
            ws_symbol="xrpusdt",
            order_route="binance_usdm_trade_ws",
        ),
    )


class _FakeMarketDataService:
    def __init__(self) -> None:
        self.unsubscribed: list[tuple[InstrumentId, object]] = []

    def subscribe_l1(self, instrument: InstrumentId, callback, *, enable_depth20: bool = True) -> None:
        return

    def unsubscribe_l1(self, instrument: InstrumentId, callback) -> None:
        self.unsubscribed.append((instrument, callback))


class _BrokenSignal:
    def emit(self, *_args, **_kwargs) -> None:
        raise RuntimeError("Signal source has been deleted")


class _DummyCoordinator:
    def __init__(self) -> None:
        self.instrument_registry = InstrumentRegistry()
        self.instrument_registry.replace_exchange_instruments("binance", [_instrument()])
        self.market_data_service = _FakeMarketDataService()
        self.public_quote_received = _BrokenSignal()
        self.public_quote_error = _BrokenSignal()
        self._subscription_lock = threading.RLock()
        self._subscriptions: dict[str, tuple[InstrumentId, object]] = {}

    def _is_market_type_loaded(self, exchange: str, market_type: str) -> bool:
        return True

    def _prefetch_market_type(self, exchange: str, market_type: str) -> None:
        return


class UiCoordinatorQuoteSubscriptionTests(unittest.TestCase):
    def test_subscribe_public_quote_unsubscribes_on_deleted_signal(self) -> None:
        coordinator = _DummyCoordinator()

        UiCoordinatorPartsMixin.subscribe_public_quote(
            coordinator,
            slot_name="left",
            exchange="binance",
            market_type="perpetual",
            symbol="XRPUSDT",
        )

        instrument, callback = coordinator._subscriptions["left"]
        with self.assertRaisesRegex(RuntimeError, "deleted"):
            callback(
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

        self.assertEqual(len(coordinator.market_data_service.unsubscribed), 1)
        self.assertNotIn("left", coordinator._subscriptions)


if __name__ == "__main__":
    unittest.main()
