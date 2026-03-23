from __future__ import annotations

import threading
import unittest
from decimal import Decimal

from app.core.instruments.registry import InstrumentRegistry
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec
from app.core.models.instrument_types import UiInstrumentType
from app.ui.coordinator_service_parts import UiCoordinatorPartsMixin


def _instrument(*, exchange: str, market_type: str, symbol: str) -> InstrumentId:
    normalized_symbol = str(symbol).strip().upper()
    return InstrumentId(
        key=InstrumentKey(exchange=exchange, market_type=market_type, symbol=normalized_symbol),
        spec=InstrumentSpec(
            base_asset=normalized_symbol.replace("USDT", ""),
            quote_asset="USDT",
            contract_type=market_type,
            settle_asset="USDT",
            price_precision=Decimal("0.1"),
            qty_precision=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_channel="books1",
            ws_symbol=normalized_symbol,
            order_route=f"{exchange}_{market_type}",
        ),
    )


class _StaticLoader:
    def __init__(self, instruments: list[InstrumentId]) -> None:
        self._instruments = list(instruments)

    def load_instruments(self) -> list[InstrumentId]:
        return list(self._instruments)


class _DummyCoordinator:
    def __init__(self) -> None:
        self.instrument_registry = InstrumentRegistry()
        self._load_lock = threading.Lock()
        self._binance_spot_loaded = False
        self._binance_perp_loaded = False
        self._bybit_spot_loaded = False
        self._bybit_perp_loaded = False
        self._binance_delivery_loaded = False
        self._bybit_delivery_loaded = False
        self._bitget_spot_loaded = False
        self._bitget_perp_loaded = False
        self._bitget_coin_delivery_loaded = False
        self._bitget_spot_loader = _StaticLoader([_instrument(exchange="bitget", market_type="spot", symbol="BTCUSDT")])
        self._bitget_linear_loader = _StaticLoader([_instrument(exchange="bitget", market_type="linear_perp", symbol="ETHUSDT")])
        self._bitget_coin_delivery_loader = _StaticLoader([_instrument(exchange="bitget", market_type="bitget_coin_delivery", symbol="BTCUSD_260327")])


class UiCoordinatorBitgetLoadingTests(unittest.TestCase):
    def test_bitget_spot_load_preserves_existing_perpetuals(self) -> None:
        coordinator = _DummyCoordinator()

        UiCoordinatorPartsMixin._ensure_market_type_loaded(coordinator, "bitget", UiInstrumentType.PERPETUAL.value)
        UiCoordinatorPartsMixin._ensure_market_type_loaded(coordinator, "bitget", UiInstrumentType.SPOT.value)

        bitget_symbols = {instrument.symbol for instrument in coordinator.instrument_registry.list_by_exchange("bitget")}
        self.assertEqual(bitget_symbols, {"BTCUSDT", "ETHUSDT"})


if __name__ == "__main__":
    unittest.main()
