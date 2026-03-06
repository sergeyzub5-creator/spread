from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.bybit.http_client import BybitV5HttpClient
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BybitSpotInstrumentLoader:
    EXCHANGE = "bybit"
    INSTRUMENTS_PATH = "/v5/market/instruments-info"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._client = BybitV5HttpClient(timeout_seconds=timeout_seconds)

    def load_instruments(self) -> list[InstrumentId]:
        payload = self._client.get(self.INSTRUMENTS_PATH, params={"category": "spot"}, auth=False)
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        items = result.get("list", []) if isinstance(result, dict) else []
        instruments: list[InstrumentId] = []
        for item in items:
            instrument = self._build_instrument(item)
            if instrument is not None:
                instruments.append(instrument)
        return instruments

    def _build_instrument(self, item: dict[str, Any]) -> InstrumentId | None:
        status = str(item.get("status", "")).strip().upper()
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol or status != "TRADING":
            return None

        lot_filter = item.get("lotSizeFilter", {}) if isinstance(item.get("lotSizeFilter"), dict) else {}
        price_filter = item.get("priceFilter", {}) if isinstance(item.get("priceFilter"), dict) else {}
        key = InstrumentKey(exchange=self.EXCHANGE, market_type="spot", symbol=symbol)
        spec = InstrumentSpec(
            base_asset=str(item.get("baseCoin", "")),
            quote_asset=str(item.get("quoteCoin", "")),
            contract_type="spot",
            settle_asset=str(item.get("quoteCoin", "")),
            price_precision=Decimal(str(price_filter.get("tickSize", "0"))),
            qty_precision=Decimal(str(lot_filter.get("basePrecision", "0"))),
            min_qty=Decimal(str(lot_filter.get("minOrderQty", "0"))),
            min_notional=Decimal(str(lot_filter.get("minOrderAmt", "0"))),
        )
        routing = InstrumentRouting(
            ws_channel="orderbook.1",
            ws_symbol=symbol,
            order_route="bybit_spot_ws_api",
        )
        return InstrumentId(key=key, spec=spec, routing=routing)
